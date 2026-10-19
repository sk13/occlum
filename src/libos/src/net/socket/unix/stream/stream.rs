use super::address_space::ADDRESS_SPACE;
use super::endpoint::{end_pair, Ancillary, Endpoint, RelayNotifier};
use super::options::{AtomicTimeouts, SockOptions};
use super::*;
use events::{Event, EventFilter, Notifier, Observer};
use fs::channel::IoOpts;
use fs::{CreationFlags, FileMode, IoEvents};
use net::socket::{CMessages, CmsgData};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

/// SOCK_STREAM Unix socket. It has three statuses: unconnected, listening and connected.  When a
/// socket is created, it is in unconnected status.  It will transfer to listening after listen is
/// called and connected after connect is called. A socket in connected status can be obtained
/// through a listening socket calling accept. Listening and connected are ultimate statuses. They
/// will not transfer to other statuses.
pub struct Stream {
    // The inode number that fstat() shows, which no other socket has
    pub(super) ino: usize,
    inner: SgxMutex<Status>,
    // Use the internal notifier of RelayNotifier as the notifier of stream socket. It relays the
    // events of the endpoint, too.
    pub(super) notifier: Arc<RelayNotifier>,
    // The IP addresses that the socket shows if it replaces a TCP socket
    inet: SgxMutex<Option<InetView>>,
    // The options of SOL_SOCKET that the program set (see options.rs)
    pub(super) options: SgxMutex<SockOptions>,
    // The timeouts of the options for the calls that read and write, which read them
    // without the lock of the options (see `AtomicTimeouts`)
    pub(super) timeouts: AtomicTimeouts,
    // Whether accept returned the socket. Its endpoint has the address that
    // the listening socket bound (see connect), so the socket must not release
    // it when it is closed.
    accepted: bool,
}

/// The IP socket addresses that a Unix socket shows to the program if it
/// replaces a TCP socket that the network policy redirects (see
/// `net::redirect`): the local address, and the peer address if connected.
#[derive(Debug, Clone)]
pub struct InetView {
    pub local: AnyAddr,
    pub peer: Option<AnyAddr>,
}

impl InetView {
    /// Returns AF_INET or AF_INET6.
    pub fn family(&self) -> i32 {
        match self.local {
            AnyAddr::Ipv6(_) => libc::AF_INET6,
            _ => libc::AF_INET,
        }
    }
}

impl Stream {
    pub fn new(flags: SocketFlags) -> Self {
        Self {
            ino: next_ino(),
            inner: SgxMutex::new(Status::Idle(Info::new(
                flags.contains(SocketFlags::SOCK_NONBLOCK),
            ))),
            notifier: Arc::new(RelayNotifier::new()),
            inet: SgxMutex::new(None),
            options: SgxMutex::new(SockOptions::default()),
            timeouts: AtomicTimeouts::default(),
            accepted: false,
        }
    }

    pub fn socketpair(flags: SocketFlags) -> Result<(Self, Self)> {
        let nonblocking = flags.contains(SocketFlags::SOCK_NONBLOCK);
        let (end_a, end_b) = end_pair(nonblocking)?;
        // The process that creates the sockets is the peer of both ends, like
        // in Linux
        end_a.set_ancillary(Ancillary::of_current_process());
        end_b.set_ancillary(Ancillary::of_current_process());
        let notifier_a = Arc::new(RelayNotifier::new());
        let notifier_b = Arc::new(RelayNotifier::new());
        notifier_a.observe_endpoint(&end_a);
        notifier_b.observe_endpoint(&end_b);

        let socket_a = Self {
            ino: next_ino(),
            inner: SgxMutex::new(Status::Connected(end_a)),
            notifier: notifier_a,
            inet: SgxMutex::new(None),
            options: SgxMutex::new(SockOptions::default()),
            timeouts: AtomicTimeouts::default(),
            accepted: false,
        };

        let socket_b = Self {
            ino: next_ino(),
            inner: SgxMutex::new(Status::Connected(end_b)),
            notifier: notifier_b,
            inet: SgxMutex::new(None),
            options: SgxMutex::new(SockOptions::default()),
            timeouts: AtomicTimeouts::default(),
            accepted: false,
        };

        Ok((socket_a, socket_b))
    }

    pub fn addr(&self) -> UnixAddr {
        let addr_opt = match &*self.inner() {
            Status::Idle(info) => info.addr().clone(),
            Status::Connected(endpoint) => endpoint.addr(),
            Status::Listening(addr) => Some(addr).cloned(),
        };

        addr_opt.unwrap_or(UnixAddr::Unnamed)
    }

    /// Returns the address of the peer, which is unnamed unless the peer is
    /// bound, like in Linux.
    pub fn peer_addr(&self) -> Result<UnixAddr> {
        if let Status::Connected(endpoint) = &*self.inner() {
            return Ok(endpoint.peer_addr().unwrap_or(UnixAddr::Unnamed));
        }
        return_errno!(ENOTCONN, "the socket is not connected");
    }

    /// Returns the IP addresses that the socket shows, if it replaces a TCP
    /// socket.
    pub fn inet_view(&self) -> Option<InetView> {
        self.inet.lock().unwrap().clone()
    }

    pub fn set_inet_view(&self, view: InetView) {
        *self.inet.lock().unwrap() = Some(view);
    }

    pub fn bind(&self, addr: &UnixAddr) -> Result<()> {
        let bound = self.addr() != UnixAddr::Unnamed;
        // Linux does not choose another name for a socket that has one
        if bound && *addr == UnixAddr::Unnamed {
            return Ok(());
        }

        let mut unix_addr = addr.clone();
        let addr = &mut unix_addr;

        if let UnixAddr::File(inode_num, path) = addr {
            // Linux creates the file before it finds out that the socket is
            // bound already, so the name that exists is in use
            if bound {
                let exists = {
                    let current = current!();
                    let fs = current.fs().read().unwrap();
                    fs.lookup_inode_no_follow(path.path_str()).is_ok()
                };
                return_errno!(
                    if exists { EADDRINUSE } else { EINVAL },
                    "the socket is already bound"
                );
            }

            // create the corresponding file in the fs and fill Addr with its inode
            let corresponding_inode_num = {
                let current = current!();
                let fs = current.fs().read().unwrap();
                let file_ref = fs.open_file(
                    path.path_str(),
                    CreationFlags::O_CREAT.bits(),
                    FileMode::from_bits(0o777).unwrap(),
                )?;
                file_ref.metadata()?.inode
            };
            *inode_num = Some(corresponding_inode_num);
        }

        match &mut *self.inner() {
            Status::Idle(ref mut info) => {
                if info.addr().is_some() {
                    return_errno!(EINVAL, "the socket is already bound");
                }

                // check the global address space to see if the address is available before bind
                let addr = ADDRESS_SPACE.add_binder(addr)?;
                info.set_addr(&addr);
            }
            Status::Connected(endpoint) => {
                if endpoint.addr().is_some() {
                    return_errno!(EINVAL, "the socket is already bound");
                }

                let addr = ADDRESS_SPACE.add_binder(addr)?;
                endpoint.set_addr(&addr);
            }
            Status::Listening(_) => return_errno!(EINVAL, "the socket is already bound"),
        }

        Ok(())
    }

    pub fn listen(&self, backlog: i32) -> Result<()> {
        // do_listen() limits the backlog to SOMAXCONN
        if backlog < 0 {
            return_errno!(EINVAL, "negative backlog is not supported");
        }
        let backlog = backlog as usize;

        let mut inner = self.inner();
        match &*inner {
            Status::Idle(info) => {
                if let Some(addr) = info.addr() {
                    warn!("addr = {:?}", addr);
                    ADDRESS_SPACE.add_listener(
                        addr,
                        backlog,
                        info.nonblocking(),
                        self.notifier.clone(),
                    )?;
                    // A socket that was shut down listens shut down, like in Linux
                    if let (Some(how), Some(listener)) =
                        (info.shut_down_for(), ADDRESS_SPACE.get_listener_ref(addr))
                    {
                        listener.shutdown(how);
                    }
                    *inner = Status::Listening(addr.clone());
                } else {
                    return_errno!(EINVAL, "the socket is not bound");
                }
            }
            // Like Linux, which takes the socket that accept returned for a
            // bound one, and does not change it
            Status::Connected(_) if self.accepted => (),
            Status::Connected(_) => return_errno!(EINVAL, "the socket is already connected"),
            // Modify the backlog of the queue holding incoming sockets
            Status::Listening(addr) => ADDRESS_SPACE.set_backlog(&addr, backlog)?,
        }

        Ok(())
    }

    /// The establishment of the connection is very fast and can be done immediately,
    /// unless the backlog of the listener is full. Then, like in Linux, a blocking
    /// socket waits for an accept (as long as its SO_SNDTIMEO says, which ends with
    /// EAGAIN), and a nonblocking socket fails with EAGAIN. No lock is held
    /// meanwhile.
    pub fn connect(&self, addr: &UnixAddr) -> Result<()> {
        debug!("connect to {:?}", addr);

        if let UnixAddr::Unnamed = addr {
            return_errno!(EINVAL, "the address has no name");
        }

        let nonblocking = match &*self.inner() {
            Status::Idle(info) => info.nonblocking(),
            Status::Connected(endpoint) => return_errno!(EISCONN, "already connected"),
            Status::Listening(addr) => return_errno!(EINVAL, "invalid socket for connect"),
        };
        let (listener, listener_addr) = ADDRESS_SPACE.find_listener(addr)?;
        let room = listener.reserve_incoming(nonblocking, self.send_timeout())?;

        // Another thread may have changed the socket while this one waited
        let mut inner = self.inner();
        match &*inner {
            Status::Idle(info) => {
                let self_addr_opt = info.addr();
                let (end_self, end_incoming) = end_pair(info.nonblocking())?;
                // The socket may have set the size of its send buffer before it connects. It is
                // read with the status locked, see `setsockopt`.
                end_self.set_send_buf_size(self.send_buf_size());
                // A socket that was shut down stays so, like in Linux
                if let Some(how) = info.shut_down_for() {
                    end_self.shutdown(how)?;
                }
                // The name of the listener is the one that it bound, whatever
                // the name that this socket used, which may have changed
                // meanwhile, e.g., by renaming the socket file
                end_incoming.set_addr(&listener_addr);
                end_incoming.set_embryo(true);
                if let Some(self_addr) = self_addr_opt {
                    end_self.set_addr(self_addr);
                }
                end_self.set_ancillary(Ancillary::of_current());
                // Before the listener gets the peer, which may write right away
                self.notifier.observe_endpoint(&end_self);

                // ECONNREFUSED if the listener was shut down after it was found
                room.push(end_incoming)?;

                *inner = Status::Connected(end_self);
                Ok(())
            }
            Status::Connected(endpoint) => return_errno!(EISCONN, "already connected"),
            Status::Listening(addr) => return_errno!(EINVAL, "invalid socket for connect"),
        }
    }

    pub fn accept(&self, flags: SocketFlags) -> Result<(Self, Option<UnixAddr>)> {
        let status = (*self.inner()).clone();
        match status {
            Status::Listening(addr) => {
                let endpoint = ADDRESS_SPACE.pop_incoming(&addr, self.recv_timeout())?;
                endpoint.set_embryo(false);
                endpoint.set_nonblocking(flags.contains(SocketFlags::SOCK_NONBLOCK));
                endpoint.set_ancillary(Ancillary::of_current());
                let notifier = Arc::new(RelayNotifier::new());
                notifier.observe_endpoint(&endpoint);

                let peer_addr = Some(endpoint.peer_addr().unwrap_or(UnixAddr::Unnamed));

                debug!("accept socket from {:?}", peer_addr);

                // The cache of the timeouts has to have the timeouts of the options, which a socket
                // that replaces a TCP socket takes over from the listening one
                let options = self.options_of_accepted();
                let timeouts = AtomicTimeouts::of(&options);
                // So has the size of the send buffer, which a TCP socket passes on, too
                endpoint.set_send_buf_size(options.send_buf_size());
                Ok((
                    Self {
                        ino: next_ino(),
                        inner: SgxMutex::new(Status::Connected(endpoint)),
                        notifier: notifier,
                        inet: SgxMutex::new(None),
                        options: SgxMutex::new(options),
                        timeouts,
                        accepted: true,
                    },
                    peer_addr,
                ))
            }
            _ => return_errno!(EINVAL, "the socket is not listening"),
        }
    }

    pub fn sendto(&self, buf: &[u8], flags: SendFlags, addr: Option<&UnixAddr>) -> Result<usize> {
        self.check_no_addr(addr.is_some())?;
        self.send(&[buf], flags)
    }

    pub fn recvfrom(&self, buf: &mut [u8], flags: RecvFlags) -> Result<(usize, Option<UnixAddr>)> {
        let data_len = self.recv(&mut [buf], flags)?;
        let addr = self.recv_addr();

        debug!("recvfrom {:?}", addr);

        Ok((data_len, addr))
    }

    pub fn sendmsg(
        &self,
        bufs: &[&[u8]],
        flags: SendFlags,
        addr: Option<&AnyAddr>,
        control: Option<&[u8]>,
    ) -> Result<usize> {
        self.check_no_addr(addr.is_some())?;
        let data_len = self.send(bufs, flags)?;
        if let Some(msg_control) = control {
            self.send(&[msg_control], flags)?;
        }

        Ok(data_len)
    }

    pub fn recvmsg(
        &self,
        bufs: &mut [&mut [u8]],
        flags: RecvFlags,
        control: Option<&mut [u8]>,
    ) -> Result<(usize, Option<AnyAddr>, MsgFlags, usize)> {
        let data_len = self.recv(bufs, flags)?;

        // The name of the peer is the only other field that is supported
        let mut msg_flags = MsgFlags::empty();
        let control_len = if let Some(msg_control) = control {
            let control_len = self.read(msg_control)?;

            // For each control message that contains file descriptors (SOL_SOCKET and SCM_RIGHTS),
            // reassign each fd in the message in receive end. If that is not possible, the
            // control data is dropped.
            match self.reassign_fds(&mut msg_control[..control_len]) {
                Ok(()) => control_len,
                Err(e) => {
                    warn!("drop the control data: {:?}", e);
                    msg_flags |= MsgFlags::MSG_CTRUNC;
                    0
                }
            }
        } else {
            0
        };

        Ok((
            data_len,
            self.recv_addr().map(AnyAddr::Unix),
            msg_flags,
            control_len,
        ))
    }

    /// Receives bytes into the buffers, which is what all the calls that read from the socket do.
    /// Unless the socket is nonblocking, or the call has MSG_DONTWAIT, it waits for bytes as long
    /// as SO_RCVTIMEO says, which ends with EAGAIN. MSG_PEEK does not take the bytes. MSG_WAITALL
    /// waits until the buffers are full, but a peek returns the bytes that are queued, like in
    /// Linux.
    pub(super) fn recv(&self, bufs: &mut [&mut [u8]], flags: RecvFlags) -> Result<usize> {
        let endpoint = self.recv_endpoint()?;
        let opts = IoOpts {
            nonblocking: flags.contains(RecvFlags::MSG_DONTWAIT),
            timeout: self.recv_timeout(),
            all: flags.contains(RecvFlags::MSG_WAITALL),
            ..Default::default()
        };
        // Like Linux, a receive into no bytes waits for the bytes of the socket, or its end, which
        // it does not take: EAGAIN if there are none and the call does not wait
        if bufs.iter().all(|buf| buf.is_empty()) {
            return endpoint.peek(&mut [&mut [0]], &opts).map(|_| 0);
        }
        if flags.contains(RecvFlags::MSG_PEEK) {
            endpoint.peek(bufs, &opts)
        } else {
            endpoint.read(bufs, &opts)
        }
    }

    /// Reads the bytes into the buffers, which is what read() and readv() do. Unlike the receive
    /// calls, they return 0 at once if there is no room for bytes, as Linux does, also on a socket
    /// that is not connected.
    pub(super) fn read_bufs(&self, bufs: &mut [&mut [u8]]) -> Result<usize> {
        if bufs.iter().all(|buf| buf.is_empty()) {
            return Ok(0);
        }
        self.recv(bufs, RecvFlags::empty())
    }

    /// Sends the bytes of the buffers, which is what all the calls that write to the socket do. A
    /// call waits for room until all the bytes are written, as long as SO_SNDTIMEO says for each
    /// wait, unless the socket is nonblocking, or the call has MSG_DONTWAIT, which write what
    /// fits. If a signal or the timeout ends the wait, the call returns the number of the bytes
    /// that it has written, or fails with EINTR or EAGAIN if it has written none.
    pub(super) fn send(&self, bufs: &[&[u8]], flags: SendFlags) -> Result<usize> {
        let endpoint = self.connected_endpoint()?;
        let opts = IoOpts {
            nonblocking: flags.contains(SendFlags::MSG_DONTWAIT),
            timeout: self.send_timeout(),
            all: true,
            ..Default::default()
        };
        // Like Linux, SIGPIPE is for the call that finds that the socket cannot send, not for the
        // call that has waited for room, and then has found it
        let sigpipe = !flags.contains(SendFlags::MSG_NOSIGNAL) && endpoint.is_write_shutdown();
        let result = endpoint.write(bufs, &opts);
        if sigpipe && result.has_errno(EPIPE) {
            crate::signal::raise_sigpipe();
        }
        result
    }

    /// Returns the error of the socket, which is the reset of a connection, and clears it. Zero if
    /// there is none.
    pub(super) fn take_error(&self) -> i32 {
        match &*self.inner() {
            Status::Connected(endpoint) if endpoint.take_reset() => libc::ECONNRESET,
            _ => 0,
        }
    }

    /// Returns the endpoint of a connected socket
    fn connected_endpoint(&self) -> Result<Endpoint> {
        match &*self.inner() {
            Status::Connected(endpoint) => Ok(endpoint.clone()),
            _ => return_errno!(ENOTCONN, "unconnected socket"),
        }
    }

    /// Returns the endpoint that a receive reads from. A socket that is not connected has none,
    /// which makes the receive fail with EINVAL, as in Linux (a send fails with ENOTCONN). A socket
    /// that replaces a TCP socket fails with ENOTCONN, as TCP does.
    fn recv_endpoint(&self) -> Result<Endpoint> {
        match self.connected_endpoint() {
            Err(_) if self.inet_view().is_none() => {
                return_errno!(EINVAL, "the socket is not connected")
            }
            endpoint => endpoint,
        }
    }

    /// Returns the address that a receive tells: the name of the peer, if it has one. Like in
    /// Linux, there is no address for an unnamed peer, nor for a TCP socket.
    fn recv_addr(&self) -> Option<UnixAddr> {
        self.peer_addr()
            .ok()
            .filter(|addr| *addr != UnixAddr::Unnamed && self.inet_view().is_none())
    }

    /// Fails if a send has an address, as the sockets of a stream have no address to send to,
    /// like in Linux: EISCONN if the socket is connected, else EOPNOTSUPP. A socket that replaces
    /// a TCP socket ignores the address, as TCP does.
    fn check_no_addr(&self, has_addr: bool) -> Result<()> {
        if !has_addr || self.inet_view().is_some() {
            return Ok(());
        }
        match &*self.inner() {
            Status::Connected(_) => return_errno!(EISCONN, "the socket is connected"),
            _ => return_errno!(EOPNOTSUPP, "a stream socket has no address to send to"),
        }
    }

    /// Replaces the file descriptors in the control messages, which are those
    /// of the sender, by descriptors of the current process for the same files.
    /// Either all of them are replaced, or the current process has none of
    /// the files.
    fn reassign_fds(&self, msg_control: &mut [u8]) -> Result<()> {
        let mut received_fds = Vec::new();
        let result = (|| {
            for cmsg in CMessages::from_bytes(msg_control) {
                if let CmsgData::ScmRights(mut scm_rights) = cmsg {
                    scm_rights.iter_and_reassign_fds(|send_fd| {
                        let ipc_file = self.sender_file(send_fd)?;
                        let fd = current!().add_file(ipc_file, false);
                        received_fds.push(fd);
                        Ok(fd)
                    })?;
                }
                // Unix credentials need not to be handled here
            }
            Ok(())
        })();
        if result.is_err() {
            for fd in received_fds {
                let _ = current!().close_file(fd);
            }
        }
        result
    }

    /// Returns the file of a file descriptor of the sender, which is the
    /// thread that connected or accepted. As this is looked up when the
    /// message is received, the thread may have exited, or closed the
    /// descriptor, meanwhile. A socket pair does not know its sender.
    fn sender_file(&self, fd: FileDesc) -> Result<FileRef> {
        let send_tid = self
            .peer_ancillary()
            .ok_or_else(|| errno!(ESRCH, "the sender is unknown"))?
            .tid();
        process::table::get_thread(send_tid)?.file(fd)
    }

    /// perform shutdown on the socket.
    pub fn shutdown(&self, how: Shutdown) -> Result<()> {
        // A socket that is not connected remembers it, like in Linux, where it has the flags of
        // the shutdown, too. It is ENOTCONN for a TCP socket, which this socket may replace.
        let inet = self.inet_view().is_some();
        let events = match &mut *self.inner() {
            Status::Idle(_) if inet => return_errno!(ENOTCONN, "The socket is not connected."),
            Status::Idle(info) => {
                info.shutdown(how);
                Some(info.events())
            }
            _ => None,
        };
        if let Some(events) = events {
            self.notifier.notifier().broadcast(&events);
            return Ok(());
        }

        // Release the lock before waking up threads, like the other operations
        let status = (*self.inner()).clone();
        match status {
            Status::Connected(end) => {
                end.shutdown(how)?;
                // The channels notify the peer of the shutdown, but not this
                // socket. Without the notification, the threads that wait for
                // events on it with poll, select or epoll would not notice
                // the end of the file (or HUP) they are supposed to get.
                self.notifier.notifier().broadcast(&end.poll());
                Ok(())
            }
            // The way to stop the threads that accept: they fail, and new
            // connections are refused. The listener notifies the threads that
            // wait for events.
            Status::Listening(addr) => {
                let listener = ADDRESS_SPACE
                    .get_listener_ref(&addr)
                    .ok_or_else(|| errno!(EINVAL, "the socket is not listening"))?;
                listener.shutdown(how);
                Ok(())
            }
            _ => return_errno!(ENOTCONN, "The socket is not connected."),
        }
    }

    pub(super) fn nonblocking(&self) -> bool {
        match &*self.inner() {
            Status::Idle(info) => info.nonblocking(),
            Status::Connected(endpoint) => endpoint.nonblocking(),
            Status::Listening(addr) => ADDRESS_SPACE.get_listener_ref(&addr).unwrap().nonblocking(),
        }
    }

    pub(super) fn set_nonblocking(&self, nonblocking: bool) {
        match &mut *self.inner() {
            Status::Idle(ref mut info) => info.set_nonblocking(nonblocking),
            Status::Connected(ref mut endpoint) => endpoint.set_nonblocking(nonblocking),
            Status::Listening(addr) => ADDRESS_SPACE
                .get_listener_ref(&addr)
                .unwrap()
                .set_nonblocking(nonblocking),
        }
    }

    pub(super) fn inner(&self) -> SgxMutexGuard<'_, Status> {
        self.inner.lock().unwrap()
    }

    fn ancillary(&self) -> Option<Ancillary> {
        match &*self.inner() {
            Status::Idle(_) => None,
            Status::Listening(_) => None,
            Status::Connected(endpoint) => endpoint.ancillary(),
        }
    }

    pub(super) fn peer_ancillary(&self) -> Option<Ancillary> {
        if let Status::Connected(endpoint) = &*self.inner() {
            endpoint.peer_ancillary()
        } else {
            None
        }
    }

    fn set_ancillary(&self, ancillary: Ancillary) {
        if let Status::Connected(endpoint) = &*self.inner() {
            endpoint.set_ancillary(ancillary)
        }
    }
}

/// Returns an inode number for a socket, which is not zero, and which no other
/// socket has.
fn next_ino() -> usize {
    static NEXT_INO: AtomicUsize = AtomicUsize::new(1);
    NEXT_INO.fetch_add(1, Ordering::Relaxed)
}

impl Debug for Stream {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Stream")
            .field("addr", &self.addr())
            .field("nonblocking", &self.nonblocking())
            .finish()
    }
}

impl Drop for Stream {
    fn drop(&mut self) {
        match &*self.inner() {
            Status::Idle(info) => {
                if let Some(addr) = info.addr() {
                    ADDRESS_SPACE.remove_addr(&addr);
                }
            }
            Status::Listening(addr) => {
                let listener = ADDRESS_SPACE.get_listener_ref(&addr).unwrap();
                ADDRESS_SPACE.remove_addr(&addr);
                // handle the blocking of other sockets holding the reference to the listener,
                // e.g., waiting for room in a listener full of incoming sockets
                listener.close();
            }
            // The address that the socket bound, before or after it connected
            Status::Connected(endpoint) if !self.accepted => {
                if let Some(addr) = endpoint.addr() {
                    ADDRESS_SPACE.remove_addr(&addr);
                }
            }
            _ => {}
        }
    }
}

#[derive(Clone)]
pub enum Status {
    Idle(Info),
    // The listeners are stored in a global data structure indexed by the address.
    // The consitency of Status with that data structure should be carefully maintained.
    Listening(UnixAddr),
    Connected(Endpoint),
}

#[derive(Debug, Clone)]
pub struct Info {
    addr: Option<UnixAddr>,
    nonblocking: bool,
    // The socket has been shut down for reading or writing, which it stays after it is
    // connected or listens, like in Linux
    shut_rd: bool,
    shut_wr: bool,
}

impl Info {
    pub fn new(nonblocking: bool) -> Self {
        Self {
            addr: None,
            nonblocking: nonblocking,
            shut_rd: false,
            shut_wr: false,
        }
    }

    pub fn addr(&self) -> &Option<UnixAddr> {
        &self.addr
    }

    pub fn set_addr(&mut self, addr: &UnixAddr) {
        self.addr = Some(addr.clone());
    }

    pub fn nonblocking(&self) -> bool {
        self.nonblocking
    }

    pub fn set_nonblocking(&mut self, nonblocking: bool) {
        self.nonblocking = nonblocking;
    }

    pub fn shutdown(&mut self, how: Shutdown) {
        self.shut_rd |= how.should_shut_read();
        self.shut_wr |= how.should_shut_write();
    }

    /// Returns what the socket has been shut down for, if it has
    pub fn shut_down_for(&self) -> Option<Shutdown> {
        match (self.shut_rd, self.shut_wr) {
            (true, true) => Some(Shutdown::Both),
            (true, false) => Some(Shutdown::Read),
            (false, true) => Some(Shutdown::Write),
            (false, false) => None,
        }
    }

    /// Returns the events of a socket that is not connected: it can always write (the write
    /// fails), and it has hung up, like in Linux, where it is closed. A socket that has been shut
    /// down for reading is readable, as a read fails at once.
    pub fn events(&self) -> IoEvents {
        let mut events = IoEvents::OUT | IoEvents::HUP;
        if self.shut_rd {
            events |= IoEvents::IN | IoEvents::RDHUP;
        }
        events
    }
}
