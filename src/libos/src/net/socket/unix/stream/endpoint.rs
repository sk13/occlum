use super::control::{ControlPair, ControlQueue, Mark, Outgoing, Received};
use super::*;
use events::{Event, EventFilter, Notifier, Observer};
use fs::channel::{Channel, Consumer, IoOpts, Producer};
use fs::{IoEvents, IoNotifier};
use std::any::Any;
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::sync::{Arc, Weak};

pub type Endpoint = Arc<Inner>;

/// Constructor of two connected Endpoints
pub fn end_pair(nonblocking: bool) -> Result<(Endpoint, Endpoint)> {
    let (pro_a, con_a) = Channel::new(DEFAULT_BUF_SIZE)?.split();
    let (pro_b, con_b) = Channel::new(DEFAULT_BUF_SIZE)?.split();

    // Each end shares its name with the other end, which can tell it, as Linux
    // does, when the first end is gone
    let name_a = Arc::new(RwLock::new(None));
    let name_b = Arc::new(RwLock::new(None));
    let flags = Arc::new(AtomicU16::new(0));

    // The control messages of the two channels, which the end that writes into a channel and the
    // end that reads from it share
    let control = ControlPair::new();

    let mut end_a = Arc::new(Inner {
        addr: name_a.clone(),
        peer_addr: name_b.clone(),
        reader: con_a,
        writer: pro_b,
        control: control.clone(),
        side: 0,
        peer: Weak::default(),
        flags: flags.clone(),
        flags_shift: 0,
        embryo: AtomicBool::new(false),
        ancillary: RwLock::new(None),
        peer_ancillary: RwLock::new(None),
    });
    let end_b = Arc::new(Inner {
        addr: name_b,
        peer_addr: name_a,
        reader: con_b,
        writer: pro_a,
        control,
        side: 1,
        peer: Arc::downgrade(&end_a),
        flags,
        flags_shift: 8,
        embryo: AtomicBool::new(false),
        ancillary: RwLock::new(None),
        peer_ancillary: RwLock::new(None),
    });

    unsafe {
        Arc::get_mut_unchecked(&mut end_a).peer = Arc::downgrade(&end_b);
    }

    end_a.set_nonblocking(nonblocking);
    end_b.set_nonblocking(nonblocking);

    Ok((end_a, end_b))
}

// The flags of an end of a connection, which are those of the field sk_shutdown of a socket of
// Linux:
/// The end reads nothing more: it has shut down reading, or the peer has shut down writing, or
/// it is closed.
const RCV_SHUTDOWN: u16 = 1;
/// The end writes nothing more: it has shut down writing, or the peer has shut down reading, or
/// it is closed.
const SND_SHUTDOWN: u16 = 2;
/// The connection was reset: the peer is closed, and it had bytes that it did not read, or accept()
/// had not returned it. This is the error of the socket in Linux (sk_err). A read at the end of the
/// file fails with ECONNRESET, or SO_ERROR returns it, which clears the flag.
const RESET: u16 = 4;

/// One end of the connected unix socket
pub struct Inner {
    addr: Arc<RwLock<Option<UnixAddr>>>,
    peer_addr: Arc<RwLock<Option<UnixAddr>>>,
    reader: Consumer<u8>,
    writer: Producer<u8>,
    // The control messages of the connection: the queue of the end `side` (0 or 1) holds those that
    // the peer sends to this end, and that of the other end those that this end sends
    control: Arc<ControlPair>,
    side: usize,
    peer: Weak<Self>,
    // The flags of the shutdown of both ends, which a poll tells (see `RCV_SHUTDOWN`): each end
    // has a byte of the word, at `flags_shift`. A shutdown changes the flags of both ends in one
    // step, so that a poll sees the connection as it was before or after it, never half of it.
    // The channels have flags of their own for the calls that wait. The shutdown of an end changes
    // them before these, and the drop of an end after.
    flags: Arc<AtomicU16>,
    flags_shift: u16,
    // The connection waits in the queue of a listener: accept() has not returned it yet
    embryo: AtomicBool,
    ancillary: RwLock<Option<Ancillary>>,
    // The ancillary data of the peer, which the peer puts here when it sets
    // its own. Unlike a lookup through `peer`, it stays when the peer is closed.
    peer_ancillary: RwLock<Option<Ancillary>>,
}

impl Inner {
    pub fn addr(&self) -> Option<UnixAddr> {
        self.addr.read().unwrap().clone()
    }

    pub fn set_addr(&self, addr: &UnixAddr) {
        *self.addr.write().unwrap() = Some(addr.clone());
    }

    /// The name that the peer has bound, which is known after the peer is closed, too.
    pub fn peer_addr(&self) -> Option<UnixAddr> {
        self.peer_addr.read().unwrap().clone()
    }

    pub fn set_nonblocking(&self, nonblocking: bool) {
        self.reader.set_nonblocking(nonblocking);
        self.writer.set_nonblocking(nonblocking);
    }

    pub fn nonblocking(&self) -> bool {
        let cons_nonblocking = self.reader.is_nonblocking();
        let prod_nonblocking = self.writer.is_nonblocking();
        assert_eq!(cons_nonblocking, prod_nonblocking);
        cons_nonblocking
    }

    /// The control messages that the peer sends to this end
    fn control_in(&self) -> &ControlQueue {
        self.control.queue(self.side)
    }

    /// The control messages that this end sends to its peer
    fn control_out(&self) -> &ControlQueue {
        self.control.queue(1 - self.side)
    }

    /// Reads the bytes, and the control messages that belong to them
    pub fn read(
        &self,
        bufs: &mut [&mut [u8]],
        opts: &IoOpts,
        received: &mut Received,
    ) -> Result<usize> {
        let mut gate = self.control_in().gate(received, false);
        let result = self.reader.pop_slices_gated(bufs, opts, Some(&mut gate));
        self.fail_if_reset(result, bufs)
    }

    /// Reads the bytes without taking them, and the files of their control messages as copies
    pub fn peek(
        &self,
        bufs: &mut [&mut [u8]],
        opts: &IoOpts,
        received: &mut Received,
    ) -> Result<usize> {
        let mut gate = self.control_in().gate(received, true);
        let result = self.reader.peek_slices_gated(bufs, opts, Some(&mut gate));
        self.fail_if_reset(result, bufs)
    }

    /// The end of the file that a read finds is an error, once, if the connection was reset. The
    /// bytes that were sent before come first.
    fn fail_if_reset(&self, result: Result<usize>, bufs: &[&mut [u8]]) -> Result<usize> {
        match result {
            Ok(0) if bufs.iter().any(|buf| !buf.is_empty()) && self.take_reset() => {
                return_errno!(ECONNRESET, "the connection was reset")
            }
            result => result,
        }
    }

    /// Writes the bytes with the control messages
    pub fn write(&self, bufs: &[&[u8]], opts: &IoOpts, control: Option<Outgoing>) -> Result<usize> {
        let was_shutdown = self.is_write_shutdown();
        let result = if control.is_none() && !self.control_out().records_creds() {
            self.writer.push_slices_with(bufs, opts)
        } else {
            let mut mark = Mark::new(self.control_out(), control.unwrap_or_default());
            self.writer.push_slices_marked(bufs, opts, Some(&mut mark))
        };
        // A write that waited, and finds that the peer is closed, fails with the error of the
        // socket first, like in Linux. A write that finds it closed at once fails with EPIPE, and
        // leaves the error for the next read.
        if !was_shutdown && result.has_errno(EPIPE) && self.take_reset() {
            return_errno!(ECONNRESET, "the connection was reset");
        }
        result
    }

    /// Tells that the connection is in the queue of a listener, as long as accept() has not
    /// returned it. If the endpoint is closed meanwhile, its peer gets a reset.
    pub fn set_embryo(&self, embryo: bool) {
        self.embryo.store(embryo, Ordering::Release);
    }

    /// Reads the reset of the connection, if there is one, which clears it. Like Linux does with
    /// the error of a socket, once.
    pub fn take_reset(&self) -> bool {
        let reset = self.own_flags(RESET);
        self.flags.load(Ordering::SeqCst) & reset != 0
            && self.flags.fetch_and(!reset, Ordering::SeqCst) & reset != 0
    }

    /// Tells whether the socket has SO_PASSCRED, which makes the receiver get the credentials of
    /// the sender
    pub fn set_passcred(&self, on: bool) {
        self.control_in().set_recv_passcred(on);
        self.control_out().set_send_passcred(on);
    }

    /// Changes the number of bytes that the endpoint may have in flight to its peer. If it can take
    /// more bytes now, returns true, and the caller has to call `notify_send_room()`.
    pub fn set_send_buf_size(&self, size: usize) -> bool {
        self.writer.set_capacity(size)
    }

    /// Wakes up the threads that wait for room to send, and those that poll the endpoint, after
    /// `set_send_buf_size()` has returned true
    pub fn notify_send_room(&self) {
        self.writer.notify_room()
    }

    /// Whether the endpoint cannot write: it has been shut down for writing, or its peer for
    /// reading (the peer is closed)
    pub fn is_write_shutdown(&self) -> bool {
        self.writer.is_self_shutdown() || self.writer.is_peer_shutdown()
    }

    pub fn bytes_to_read(&self) -> usize {
        self.reader.items_to_consume()
    }

    /// The number of bytes that the endpoint has written, which its peer has not read. There are
    /// none once the peer is closed, as the bytes that it did not read are dropped.
    pub fn bytes_in_flight(&self) -> usize {
        self.peer.upgrade().map_or(0, |peer| peer.bytes_to_read())
    }

    /// Shut down the reading and/or the writing of the endpoint. As in Linux,
    /// this is allowed after the peer has closed, too.
    pub fn shutdown(&self, how: Shutdown) -> Result<()> {
        // Like Linux, the shutdown of an end is that of the other end, too: after this one has
        // shut down reading, the peer cannot write, and after it has shut down writing, the peer
        // reads the end of the file
        let mut flags = 0;
        if how.should_shut_read() {
            flags |= self.own_flags(RCV_SHUTDOWN) | self.peer_flags(SND_SHUTDOWN);
        }
        if how.should_shut_write() {
            flags |= self.own_flags(SND_SHUTDOWN) | self.peer_flags(RCV_SHUTDOWN);
        }
        // The channels are shut down first, which ends the writes that are going on and fails
        // the ones that follow, so that the bytes that a write reports are queued before the flags
        // of the poll tell the end of the file. The flags come before the channels notify the
        // observers, which ask for the events when they are woken up.
        let read = how.should_shut_read() && self.reader.begin_shutdown();
        let write = how.should_shut_write() && self.writer.begin_shutdown();
        self.flags.fetch_or(flags, Ordering::SeqCst);
        if read {
            self.reader.finish_shutdown()
        }
        if write {
            self.writer.finish_shutdown()
        }

        Ok(())
    }

    /// Returns the events of the endpoint. The flags of the shutdown, and the reset, are read in
    /// one step. The endpoint is writable if there is room, whether it can write or not, like in
    /// Linux.
    pub fn poll(&self) -> IoEvents {
        let flags = self.flags.load(Ordering::SeqCst) >> self.flags_shift;
        let mut events = IoEvents::empty();

        if flags & RCV_SHUTDOWN != 0 {
            events |= IoEvents::IN | IoEvents::RDHUP;
            if flags & SND_SHUTDOWN != 0 {
                events |= IoEvents::HUP;
            }
        }
        if flags & RESET != 0 {
            events |= IoEvents::ERR;
        }
        if self.reader.items_to_consume() > 0 {
            events |= IoEvents::IN;
        }
        // The bytes that the peer did not read are gone when it is closed, which makes room
        if self.writer.room() > 0 || self.peer.strong_count() == 0 {
            events |= IoEvents::OUT;
        }
        events
    }

    /// Returns the bits of a flag in the byte of this end of the shared word
    fn own_flags(&self, flags: u16) -> u16 {
        flags << self.flags_shift
    }

    /// Returns the bits of a flag in the byte of the peer of this end
    fn peer_flags(&self, flags: u16) -> u16 {
        flags << (8 - self.flags_shift)
    }

    pub fn ancillary(&self) -> Option<Ancillary> {
        self.ancillary.read().unwrap().clone()
    }

    pub fn set_ancillary(&self, ancillary: Ancillary) {
        if let Some(peer) = self.peer.upgrade() {
            *peer.peer_ancillary.write().unwrap() = Some(ancillary.clone());
        }
        self.ancillary.write().unwrap().insert(ancillary);
    }

    pub fn peer_ancillary(&self) -> Option<Ancillary> {
        self.peer_ancillary.read().unwrap().clone()
    }

    pub(self) fn register_relay_notifier(&self, observer: &Arc<RelayNotifier>) {
        self.reader.notifier().register(
            Arc::downgrade(observer) as Weak<dyn Observer<_>>,
            None,
            None,
        );

        self.writer.notifier().register(
            Arc::downgrade(observer) as Weak<dyn Observer<_>>,
            None,
            None,
        );
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        // The peer reads the end of the file, and cannot write any more, and the connection is
        // reset if bytes of the peer are not read, or accept() did not return it, like in Linux.
        // Its flags change before the channels, which notify it of the close as they are dropped.
        let mut flags = RCV_SHUTDOWN | SND_SHUTDOWN;
        if self.embryo.load(Ordering::Acquire) || self.bytes_to_read() > 0 {
            flags |= RESET;
        }
        self.flags
            .fetch_or(self.peer_flags(flags), Ordering::SeqCst);
        // The control messages that nobody reads any more: their files are released
        self.control_in().close();
    }
}

/// The process that connected, accepted or created the socket pair, which SO_PEERCRED shows to
/// the peer.
#[derive(Clone, Debug)]
pub struct Ancillary {
    pid: pid_t,
}

impl Ancillary {
    /// The ancillary data of the current process
    pub fn of_current() -> Self {
        Self {
            pid: current!().process().pid(),
        }
    }

    pub fn pid(&self) -> pid_t {
        self.pid
    }
}

// The limit of the bytes that an endpoint has in flight to its peer, until the program sets
// SO_SNDBUF (see `Endpoint::set_send_buf_size`). This value is got from
// /proc/sys/net/core/rmem_max and wmem_max that are same on linux.
pub const DEFAULT_BUF_SIZE: usize = 208 * 1024;

/// An observer used to observe both reader and writer of the endpoint. It also contains a
/// notifier that relays the notification of the endpoint.
pub(super) struct RelayNotifier {
    notifier: IoNotifier,
    endpoint: SgxMutex<Option<Endpoint>>,
}

impl RelayNotifier {
    pub fn new() -> Self {
        let notifier = IoNotifier::new();
        let endpoint = SgxMutex::new(None);
        Self { notifier, endpoint }
    }

    pub fn notifier(&self) -> &IoNotifier {
        &self.notifier
    }

    pub fn observe_endpoint(self: &Arc<Self>, endpoint: &Endpoint) {
        // Before registering, as the peer may write, and so trigger events,
        // right away
        *self.endpoint.lock().unwrap() = Some(endpoint.clone());
        endpoint.register_relay_notifier(self);
    }
}

impl Observer<IoEvents> for RelayNotifier {
    fn on_event(&self, event: &IoEvents, _metadata: &Option<Weak<dyn Any + Send + Sync>>) {
        let endpoint = self.endpoint.lock().unwrap();
        // Only endpoint can broadcast events

        let mut event = event.clone();
        // The event of the channel should not be broadcasted directly to socket.
        // The event transformation should be consistant with poll.
        if event.contains(IoEvents::HUP) {
            // The peer will send nothing more, so reading returns the end of
            // the file. Thus, the socket is readable, too, and a poller that
            // does not ask for RDHUP, e.g., poll() with POLLIN, has to be
            // woken up. The socket has HUP if it cannot send, either.
            event -= IoEvents::HUP;
            event |= IoEvents::IN | IoEvents::RDHUP | IoEvents::HUP;
        }

        if event.contains(IoEvents::ERR) {
            // The peer cannot read any more, which Linux tells a poller, too, as it wakes it up
            // whatever it waits for: the socket is writable if it has room, which a poller that
            // waits for room, or an edge triggered epoll, has to find out
            event -= IoEvents::ERR;
            event |= IoEvents::HUP | IoEvents::OUT;
        }

        // A notifier can only have events after observe_endpoint
        self.notifier()
            .broadcast(&(endpoint.as_ref().unwrap().poll() & event));
    }
}
