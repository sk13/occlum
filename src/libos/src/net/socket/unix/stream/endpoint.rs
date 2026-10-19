use super::*;
use events::{Event, EventFilter, Notifier, Observer};
use fs::channel::{Channel, Consumer, IoOpts, Producer};
use fs::{IoEvents, IoNotifier};
use std::any::Any;
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

    let mut end_a = Arc::new(Inner {
        addr: name_a.clone(),
        peer_addr: name_b.clone(),
        reader: con_a,
        writer: pro_b,
        peer: Weak::default(),
        ancillary: RwLock::new(None),
        peer_ancillary: RwLock::new(None),
    });
    let end_b = Arc::new(Inner {
        addr: name_b,
        peer_addr: name_a,
        reader: con_b,
        writer: pro_a,
        peer: Arc::downgrade(&end_a),
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

/// One end of the connected unix socket
pub struct Inner {
    addr: Arc<RwLock<Option<UnixAddr>>>,
    peer_addr: Arc<RwLock<Option<UnixAddr>>>,
    reader: Consumer<u8>,
    writer: Producer<u8>,
    peer: Weak<Self>,
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

    pub fn read(&self, bufs: &mut [&mut [u8]], opts: &IoOpts) -> Result<usize> {
        self.reader.pop_slices_with(bufs, opts)
    }

    /// Reads the bytes without taking them
    pub fn peek(&self, bufs: &mut [&mut [u8]], opts: &IoOpts) -> Result<usize> {
        self.reader.peek_slices_with(bufs, opts)
    }

    pub fn write(&self, bufs: &[&[u8]], opts: &IoOpts) -> Result<usize> {
        self.writer.push_slices_with(bufs, opts)
    }

    /// Whether the endpoint cannot write: it has been shut down for writing, or its peer for
    /// reading (the peer is closed)
    pub fn is_write_shutdown(&self) -> bool {
        self.writer.is_self_shutdown() || self.writer.is_peer_shutdown()
    }

    pub fn bytes_to_read(&self) -> usize {
        self.reader.items_to_consume()
    }

    /// Shut down the reading and/or the writing of the endpoint. As in Linux,
    /// this is allowed after the peer has closed, too.
    pub fn shutdown(&self, how: Shutdown) -> Result<()> {
        if how.should_shut_read() {
            self.reader.shutdown()
        }

        if how.should_shut_write() {
            self.writer.shutdown()
        }

        Ok(())
    }

    pub fn poll(&self) -> IoEvents {
        let mut events = IoEvents::empty();
        let reader_events = self.reader.poll();
        let writer_events = self.writer.poll();

        if reader_events.contains(IoEvents::HUP) || self.reader.is_self_shutdown() {
            events |= IoEvents::RDHUP | IoEvents::IN;
            if writer_events.contains(IoEvents::ERR) || self.writer.is_self_shutdown() {
                events |= IoEvents::HUP | IoEvents::OUT;
            }
        }

        events |= (reader_events & IoEvents::IN) | (writer_events & IoEvents::OUT);
        events
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

/// Ancillary data of connected unix socket's sent/received control message.
#[derive(Clone, Debug)]
pub struct Ancillary {
    pub(super) tid: pid_t, // currently store tid to locate file table
    pid: pid_t,
}

impl Ancillary {
    /// The ancillary data of the current thread, which connects or accepts
    pub fn of_current() -> Self {
        let current = current!();
        Self {
            tid: current.tid(),
            pid: current.process().pid(),
        }
    }

    /// The ancillary data of the current process without a thread, for the ends
    /// of a socket pair. recvmsg() looks up the descriptors that the peer sends
    /// in the file table of the thread of the peer's ancillary data, which
    /// connect() and accept() set. A socket pair has no such thread, so the tid
    /// is 0: no thread.
    pub fn of_current_process() -> Self {
        Self {
            tid: 0,
            pid: current!().process().pid(),
        }
    }

    pub fn tid(&self) -> pid_t {
        self.tid
    }

    pub fn pid(&self) -> pid_t {
        self.pid
    }
}

// TODO: Add SO_SNDBUF and SO_RCVBUF to set/getsockopt to dynamcally change the size.
// This value is got from /proc/sys/net/core/rmem_max and wmem_max that are same on linux.
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
            event -= IoEvents::ERR;
            event |= IoEvents::HUP;
        }

        // A notifier can only have events after observe_endpoint
        self.notifier()
            .broadcast(&(endpoint.as_ref().unwrap().poll() & event));
    }
}
