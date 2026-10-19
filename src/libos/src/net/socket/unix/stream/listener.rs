use super::endpoint::{Endpoint, RelayNotifier};
use super::*;
use crate::events::{Waiter, WaiterQueue};
use fs::IoEvents;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// The listener status of a stream unix socket.
/// It contains the queue of the connections that connect() made and accept()
/// has not taken yet. Like in Linux, the queue takes one connection more than
/// the backlog that listen() was called with.
///
/// No lock is held while a thread waits: accept() waits for a connection, and
/// connect() waits for room in the queue if it is full. Changing the backlog
/// does not touch the connections in the queue.
pub struct Listener {
    queue: SgxMutex<Queue>,
    // The threads that wait in accept() for a connection
    acceptors: WaiterQueue,
    // The threads that wait in connect() for room in the queue
    connectors: WaiterQueue,
    // The nonblocking status of the socket, which accept() depends on
    nonblocking: AtomicBool,
    notifier: Arc<RelayNotifier>,
}

struct Queue {
    // The oldest connection first
    incoming: VecDeque<Endpoint>,
    // The connections that connect() has room for, but is still making
    reserved: usize,
    // What listen() was called with. If it gets smaller than the number of
    // connections that are queued, they stay, but there is no room for more of
    // them until accept() has taken enough.
    backlog: usize,
    // The socket is closed, which refuses the connections
    closed: bool,
}

impl Queue {
    /// Returns the number of connections that connect() has room for. Like in
    /// Linux, it is one more than the backlog allows.
    fn free_slots(&self) -> usize {
        (self.backlog + 1).saturating_sub(self.incoming.len() + self.reserved)
    }
}

/// Room in the queue of a listener for a connection that connect() is making,
/// so that connect() does not create the connection before it knows that there
/// is room for it. The room is given back when the reservation is dropped,
/// unless it was used to queue the connection.
pub struct Reservation<'a> {
    listener: &'a Listener,
    used: bool,
}

impl Reservation<'_> {
    /// Queues the connection, which accept() takes from now on. Fails with
    /// ECONNREFUSED if the listener has been closed since the room was
    /// reserved.
    pub fn push(mut self, endpoint: Endpoint) -> Result<()> {
        self.used = true;
        let listener = self.listener;
        {
            let mut queue = listener.queue.lock().unwrap();
            queue.reserved -= 1;
            if queue.closed {
                return_errno!(ECONNREFUSED, "the listener is closed");
            }
            queue.incoming.push_back(endpoint);
        }
        listener.acceptors.dequeue_and_wake_all();
        listener.notifier.notifier().broadcast(&IoEvents::IN);
        Ok(())
    }
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        if !self.used {
            self.listener.queue.lock().unwrap().reserved -= 1;
            self.listener.connectors.dequeue_and_wake_one();
        }
    }
}

impl Listener {
    pub(super) fn new(backlog: usize, nonblocking: bool, notifier: Arc<RelayNotifier>) -> Self {
        Self {
            queue: SgxMutex::new(Queue {
                incoming: VecDeque::new(),
                reserved: 0,
                backlog,
                closed: false,
            }),
            acceptors: WaiterQueue::new(),
            connectors: WaiterQueue::new(),
            nonblocking: AtomicBool::new(nonblocking),
            notifier,
        }
    }

    /// Changes the backlog, which listen() gets called again with
    pub fn set_backlog(&self, backlog: usize) {
        let free_slots = {
            let mut queue = self.queue.lock().unwrap();
            queue.backlog = backlog;
            queue.free_slots()
        };
        // The threads that wait for room get it, as far as it goes
        self.connectors.dequeue_and_wake_nr(free_slots);
    }

    /// Reserves room in the queue for a connection that connect() is going to
    /// make. If the queue is full, a nonblocking socket fails with EAGAIN, and
    /// a blocking one waits for an accept() to make room, as long as the
    /// timeout allows, which ends with EAGAIN, too. EINTR if a signal
    /// interrupts the wait. ECONNREFUSED if the listener is closed, also while
    /// the thread waits.
    pub fn reserve_incoming(
        &self,
        nonblocking: bool,
        timeout: Option<Duration>,
    ) -> Result<Reservation<'_>> {
        wait_for(&self.connectors, timeout, || {
            let reservation = self.try_reserve()?;
            if reservation.is_none() && nonblocking {
                return_errno!(EAGAIN, "the backlog is full");
            }
            Ok(reservation)
        })
        .map_err(|e| {
            // An accept() that made room woke up one thread only. If this
            // thread has been woken up, but gives up, the next one gets the room.
            if self.queue.lock().unwrap().free_slots() > 0 {
                self.connectors.dequeue_and_wake_one();
            }
            e
        })
    }

    fn try_reserve(&self) -> Result<Option<Reservation<'_>>> {
        let mut queue = self.queue.lock().unwrap();
        if queue.closed {
            return_errno!(ECONNREFUSED, "the listener is closed");
        }
        if queue.free_slots() == 0 {
            return Ok(None);
        }
        queue.reserved += 1;
        Ok(Some(Reservation {
            listener: self,
            used: false,
        }))
    }

    /// Returns the oldest connection. If there is none, a nonblocking socket
    /// fails with EAGAIN, and a blocking one waits for a connection, or for a
    /// signal, which fails with EINTR.
    pub fn pop_incoming(&self) -> Result<Endpoint> {
        wait_for(&self.acceptors, None, || self.try_pop())
    }

    fn try_pop(&self) -> Result<Option<Endpoint>> {
        let (endpoint, free_slots) = {
            let mut queue = self.queue.lock().unwrap();
            let endpoint = match queue.incoming.pop_front() {
                Some(endpoint) => endpoint,
                None if self.nonblocking() => return_errno!(EAGAIN, "no connection is incoming"),
                None => return Ok(None),
            };
            (endpoint, queue.free_slots())
        };
        if free_slots > 0 {
            self.connectors.dequeue_and_wake_one();
        }
        Ok(Some(endpoint))
    }

    pub fn nonblocking(&self) -> bool {
        self.nonblocking.load(Ordering::Acquire)
    }

    pub fn set_nonblocking(&self, nonblocking: bool) {
        self.nonblocking.store(nonblocking, Ordering::Release);

        if nonblocking {
            // The threads that wait in accept() fail now
            self.acceptors.dequeue_and_wake_all();
        }
    }

    /// Closes the listener, which refuses the connections from now on, and
    /// drops those that are queued. A thread that is making a connection may
    /// still have the listener.
    pub fn close(&self) {
        let incoming = {
            let mut queue = self.queue.lock().unwrap();
            queue.closed = true;
            std::mem::take(&mut queue.incoming)
        };
        // The threads that wait for room fail now
        self.connectors.dequeue_and_wake_all();
        // Not with the lock held: the endpoints notify their peers when they are
        // dropped
        drop(incoming);
    }

    pub fn poll_new(&self) -> IoEvents {
        let mut events = IoEvents::empty();
        if !self.queue.lock().unwrap().incoming.is_empty() {
            events |= IoEvents::IN;
        }
        events
    }
}

/// Returns what `attempt` returns once it returns something. The thread waits
/// for a wake-up of the queue between the attempts, as long as the timeout
/// allows. Then it has one more attempt, and fails with EAGAIN if it does not
/// get anything. EINTR if a signal interrupts the wait.
fn wait_for<T>(
    queue: &WaiterQueue,
    mut timeout: Option<Duration>,
    mut attempt: impl FnMut() -> Result<Option<T>>,
) -> Result<T> {
    // Try without creating a waiter
    if let Some(item) = attempt()? {
        return Ok(item);
    }

    let waiter = Waiter::new();
    // The waiter stays in the queue unless it is woken up, so it is dequeued
    // whenever the loop is left, like the waits of the channels
    let _dequeue = queue.dequeue_on_drop(&waiter);
    let mut timed_out = false;
    loop {
        // Before the next attempt, so that no wake-up gets lost between the
        // attempt and the wait
        queue.reset_and_enqueue(&waiter);
        if let Some(item) = attempt()? {
            return Ok(item);
        }
        if timed_out {
            return_errno!(EAGAIN, "the timeout is over");
        }
        match waiter.wait_mut(timeout.as_mut()) {
            Ok(()) => (),
            Err(e) if e.errno() == ETIMEDOUT => timed_out = true,
            Err(e) => return Err(e),
        }
    }
}
