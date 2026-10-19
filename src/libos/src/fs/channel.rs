use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Weak;
use std::time::Duration;

use self::ring::{NoMemory, PushError, Ring, RingConsumer, RingProducer, RingStatus};
use super::{IoEvents, IoNotifier};
use crate::events::{
    Event, EventFilter, Notifier, Observer, Waiter, WaiterQueue, WaiterQueueObserver,
};
use crate::prelude::*;

mod ring;

/// A unidirectional communication channel, intended to implement IPC, e.g., pipe,
/// unix domain sockets, etc.
pub struct Channel<I> {
    producer: Producer<I>,
    consumer: Consumer<I>,
}

impl<I> Channel<I> {
    /// Create a new channel.
    pub fn new(capacity: usize) -> Result<Self> {
        let state = Arc::new(State::new());

        // The queue allocates its memory when items are pushed, not here
        let ring = Ring::new(capacity);
        let status = ring.status();
        let (ring_producer, ring_consumer) = ring.split();
        let mut producer = Producer::new(ring_producer, state.clone(), status.clone());
        let mut consumer = Consumer::new(ring_consumer, state.clone(), status);

        // The events on an endpoint is not triggered by itself, but its peer.
        // For example, a producer becomes writable (IoEvents::OUT) only if
        // its peer consumer gets read. So an endpoint needs to hold a
        // reference to the notifier of its peer.
        producer.peer_notifier = Arc::downgrade(&consumer.notifier);
        consumer.peer_notifier = Arc::downgrade(&producer.notifier);
        // An endpoint registers itself as an observer to its own notifier so
        // that it can be waken up by its peer.
        producer.notifier.register(
            Arc::downgrade(&producer.observer) as Weak<dyn Observer<_>>,
            None,
            None,
        );
        consumer.notifier.register(
            Arc::downgrade(&consumer.observer) as Weak<dyn Observer<_>>,
            None,
            None,
        );

        Ok(Self { producer, consumer })
    }

    /// Push an item into the channel.
    pub fn push(&self, item: I) -> Result<()> {
        self.producer.push(item)
    }

    /// Push an non-copy item into the channel.
    ///
    /// Non-copy items need special treatment because once passed as an argument
    /// to this method, an non-copy object is considered **moved** from the
    /// caller to the callee (this method) by Rust. This makes it impossible for
    /// the caller to retry calling this method with the same input item
    /// in case of an `EAGAIN` or `EINTR` error. For this reason, we need a way
    /// for the caller to get back the ownership of the input item upon error.
    /// Thus, an extra argument is added to this method.
    // TODO: implement this method in the future when pushing items individually is
    // really needed
    pub fn push_noncopy(&self, item: I, retry: &mut Option<I>) -> Result<()> {
        unimplemented!();
    }

    /// Pop an item out of the channel.
    pub fn pop(&self) -> Result<Option<I>> {
        self.consumer.pop()
    }

    /// Turn the channel into a pair of producer and consumer.
    pub fn split(self) -> (Producer<I>, Consumer<I>) {
        let Channel { producer, consumer } = self;
        (producer, consumer)
    }

    pub fn consumer(&self) -> &Consumer<I> {
        &self.consumer
    }

    pub fn producer(&self) -> &Producer<I> {
        &self.producer
    }

    pub fn capacity(&self) -> usize {
        self.consumer.capacity()
    }

    pub fn items_to_consume(&self) -> usize {
        self.consumer.items_to_consume()
    }

    pub fn set_nonblocking(&self, nonblocking: bool) {
        self.consumer.set_nonblocking(nonblocking);
        self.producer.set_nonblocking(nonblocking);
    }

    pub fn shutdown(&self) {
        self.consumer.shutdown();
        self.producer.shutdown();
    }
}

impl<I: Copy> Channel<I> {
    /// Push a slice of items into the channel.
    pub fn push_slice(&self, items: &[I]) -> Result<usize> {
        self.producer.push_slice(items)
    }

    /// Pop a slice of items from the channel.
    pub fn pop_slice(&self, items: &mut [I]) -> Result<usize> {
        self.consumer.pop_slice(items)
    }
}

/// How a call that moves items through a channel waits, besides the flag of its endpoint.
#[derive(Clone, Copy, Debug, Default)]
pub struct IoOpts {
    /// The call does not wait for the other end, as if the endpoint were nonblocking
    /// (`MSG_DONTWAIT`).
    pub nonblocking: bool,
    /// The time that the call waits for the other end, which is not limited if it is `None`. The
    /// call fails with EAGAIN if it has waited that long, or it returns the items that it has moved
    /// so far. A writer waits that long for room, so its time starts again when it has made
    /// progress; a reader waits that long in all.
    pub timeout: Option<Duration>,
    /// The call moves all the items if it blocks: it waits for the other end as long as it takes
    /// (`write` of a blocking pipe or socket, `MSG_WAITALL`), unless the end of the channel, an
    /// interrupt, or the timeout stops it, which return the items that were moved so far, if some
    /// were. A call that does not block moves what it can. Without it, a call that has moved
    /// items returns them.
    pub all: bool,
    /// A write of at most this number of items moves all of them or none: it waits for room for
    /// them all, or fails with EAGAIN. A pipe does so for PIPE_BUF bytes, which it must not
    /// interleave with the bytes of another write. The writes that are longer move what fits.
    pub atomic: usize,
}

/// What an attempt of a call that waits has come to.
pub(crate) enum Attempt<T> {
    /// The call is over
    Done(T),
    /// The call has to wait for the other end
    Wait,
    /// The call has moved some items, but not all, and has to wait for the other end for more
    WaitAfterProgress,
}

/// Makes attempts until one of them is over. A thread that makes no progress waits for a wake-up of
/// the queue between the attempts, for the time of the timeout, which is counted over all the
/// waits. After that time it has one more attempt, and fails with EAGAIN if that is not over. If
/// an attempt has made progress, the time starts again. EINTR if a signal interrupts the wait.
///
/// The calls that wait on a channel and the unix listener, which waits for connections and for room
/// in its queue, all wait with it, so they count their timeouts alike.
pub(crate) fn wait_for<T>(
    queue: &WaiterQueue,
    timeout: Option<Duration>,
    mut attempt: impl FnMut() -> Result<Attempt<T>>,
) -> Result<T> {
    // Try without creating a waiter. This saves some CPU cycles if the first attempt succeeds.
    if let Attempt::Done(value) = attempt()? {
        return Ok(value);
    }

    let waiter = Waiter::new();
    // The waiter stays in the queue unless the queue wakes it up. Dequeue it whenever the loop is
    // left (a successful attempt, an error, or an interrupted wait), or the queue of an idle
    // channel would grow with every call.
    let _dequeue = queue.dequeue_on_drop(&waiter);
    let mut remaining = timeout;
    let mut timed_out = false;
    loop {
        // Before the attempt, so that no wake-up gets lost between the attempt and the wait
        queue.reset_and_enqueue(&waiter);
        match attempt()? {
            Attempt::Done(value) => return Ok(value),
            Attempt::Wait => (),
            Attempt::WaitAfterProgress => {
                remaining = timeout;
                timed_out = false;
            }
        }
        if timed_out {
            return_errno!(EAGAIN, "the timeout is over");
        }
        // The time that is left is kept if the waiter has to go on waiting
        match waiter.wait_mut(remaining.as_mut()) {
            Ok(()) => (),
            Err(e) if e.errno() == ETIMEDOUT => timed_out = true,
            Err(e) => return Err(e),
        }
    }
}

// A macro to implemennt the common part of the two end point types, Producer<I>
// and Consumer<I>.
macro_rules! impl_end_point_type {
    ($(#[$attr:meta])* $vis:vis struct $end_point:ident<$i:ident> {
        inner: $inner:ident<$_:ident>,
    }) => (
        /// An endpoint is either the producer or consumer of a channel.
        $(#[$attr])* $vis struct $end_point<$i> {
            inner: SgxMutex<$inner<$i>>,
            // How many items the queue holds, which the calls that count them read without the
            // lock of the end: the lock is held while items are copied, and the events of the
            // other end, which every push and pop triggers, poll the channel.
            status: RingStatus<$i>,
            state: Arc<State>,
            observer: Arc<WaiterQueueObserver<IoEvents>>,
            notifier: Arc<IoNotifier>,
            peer_notifier: Weak<IoNotifier>,
            is_nonblocking: AtomicBool,
        }

        impl<$i> $end_point<$i> {
            fn new(inner: $inner<$i>, state: Arc<State>, status: RingStatus<$i>) -> Self {
                let inner = SgxMutex::new(inner);
                let observer = WaiterQueueObserver::new();
                let notifier = Arc::new(IoNotifier::new());
                let peer_notifier = Default::default();
                let is_nonblocking = AtomicBool::new(false);
                Self {
                    inner,
                    status,
                    state,
                    observer,
                    notifier,
                    peer_notifier,
                    is_nonblocking,
                }
            }

            /// Returns the I/O notifier.
            ///
            /// An interesting observer can receive I/O events of the endpoint by
            /// registering itself to this notifier.
            pub fn notifier(&self) -> &IoNotifier {
                &self.notifier
            }

            /// Returns whether the endpoint is non-blocking.
            ///
            /// By default, a channel is blocking.
            pub fn is_nonblocking(&self) -> bool {
                self.is_nonblocking.load(Ordering::Acquire)
            }

            /// Set whether the endpoint is non-blocking.
            pub fn set_nonblocking(&self, nonblocking: bool) {
                self.is_nonblocking.store(nonblocking, Ordering::Release);

                if nonblocking {
                    // Wake all threads that are blocked on pushing/popping this endpoint
                    self.observer.waiter_queue().dequeue_and_wake_all();
                }
            }

            fn trigger_peer_events(&self, events: &IoEvents) {
                if let Some(peer_notifier) = self.peer_notifier.upgrade() {
                    peer_notifier.broadcast(events);
                }
            }
        }
    )
}

// Just like a normal loop, except that a waiter queue (as well as a waiter)
// is used to avoid busy loop. This macro is used in the push/pop implementation
// below.
macro_rules! waiter_loop {
    ($loop_body: block, $waiter_queue: expr) => {
        // Try without creating a waiter. This saves some CPU cycles if the
        // first attempt succeeds.
        {
            $loop_body
        }

        // The main loop
        let waiter = Waiter::new();
        let waiter_queue = $waiter_queue;
        // The waiter stays in the queue unless the queue wakes it up. Dequeue
        // it whenever the loop is left (a successful attempt, an error, or an
        // interrupted wait), or the queue of an idle channel would grow with
        // every call.
        let _dequeue = waiter_queue.dequeue_on_drop(&waiter);
        loop {
            waiter_queue.reset_and_enqueue(&waiter);

            {
                $loop_body
            }

            waiter.wait(None)?;
        }
    };
}

impl_end_point_type! {
    /// Producer is the writable endpoint of a channel.
    pub struct Producer<I> {
        inner: RingProducer<I>,
    }
}

impl<I> Producer<I> {
    pub fn push(&self, mut item: I) -> Result<()> {
        waiter_loop!(
            {
                let mut rb_producer = self.inner.lock().unwrap();
                if self.is_self_shutdown() || self.is_peer_shutdown() {
                    return_errno!(EPIPE, "one or both endpoints have been shutdown");
                }

                item = match rb_producer.push(item) {
                    Ok(()) => {
                        drop(rb_producer);
                        self.trigger_peer_events(&IoEvents::IN);
                        return Ok(());
                    }
                    Err(PushError::Full(item)) => item,
                    Err(PushError::NoMemory(_)) => {
                        return_errno!(ENOMEM, "no memory for the item");
                    }
                };

                if self.is_nonblocking() {
                    return_errno!(EAGAIN, "try again later");
                }
            },
            self.observer.waiter_queue()
        );
    }

    pub fn poll(&self) -> IoEvents {
        let mut events = IoEvents::empty();

        let writable = self.status.remaining() > 0 || self.is_self_shutdown();
        if writable {
            events |= IoEvents::OUT;
        }

        if self.is_peer_shutdown() {
            events |= IoEvents::ERR;
        }

        events
    }

    pub fn shutdown(&self) {
        {
            // It is important to hold this lock while updating the state
            let mut inner = self.inner.lock().unwrap();
            if self.state.is_producer_shutdown() {
                return;
            }
            self.state.set_producer_shutdown();
            // Nothing is pushed any more, so the queue need not keep chunks for the producer
            inner.close();
        }

        // The shutdown of the producer triggers hangup events on the consumer
        self.trigger_peer_events(&IoEvents::HUP);
        // Wake all threads that are blocked on pushing to this producer
        self.observer.waiter_queue().dequeue_and_wake_all();
    }

    pub fn is_self_shutdown(&self) -> bool {
        self.state.is_producer_shutdown()
    }

    pub fn is_peer_shutdown(&self) -> bool {
        self.state.is_consumer_shutdown()
    }

    /// Returns the number of items that were pushed since the channel was
    /// created.
    pub fn total_pushed(&self) -> usize {
        self.inner.lock().unwrap().total_pushed()
    }

    /// Returns the number of items that the channel has room for.
    pub fn room(&self) -> usize {
        self.status.remaining()
    }

    /// Changes the number of items that the channel holds. A channel that holds more items than
    /// the new capacity keeps them, but it takes no more until it holds fewer. If the channel can
    /// take more now, it returns true, and the caller has to call `notify_room()`, which wakes up
    /// the threads that wait for room and those that poll the channel. It is a call of its own, so
    /// that the caller can do it without holding the locks that those threads need.
    pub fn set_capacity(&self, capacity: usize) -> bool {
        let rb_producer = self.inner.lock().unwrap();
        let grown = capacity > rb_producer.capacity();
        rb_producer.set_capacity(capacity);
        grown
    }

    /// Wakes up the threads that wait for room and those that poll the channel, after
    /// `set_capacity()` has returned true.
    pub fn notify_room(&self) {
        self.notifier.broadcast(&IoEvents::OUT);
    }
}

impl<I: Copy> Producer<I> {
    pub fn push_slice(&self, items: &[I]) -> Result<usize> {
        self.push_slices(&[items])
    }

    pub fn push_slices(&self, item_slices: &[&[I]]) -> Result<usize> {
        self.push_slices_with(item_slices, &IoOpts::default())
    }

    /// Pushes the items of the slices, and returns their number. A call that has to wait for room
    /// stops at the first push if the options do not say `all`. It fails with EPIPE if an end of
    /// the channel is shut down, also if there are no items, unless it has pushed items already,
    /// in which case it returns their number.
    pub fn push_slices_with(&self, item_slices: &[&[I]], opts: &IoOpts) -> Result<usize> {
        let len: usize = item_slices.iter().map(|slice| slice.len()).sum();
        // The items that were pushed, which the call returns if it stops early
        let mut done = 0;

        let result = wait_for(self.observer.waiter_queue(), opts.timeout, || {
            let mut rb_producer = self.inner.lock().unwrap();
            if self.is_self_shutdown() || self.is_peer_shutdown() {
                return_errno!(EPIPE, "one or both endpoints have been shutdown");
            }
            if len == 0 {
                return Ok(Attempt::Done(0));
            }

            let nonblocking = self.is_nonblocking() || opts.nonblocking;
            let mut pushed = 0;
            let mut no_memory = false;
            // A write that has to be atomic pushes nothing if there is no room for all of it
            let slices: &[&[I]] =
                if done == 0 && len <= opts.atomic && rb_producer.remaining() < len {
                    &[]
                } else {
                    item_slices
                };
            // The slices begin with the items that were pushed by an earlier attempt
            let mut skip = done;
            for items in slices {
                if skip >= items.len() {
                    skip -= items.len();
                    continue;
                }
                let items = &items[skip..];
                skip = 0;
                let count = match rb_producer.push_slice(items) {
                    Ok(count) => count,
                    Err(NoMemory) => {
                        no_memory = true;
                        break;
                    }
                };
                pushed += count;
                if count < items.len() {
                    break;
                }
            }
            drop(rb_producer);
            if pushed > 0 {
                done += pushed;
                self.trigger_peer_events(&IoEvents::IN);
            }

            // Report what was pushed before the memory ran out, if anything
            if no_memory && done == 0 {
                return_errno!(ENOMEM, "no memory for the items");
            }
            if done == len || no_memory || (done > 0 && (!opts.all || nonblocking)) {
                return Ok(Attempt::Done(done));
            }
            if nonblocking {
                return_errno!(EAGAIN, "try again later");
            }
            Ok(if pushed > 0 {
                Attempt::WaitAfterProgress
            } else {
                Attempt::Wait
            })
        });

        // The items that were pushed are what the call has done, whatever stopped it
        match result {
            Err(_) if done > 0 => Ok(done),
            result => result,
        }
    }
}

impl<I> Drop for Producer<I> {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl_end_point_type! {
    /// Consumer is the readable endpoint of a channel.
    pub struct Consumer<I> {
        inner: RingConsumer<I>,
    }
}

impl<I> Consumer<I> {
    pub fn pop(&self) -> Result<Option<I>> {
        waiter_loop!(
            {
                let mut rb_consumer = self.inner.lock().unwrap();
                // Get the flag before the items: the peer shuts down after its
                // last push, so no item is left behind if the flag is set
                let is_peer_shutdown = self.is_peer_shutdown();

                if let Some(item) = rb_consumer.pop() {
                    drop(rb_consumer);
                    self.trigger_peer_events(&IoEvents::OUT);
                    return Ok(Some(item));
                }

                // Like the end of a file: no more items will be consumed. The
                // items that arrived before this endpoint shut down are still
                // consumed, just like a socket delivers its data after
                // shutdown(SHUT_RD).
                if is_peer_shutdown || self.is_self_shutdown() {
                    return Ok(None);
                }
                if self.is_nonblocking() {
                    return_errno!(EAGAIN, "try again later");
                }
            },
            self.observer.waiter_queue()
        );
    }

    pub fn poll(&self) -> IoEvents {
        let mut events = IoEvents::empty();

        let readable = !self.status.is_empty() || self.is_self_shutdown();
        if readable {
            events |= IoEvents::IN;
        }

        if self.is_peer_shutdown() {
            events |= IoEvents::HUP;
        }

        events
    }

    pub fn shutdown(&self) {
        {
            // It is important to hold this lock while updating the state
            let inner = self.inner.lock().unwrap();
            if self.state.is_consumer_shutdown() {
                return;
            }
            self.state.set_consumer_shutdown();
        }

        // The consumer being shutdown triggers error on the producer
        self.trigger_peer_events(&IoEvents::ERR);
        // Wake all threads that are blocked on popping from this consumer
        self.observer.waiter_queue().dequeue_and_wake_all();
    }

    pub fn is_self_shutdown(&self) -> bool {
        self.state.is_consumer_shutdown()
    }

    pub fn is_peer_shutdown(&self) -> bool {
        self.state.is_producer_shutdown()
    }

    pub fn items_to_consume(&self) -> usize {
        self.status.len()
    }

    pub fn capacity(&self) -> usize {
        self.status.capacity()
    }

    // Get the length of data stored in the buffer
    pub fn ready_len(&self) -> usize {
        self.status.len()
    }

    /// Returns the number of items that were popped since the channel was
    /// created.
    pub fn total_popped(&self) -> usize {
        self.inner.lock().unwrap().total_popped()
    }
}

impl<I: Copy> Consumer<I> {
    pub fn pop_slice(&self, items: &mut [I]) -> Result<usize> {
        self.pop_slices(&mut [items])
    }

    pub fn pop_slices(&self, item_slices: &mut [&mut [I]]) -> Result<usize> {
        self.pop_slices_with(item_slices, &IoOpts::default())
    }

    /// Pops items into the slices, and returns their number. A call that has to wait for items
    /// stops at the first pop if the options do not say `all`. It returns 0 at the end of the
    /// channel, or the number of the items that it has popped if it reaches it.
    pub fn pop_slices_with(&self, item_slices: &mut [&mut [I]], opts: &IoOpts) -> Result<usize> {
        let len: usize = item_slices.iter().map(|slice| slice.len()).sum();
        if len == 0 {
            return Ok(0);
        }
        // The items that were popped, which the call returns if it stops early
        let mut done = 0;

        let result = wait_for(self.observer.waiter_queue(), opts.timeout, || {
            let mut rb_consumer = self.inner.lock().unwrap();
            // Get the flag before the items, see pop()
            let is_peer_shutdown = self.is_peer_shutdown();

            // The slices begin with the items that were popped by an earlier attempt
            let mut skip = done;
            let mut popped = 0;
            for items in item_slices.iter_mut() {
                if skip >= items.len() {
                    skip -= items.len();
                    continue;
                }
                let items = &mut items[skip..];
                skip = 0;
                let count = rb_consumer.pop_slice(items);
                popped += count;
                if count < items.len() {
                    break;
                }
            }
            drop(rb_consumer);
            if popped > 0 {
                done += popped;
                self.trigger_peer_events(&IoEvents::OUT);
            }

            // The end of the file, see pop()
            let nonblocking = self.is_nonblocking() || opts.nonblocking;
            if done == len || (done > 0 && !opts.all) || is_peer_shutdown || self.is_self_shutdown()
            {
                return Ok(Attempt::Done(done));
            }
            if nonblocking && done > 0 {
                return Ok(Attempt::Done(done));
            }
            if nonblocking {
                return_errno!(EAGAIN, "try again later");
            }
            Ok(Attempt::Wait)
        });

        // The items that were popped are what the call has done, whatever stopped it
        match result {
            Err(_) if done > 0 => Ok(done),
            result => result,
        }
    }

    /// Copies the first items of the channel into the slices without popping them, and returns
    /// their number. It waits for items like `pop_slices_with` does, if there are none, but it
    /// does not wait for more than there are, whatever `opts.all` says.
    pub fn peek_slices_with(&self, item_slices: &mut [&mut [I]], opts: &IoOpts) -> Result<usize> {
        let len: usize = item_slices.iter().map(|slice| slice.len()).sum();
        if len == 0 {
            return Ok(0);
        }

        wait_for(self.observer.waiter_queue(), opts.timeout, || {
            let rb_consumer = self.inner.lock().unwrap();
            // Get the flag before the items, see pop()
            let is_peer_shutdown = self.is_peer_shutdown();

            let mut peeked = 0;
            for items in item_slices.iter_mut() {
                let count = rb_consumer.peek_slice_at(peeked, items);
                peeked += count;
                if count < items.len() {
                    break;
                }
            }
            drop(rb_consumer);

            // The end of the file, see pop()
            if peeked > 0 || is_peer_shutdown || self.is_self_shutdown() {
                return Ok(Attempt::Done(peeked));
            }
            if self.is_nonblocking() || opts.nonblocking {
                return_errno!(EAGAIN, "try again later");
            }
            Ok(Attempt::Wait)
        })
    }
}

impl<I> Drop for Consumer<I> {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// The state of a channel shared by the two endpoints of a channel.
struct State {
    is_producer_shutdown: AtomicBool,
    is_consumer_shutdown: AtomicBool,
}

impl State {
    pub fn new() -> Self {
        Self {
            is_producer_shutdown: AtomicBool::new(false),
            is_consumer_shutdown: AtomicBool::new(false),
        }
    }

    pub fn is_producer_shutdown(&self) -> bool {
        self.is_producer_shutdown.load(Ordering::Acquire)
    }

    pub fn is_consumer_shutdown(&self) -> bool {
        self.is_consumer_shutdown.load(Ordering::Acquire)
    }

    pub fn set_producer_shutdown(&self) {
        self.is_producer_shutdown.store(true, Ordering::Release)
    }

    pub fn set_consumer_shutdown(&self) {
        self.is_consumer_shutdown.store(true, Ordering::Release)
    }
}
