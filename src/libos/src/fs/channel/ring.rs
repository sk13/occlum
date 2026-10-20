//! A single-producer single-consumer queue whose memory follows its contents.
//!
//! The ring buffer that the channels used before (the `ringbuf` crate) allocated its whole
//! capacity when it was created. That was 208 KiB for each direction of a unix socket connection
//! and 1 MiB for a pipe, whether or not anything was ever written: a connection that waited in a
//! backlog took 416 KiB of the kernel heap, so 91 of them exhausted the 40 MB heap of the test
//! configuration and aborted the enclave.
//!
//! A `Ring` keeps its items in a list of chunks instead. The producer appends a chunk when the one
//! that it fills is full, and the consumer gives up a chunk when it has read it, so the memory in
//! use follows the number of queued items. The capacity is only a limit on that number. An idle
//! queue has no chunk at all, one that carries small messages has chunks of a few hundred bytes,
//! and one that carries a bulk transfer reaches the largest chunk, 16 KiB for a unix socket and
//! 64 KiB for a pipe.
//!
//! The two ends are used like those of that ring buffer: a producer and a consumer may work in
//! parallel, each of them from one thread at a time, which the channel guarantees with a lock per
//! end. No lock is needed between them. The items move through the chunks, which hold them in
//! order, and two counters tell how many items were pushed and popped altogether, so the length of
//! the queue is their difference:
//!
//! - The producer fills the last chunk. When there is no room left in it, it gets the next chunk
//!   and links it (`Chunk::next`, or `Shared::first` for the first chunk) before it publishes any
//!   item stored in the new chunk by increasing `pushed` with release ordering.
//! - The consumer reads the items below `pushed` (acquire ordering). When it has read the last
//!   item of a chunk and the next chunk exists, it gives up the chunk. The producer does not touch
//!   a chunk any more once it has linked the next one, so the consumer may give it up at once. It
//!   never gives up the last chunk: the producer may append to it.
//! - The producer and the consumer never access the same item at the same time, since the
//!   consumer only reads published items and the producer only writes after the published ones.
//!
//! The ends hold a lock of the channel while they copy items, so another thread that asks how many
//! items are queued, e.g., to tell the events of a channel, would wait for the copy. It asks a
//! `RingStatus`, which reads the two counters and takes no lock.
//!
//! # Spare chunks
//!
//! If the consumer freed every chunk that it has read and the producer allocated the next one, a
//! stream would call the global allocator twice for every chunk. The allocator is one lock for
//! all the queues of the enclave, so with a dozen streams in parallel that costs more than
//! copying the data. Therefore the consumer parks the chunks that it has read in one of
//! `SPARE_CHUNKS` slots (`Shared::spares`), and the producer takes a spare before it allocates.
//! A slot holds a pointer, and a chunk belongs to whoever holds its pointer:
//!
//! - The consumer parks a chunk in an empty slot with a store, after it has read the last item of
//!   the chunk. Only the consumer fills a slot, so a slot that it found empty is empty until it
//!   fills it.
//! - The producer takes a chunk out of a slot with an acquire swap, which makes the reads of the
//!   consumer happen before the writes that the producer makes to the chunk next. The swap
//!   gives the pointer to one taker only. Anybody that frees spares takes them out the same way,
//!   so no chunk is freed twice or used while it is freed, and as a slot is only ever filled by
//!   the consumer with a chunk that nobody else holds, there is no ABA problem.
//!
//! What a queue keeps when it is idle after a transfer is bounded:
//!
//! - The consumer parks spares as long as they hold no more than three quarters of the capacity
//!   of the queue and 256 KiB (`Shared::spare_budget`, counted in `Shared::spare_bytes`). So the
//!   queue holds its last chunk and that many bytes of spares: at most 16 KiB + 156 KiB for a
//!   unix socket (capacity 208 KiB), 64 KiB + 256 KiB for a pipe (1 MiB). A queue that has never
//!   had more than a few chunks queued at a time keeps as few, and one that has carried a small
//!   message holds one chunk of a few hundred bytes. Measured in an enclave: after 1 MB of 64 KiB
//!   writes that the reader took as they came, a socketpair holds 168 KB in all, a pipe 132 KB,
//!   and after a burst that filled the queue it holds 160 KiB per direction of a socket and
//!   320 KiB per pipe.
//! - The queue gives the spares back when the producer is done (it is dropped, or `close` is
//!   called when the socket is shut down for writing), and when the consumer is dropped. Once the
//!   producer is closed, the consumer frees the chunks that it reads. `Shared::producer_closed`
//!   and the slots are accessed with sequentially consistent ordering where the producer closes
//!   and the consumer parks, so that one of the two sees the other and no chunk is parked after
//!   `close` has freed the spares. Dropping the queue frees the chunks that are left and drops
//!   the items that are queued. No spare is left then, since the consumer is gone, and only the
//!   consumer parks. A reader that is shut down but not closed keeps its spares: it can still
//!   read the queue.
//!
//! # Tests
//!
//! The file uses nothing but `std`, so that its tests run on the host, without an enclave. They
//! count the live bytes and the allocations with a global allocator, for each test by itself (the
//! threads that a test starts count with it, those of the test harness with nobody), so that the
//! tests compare them exactly and may run at the same time. `make test-host` builds and runs
//! them, one after the other, which comes down to
//!
//! ```text
//! rustc --edition 2021 --test -C opt-level=2 -C debug-assertions=on ring.rs -o ring_tests
//! ./ring_tests --test-threads=1
//! ```
//!
//! The test `close_while_the_consumer_parks` hammers the handshake between `close` and `park`,
//! which is a few nanoseconds wide, with two threads that start each round together. It takes
//! about a second. `RING_HAMMER_ROUNDS=1000000 make test-host` makes a million rounds of it.
//!
//! The tests run clean under AddressSanitizer (`-Zsanitizer=address`). Under ThreadSanitizer (with
//! ASLR switched off) they report no race on the chunks, the slots or the counters, only the
//! fence of `Arc::drop`, which it does not know, and the internals of libtest.

use std::alloc::{self, Layout};
use std::cmp::{max, min};
use std::marker::PhantomData;
use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};
use std::sync::Arc;

/// The smallest chunk in bytes (items of a larger size make a chunk of one item). A chunk is made
/// as large as what the producer wants to write and what is queued already, so a connection that
/// exchanges small messages stays small, and one that streams data, whose consumer lags behind,
/// amortizes the cost of the allocations and of the queue over a large chunk.
const MIN_CHUNK_BYTES: usize = 512;

/// The largest chunk of a queue is a sixteenth of its capacity, between these sizes: 16 KiB for a
/// unix socket (208 KiB), 64 KiB for a pipe (1 MiB). The larger the chunks of a stream, the fewer
/// operations on chunks it needs for the same data, and the pipes that are limited by them are the
/// ones that need large chunks.
const LARGEST_CHUNK_MIN: usize = 16 * 1024;
const LARGEST_CHUNK_MAX: usize = 64 * 1024;

/// The number of slots for spare chunks.
const SPARE_CHUNKS: usize = 16;

/// The bytes of spare chunks that a queue keeps for its producer after the consumer has read them,
/// instead of freeing them: three quarters of its capacity, and at most `SPARE_BYTES_MAX`.
const SPARE_BYTES_MAX: usize = 256 * 1024;

/// The queue could not allocate a chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoMemory;

/// Why `RingProducer::push` returned the item.
#[derive(Debug, PartialEq, Eq)]
pub enum PushError<T> {
    /// The queue holds as many items as its capacity.
    Full(T),
    /// The queue could not allocate a chunk.
    NoMemory(T),
}

/// A queue that is to be split into a `RingProducer` and a `RingConsumer`.
pub struct Ring<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Ring<T> {
    /// Creates a queue that holds up to `capacity` items. It allocates nothing yet.
    pub fn new(capacity: usize) -> Self {
        assert!(
            mem::size_of::<T>() != 0,
            "zero-sized items are not supported"
        );
        Self {
            shared: Arc::new(Shared {
                capacity: AtomicUsize::new(capacity),
                pushed: AtomicUsize::new(0),
                popped: AtomicUsize::new(0),
                first: AtomicPtr::new(ptr::null_mut()),
                left_chunk: AtomicPtr::new(ptr::null_mut()),
                left_pos: AtomicUsize::new(0),
                spares: Default::default(),
                spare_bytes: AtomicUsize::new(0),
                producer_closed: AtomicBool::new(false),
                _marker: PhantomData,
            }),
        }
    }

    /// Returns a view of how many items the queue holds.
    pub fn status(&self) -> RingStatus<T> {
        RingStatus {
            shared: self.shared.clone(),
        }
    }

    /// Splits the queue into its producer and its consumer.
    pub fn split(self) -> (RingProducer<T>, RingConsumer<T>) {
        let producer = RingProducer {
            shared: self.shared.clone(),
            tail: ptr::null_mut(),
            tail_len: 0,
        };
        let consumer = RingConsumer {
            shared: self.shared,
            cursor: Cursor {
                chunk: ptr::null_mut(),
                pos: 0,
            },
        };
        (producer, consumer)
    }
}

/// A view of how many items a queue holds, for the threads that only want to know that, e.g., to
/// poll a channel, and that change how many it holds at most. It takes no lock of the ends, which
/// are held while the items are copied.
pub struct RingStatus<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for RingStatus<T> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T> RingStatus<T> {
    /// Returns the number of items in the queue. If the ends go on while it reads the counters,
    /// it may be more than the queue holds at any time, but it is never below zero, and it is
    /// zero only if the queue was empty when the call ended.
    pub fn len(&self) -> usize {
        // The number of the popped items is read first: it is never above the number of the
        // pushed items, which grows, so the difference cannot be negative.
        let popped = self.shared.popped.load(Ordering::Acquire);
        let pushed = self.shared.pushed.load(Ordering::Acquire);
        pushed.wrapping_sub(popped)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the largest number of items that the queue holds.
    pub fn capacity(&self) -> usize {
        self.shared.capacity.load(Ordering::Acquire)
    }

    /// Returns the number of items that the queue has room for.
    pub fn remaining(&self) -> usize {
        self.capacity().saturating_sub(self.len())
    }

    /// Changes the capacity, and returns the one that the queue had. A queue that holds more items
    /// than the new capacity keeps them, but it takes no more until it holds fewer.
    pub fn set_capacity(&self, capacity: usize) -> usize {
        self.shared.capacity.swap(capacity, Ordering::AcqRel)
    }
}

/// The part of the queue that the two ends share.
struct Shared<T> {
    capacity: AtomicUsize,
    /// The number of items that were pushed so far. Only the producer changes it.
    pushed: AtomicUsize,
    /// The number of items that were popped so far. Only the consumer changes it.
    popped: AtomicUsize,
    /// The first chunk. The producer sets it once, when it pushes the first item.
    first: AtomicPtr<Chunk<T>>,
    /// The chunk of the next item and the position of that item in it, as the consumer left
    /// them when it was dropped. If the consumer never read an item, `left_chunk` is null.
    left_chunk: AtomicPtr<Chunk<T>>,
    left_pos: AtomicUsize,
    /// The chunks that the consumer has read, kept for the producer to fill again, or null. They
    /// hold no item that is still to be dropped.
    spares: [AtomicPtr<Chunk<T>>; SPARE_CHUNKS],
    /// The bytes of the chunks in `spares`, which is a little more than what they hold when the
    /// producer is taking one.
    spare_bytes: AtomicUsize,
    /// Set when the producer pushes no more: the consumer frees the chunks that it has read.
    producer_closed: AtomicBool,
    _marker: PhantomData<T>,
}

impl<T> Shared<T> {
    /// Parks a chunk that the consumer has read in a slot for the producer, or frees it if the
    /// spares would hold more than their share of the memory or all the slots are taken. Only the
    /// consumer calls it.
    unsafe fn park(&self, chunk: *mut Chunk<T>) {
        let bytes = (*chunk).len * mem::size_of::<T>();
        if self.spare_bytes.load(Ordering::Relaxed) + bytes <= self.spare_budget() {
            for slot in self.spares.iter() {
                // Only the consumer fills a slot, so a slot that is empty stays empty until then
                if slot.load(Ordering::Relaxed).is_null() {
                    // Count the bytes first, as the producer may take the chunk at once
                    self.spare_bytes.fetch_add(bytes, Ordering::Relaxed);
                    slot.store(chunk, Ordering::SeqCst);
                    // If the producer is closed, it may have freed the spares before this one was
                    // parked, so free them again. One of the two sees the other: see the header.
                    if self.producer_closed.load(Ordering::SeqCst) {
                        self.free_spares();
                    }
                    return;
                }
            }
        }
        Chunk::free(chunk);
    }

    /// Takes a spare chunk of at least `len` items, or returns null if there is none. The spares
    /// that are too small are freed, since the producer would not use them again. Only the
    /// producer calls it.
    fn take_spare(&self, len: usize) -> *mut Chunk<T> {
        for slot in self.spares.iter() {
            if slot.load(Ordering::Relaxed).is_null() {
                continue;
            }
            let chunk = slot.swap(ptr::null_mut(), Ordering::Acquire);
            if chunk.is_null() {
                continue;
            }
            unsafe {
                self.spare_bytes
                    .fetch_sub((*chunk).len * mem::size_of::<T>(), Ordering::Relaxed);
                if (*chunk).len >= len {
                    (*chunk).next.store(ptr::null_mut(), Ordering::Relaxed);
                    return chunk;
                }
                Chunk::free(chunk);
            }
        }
        ptr::null_mut()
    }

    /// Frees the spare chunks. Both ends and the queue itself call it, so each chunk is taken out
    /// of its slot by an atomic swap.
    fn free_spares(&self) {
        for slot in self.spares.iter() {
            if slot.load(Ordering::SeqCst).is_null() {
                continue;
            }
            let chunk = slot.swap(ptr::null_mut(), Ordering::SeqCst);
            if !chunk.is_null() {
                unsafe {
                    self.spare_bytes
                        .fetch_sub((*chunk).len * mem::size_of::<T>(), Ordering::Relaxed);
                    Chunk::free(chunk);
                }
            }
        }
    }

    /// Returns the capacity of the queue in bytes.
    fn capacity_bytes(&self) -> usize {
        self.capacity
            .load(Ordering::Relaxed)
            .saturating_mul(mem::size_of::<T>())
    }

    /// Returns the size in bytes of the largest chunk: a sixteenth of the capacity of the queue,
    /// within the limits.
    fn largest_chunk_bytes(&self) -> usize {
        (self.capacity_bytes() / 16)
            .checked_next_power_of_two()
            .unwrap_or(LARGEST_CHUNK_MAX)
            .max(LARGEST_CHUNK_MIN)
            .min(LARGEST_CHUNK_MAX)
    }

    /// Returns the bytes of spare chunks that the consumer may park: three quarters of the
    /// capacity of the queue, and at most `SPARE_BYTES_MAX`. A queue that is larger can be behind
    /// by more chunks, and one that is idle after a transfer keeps no more.
    fn spare_budget(&self) -> usize {
        min(SPARE_BYTES_MAX, self.capacity_bytes() / 4 * 3)
    }
}

impl<T> Drop for Shared<T> {
    // Both ends are dropped: free the chunks and drop the items that were not popped.
    fn drop(&mut self) {
        let mut chunk = self.left_chunk.load(Ordering::Acquire);
        let mut pos = self.left_pos.load(Ordering::Acquire);
        if chunk.is_null() {
            chunk = self.first.load(Ordering::Acquire);
            pos = 0;
        }
        let pushed = self.pushed.load(Ordering::Acquire);
        let mut queued = pushed.wrapping_sub(self.popped.load(Ordering::Acquire));
        while !chunk.is_null() {
            unsafe {
                if mem::needs_drop::<T>() && queued > 0 {
                    let count = min(queued, (*chunk).len - pos);
                    for i in 0..count {
                        ptr::drop_in_place(Chunk::items(chunk).add(pos + i));
                    }
                    queued -= count;
                }
                let next = (*chunk).next.load(Ordering::Acquire);
                Chunk::free(chunk);
                chunk = next;
                pos = 0;
            }
        }
    }
}

/// A piece of the queue: a header followed by room for `len` items.
#[repr(C)]
struct Chunk<T> {
    /// The chunk that follows, once the producer has made it.
    next: AtomicPtr<Chunk<T>>,
    len: usize,
    _marker: PhantomData<T>,
}

impl<T> Chunk<T> {
    /// The offset of the items from the start of the chunk.
    const ITEMS_OFFSET: usize =
        (mem::size_of::<Self>() + mem::align_of::<T>() - 1) & !(mem::align_of::<T>() - 1);

    fn layout(len: usize) -> Layout {
        let size = len * mem::size_of::<T>() + Self::ITEMS_OFFSET;
        let align = max(mem::align_of::<Self>(), mem::align_of::<T>());
        Layout::from_size_align(size, align).unwrap()
    }

    /// Allocates a chunk of `len` items, or returns null if there is no memory.
    fn alloc(len: usize) -> *mut Self {
        unsafe {
            let chunk = alloc::alloc(Self::layout(len)) as *mut Self;
            if !chunk.is_null() {
                ptr::write(
                    chunk,
                    Self {
                        next: AtomicPtr::new(ptr::null_mut()),
                        len,
                        _marker: PhantomData,
                    },
                );
            }
            chunk
        }
    }

    /// Frees a chunk, which must not hold any item that is still to be dropped.
    unsafe fn free(chunk: *mut Self) {
        let layout = Self::layout((*chunk).len);
        alloc::dealloc(chunk as *mut u8, layout);
    }

    unsafe fn items(chunk: *mut Self) -> *mut T {
        (chunk as *mut u8).add(Self::ITEMS_OFFSET) as *mut T
    }
}

/// The writable end of a queue.
pub struct RingProducer<T> {
    shared: Arc<Shared<T>>,
    /// The chunk that is being filled, which is null before the first push.
    tail: *mut Chunk<T>,
    /// The number of items in `tail`.
    tail_len: usize,
}

// The chunks are only reachable through the producer and the consumer, which move items from one
// thread to the other.
unsafe impl<T: Send> Send for RingProducer<T> {}

impl<T> RingProducer<T> {
    /// Returns the largest number of items that the queue holds.
    pub fn capacity(&self) -> usize {
        self.shared.capacity.load(Ordering::Acquire)
    }

    /// Returns the number of items in the queue.
    pub fn len(&self) -> usize {
        let pushed = self.shared.pushed.load(Ordering::Relaxed);
        pushed.wrapping_sub(self.shared.popped.load(Ordering::Acquire))
    }

    /// Returns the number of items that the queue has room for.
    pub fn remaining(&self) -> usize {
        self.capacity().saturating_sub(self.len())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_full(&self) -> bool {
        self.remaining() == 0
    }

    /// Returns the number of items that were pushed so far.
    pub fn total_pushed(&self) -> usize {
        self.shared.pushed.load(Ordering::Relaxed)
    }

    /// Tells the queue that the producer pushes no more, which lets the consumer free the chunks
    /// that it reads instead of parking them. The spare chunks are freed now. The producer may
    /// push after this, but it gets no more spares.
    pub fn close(&mut self) {
        self.shared.producer_closed.store(true, Ordering::SeqCst);
        self.shared.free_spares();
    }

    /// Appends an item to the queue.
    pub fn push(&mut self, item: T) -> Result<(), PushError<T>> {
        if self.remaining() == 0 {
            return Err(PushError::Full(item));
        }
        if !self.make_room(1) {
            return Err(PushError::NoMemory(item));
        }
        unsafe {
            ptr::write(Chunk::items(self.tail).add(self.tail_len), item);
        }
        self.tail_len += 1;
        self.publish(1);
        Ok(())
    }

    /// Makes sure that there is room for an item in the last chunk, by appending a chunk if that
    /// is full. `wanted` is the number of items that the producer is going to push in all.
    /// Returns false if there is no memory.
    fn make_room(&mut self, wanted: usize) -> bool {
        if !self.tail.is_null() && self.tail_len < unsafe { (*self.tail).len } {
            return true;
        }

        let len = self.chunk_len(wanted);
        let mut chunk = self.shared.take_spare(len);
        if chunk.is_null() {
            chunk = Chunk::alloc(len);
        }
        if chunk.is_null() {
            return false;
        }
        // The consumer must find the chunk when it sees the items in it
        let link = if self.tail.is_null() {
            &self.shared.first
        } else {
            unsafe { &(*self.tail).next }
        };
        link.store(chunk, Ordering::Release);
        self.tail = chunk;
        self.tail_len = 0;
        true
    }

    /// Returns the number of items for a new chunk. It is large enough for what the producer
    /// wants to write and for what is queued already, but not for more than the capacity.
    fn chunk_len(&self, wanted: usize) -> usize {
        let item_size = mem::size_of::<T>();
        let min_len = max(1, MIN_CHUNK_BYTES / item_size);
        let max_len = max(min_len, self.shared.largest_chunk_bytes() / item_size);
        let capacity_len = self
            .capacity()
            .checked_next_power_of_two()
            .unwrap_or(max_len);
        let max_len = min(max_len, max(min_len, capacity_len));
        max(wanted, self.len())
            .min(max_len)
            .next_power_of_two()
            .max(min_len)
            .min(max_len)
    }

    /// Makes `count` items that were written visible to the consumer.
    fn publish(&mut self, count: usize) {
        let pushed = self.shared.pushed.load(Ordering::Relaxed);
        self.shared
            .pushed
            .store(pushed.wrapping_add(count), Ordering::Release);
    }
}

impl<T> Drop for RingProducer<T> {
    fn drop(&mut self) {
        self.close();
    }
}

impl<T: Copy> RingProducer<T> {
    /// Appends as many of the items as there is room for, and returns their number. It fails if
    /// there is room, but no memory for the first item.
    pub fn push_slice(&mut self, items: &[T]) -> Result<usize, NoMemory> {
        let count = min(items.len(), self.remaining());
        let mut done = 0;
        while done < count {
            if !self.make_room(count - done) {
                break;
            }
            // Fill the last chunk, then make the next one
            let tail_room = unsafe { (*self.tail).len } - self.tail_len;
            let n = min(tail_room, count - done);
            unsafe {
                ptr::copy_nonoverlapping(
                    items.as_ptr().add(done),
                    Chunk::items(self.tail).add(self.tail_len),
                    n,
                );
            }
            self.tail_len += n;
            done += n;
        }
        if done == 0 && count > 0 {
            return Err(NoMemory);
        }
        if done > 0 {
            self.publish(done);
        }
        Ok(done)
    }
}

/// The position of the next item of the consumer.
#[derive(Clone, Copy)]
struct Cursor<T> {
    /// The chunk of the next item, or null before the first item is read. If the consumer
    /// has read the whole chunk, the next item is in the chunk after it.
    chunk: *mut Chunk<T>,
    /// The number of items of the chunk that were read.
    pos: usize,
}

impl<T> Cursor<T> {
    /// Returns the items that follow the position in its chunk, as a pointer and a number that is
    /// at most `max`. If the chunk is used up, moves to the next chunk, which has to exist because
    /// the caller knows that more items are queued. A chunk that is left is given up if `park`.
    unsafe fn run(&mut self, shared: &Shared<T>, max: usize, park: bool) -> (*const T, usize) {
        if self.chunk.is_null() || self.pos == (*self.chunk).len {
            let next = if self.chunk.is_null() {
                shared.first.load(Ordering::Acquire)
            } else {
                (*self.chunk).next.load(Ordering::Acquire)
            };
            debug_assert!(!next.is_null());
            if park && !self.chunk.is_null() {
                shared.park(self.chunk);
            }
            self.chunk = next;
            self.pos = 0;
        }
        let count = min(max, (*self.chunk).len - self.pos);
        (Chunk::items(self.chunk).add(self.pos), count)
    }
}

/// The readable end of a queue.
pub struct RingConsumer<T> {
    shared: Arc<Shared<T>>,
    cursor: Cursor<T>,
}

unsafe impl<T: Send> Send for RingConsumer<T> {}

impl<T> Drop for RingConsumer<T> {
    fn drop(&mut self) {
        // The producer cannot use the spares for a consumer that is gone
        self.shared.free_spares();
        // Leave the position to the last end that is dropped, which frees the chunks
        self.shared
            .left_pos
            .store(self.cursor.pos, Ordering::Release);
        self.shared
            .left_chunk
            .store(self.cursor.chunk, Ordering::Release);
    }
}

impl<T> RingConsumer<T> {
    pub fn capacity(&self) -> usize {
        self.shared.capacity.load(Ordering::Acquire)
    }

    /// Returns the number of items in the queue.
    pub fn len(&self) -> usize {
        let pushed = self.shared.pushed.load(Ordering::Acquire);
        pushed.wrapping_sub(self.shared.popped.load(Ordering::Relaxed))
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_full(&self) -> bool {
        self.len() >= self.capacity()
    }

    /// Returns the number of items that were popped so far.
    pub fn total_popped(&self) -> usize {
        self.shared.popped.load(Ordering::Relaxed)
    }

    /// Removes the first item of the queue.
    pub fn pop(&mut self) -> Option<T> {
        if self.is_empty() {
            return None;
        }
        let item = unsafe {
            let (run, _) = self.cursor.run(&self.shared, 1, true);
            ptr::read(run)
        };
        self.cursor.pos += 1;
        self.finish(1);
        Some(item)
    }

    /// Makes `count` items that were read free, for the producer and for the heap.
    fn finish(&mut self, count: usize) {
        // The next chunk exists if the producer has gone on: give up the one that is used up
        unsafe {
            let chunk = self.cursor.chunk;
            if self.cursor.pos == (*chunk).len {
                let next = (*chunk).next.load(Ordering::Acquire);
                if !next.is_null() {
                    self.shared.park(chunk);
                    self.cursor.chunk = next;
                    self.cursor.pos = 0;
                }
            }
        }
        let popped = self.shared.popped.load(Ordering::Relaxed);
        self.shared
            .popped
            .store(popped.wrapping_add(count), Ordering::Release);
    }
}

impl<T: Copy> RingConsumer<T> {
    /// Removes the first items of the queue as long as there is room for them in `items`, and
    /// returns their number.
    pub fn pop_slice(&mut self, items: &mut [T]) -> usize {
        let count = min(items.len(), self.len());
        let mut done = 0;
        while done < count {
            unsafe {
                let (run, n) = self.cursor.run(&self.shared, count - done, true);
                ptr::copy_nonoverlapping(run, items.as_mut_ptr().add(done), n);
                self.cursor.pos += n;
                done += n;
            }
        }
        if done > 0 {
            self.finish(done);
        }
        done
    }

    /// Copies the first items of the queue as long as there is room for them in `items`, without
    /// removing them, and returns their number.
    pub fn peek_slice(&self, items: &mut [T]) -> usize {
        self.peek_slice_at(0, items)
    }

    /// Does the same for the items that follow the first `skip` items of the queue.
    pub fn peek_slice_at(&self, skip: usize, items: &mut [T]) -> usize {
        let count = min(items.len(), self.len().saturating_sub(skip));
        if count == 0 {
            return 0;
        }
        let mut cursor = self.cursor;
        let mut skipped = 0;
        while skipped < skip {
            // The items that are skipped are queued, as there are more behind them
            unsafe {
                let (_, n) = cursor.run(&self.shared, skip - skipped, false);
                cursor.pos += n;
                skipped += n;
            }
        }
        let mut done = 0;
        while done < count {
            unsafe {
                let (run, n) = cursor.run(&self.shared, count - done, false);
                ptr::copy_nonoverlapping(run, items.as_mut_ptr().add(done), n);
                cursor.pos += n;
                done += n;
            }
        }
        done
    }
}

#[cfg(test)]
#[path = "ring/tests.rs"]
mod tests;
