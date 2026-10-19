// Tests of the queue. They need a global allocator that counts the live bytes and injects
// allocation failures. It counts them per test, see the header of ring.rs.
use super::*;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

// What the allocator knows about the allocations of one test: the bytes that are live, the number
// of allocations, and a budget of live bytes above which allocations fail
struct Account {
    live: AtomicUsize,
    allocs: AtomicUsize,
    budget: AtomicUsize,
    failed: AtomicUsize,
}

// The tests that a run can have. Each of them takes an account of its own
const ACCOUNTS: usize = 64;

const NEW_ACCOUNT: Account = Account {
    live: AtomicUsize::new(0),
    allocs: AtomicUsize::new(0),
    budget: AtomicUsize::new(usize::MAX),
    failed: AtomicUsize::new(0),
};
static ACCOUNT_TABLE: [Account; ACCOUNTS] = [NEW_ACCOUNT; ACCOUNTS];
static NEXT_ACCOUNT: AtomicUsize = AtomicUsize::new(1);

thread_local! {
    // The account that the allocations of this thread are counted in. The allocations of a thread
    // that belongs to no test (the test harness) go to the account 0, which nobody reads.
    static ACCOUNT: Cell<usize> = const { Cell::new(0) };
}

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let account = account();
        if layout.size() >= 400
            && account.live.load(Ordering::Relaxed) + layout.size()
                > account.budget.load(Ordering::Relaxed)
        {
            account.failed.fetch_add(1, Ordering::Relaxed);
            return std::ptr::null_mut();
        }
        let p = System.alloc(layout);
        if !p.is_null() {
            account.live.fetch_add(layout.size(), Ordering::Relaxed);
            account.allocs.fetch_add(1, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        System.dealloc(p, layout);
        account().live.fetch_sub(layout.size(), Ordering::Relaxed);
    }
}

#[global_allocator]
static A: Counting = Counting;

fn account() -> &'static Account {
    &ACCOUNT_TABLE[ACCOUNT.with(|a| a.get())]
}

// Every test calls it first: from here on, its allocations and those of the threads that it starts
// with `spawn` are counted by themselves, whatever the other tests and the harness do
fn track() {
    let n = NEXT_ACCOUNT.fetch_add(1, Ordering::Relaxed);
    assert!(n < ACCOUNTS, "more tests than accounts");
    ACCOUNT.with(|a| a.set(n));
}

// A thread that counts its allocations in the account of the test that started it
struct Handle<R> {
    handle: thread::JoinHandle<R>,
    account: usize,
}

// Starts a thread of the test. The standard library allocates for a thread in the parent and frees
// in the child, or the other way round, so the parent and the child work in the account 0 while
// they do that, which nobody reads. The closure runs in the account of the test.
fn spawn<F, R>(f: F) -> Handle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let account = ACCOUNT.with(|a| a.replace(0));
    let handle = thread::spawn(move || {
        ACCOUNT.with(|a| a.set(account));
        let result = f();
        ACCOUNT.with(|a| a.set(0));
        result
    });
    ACCOUNT.with(|a| a.set(account));
    Handle { handle, account }
}

impl<R> Handle<R> {
    fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }

    fn join(self) -> thread::Result<R> {
        ACCOUNT.with(|a| a.set(0));
        let result = self.handle.join();
        ACCOUNT.with(|a| a.set(self.account));
        result
    }
}

// The bytes of the test that are live
fn live() -> usize {
    account().live.load(Ordering::Relaxed)
}

// The allocations that the test has made
fn allocs() -> usize {
    account().allocs.load(Ordering::Relaxed)
}

// The allocations of the test that failed because of the budget
fn failures() -> usize {
    account().failed.load(Ordering::Relaxed)
}

// Allocations of 400 bytes or more fail while the live bytes of the test would exceed `bytes`
fn set_budget(bytes: usize) {
    account().budget.store(bytes, Ordering::SeqCst);
}

// The header of a chunk, and the bytes of the queue besides its chunks
const HEADER: usize = 16;
const QUEUE: usize = 512;

// The size of the chunks of a queue that streams data
fn largest_chunk(ring: &Ring<u8>) -> usize {
    ring.shared.largest_chunk_bytes()
}

// What a queue holds when it is idle after a transfer, at most: its last chunk and the spares
fn idle_max(ring: &Ring<u8>) -> usize {
    largest_chunk(ring) + HEADER + ring.shared.spare_budget() + SPARE_CHUNKS * HEADER + QUEUE
}

#[test]
fn the_chunks_and_the_spares_follow_the_capacity() {
    let sizes = |capacity: usize| {
        let ring = Ring::<u8>::new(capacity);
        (
            ring.shared.largest_chunk_bytes(),
            ring.shared.spare_budget(),
        )
    };
    // The largest chunk is a sixteenth of the capacity between 16 and 64 KiB, the spares are three
    // quarters of it, and at most 256 KiB
    assert_eq!(sizes(0), (16 * 1024, 0));
    assert_eq!(sizes(1000), (16 * 1024, 750));
    assert_eq!(sizes(208 * 1024), (16 * 1024, 156 * 1024));
    assert_eq!(sizes(256 * 1024), (16 * 1024, 192 * 1024));
    assert_eq!(sizes(512 * 1024), (32 * 1024, 256 * 1024));
    assert_eq!(sizes(1 << 20), (64 * 1024, 256 * 1024));
    assert_eq!(sizes(1 << 40), (64 * 1024, 256 * 1024));
    assert_eq!(sizes(usize::MAX), (64 * 1024, 256 * 1024));
    // The capacity is in items
    let ring = Ring::<u64>::new(208 * 1024);
    assert_eq!(ring.shared.largest_chunk_bytes(), 64 * 1024);
    // And it can change
    let (p, c) = Ring::<u8>::new(1 << 20).split();
    p.set_capacity(100_000);
    assert_eq!(p.shared.largest_chunk_bytes(), 16 * 1024);
    assert_eq!(p.shared.spare_budget(), 75_000);
    drop(c);
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

fn byte(offset: usize) -> u8 {
    (offset as u8)
        .wrapping_mul(31)
        .wrapping_add((offset >> 8) as u8)
}

#[test]
fn idle_queue_allocates_no_chunk() {
    track();
    let before = live();
    let (p, c) = Ring::<u8>::new(1 << 20).split();
    let idle = live() - before;
    assert!(idle < 256, "an idle queue holds {} bytes", idle);
    assert_eq!(p.capacity(), 1 << 20);
    assert_eq!(c.capacity(), 1 << 20);
    assert!(p.is_empty() && c.is_empty() && !p.is_full());
    drop(p);
    drop(c);
    assert_eq!(live(), before);
}

#[test]
fn memory_follows_the_content() {
    track();
    let data: Vec<u8> = (0..1 << 20).map(byte).collect();
    let mut out = vec![0u8; 300 * 1024];
    let before = live();
    let ring = Ring::<u8>::new(208 * 1024);
    let idle_max = idle_max(&ring);
    let (mut p, mut c) = ring.split();
    // One small message: one small chunk
    assert_eq!(p.push_slice(b"hello"), Ok(5));
    let one = live() - before;
    assert!(one < 1024 + 256, "one message holds {} bytes", one);
    let mut buf = [0u8; 16];
    assert_eq!(c.pop_slice(&mut buf), 5);
    assert_eq!(&buf[..5], b"hello");
    assert!(c.is_empty());
    // The last chunk stays, since the producer may append to it
    assert!(live() - before < 1024 + 256);
    // Fill it up: the memory is about the capacity, the chunks are large
    let n = p.push_slice(&data).unwrap();
    assert_eq!(n, 208 * 1024);
    assert!(p.is_full());
    let full = live() - before;
    assert!(
        full >= 208 * 1024 && full < 208 * 1024 + 2 * 16384 + 4096,
        "full: {}",
        full
    );
    assert_eq!(p.push_slice(&data), Ok(0));
    // Drain: the last chunk stays, and so do the spares, the others are freed
    assert_eq!(c.pop_slice(&mut out), 208 * 1024);
    assert!(out[..208 * 1024]
        .iter()
        .enumerate()
        .all(|(i, b)| *b == byte(i)));
    let drained = live() - before;
    assert!(
        drained > 8 * 16 * 1024 && drained <= idle_max,
        "drained: {}",
        drained
    );
    assert!(!p.is_full() && c.is_empty());
    // The producer that is done gives the spares back
    p.close();
    let closed = live() - before;
    assert!(closed <= 16 * 1024 + HEADER + QUEUE, "closed: {}", closed);
    drop(c);
    drop(p);
    assert_eq!(live(), before);
}

// A stream whose chunks are read before the producer needs the next ones takes the spares
// instead of calling the allocator
#[test]
fn a_stream_reuses_its_chunks() {
    track();
    let data: Vec<u8> = (0..64 * 1024).map(byte).collect();
    let mut out = vec![0u8; 64 * 1024];
    // The capacity of a unix socket, which has chunks of 16 KiB, and that of a pipe, 64 KiB
    for &capacity in &[208 * 1024, 1 << 20] {
        let before = live();
        let (mut p, mut c) = Ring::<u8>::new(capacity).split();
        // The consumer is behind by 0, 1 and 2 writes, which is up to 8 chunks that are queued
        for lag in 0..3 {
            for _ in 0..lag {
                assert_eq!(p.push_slice(&data), Ok(data.len()));
            }
            for _ in 0..20 {
                assert_eq!(p.push_slice(&data), Ok(data.len()));
                assert_eq!(c.pop_slice(&mut out), out.len());
                assert_eq!(out, data);
            }
            let count = allocs();
            for _ in 0..1000 {
                assert_eq!(p.push_slice(&data), Ok(data.len()));
                assert_eq!(c.pop_slice(&mut out), out.len());
            }
            assert_eq!(allocs() - count, 0, "capacity {}, lag {}", capacity, lag);
            for _ in 0..lag {
                assert_eq!(c.pop_slice(&mut out), out.len());
            }
        }
        drop(c);
        drop(p);
        assert_eq!(live(), before);
    }
}

// A consumer that is behind by more than the spares frees the chunks that do not fit, and the
// producer allocates them again: what an idle queue keeps is bounded
#[test]
fn the_spares_are_bounded() {
    track();
    // (capacity, size of the chunks, number of spares that fit): 208 KiB / 16 KiB has 13 chunks,
    // 156 KiB of spares are 9 of them, and 1 MiB / 64 KiB has 16 chunks, 256 KiB are 4 of them
    for &(capacity, chunk_size, fit) in &[(208 * 1024, 16 * 1024, 9), (1 << 20, 64 * 1024, 4)] {
        let data = vec![0xa5u8; capacity];
        let mut out = vec![0u8; capacity];
        let ring = Ring::<u8>::new(capacity);
        assert_eq!(ring.shared.largest_chunk_bytes(), chunk_size);
        let (mut p, mut c) = ring.split();
        let base = live();
        let chunks = capacity / chunk_size;
        for round in 0..5 {
            let count = allocs();
            assert_eq!(p.push_slice(&data), Ok(data.len()));
            if round > 0 {
                // The chunks that were spare are used again, the others are allocated
                assert_eq!(allocs() - count, chunks - fit, "round {}", round);
            }
            assert_eq!(c.pop_slice(&mut out), data.len());
            assert_eq!(out, data);
            // The last chunk and the spares are kept, and no more
            assert_eq!(
                live() - base,
                (1 + fit) * (chunk_size + HEADER),
                "capacity {}, round {}",
                capacity,
                round
            );
        }
        drop(c);
        drop(p);
    }
}

// Streams that carry 8 chunks, and then give back what the queue keeps
fn stream_then_idle(close_first: bool, drop_first: bool) {
    // A queue of 1 MiB has chunks of 64 KiB
    let chunk = 64 * 1024;
    let data = vec![0x11u8; 8 * chunk];
    let mut out = vec![0u8; 8 * chunk];
    let before = live();
    let ring = Ring::<u8>::new(1 << 20);
    assert_eq!(largest_chunk(&ring), chunk);
    let idle_max = idle_max(&ring);
    let (mut p, mut c) = ring.split();
    let base = live();
    for _ in 0..4 {
        assert_eq!(p.push_slice(&data), Ok(data.len()));
        assert_eq!(c.pop_slice(&mut out), data.len());
    }
    // The last chunk and 4 spares
    assert_eq!(live() - base, 5 * (chunk + HEADER));
    // More data is queued when the producer is done
    assert_eq!(p.push_slice(&data), Ok(data.len()));
    if close_first {
        // The spares are freed at once, and the consumer frees what it reads
        p.close();
        assert!(live() - before <= 8 * (chunk + HEADER) + 2 * (chunk + HEADER) + QUEUE);
        assert_eq!(c.pop_slice(&mut out), data.len());
        assert!(
            live() - before <= chunk + HEADER + QUEUE,
            "{}",
            live() - before
        );
        // Nothing is parked any more
        assert_eq!(p.push_slice(&data), Ok(data.len()));
        assert_eq!(c.pop_slice(&mut out), data.len());
        assert!(
            live() - before <= chunk + HEADER + QUEUE,
            "{}",
            live() - before
        );
        assert_eq!(p.push_slice(&data), Ok(data.len()));
    } else {
        assert_eq!(c.pop_slice(&mut out), data.len());
        assert!(live() - before <= idle_max);
    }
    if drop_first {
        // The consumer is gone, so the spares are no use
        drop(c);
        assert!(live() - before <= 8 * (chunk + HEADER) + 2 * (chunk + HEADER) + QUEUE);
        drop(p);
    } else {
        drop(p);
        drop(c);
    }
    assert_eq!(live(), before);
}

#[test]
fn the_spares_are_given_back() {
    track();
    for &close_first in &[false, true] {
        for &drop_first in &[false, true] {
            stream_then_idle(close_first, drop_first);
        }
    }
}

#[test]
fn a_dropped_end_gives_the_spares_back() {
    track();
    let chunk = 64 * 1024;
    let data = vec![0x22u8; 8 * chunk];
    let mut out = vec![0u8; 8 * chunk];
    for &producer_first in &[false, true] {
        let before = live();
        let ring = Ring::<u8>::new(1 << 20);
        assert_eq!(largest_chunk(&ring), chunk);
        let idle_max = idle_max(&ring);
        let (mut p, mut c) = ring.split();
        for _ in 0..3 {
            assert_eq!(p.push_slice(&data), Ok(data.len()));
            assert_eq!(c.pop_slice(&mut out), data.len());
        }
        let idle = live() - before;
        assert!(idle > 4 * chunk && idle <= idle_max);
        if producer_first {
            drop(p);
            // The consumer may still read, but nothing is parked
            assert!(live() - before <= chunk + HEADER + QUEUE);
            drop(c);
        } else {
            drop(c);
            assert!(live() - before <= chunk + HEADER + QUEUE);
            drop(p);
        }
        assert_eq!(live(), before);
    }
}

// A spare that is too small for the chunk that the producer needs is freed. One that is larger
// than it needs is used.
#[test]
fn spares_of_any_size_are_used_or_freed() {
    track();
    let mut out = vec![0u8; 100_000];
    let (mut p, mut c) = Ring::<u8>::new(1 << 20).split();
    let base = live();
    // A chunk of 512 bytes, read to its end, with the next chunk in place: a spare
    assert_eq!(p.push_slice(&[1u8; 512]), Ok(512));
    assert_eq!(p.push_slice(&[2u8; 1]), Ok(1));
    assert_eq!(c.pop_slice(&mut out[..512]), 512);
    assert_eq!(live() - base, 2 * (512 + HEADER));
    // The next chunk has 32 KiB: the spare is too small, and is freed
    let count = allocs();
    assert_eq!(p.push_slice(&[3u8; 20_000]), Ok(20_000));
    // The chunk of the 1 byte and a chunk of 32 KiB
    assert_eq!(live() - base, (512 + HEADER) + (32768 + HEADER));
    assert_eq!(allocs() - count, 1);
    assert_eq!(c.pop_slice(&mut out), 20_001);
    assert_eq!(out[0], 2);
    assert!(out[1..20_001].iter().all(|b| *b == 3));
    // The big chunk that was read is a spare now, and serves small messages
    let count = allocs();
    let idle = live() - base;
    for i in 0..50u8 {
        assert_eq!(p.push_slice(&[i; 300]), Ok(300));
        assert_eq!(c.pop_slice(&mut out), 300);
    }
    assert!(live() - base <= idle);
    assert!(allocs() - count <= 1);
    drop(c);
    drop(p);
}

// Every slot for spare chunks is used, taken and freed, the last one too
#[test]
fn all_the_spare_slots_are_used() {
    track();
    let before = live();
    let ring = Ring::<u8>::new(1 << 20);
    let shared = ring.shared.clone();
    let base = live();
    let parked = |n: usize| {
        let slots = shared.spares.iter();
        let used = slots
            .filter(|s| !s.load(Ordering::SeqCst).is_null())
            .count();
        assert_eq!(used, n);
        assert_eq!(
            shared.spare_bytes.load(Ordering::SeqCst),
            n * MIN_CHUNK_BYTES
        );
    };
    // The consumer parks small chunks, which fit the budget as many times as there are slots
    for i in 0..SPARE_CHUNKS {
        unsafe { shared.park(Chunk::<u8>::alloc(MIN_CHUNK_BYTES)) };
        parked(i + 1);
        assert!(!shared.spares[i].load(Ordering::SeqCst).is_null());
    }
    assert_eq!(live() - base, SPARE_CHUNKS * (MIN_CHUNK_BYTES + HEADER));
    // No slot is empty: another chunk is freed
    unsafe { shared.park(Chunk::<u8>::alloc(MIN_CHUNK_BYTES)) };
    parked(SPARE_CHUNKS);
    assert_eq!(live() - base, SPARE_CHUNKS * (MIN_CHUNK_BYTES + HEADER));
    // The producer takes them all, one at a time
    for i in 0..SPARE_CHUNKS {
        let chunk = shared.take_spare(MIN_CHUNK_BYTES);
        assert!(!chunk.is_null());
        unsafe { Chunk::free(chunk) };
        parked(SPARE_CHUNKS - 1 - i);
    }
    assert!(shared.take_spare(1).is_null());
    assert_eq!(live(), base);
    // And closing the producer frees all of them
    let (mut p, c) = ring.split();
    for _ in 0..SPARE_CHUNKS {
        unsafe { shared.park(Chunk::<u8>::alloc(MIN_CHUNK_BYTES)) };
    }
    parked(SPARE_CHUNKS);
    p.close();
    parked(0);
    assert_eq!(live(), base);
    drop(p);
    drop(c);
    drop(shared);
    assert_eq!(live(), before);
}

// A spare that the producer takes ends the list of chunks, for it is linked as the last chunk:
// the consumer must not follow a pointer from the time when the chunk was in the middle of it
#[test]
fn a_spare_ends_the_list_when_it_is_taken() {
    track();
    let before = live();
    let (mut p, mut c) = Ring::<u8>::new(1 << 20).split();
    let mut out = [0u8; MIN_CHUNK_BYTES];
    // The first chunk is full and the second one holds a byte
    assert_eq!(p.push_slice(&[1u8; MIN_CHUNK_BYTES]), Ok(MIN_CHUNK_BYTES));
    assert_eq!(p.push_slice(&[2u8; 1]), Ok(1));
    // The consumer reads the first chunk and parks it, and it still points to the second one
    assert_eq!(c.pop_slice(&mut out), MIN_CHUNK_BYTES);
    let spare = p.shared.spares[0].load(Ordering::SeqCst);
    assert!(!spare.is_null());
    assert!(!unsafe { (*spare).next.load(Ordering::SeqCst) }.is_null());
    let taken = p.shared.take_spare(1);
    assert_eq!(taken, spare);
    assert!(unsafe { (*taken).next.load(Ordering::SeqCst) }.is_null());
    unsafe { Chunk::free(taken) };
    drop(p);
    drop(c);
    assert_eq!(live(), before);
}

// The chunks of a queue grow with what is queued, so that a stream of small writes, whose consumer
// is behind, does not take a chunk of the smallest size for each 512 bytes
#[test]
fn the_chunks_grow_with_the_queue() {
    track();
    let before = live();
    let (mut p, mut c) = Ring::<u8>::new(1 << 20).split();
    let count = allocs();
    // 64 KiB in writes of one byte: chunks of 512, 512, 1024, 2048, ... 32 KiB, eight of them,
    // where chunks of 512 bytes would take 128
    for i in 0..64 * 1024 {
        assert_eq!(p.push_slice(&[byte(i)]), Ok(1));
    }
    let chunks = allocs() - count;
    assert!(chunks <= 10, "{} chunks hold 64 KiB", chunks);
    let mut out = vec![0u8; 64 * 1024];
    assert_eq!(c.pop_slice(&mut out), 64 * 1024);
    assert!(out.iter().enumerate().all(|(i, b)| *b == byte(i)));
    drop(out);
    drop(p);
    drop(c);
    assert_eq!(live(), before);
}

#[test]
fn counters_and_lengths() {
    track();
    let (mut p, mut c) = Ring::<u8>::new(1000).split();
    assert_eq!(p.total_pushed(), 0);
    assert_eq!(c.total_popped(), 0);
    assert_eq!(p.push_slice(&[1u8; 600]), Ok(600));
    assert_eq!(p.len(), 600);
    assert_eq!(c.len(), 600);
    assert_eq!(p.remaining(), 400);
    assert_eq!(p.push_slice(&[2u8; 600]), Ok(400));
    assert!(p.is_full() && c.is_full());
    assert_eq!(p.total_pushed(), 1000);
    let mut buf = [0u8; 300];
    assert_eq!(c.pop_slice(&mut buf), 300);
    assert_eq!(c.total_popped(), 300);
    assert_eq!(p.len(), 700);
    assert!(!p.is_full());
    assert_eq!(p.push_slice(&[3u8; 1000]), Ok(300));
    // everything in order
    let mut all = vec![0u8; 2000];
    assert_eq!(c.pop_slice(&mut all), 1000);
    assert!(all[..300].iter().all(|b| *b == 1));
    assert!(all[300..700].iter().all(|b| *b == 2));
    assert!(all[700..1000].iter().all(|b| *b == 3));
    assert_eq!(c.total_popped(), 1300);
    assert_eq!(p.total_pushed(), 1300);
    assert_eq!(c.pop_slice(&mut all), 0);
}

#[test]
fn zero_capacity_takes_nothing() {
    track();
    let before = live();
    let (mut p, mut c) = Ring::<u32>::new(0).split();
    assert!(p.is_full() && c.is_empty());
    assert_eq!(p.push(1), Err(PushError::Full(1)));
    assert_eq!(p.push_slice(&[1, 2]), Ok(0));
    assert_eq!(c.pop(), None);
    drop(p);
    drop(c);
    assert_eq!(live(), before);
}

#[test]
fn set_capacity_changes_the_limit() {
    track();
    let (mut p, mut c) = Ring::<u8>::new(100).split();
    assert_eq!(p.push_slice(&[7u8; 300]), Ok(100));
    p.set_capacity(150);
    assert_eq!(c.capacity(), 150);
    assert_eq!(p.push_slice(&[8u8; 300]), Ok(50));
    // shrinking below the content keeps the items, and takes no more until they are fewer
    p.set_capacity(10);
    assert_eq!(p.len(), 150);
    assert!(p.is_full());
    assert_eq!(p.push_slice(&[9u8; 5]), Ok(0));
    let mut buf = [0u8; 145];
    assert_eq!(c.pop_slice(&mut buf), 145);
    assert!(!p.is_full());
    assert_eq!(p.push_slice(&[9u8; 5]), Ok(5));
    assert!(p.is_full());
}

#[test]
fn items_in_order_across_chunks() {
    track();
    let before = live();
    let (mut p, mut c) = Ring::<u32>::new(100_000).split();
    let mut next_in = 0u32;
    let mut next_out = 0u32;
    let mut rng = Rng(12345);
    for _ in 0..20_000 {
        if rng.below(2) == 0 {
            let n = 1 + rng.below(3000);
            let items: Vec<u32> = (0..n as u32).map(|i| next_in + i).collect();
            let pushed = p.push_slice(&items).unwrap();
            next_in += pushed as u32;
        } else if rng.below(4) == 0 {
            if p.push(next_in).is_ok() {
                next_in += 1;
            }
        } else if rng.below(3) == 0 {
            if let Some(v) = c.pop() {
                assert_eq!(v, next_out);
                next_out += 1;
            }
        } else {
            let n = 1 + rng.below(3000);
            let mut items = vec![0u32; n];
            let mut peeked = vec![0u32; n];
            let k = c.peek_slice(&mut peeked);
            let popped = c.pop_slice(&mut items);
            assert_eq!(k, popped);
            assert_eq!(peeked[..k], items[..popped]);
            for v in &items[..popped] {
                assert_eq!(*v, next_out);
                next_out += 1;
            }
        }
        assert_eq!((next_in - next_out) as usize, p.len());
    }
    drop(p);
    drop(c);
    assert_eq!(live(), before);
}

// Peeking past the first items, over the boundaries of the chunks
#[test]
fn peek_slice_at_skips_items() {
    track();
    let before = live();
    let (mut p, mut c) = Ring::<u32>::new(100_000).split();
    let mut next = 0u32;
    // Chunks of different sizes, some of them read
    for n in [1usize, 3, 200, 129, 4000, 17, 6000, 1] {
        let items: Vec<u32> = (0..n as u32).map(|i| next + i).collect();
        assert_eq!(p.push_slice(&items), Ok(n));
        next += n as u32;
    }
    let mut popped = vec![0u32; 1500];
    assert_eq!(c.pop_slice(&mut popped), 1500);
    let queued = next as usize - 1500;
    assert_eq!(c.len(), queued);
    for skip in [
        0usize,
        1,
        2,
        100,
        127,
        128,
        129,
        1000,
        2500,
        4096,
        6000,
        queued - 1,
        queued,
    ] {
        for len in [0usize, 1, 5, 128, 3000, 10_000] {
            let mut peeked = vec![0u32; len];
            let n = c.peek_slice_at(skip, &mut peeked);
            assert_eq!(n, len.min(queued - skip), "skip {} len {}", skip, len);
            for (i, v) in peeked[..n].iter().enumerate() {
                assert_eq!(
                    *v as usize,
                    1500 + skip + i,
                    "skip {} len {} at {}",
                    skip,
                    len,
                    i
                );
            }
        }
    }
    // Skipping more than there is peeks nothing
    let mut one = [0u32; 1];
    assert_eq!(c.peek_slice_at(queued + 5, &mut one), 0);
    assert_eq!(c.len(), queued);
    drop(popped);
    drop(p);
    drop(c);
    assert_eq!(live(), before);
}

// Records which items are dropped: how often each value was dropped
struct Drops {
    each: Vec<AtomicUsize>,
}

impl Drops {
    fn new(values: usize) -> Arc<Self> {
        Arc::new(Self {
            each: (0..values).map(|_| AtomicUsize::new(0)).collect(),
        })
    }

    // Checks that the values for which `dropped` is true were dropped once, and no other value
    fn assert_dropped(&self, dropped: impl Fn(usize) -> bool) {
        for (value, count) in self.each.iter().enumerate() {
            let count = count.load(Ordering::SeqCst);
            assert_eq!(
                count,
                dropped(value) as usize,
                "the value {} was dropped {} times",
                value,
                count
            );
        }
    }
}

struct Item {
    value: u64,
    drops: Arc<Drops>,
}

impl Drop for Item {
    fn drop(&mut self) {
        self.drops.each[self.value as usize].fetch_add(1, Ordering::SeqCst);
    }
}

fn items_left_behind(consumer_first: bool, popped: usize, total: usize) {
    let drops = Drops::new(total);
    let before = live();
    let (mut p, mut c) = Ring::<Item>::new(total + 5).split();
    for i in 0..total {
        assert!(p
            .push(Item {
                value: i as u64,
                drops: drops.clone(),
            })
            .is_ok());
    }
    for i in 0..popped {
        let item = c.pop().unwrap();
        assert_eq!(item.value, i as u64);
        drop(item);
    }
    drops.assert_dropped(|v| v < popped);
    if consumer_first {
        drop(c);
        // The items stay queued while the producer lives: it could push more
        drops.assert_dropped(|v| v < popped);
        drop(p);
    } else {
        drop(p);
        drops.assert_dropped(|v| v < popped);
        drop(c);
    }
    // The items that were not popped are dropped with the queue, each of them once
    drops.assert_dropped(|v| v < total);
    assert_eq!(live(), before);
}

#[test]
fn queued_items_are_dropped_with_the_queue() {
    track();
    // Items of 16 bytes: chunks of 32, 64, ... items
    for &(popped, total) in &[
        (0, 0),
        (0, 1),
        (1, 1),
        (0, 100),
        (31, 100),
        (32, 100),
        (33, 100),
        (100, 100),
        (500, 1000),
        (0, 5000),
        (4999, 5000),
    ] {
        items_left_behind(true, popped, total);
        items_left_behind(false, popped, total);
    }
}

#[test]
fn push_error_returns_the_item() {
    track();
    let drops = Drops::new(2);
    let (mut p, c) = Ring::<Item>::new(1).split();
    let item = |v| Item {
        value: v,
        drops: drops.clone(),
    };
    assert!(p.push(item(0)).is_ok());
    match p.push(item(1)) {
        Err(PushError::Full(back)) => assert_eq!(back.value, 1),
        _ => panic!("expected Full"),
    }
    // The item that was returned was dropped here, and the one in the queue is not
    drops.assert_dropped(|v| v == 1);
    drop(p);
    drop(c);
    drops.assert_dropped(|v| v < 2);
}

#[test]
fn allocation_failure_is_reported() {
    track();
    let data = vec![0x5au8; 1 << 20];
    let before = live();
    // A queue with chunks of 16 KiB
    let (mut p, mut c) = Ring::<u8>::new(208 * 1024).split();
    // Room for a few chunks only
    set_budget(before + 3 * 16384);
    let mut total = 0;
    let mut failed = false;
    let failed_before = failures();
    for _ in 0..100 {
        match p.push_slice(&data[..40000]) {
            Ok(n) => {
                total += n;
                assert!(n > 0);
            }
            Err(NoMemory) => {
                failed = true;
                break;
            }
        }
    }
    set_budget(usize::MAX);
    assert!(
        failed,
        "the budget was not exhausted: {} bytes pushed",
        total
    );
    assert!(failures() > failed_before);
    assert!(total >= 2 * 16384, "{}", total);
    assert_eq!(p.len(), total);
    // What was pushed is readable, and a read frees memory so that the producer goes on
    let mut out = vec![0u8; total];
    let before = before + out.len();
    assert_eq!(c.pop_slice(&mut out), total);
    assert!(out.iter().all(|b| *b == 0x5a));
    assert_eq!(p.push_slice(&data[..100]), Ok(100));
    // A single item
    let (mut p2, c2) = Ring::<u64>::new(10).split();
    set_budget(0);
    assert_eq!(p2.push(5), Err(PushError::NoMemory(5)));
    assert_eq!(p2.push_slice(&[1, 2, 3]), Err(NoMemory));
    set_budget(usize::MAX);
    assert_eq!(p2.push(5), Ok(()));
    drop(p2);
    drop(c2);
    drop(p);
    drop(c);
    assert_eq!(live(), before);
}

#[test]
fn partial_push_when_memory_ends_in_the_middle() {
    track();
    let data: Vec<u8> = (0..100_000).map(byte).collect();
    let mut out = vec![0u8; data.len() + 1];
    let before = live();
    // A queue with chunks of 16 KiB
    let (mut p, mut c) = Ring::<u8>::new(208 * 1024).split();
    // A big push where the third chunk cannot be allocated: the first two are written
    set_budget(before + 2 * (16384 + HEADER) + QUEUE);
    let n = p.push_slice(&data).unwrap();
    set_budget(usize::MAX);
    assert_eq!(n, 2 * 16384);
    assert_eq!(p.push_slice(&data[n..]), Ok(data.len() - n));
    assert_eq!(c.pop_slice(&mut out), data.len());
    assert_eq!(&out[..data.len()], &data[..]);
    drop(p);
    drop(c);
    assert_eq!(live(), before);
}

fn stress(capacity: usize, total: usize, max_chunk: usize, seed: u64, drop_early: bool) {
    let stop = Arc::new(AtomicBool::new(false));
    let before = live();
    let (mut p, mut c) = Ring::<u8>::new(capacity).split();
    let writer = spawn(move || {
        let mut rng = Rng(seed);
        let mut sent = 0;
        let mut buf = vec![0u8; max_chunk];
        while sent < total {
            let n = min3(1 + rng.below(max_chunk), total - sent, buf.len());
            for i in 0..n {
                buf[i] = byte(sent + i);
            }
            let mut off = 0;
            while off < n {
                let k = p.push_slice(&buf[off..n]).unwrap();
                off += k;
                if k == 0 {
                    thread::yield_now();
                }
            }
            sent += n;
        }
        drop(p);
        sent
    });
    let stop2 = stop.clone();
    let reader = spawn(move || {
        let mut rng = Rng(seed ^ 0x9e3779b97f4a7c15);
        let mut got = 0;
        let mut buf = vec![0u8; max_chunk];
        while got < total {
            let n = 1 + rng.below(max_chunk);
            // sometimes peek first
            let mut peek = vec![0u8; 1 + rng.below(100)];
            let k = if rng.below(4) == 0 {
                c.peek_slice(&mut peek)
            } else {
                0
            };
            let r = c.pop_slice(&mut buf[..n]);
            for i in 0..k.min(r) {
                assert_eq!(peek[i], buf[i]);
            }
            for i in 0..r {
                assert_eq!(buf[i], byte(got + i), "at {}", got + i);
            }
            got += r;
            assert_eq!(c.total_popped(), got);
            if r == 0 {
                thread::yield_now();
            }
            if drop_early && got > total / 2 {
                break;
            }
        }
        stop2.store(true, Ordering::SeqCst);
        drop(c);
        got
    });
    let sent = writer.join().unwrap();
    let got = reader.join().unwrap();
    assert_eq!(sent, total);
    assert!(got >= if drop_early { total / 2 } else { total });
    assert_eq!(live(), before);
    drop(stop);
}

fn min3(a: usize, b: usize, c: usize) -> usize {
    a.min(b).min(c)
}

#[test]
fn stress_two_threads() {
    track();
    for (i, &cap) in [
        1usize,
        7,
        100,
        511,
        512,
        513,
        4096,
        16384,
        20000,
        208 * 1024,
    ]
    .iter()
    .enumerate()
    {
        for &max_chunk in &[1usize, 10, 700, 5000, 70000] {
            let total = if cap < 100 { 100_000 } else { 3_000_000 };
            stress(
                cap,
                total,
                max_chunk,
                1000 + i as u64 * 17 + max_chunk as u64,
                false,
            );
        }
    }
}

// The producer closes while the consumer reads and parks the chunks: when the consumer has read
// everything, no spare chunk is left, only the last chunk
#[test]
fn stress_close_while_the_consumer_parks() {
    track();
    for round in 0..3000u64 {
        let before = live();
        let (mut p, mut c) = Ring::<u8>::new(1 << 20).split();
        let total = 16384 * (1 + (round as usize % 9)) + (round as usize % 7);
        let writer = spawn(move || {
            let data = vec![9u8; total];
            let mut sent = 0;
            while sent < total {
                sent += p.push_slice(&data[sent..]).unwrap();
            }
            // Close at some point in the reader's loop
            for _ in 0..(round % 5) * 3 {
                thread::yield_now();
            }
            p.close();
            p
        });
        let reader = spawn(move || {
            let mut out = vec![0u8; 20000];
            let mut got = 0;
            while got < total {
                let n = c.pop_slice(&mut out);
                got += n;
                if n == 0 {
                    thread::yield_now();
                }
            }
            c
        });
        let c = reader.join().unwrap();
        let p = writer.join().unwrap();
        let held = live() - before;
        assert!(
            held <= 64 * 1024 + HEADER + QUEUE,
            "round {}: {} bytes are held",
            round,
            held
        );
        drop(p);
        drop(c);
        assert_eq!(live(), before);
    }
}

// How many rounds the next test makes, unless the environment says otherwise
const HAMMER_ROUNDS: u64 = 100_000;

// Two threads that live for all the rounds of a test, and start each round together
struct Duel {
    // The number of the round to run. The threads wait for it to change
    round: AtomicU64,
    // The number of threads that have finished the round
    done: AtomicUsize,
    // The ends of the queue of the round, which a thread takes and gives back
    producer: Mutex<Option<RingProducer<u8>>>,
    consumer: Mutex<Option<RingConsumer<u8>>>,
    // How long the producer waits before it closes the queue, in turns of a loop
    delay: AtomicUsize,
}

// Waits for the condition without sleeping: waking up a thread takes much longer than the windows
// that the test is after
fn spin_until(cond: impl Fn() -> bool) {
    let mut spins = 0u32;
    while !cond() {
        spins += 1;
        if spins % 2048 == 0 {
            thread::yield_now();
        } else {
            std::hint::spin_loop();
        }
    }
}

fn busy(turns: usize) {
    for i in 0..turns {
        std::hint::black_box(i);
    }
}

// The producer writes and closes the queue while the consumer reads and parks the chunks that it
// has read. One of the two must see the other, so that the chunks that were parked at the end are
// freed (see the header of ring.rs): when both are done, no chunk is left in a slot. The window in
// which both could miss the other is a few nanoseconds wide, so the threads start each round
// together, and the producer waits a different time before it closes, in rounds that take a few
// microseconds each. The environment variable RING_HAMMER_ROUNDS sets the number of rounds, for
// a long run (one million rounds take about ten seconds).
#[test]
fn close_while_the_consumer_parks() {
    track();
    const WRITES: usize = 24;
    const WRITE: usize = 513;
    const MAX_DELAY: usize = 400;
    let rounds = std::env::var("RING_HAMMER_ROUNDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(HAMMER_ROUNDS);
    let before = live();
    let duel = Arc::new(Duel {
        round: AtomicU64::new(0),
        done: AtomicUsize::new(0),
        producer: Mutex::new(None),
        consumer: Mutex::new(None),
        delay: AtomicUsize::new(0),
    });
    let producer = {
        let duel = duel.clone();
        spawn(move || {
            let data: Vec<u8> = (0..WRITES * WRITE).map(byte).collect();
            let mut seen = 0;
            loop {
                spin_until(|| duel.round.load(Ordering::SeqCst) != seen);
                seen = duel.round.load(Ordering::SeqCst);
                if seen == u64::MAX {
                    return;
                }
                let mut p = duel.producer.lock().unwrap().take().unwrap();
                for write in data.chunks(WRITE) {
                    assert_eq!(p.push_slice(write), Ok(WRITE));
                }
                busy(duel.delay.load(Ordering::Relaxed));
                p.close();
                *duel.producer.lock().unwrap() = Some(p);
                duel.done.fetch_add(1, Ordering::SeqCst);
            }
        })
    };
    let consumer = {
        let duel = duel.clone();
        spawn(move || {
            let expected: Vec<u8> = (0..WRITES * WRITE).map(byte).collect();
            let mut seen = 0;
            loop {
                spin_until(|| duel.round.load(Ordering::SeqCst) != seen);
                seen = duel.round.load(Ordering::SeqCst);
                if seen == u64::MAX {
                    return;
                }
                let mut c = duel.consumer.lock().unwrap().take().unwrap();
                let mut out = [0u8; 700];
                let mut got = 0;
                while got < WRITES * WRITE {
                    let n = c.pop_slice(&mut out);
                    assert!(out[..n] == expected[got..got + n]);
                    got += n;
                    if n == 0 {
                        // The producer is gone if the test is over
                        if duel.round.load(Ordering::Relaxed) == u64::MAX {
                            return;
                        }
                        std::hint::spin_loop();
                    }
                }
                *duel.consumer.lock().unwrap() = Some(c);
                duel.done.fetch_add(1, Ordering::SeqCst);
            }
        })
    };
    let mut rng = Rng(0x2545f4914f6cdd1d);
    let mut leaks = 0;
    let mut first = Vec::new();
    for round in 0..rounds {
        let ring = Ring::<u8>::new(1 << 20);
        let shared = ring.shared.clone();
        let (p, c) = ring.split();
        *duel.producer.lock().unwrap() = Some(p);
        *duel.consumer.lock().unwrap() = Some(c);
        duel.delay.store(rng.below(MAX_DELAY), Ordering::Relaxed);
        duel.done.store(0, Ordering::SeqCst);
        duel.round.store(round + 1, Ordering::SeqCst);
        // A thread that has panicked does not finish the round
        spin_until(|| {
            duel.done.load(Ordering::SeqCst) == 2
                || producer.is_finished()
                || consumer.is_finished()
        });
        if producer.is_finished() || consumer.is_finished() {
            break;
        }
        let parked = shared
            .spares
            .iter()
            .filter(|s| !s.load(Ordering::SeqCst).is_null())
            .count();
        if parked != 0 {
            leaks += 1;
            if first.len() < 5 {
                first.push((round, parked, duel.delay.load(Ordering::Relaxed)));
            }
        }
        drop(duel.producer.lock().unwrap().take());
        drop(duel.consumer.lock().unwrap().take());
    }
    duel.round.store(u64::MAX, Ordering::SeqCst);
    producer.join().unwrap();
    consumer.join().unwrap();
    assert_eq!(
        leaks, 0,
        "{} of {} rounds left spare chunks behind, the first ones (round, chunks, delay): {:?}",
        leaks, rounds, first
    );
    drop(duel);
    assert_eq!(live(), before);
}

#[test]
fn stress_consumer_dropped_early() {
    track();
    for &cap in &[1000usize, 40000] {
        // The producer keeps pushing while the consumer goes away: it would block forever when
        // the queue is full, so the producer of this test stops when the queue is full
        let before = live();
        let (mut p, mut c) = Ring::<u8>::new(cap).split();
        let t = spawn(move || {
            let mut buf = vec![0u8; 100];
            let mut n = 0;
            while n < cap / 2 {
                n += c.pop_slice(&mut buf);
            }
            drop(c);
        });
        let mut pushed = 0;
        while pushed < cap {
            pushed += p.push_slice(&[1u8; 77]).unwrap();
        }
        t.join().unwrap();
        drop(p);
        assert_eq!(live(), before);
    }
}

#[test]
fn stress_items_with_drop() {
    track();
    for round in 0..20 {
        let drops = Drops::new(20_000);
        let created = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let keep = stop.clone();
        let before = live();
        let (mut p, mut c) = Ring::<Item>::new(500 + round * 100).split();
        let (drops2, created2, stop2) = (drops.clone(), created.clone(), stop.clone());
        let w = spawn(move || {
            'outer: for i in 0..20_000u64 {
                created2.fetch_add(1, Ordering::SeqCst);
                let mut item = Item {
                    value: i,
                    drops: drops2.clone(),
                };
                loop {
                    match p.push(item) {
                        Ok(()) => break,
                        Err(PushError::Full(back)) => {
                            item = back;
                            if stop2.load(Ordering::SeqCst) {
                                break 'outer;
                            }
                            thread::yield_now();
                        }
                        Err(PushError::NoMemory(_)) => panic!(),
                    }
                }
            }
            p
        });
        let r = spawn(move || {
            let mut next = 0u64;
            // read only part of the items: the rest is dropped with the queue
            while next < 15_000 {
                match c.pop() {
                    Some(item) => {
                        assert_eq!(item.value, next);
                        next += 1;
                    }
                    None => thread::yield_now(),
                }
            }
            stop.store(true, Ordering::SeqCst);
            c
        });
        let c = r.join().unwrap();
        let p = w.join().unwrap();
        // The queue holds items still, and drops them with the last end
        drop(c);
        drop(p);
        // Each item that was made is dropped once, whether it was popped or not
        let created = created.load(Ordering::SeqCst);
        drops.assert_dropped(|v| v < created);
        assert_eq!(live(), before);
        drop(keep);
    }
}

// The cheap properties that the producer relies on
#[test]
fn types_are_send() {
    fn send<T: Send>() {}
    send::<RingProducer<u8>>();
    send::<RingConsumer<u8>>();
    send::<RingProducer<Arc<u8>>>();
}
