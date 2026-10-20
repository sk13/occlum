//! The tests of `Entries`: some cases that Linux shows, and a comparison with a model of the
//! buffers of Linux for random sends and receives.

use super::*;

type Fd = u32;
type Cred = u8;
type E = Entries<Fd, Cred>;
type M = Message<Fd, Cred>;
type Call_ = Call<Fd, Cred>;

/// A stream and its entries, as the unix socket sends and receives
struct Stream {
    entries: E,
    pushed: usize,
    popped: usize,
}

impl Stream {
    fn new() -> Self {
        Self {
            entries: E::default(),
            pushed: 0,
            popped: 0,
        }
    }

    /// Sends `len` bytes with control data, like the socket does for a send that pushes `count`
    /// of the bytes, and returns whether the entries took the message.
    fn send_partial(&mut self, len: usize, count: usize, fds: &[Fd], cred: Option<Cred>) -> bool {
        let mut msg = M {
            cred,
            fds: fds.to_vec(),
        };
        let registered = (!fds.is_empty() || cred.is_some())
            && self.entries.register(self.pushed, len, &mut msg);
        if !fds.is_empty() && !registered {
            return false;
        }
        self.pushed += count;
        if registered {
            self.entries.pushed(count);
        }
        true
    }

    fn send(&mut self, len: usize, fds: &[Fd], cred: Option<Cred>) {
        assert!(self.send_partial(len, len, fds, cred));
    }

    fn queued(&self) -> usize {
        self.pushed - self.popped
    }

    /// Receives with one attempt, like a call that does not wait for more. Returns the number of
    /// the bytes, the descriptors, the sender that the call delivers, and whether it is over.
    fn recv(
        &mut self,
        want: usize,
        passcred: bool,
        peek: bool,
    ) -> (usize, Vec<Fd>, Option<Option<Cred>>, bool) {
        let mut call = Call_::default();
        let (limit, last) =
            self.entries
                .take(self.popped, self.queued(), want, passcred, peek, &mut call);
        if !peek {
            self.popped += limit;
        }
        (limit, call.fds, call.cred, last)
    }
}

#[test]
fn plain_bytes_have_no_entries() {
    let mut s = Stream::new();
    s.pushed = 10;
    let (n, fds, cred, last) = s.recv(100, false, false);
    assert_eq!((n, fds, cred, last), (10, vec![], None, false));
    assert_eq!(s.entries.len(), 0);
}

// Linux: "ab", "c" with a descriptor, "de": the first receive gets "abc" and the descriptor,
// the second "de"
#[test]
fn a_receive_goes_into_the_message_with_descriptors_and_stops_at_its_end() {
    let mut s = Stream::new();
    s.send(2, &[], None);
    s.send(1, &[7], None);
    s.send(2, &[], None);
    assert_eq!(s.recv(16, false, false), (3, vec![7], None, true));
    assert_eq!(s.recv(16, false, false), (2, vec![], None, false));
    assert_eq!(s.entries.len(), 0);
}

// Linux: a message "abc" with a descriptor, received one byte at a time
#[test]
fn the_descriptors_come_with_the_first_byte() {
    let mut s = Stream::new();
    s.send(3, &[7, 8], None);
    assert_eq!(s.recv(1, false, false), (1, vec![7, 8], None, true));
    assert_eq!(s.recv(1, false, false), (1, vec![], None, false));
    assert_eq!(s.recv(1, false, false), (1, vec![], None, false));
    assert_eq!(s.entries.len(), 0);
}

// Linux: two messages with descriptors are received one by one
#[test]
fn a_receive_takes_the_descriptors_of_one_message() {
    let mut s = Stream::new();
    s.send(1, &[1], None);
    s.send(1, &[2], None);
    assert_eq!(s.recv(16, false, false), (1, vec![1], None, true));
    assert_eq!(s.recv(16, false, false), (1, vec![2], None, true));
    assert_eq!(s.recv(16, false, false).0, 0);
}

// Linux: a peek shows the descriptors every time, a receive that starts after the first byte
// does not
#[test]
fn a_peek_leaves_the_descriptors() {
    let mut s = Stream::new();
    s.send(2, &[5], None);
    assert_eq!(s.recv(16, false, true), (2, vec![5], None, true));
    assert_eq!(s.recv(16, false, true), (2, vec![5], None, true));
    assert_eq!(s.recv(1, false, false), (1, vec![5], None, true));
    assert_eq!(s.recv(16, false, true), (1, vec![], None, false));
    assert_eq!(s.recv(16, false, false), (1, vec![], None, false));
    assert_eq!(s.entries.len(), 0);
}

// Linux: a peek shows the descriptors of the first message that has some, also if it takes fewer
// bytes than there are before that message
#[test]
fn a_peek_looks_ahead_for_descriptors() {
    let mut s = Stream::new();
    s.send(2, &[], None);
    s.send(1, &[1], None);
    s.send(1, &[], None);
    s.send(1, &[2], None);
    // the first two peeks end before the message with descriptors, the others go into it
    assert_eq!(s.recv(1, false, true), (1, vec![1], None, false));
    assert_eq!(s.recv(2, false, true), (2, vec![1], None, false));
    assert_eq!(s.recv(3, false, true), (3, vec![1], None, true));
    assert_eq!(s.recv(4, false, true), (3, vec![1], None, true));
    // the descriptors are taken with the first byte of their message, and the next ones show
    assert_eq!(s.recv(1, false, false), (1, vec![], None, false));
    assert_eq!(s.recv(1, false, true), (1, vec![1], None, false));
    assert_eq!(s.recv(2, false, false), (2, vec![1], None, true));
    assert_eq!(s.recv(1, false, true), (1, vec![2], None, false));
    assert_eq!(s.recv(1, false, false), (1, vec![], None, false));
    assert_eq!(s.recv(1, false, true), (1, vec![2], None, true));
}

// Linux: but not beyond a message of another sender, if the receive delivers credentials, or
// the bytes that are queued
#[test]
fn a_peek_looks_ahead_as_far_as_the_sender_is_the_same() {
    let mut s = Stream::new();
    s.send(1, &[], Some(1));
    s.send(1, &[7], Some(2));
    assert_eq!(s.recv(1, true, true), (1, vec![], Some(Some(1)), false));
    assert_eq!(s.recv(1, false, true), (1, vec![7], None, false));
    let mut s = Stream::new();
    s.send(1, &[], Some(1));
    s.send(2, &[], Some(1));
    s.send(1, &[7], Some(1));
    assert_eq!(s.recv(1, true, true), (1, vec![7], Some(Some(1)), false));
    // the bytes that are not published yet are not there
    let mut s = Stream::new();
    s.send(1, &[], None);
    let mut msg = M {
        cred: None,
        fds: vec![9],
    };
    assert!(s.entries.register(1, 3, &mut msg));
    assert_eq!(s.recv(1, false, true), (1, vec![], None, false));
    s.entries.pushed(0);
}

// Linux: with SO_PASSCRED the credentials of the sender come with the data, once for the bytes
// of the same sender, and unknown ones are zeros of the sender
#[test]
fn credentials_of_the_same_sender_are_delivered_once() {
    let mut s = Stream::new();
    s.send(1, &[], Some(1));
    s.send(1, &[], Some(1));
    assert_eq!(s.entries.len(), 1);
    assert_eq!(s.recv(16, true, false), (2, vec![], Some(Some(1)), false));
    assert_eq!(s.entries.len(), 0);
}

#[test]
fn a_receive_stops_at_another_sender() {
    let mut s = Stream::new();
    s.send(1, &[], Some(1));
    s.send(2, &[], Some(2));
    s.send(1, &[], Some(2));
    assert_eq!(s.recv(16, true, false), (1, vec![], Some(Some(1)), true));
    assert_eq!(s.recv(16, true, false), (3, vec![], Some(Some(2)), false));
}

#[test]
fn bytes_without_credentials_come_from_nobody() {
    let mut s = Stream::new();
    s.send(2, &[], None);
    s.send(1, &[], Some(3));
    s.send(2, &[], None);
    assert_eq!(s.recv(16, true, false), (2, vec![], Some(None), true));
    assert_eq!(s.recv(16, true, false), (1, vec![], Some(Some(3)), true));
    assert_eq!(s.recv(16, true, false), (2, vec![], Some(None), false));
    // without SO_PASSCRED all bytes come together
    s.send(2, &[], None);
    s.send(1, &[], Some(3));
    s.send(2, &[], None);
    assert_eq!(s.recv(16, false, false), (5, vec![], None, false));
}

#[test]
fn a_message_with_descriptors_keeps_its_sender() {
    let mut s = Stream::new();
    s.send(1, &[9], Some(4));
    s.send(1, &[], Some(4));
    // the first receive ends after the descriptors, the second goes on with the credentials
    assert_eq!(s.recv(16, true, false), (1, vec![9], Some(Some(4)), true));
    assert_eq!(s.recv(16, true, false), (1, vec![], Some(Some(4)), false));
}

// The bytes of an entry that has given its descriptors away go together with the bytes that
// follow it from the same sender
#[test]
fn an_entry_extends_after_it_has_given_its_descriptors() {
    let mut s = Stream::new();
    s.send(3, &[9], Some(4));
    assert_eq!(s.recv(1, true, false), (1, vec![9], Some(Some(4)), true));
    s.send(2, &[], Some(4));
    assert_eq!(s.entries.len(), 1);
    assert_eq!(s.recv(16, true, false), (4, vec![], Some(Some(4)), false));
}

#[test]
fn a_call_that_has_taken_descriptors_takes_nothing_more() {
    let mut s = Stream::new();
    s.send(2, &[1], None);
    s.send(2, &[], None);
    let mut call = Call_::default();
    let queued = s.queued();
    assert_eq!(
        s.entries.take(0, queued, 16, false, false, &mut call),
        (2, true)
    );
    assert_eq!(s.entries.take(2, 2, 14, false, false, &mut call), (0, true));
    assert_eq!(call.fds, vec![1]);
}

#[test]
fn a_call_that_waits_for_more_keeps_its_sender() {
    let mut s = Stream::new();
    s.send(2, &[], Some(1));
    let mut call = Call_::default();
    assert_eq!(s.entries.take(0, 2, 8, true, false, &mut call), (2, false));
    s.popped = 2;
    // more bytes of another sender: the call is over, whatever it still wants
    s.send(2, &[], Some(2));
    assert_eq!(s.entries.take(2, 2, 6, true, false, &mut call), (0, true));
    assert_eq!(call.cred, Some(Some(1)));
}

#[test]
fn the_entries_are_limited() {
    let mut s = Stream::new();
    // 253 descriptors take 254 slots
    let fds: Vec<Fd> = (0..253).collect();
    let mut n = 0;
    while s.send_partial(1, 1, &fds, None) {
        n += 1;
    }
    assert_eq!(n, MAX_SLOTS / 254);
    // the next one fits after a receive
    assert_eq!(s.recv(1, false, false).1.len(), 253);
    assert!(s.send_partial(1, 1, &fds, None));
    assert!(!s.send_partial(1, 1, &fds, None));
    // messages of credentials only take a slot each, unless they extend an entry
    let mut s = Stream::new();
    for i in 0..MAX_SLOTS {
        s.send(1, &[], Some((i % 2) as Cred));
    }
    assert_eq!(s.entries.len(), MAX_SLOTS);
    assert!(!s.send_partial(1, 1, &[1], None));
    // the last slot is the limit: one more entry of credentials is not taken either
    s.send_partial(1, 1, &[], Some(9));
    assert_eq!(s.entries.len(), MAX_SLOTS);
}

// A send that pushes no bytes gives its descriptors back, which the socket drops with no lock held
#[test]
fn a_send_that_pushes_nothing_gives_its_descriptors_back() {
    let mut s = Stream::new();
    let mut msg = M {
        cred: None,
        fds: vec![1, 2],
    };
    assert!(s.entries.register(0, 4, &mut msg));
    assert_eq!(s.entries.pushed(0), vec![1, 2]);
    assert_eq!(s.entries.len(), 0);
    // the descriptors stay with the bytes that were pushed
    let mut msg = M {
        cred: None,
        fds: vec![3],
    };
    assert!(s.entries.register(0, 4, &mut msg));
    assert_eq!(s.entries.pushed(2), Vec::<Fd>::new());
    assert_eq!(s.entries.len(), 1);
}

#[test]
fn a_send_that_pushes_fewer_bytes_than_its_room() {
    let mut s = Stream::new();
    // 10 bytes of room, 3 pushed: the entry ends after 3 bytes
    assert!(s.send_partial(10, 3, &[1], None));
    s.send(2, &[], None);
    assert_eq!(s.recv(16, false, false), (3, vec![1], None, true));
    assert_eq!(s.recv(16, false, false), (2, vec![], None, false));
    // none pushed: no entry
    assert!(s.send_partial(10, 0, &[1], None));
    assert_eq!(s.entries.len(), 0);
    // credentials, 4 bytes more of the same sender after 2, of which 1 is pushed
    s.send(2, &[], Some(5));
    assert!(s.send_partial(4, 1, &[], Some(5)));
    assert_eq!(s.entries.len(), 1);
    assert_eq!(s.recv(16, true, false), (3, vec![], Some(Some(5)), false));
    // credentials, none pushed
    s.send(2, &[], Some(5));
    assert!(s.send_partial(4, 0, &[], Some(5)));
    s.send(1, &[], None);
    assert_eq!(s.recv(16, true, false), (2, vec![], Some(Some(5)), true));
}

// A receive that takes the bytes of a send in progress must leave its entry, which the send
// is to finish
#[test]
fn a_receive_may_use_up_the_entry_of_a_send_in_progress() {
    let mut s = Stream::new();
    let mut msg = M {
        cred: Some(1),
        fds: vec![],
    };
    assert!(s.entries.register(0, 4, &mut msg));
    // 4 bytes are in and visible, the sender has not told yet
    s.pushed = 4;
    assert_eq!(s.recv(16, true, false), (4, vec![], Some(Some(1)), false));
    s.entries.pushed(4);
    assert_eq!(s.entries.len(), 1);
    // the same for a message with descriptors
    let mut msg = M {
        cred: None,
        fds: vec![3],
    };
    assert!(s.entries.register(4, 2, &mut msg));
    s.pushed = 6;
    assert_eq!(s.recv(16, false, false), (2, vec![3], None, true));
    s.entries.pushed(2);
    assert_eq!(s.recv(16, false, false).0, 0);
    // the entry that was in use is gone with the next bytes
    s.send(1, &[], None);
    assert_eq!(s.recv(16, false, false), (1, vec![], None, false));
    assert_eq!(s.entries.len(), 0);
    assert_eq!(s.entries.slots, 0);
}

#[test]
fn draining_gives_the_descriptors_back() {
    let mut s = Stream::new();
    s.send(1, &[1, 2], None);
    s.send(1, &[3], Some(2));
    s.send(1, &[], Some(2));
    let mut fds = s.entries.drain_fds();
    fds.sort();
    assert_eq!(fds, vec![1, 2, 3]);
    assert_eq!(s.entries.len(), 0);
    assert_eq!(s.entries.slots, 0);
}

// ---------------------------------------------------------------------------------------------
// The model
// ---------------------------------------------------------------------------------------------

/// A buffer of Linux: the bytes of one send, with their descriptors and their sender
struct Skb {
    len: usize,
    cred: Option<Cred>,
    fds: Vec<Fd>,
}

/// What `unix_stream_read_generic` of Linux does with the buffers of a stream
struct Model {
    skbs: VecDeque<Skb>,
}

impl Model {
    /// The loop of `unix_stream_read_generic`. The receive does not wait.
    fn recv(
        &mut self,
        want: usize,
        passcred: bool,
        peek: bool,
    ) -> (usize, Vec<Fd>, Option<Option<Cred>>, bool) {
        let mut size = want;
        let mut copied = 0;
        let mut fds = Vec::new();
        let mut scm: Option<Option<Cred>> = None;
        let mut last = false;
        let mut i = 0;
        // A peek goes on to the next buffer, also if it has all the bytes that it wants, until
        // a buffer that has descriptors, a buffer of another sender, or the end of the queue
        while peek || size > 0 {
            let skb = match self.skbs.get_mut(i) {
                Some(skb) => skb,
                None => break,
            };
            match scm {
                // Never glue the messages of different senders
                Some(first) => {
                    if skb.cred != first {
                        last = true;
                        break;
                    }
                }
                None => {
                    if passcred {
                        scm = Some(skb.cred);
                    }
                }
            }
            let chunk = min(skb.len, size);
            copied += chunk;
            size -= chunk;
            if peek {
                if !skb.fds.is_empty() {
                    fds.extend(skb.fds.iter().cloned());
                    last = true;
                    break;
                }
                i += 1;
            } else {
                skb.len -= chunk;
                fds.append(&mut skb.fds);
                if skb.len > 0 {
                    // the call has no room left, and has not taken the whole buffer
                    last = !fds.is_empty();
                    break;
                }
                self.skbs.remove(i);
                if !fds.is_empty() {
                    last = true;
                    break;
                }
            }
        }
        (copied, fds, scm, last)
    }
}

/// A small random number generator
struct Rng(u64);

impl Rng {
    fn next(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

#[test]
fn matches_the_model_of_linux() {
    for seed in 1..=3000u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9e3779b97f4a7c15));
        let mut s = Stream::new();
        let mut model = Model {
            skbs: VecDeque::new(),
        };
        let mut next_fd = 0;
        let mut log = Vec::new();
        for _ in 0..rng.next(40) + 1 {
            if rng.next(5) < 3 {
                // a send of 1 to 6 bytes: plain, with credentials, with descriptors
                let len = rng.next(6) as usize + 1;
                let kind = rng.next(6);
                let cred = if kind % 2 == 1 || kind == 4 {
                    Some(rng.next(3) as Cred)
                } else {
                    None
                };
                let mut fds = Vec::new();
                if kind >= 4 {
                    for _ in 0..rng.next(3) + 1 {
                        fds.push(next_fd);
                        next_fd += 1;
                    }
                }
                log.push(format!("send {} {:?} {:?}", len, fds, cred));
                if s.send_partial(len, len, &fds, cred) {
                    model.skbs.push_back(Skb {
                        len,
                        cred,
                        fds: fds.clone(),
                    });
                } else {
                    // The entries are full, which the model does not have
                    break;
                }
            } else {
                let want = rng.next(9) as usize + 1;
                let passcred = rng.next(2) == 0;
                let peek = rng.next(4) == 0;
                if s.queued() == 0 {
                    continue;
                }
                log.push(format!("recv {} {} {}", want, passcred, peek));
                let got = s.recv(want, passcred, peek);
                let expected = model.recv(want, passcred, peek);
                // The model does not know whether a call is over if it has taken what it wanted, and
                // a peek does not wait for more
                assert_eq!(
                    (got.0, &got.1, got.2),
                    (expected.0, &expected.1, expected.2),
                    "seed {}: {:?}",
                    seed,
                    log
                );
                if expected.3 && !peek {
                    assert!(got.3, "seed {}: {:?}", seed, log);
                }
            }
        }
        // Everything that is left comes out in the end
        let mut left = 0;
        for skb in &model.skbs {
            left += skb.len;
        }
        assert_eq!(left, s.queued(), "seed {}", seed);
        while s.queued() > 0 {
            let got = s.recv(1000, false, false);
            let expected = model.recv(1000, false, false);
            assert_eq!((got.0, &got.1), (expected.0, &expected.1), "seed {}", seed);
        }
        assert_eq!(s.entries.len(), 0, "seed {}", seed);
        assert_eq!(s.entries.slots, 0, "seed {}", seed);
    }
}
