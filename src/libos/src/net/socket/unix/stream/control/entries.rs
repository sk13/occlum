//! The control data that travels with the bytes of a unix stream.
//!
//! A stream has no message boundaries, but the descriptors and the credentials that a send
//! attaches to its bytes (SCM_RIGHTS, SCM_CREDENTIALS) belong to those bytes. Linux keeps one
//! buffer per send, and a receive takes the control data of a buffer with its first byte. This
//! file keeps the control data next to the bytes of the stream instead, as ranges of its offsets:
//! an entry holds the offsets `start` and `end` of the bytes of a send, its descriptors, and the
//! credentials of its sender. The bytes that no entry covers have no control data. The rules are
//! those of Linux, for what a receive takes:
//!
//! - The descriptors of an entry are delivered with the first of its bytes that a receive takes,
//!   whether or not the receive starts at the entry: a receive that starts in bytes without
//!   control data goes on into the entry. A receive that has taken the descriptors of an entry
//!   stops at its end. So a receive takes the descriptors of one entry at most. The descriptors
//!   are gone from the entry after that, also if the receive did not ask for them.
//! - A receive that is to deliver credentials (SO_PASSCRED) delivers those of its first byte, and
//!   stops before the first byte of another sender: the bytes of an entry with other credentials,
//!   or the bytes that no entry covers, if they have been sent with other credentials.
//! - A peek takes the same bytes, and it shows the descriptors of the entry but leaves them there.
//!
//! The descriptors are a generic type here, and the credentials too, so that the unit tests of
//! this file, which compare it with a model of the buffers of Linux, run on the host without an
//! enclave (`make test-host`). The file uses nothing but `std`.

use std::cmp::min;
use std::collections::VecDeque;
use std::mem;
use std::vec::Vec;

/// The number of slots that the entries of a queue may take: an entry takes one slot and one more
/// for each of its descriptors. A send that needs more waits until a receive makes room, like it
/// waits for room in the buffer. A message holds 253 descriptors at the most, so it always fits
/// in an empty queue.
pub const MAX_SLOTS: usize = 4096;

/// The control data that a send attaches to its bytes
#[derive(Debug)]
pub struct Message<F, C> {
    /// The credentials of the sender, which the receiver may see: those that the sender names, or
    /// its own if the sender or the receiver has SO_PASSCRED
    pub cred: Option<C>,
    /// The descriptors, taken out of the message when the entry takes them
    pub fds: Vec<F>,
}

impl<F, C> Default for Message<F, C> {
    fn default() -> Self {
        Self {
            cred: None,
            fds: Vec::new(),
        }
    }
}

struct Entry<F, C> {
    start: usize,
    end: usize,
    cred: Option<C>,
    fds: Vec<F>,
}

/// What `register` has done, for `pushed` to finish
struct Pending {
    /// The offset of the first byte of the send
    at: usize,
    /// Whether the send has been added to the last entry
    merged: bool,
}

/// The state of a receive across its attempts, for `Entries::take`
#[derive(Debug)]
pub struct Call<F, C> {
    /// The descriptors that the call has taken or peeked
    pub fds: Vec<F>,
    /// The sender of the first bytes of the call, `None` as long as the call does not deliver
    /// credentials, and `Some(None)` if the first bytes have no credentials
    pub cred: Option<Option<C>>,
    /// Whether the call is over because it has taken descriptors
    ended: bool,
}

impl<F, C> Default for Call<F, C> {
    fn default() -> Self {
        Self {
            fds: Vec::new(),
            cred: None,
            ended: false,
        }
    }
}

impl<F, C: Copy + PartialEq> Call<F, C> {
    /// Whether the call has not decided anything yet
    pub fn is_new(&self) -> bool {
        self.cred.is_none() && !self.ended
    }

    /// Whether the call is over because of the control data: it has taken descriptors, or the next
    /// bytes are from another sender
    pub fn is_over(&self) -> bool {
        self.ended
    }

    /// Returns whether the bytes of the sender `cred` are those that the call delivers credentials
    /// for, or the call delivers none
    fn is_sender(&self, cred: Option<C>) -> bool {
        match self.cred {
            Some(first) => first == cred,
            None => true,
        }
    }

    /// Returns whether the call takes bytes of the sender `cred`: the first bytes of a call that
    /// delivers credentials decide who the sender is.
    fn admit(&mut self, cred: Option<C>, passcred: bool) -> bool {
        match self.cred {
            Some(first) => first == cred,
            None => {
                if passcred {
                    self.cred = Some(cred);
                }
                true
            }
        }
    }
}

/// The entries of one direction of a connection
pub struct Entries<F, C> {
    entries: VecDeque<Entry<F, C>>,
    slots: usize,
    pending: Option<Pending>,
}

impl<F, C> Default for Entries<F, C> {
    fn default() -> Self {
        Self {
            entries: VecDeque::new(),
            slots: 0,
            pending: None,
        }
    }
}

impl<F: Clone, C: Copy + PartialEq> Entries<F, C> {
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Records the control data of the bytes that a send is about to push: `at` is the offset of
    /// the first of them, and `room` the number of bytes that the send pushes at the most, so that
    /// a receive that sees the entry sees all of it. The caller calls `pushed` when the bytes are
    /// in. Returns false if the entries are full, without taking the descriptors of the message.
    ///
    /// A message with credentials only, which follows the bytes of the same sender, extends the
    /// last entry, so that a stream that carries credentials has few entries.
    pub fn register(&mut self, at: usize, room: usize, msg: &mut Message<F, C>) -> bool {
        debug_assert!(self.pending.is_none() && room > 0);
        if msg.fds.is_empty() {
            let cred = match msg.cred {
                Some(cred) => cred,
                None => return true,
            };
            if let Some(last) = self.entries.back_mut() {
                if last.end == at && last.fds.is_empty() && last.cred == Some(cred) {
                    last.end = at + room;
                    self.pending = Some(Pending { at, merged: true });
                    return true;
                }
            }
        }
        let slots = 1 + msg.fds.len();
        if self.slots + slots > MAX_SLOTS {
            return false;
        }
        self.slots += slots;
        self.entries.push_back(Entry {
            start: at,
            end: at + room,
            cred: msg.cred,
            fds: mem::take(&mut msg.fds),
        });
        self.pending = Some(Pending { at, merged: false });
        true
    }

    /// Ends what `register` has begun: `count` bytes were pushed, which may be fewer than the room
    /// or none. If none were, the entry is gone, and its descriptors are returned, for the caller
    /// to drop where it holds no lock.
    pub fn pushed(&mut self, count: usize) -> Vec<F> {
        let pending = match self.pending.take() {
            Some(pending) => pending,
            None => return Vec::new(),
        };
        if count == 0 && !pending.merged {
            let entry = self.entries.pop_back().unwrap();
            self.slots -= 1 + entry.fds.len();
            entry.fds
        } else {
            self.entries.back_mut().unwrap().end = pending.at + count;
            Vec::new()
        }
    }

    /// Decides what a receive attempt takes: `at` is the offset of the first byte that is queued,
    /// `queued` the number of bytes, and `want` the number that the call still wants. Returns
    /// how many bytes the attempt takes at the most, and whether the call is over after it,
    /// whatever it has wanted, because the attempt has taken descriptors, or because the next bytes
    /// are from another sender. The caller must take the bytes that it has been told to, for the
    /// entries to match the bytes. The descriptors that the attempt takes are moved to `call`, and
    /// the entries that it uses up are gone, unless it `peek`s, which copies the descriptors and
    /// leaves the entries. A call that is over takes nothing any more.
    pub fn take(
        &mut self,
        at: usize,
        queued: usize,
        want: usize,
        passcred: bool,
        peek: bool,
        call: &mut Call<F, C>,
    ) -> (usize, bool) {
        if call.ended {
            return (0, true);
        }
        // The offset after the last byte that the attempt takes
        let mut end = at + min(queued, want);
        if end == at {
            return (0, false);
        }
        // The offset of the first byte that is not looked at
        let mut pos = at;
        // The call is over after the attempt
        let mut last = false;

        let mut i = 0;
        while let Some(entry) = self.entries.get_mut(i) {
            i += 1;
            if entry.end <= pos {
                continue;
            }
            if entry.start >= end {
                break;
            }
            // The bytes before the entry have no control data
            if entry.start > pos {
                if !call.admit(None, passcred) {
                    end = pos;
                    last = true;
                    break;
                }
                pos = entry.start;
            }
            if !call.admit(entry.cred, passcred) {
                end = pos;
                last = true;
                break;
            }
            if !entry.fds.is_empty() {
                if peek {
                    call.fds.extend(entry.fds.iter().cloned());
                } else {
                    self.slots -= entry.fds.len();
                    call.fds.append(&mut entry.fds);
                }
                end = min(end, entry.end);
                last = true;
                break;
            }
            pos = min(entry.end, end);
            if pos >= end {
                break;
            }
        }
        // The bytes after the last entry have no control data, either
        if !last && pos < end && !call.admit(None, passcred) {
            end = pos;
            last = true;
        }
        call.ended = last;

        // Linux goes on looking at the messages that follow, although a peek has all the bytes that it
        // wants, until it finds descriptors, a message of another sender, or the end of the queue
        if peek && !last {
            self.peek_ahead(at + queued, end, call);
        }

        if !peek {
            // Drop the entries that are used up, but not the one that `pushed` is to finish
            while let Some(entry) = self.entries.front() {
                if entry.end > end || (self.pending.is_some() && self.entries.len() == 1) {
                    break;
                }
                self.slots -= 1 + entry.fds.len();
                self.entries.pop_front();
            }
        }
        (end - at, last)
    }

    /// Copies the descriptors of the first message that follows the bytes of a peek (up to `end`)
    /// and has some, unless a message of another sender comes first. `visible` is the offset after
    /// the last queued byte.
    fn peek_ahead(&self, visible: usize, end: usize, call: &mut Call<F, C>) {
        let mut pos = end;
        for entry in self.entries.iter() {
            if entry.end <= pos {
                continue;
            }
            if entry.start >= visible {
                break;
            }
            // The bytes before the entry have no control data
            if entry.start > pos && !call.is_sender(None) {
                break;
            }
            if !call.is_sender(entry.cred) {
                break;
            }
            if !entry.fds.is_empty() {
                call.fds.extend(entry.fds.iter().cloned());
                break;
            }
            pos = min(entry.end, visible);
        }
    }

    /// Takes all the descriptors, for the caller to drop them where it holds no lock
    pub fn drain_fds(&mut self) -> Vec<F> {
        let mut fds = Vec::new();
        for entry in self.entries.iter_mut() {
            fds.append(&mut entry.fds);
        }
        self.entries.clear();
        self.slots = 0;
        self.pending = None;
        fds
    }
}

#[cfg(test)]
#[path = "entries/tests.rs"]
mod tests;
