//! The control messages of a unix stream socket: the file descriptors of SCM_RIGHTS and the
//! credentials of SCM_CREDENTIALS and SO_PASSCRED.
//!
//! They do not travel in the bytes of the stream, but next to them, in a queue per direction of
//! the connection, and the offsets of the bytes tell which of them a message belongs to (see
//! `entries.rs` for the rules, which are those of Linux). The sender puts the files and the
//! credentials into the queue when it sends, so the receiver does not depend on the descriptors
//! or the thread of the sender any more: the files stay open as long as they are queued, also if
//! the sender closes its descriptors or exits, and they are released when the receiver takes them
//! or is closed.
//!
//! A queue is shared by the sending end and the receiving end of one direction. The locks are
//! taken in this order: that of the stream, which is the producer for a sender and the consumer
//! for a receiver, and then that of the queue, and nothing locks the stream while it holds the
//! queue. The files are dropped outside the locks: the last reference to a unix socket may be one
//! of them, and dropping a socket locks its stream. They are also dropped one after the other, not
//! one within the other (see `release`): a socket in a queue may hold sockets in its own queue.
//!
//! Linux collects the sockets that only the control messages in their own receive queue refer to,
//! e.g., a socket that was sent to itself and is closed. Here such a socket, and the files in its
//! queue, stay open until the LibOS ends: nothing breaks the cycle of references.

use super::*;
use entries::{Call, Entries, Message};
use fs::channel::PushMark;
use spin::Once;
use std::cell::RefCell;
use std::mem::size_of;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};

mod entries;

thread_local! {
    // The files that a release in this thread has still to drop, if one is going on
    static RELEASING: RefCell<Option<Vec<FileRef>>> = RefCell::new(None);
}

/// Drops the files of a queue that is closed. A file may be a socket whose own queue holds files,
/// which may be sockets, and so on: a program can send sockets through sockets level after level,
/// and the drops, one within the other, would use up the stack of the thread when the last of
/// them is closed. So a release that finds another one going on in this thread leaves its files
/// to it, which drops them after its own, and the nesting stays shallow, whatever the number of
/// levels.
fn release(mut files: Vec<FileRef>) {
    if files.is_empty() {
        return;
    }
    let nested = RELEASING.with(|releasing| {
        let mut releasing = releasing.borrow_mut();
        match releasing.as_mut() {
            Some(pending) => {
                pending.append(&mut files);
                true
            }
            None => {
                *releasing = Some(Vec::new());
                false
            }
        }
    });
    if nested {
        return;
    }
    drop(files);
    // The drops add the files of the sockets that they close
    while let Some(files) = RELEASING.with(|releasing| {
        let mut releasing = releasing.borrow_mut();
        let pending = releasing.as_mut().unwrap();
        if pending.is_empty() {
            *releasing = None;
            None
        } else {
            Some(std::mem::take(pending))
        }
    }) {
        drop(files);
    }
}

/// The size of the header of a control message: struct cmsghdr
const CMSG_HDR_LEN: usize = size_of::<libc::cmsghdr>();
const SCM_RIGHTS: i32 = libc::SCM_RIGHTS;
const SCM_CREDENTIALS: i32 = libc::SCM_CREDENTIALS;
/// The most descriptors that a message holds, in all its control messages (SCM_MAX_FD)
const MAX_FDS: usize = 253;

/// The credentials of a process, which a message carries: struct ucred
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ucred {
    pid: u32,
    uid: u32,
    gid: u32,
}

impl Ucred {
    /// What a receiver sees for bytes that were sent without credentials: nobody
    const UNKNOWN: Ucred = Ucred {
        pid: 0,
        uid: 65534,
        gid: 65534,
    };
    /// What it sees when the call received no bytes at all
    const ZERO: Ucred = Ucred {
        pid: 0,
        uid: 0,
        gid: 0,
    };

    /// The credentials of the current process
    pub fn of_current() -> Self {
        Self {
            pid: current!().process().pid(),
            uid: process::do_getuid().unwrap() as u32,
            gid: process::do_getgid().unwrap() as u32,
        }
    }

    fn to_bytes(&self) -> [u8; 12] {
        let mut bytes = [0; 12];
        bytes[0..4].copy_from_slice(&(self.pid as i32).to_ne_bytes());
        bytes[4..8].copy_from_slice(&self.uid.to_ne_bytes());
        bytes[8..12].copy_from_slice(&self.gid.to_ne_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        let word = |i: usize| u32::from_ne_bytes(bytes[i * 4..i * 4 + 4].try_into().unwrap());
        Self {
            pid: word(0),
            uid: word(1),
            gid: word(2),
        }
    }
}

type Queue = Entries<FileRef, Ucred>;

/// The queues of the control messages of a connection, one for each end: that of the bytes that
/// the end receives. The two ends share them in one allocation.
pub struct ControlPair {
    queues: [ControlQueue; 2],
}

impl ControlPair {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            queues: [ControlQueue::new(), ControlQueue::new()],
        })
    }

    /// Returns the queue of the bytes that the end `side` (0 or 1) receives
    pub fn queue(&self, side: usize) -> &ControlQueue {
        &self.queues[side]
    }
}

/// The control messages that travel in one direction of a connection. Most connections never carry
/// one, so the entries are allocated when the first message or credentials are queued.
pub struct ControlQueue {
    /// The number of entries, which a receive reads without the lock: if it is zero, the bytes
    /// that it takes have no control data. A sender changes it before the bytes that its entry
    /// belongs to become visible, and the receiver reads it after it has seen the bytes.
    len: AtomicUsize,
    /// Whether the receiver (`RECV`) or the sender (`SEND`) has SO_PASSCRED, or the receiver waits
    /// in the queue of a listener (`EMBRYO`)
    passcred: AtomicU8,
    /// Whether the receiver is closed, so that nobody looks at the entries any more
    closed: AtomicBool,
    entries: Once<Box<SgxMutex<Queue>>>,
}

const RECV: u8 = 1;
const SEND: u8 = 2;
const EMBRYO: u8 = 4;

impl ControlQueue {
    fn new() -> Self {
        Self {
            len: AtomicUsize::new(0),
            passcred: AtomicU8::new(0),
            closed: AtomicBool::new(false),
            entries: Once::new(),
        }
    }

    fn entries(&self) -> &SgxMutex<Queue> {
        self.entries
            .call_once(|| Box::new(SgxMutex::new(Queue::default())))
    }

    pub fn set_recv_passcred(&self, on: bool) {
        self.set_passcred(RECV, on)
    }

    pub fn set_send_passcred(&self, on: bool) {
        self.set_passcred(SEND, on)
    }

    /// Tells that the receiver has not been accepted yet: it has no socket file, so the sender's
    /// credentials are recorded
    pub fn set_embryo(&self, on: bool) {
        self.set_passcred(EMBRYO, on)
    }

    fn set_passcred(&self, who: u8, on: bool) {
        if on {
            self.passcred.fetch_or(who, Ordering::Release);
        } else {
            self.passcred.fetch_and(!who, Ordering::Release);
        }
    }

    /// Whether the receiver gets the credentials of the sender
    fn recv_passcred(&self) -> bool {
        self.passcred.load(Ordering::Acquire) & RECV != 0
    }

    /// Whether a send has to tell its credentials: the receiver or the sender has SO_PASSCRED, or
    /// the receiver is not accepted yet
    pub fn records_creds(&self) -> bool {
        self.passcred.load(Ordering::Acquire) != 0
    }

    /// Drops the queued files, for the receiver is closed. A sender that is still there does not
    /// queue any more.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        // The entries are there if a message was queued, or a receive asked for credentials. A
        // send that creates them just now may still queue a message, which goes when its sender is
        // closed, too.
        let fds = match self.entries.get() {
            Some(entries) => {
                let mut entries = entries.lock().unwrap();
                self.len.store(0, Ordering::Release);
                entries.drain_fds()
            }
            None => Vec::new(),
        };
        release(fds);
    }

    /// Returns what a receive asks before each attempt (see `PopGate`): how many of the queued
    /// bytes it takes. The control data that belongs to them goes to `received`. A peek takes
    /// copies of the files and leaves them queued.
    pub fn gate<'a>(
        &'a self,
        received: &'a mut Received,
        peek: bool,
    ) -> impl FnMut(usize, usize, usize) -> (usize, bool) + 'a {
        move |at, queued, want| {
            let passcred = self.recv_passcred();
            received.passcred = passcred;
            // Nothing is queued after the bytes that the receive sees now, as an entry is queued
            // before its bytes
            if received.call.is_new() && !passcred && self.len.load(Ordering::Acquire) == 0 {
                return (queued.min(want), false);
            }
            let mut entries = self.entries().lock().unwrap();
            let result = entries.take(at, queued, want, passcred, peek, &mut received.call);
            self.len.store(entries.len(), Ordering::Release);
            result
        }
    }
}

/// The control data that a receive has taken
#[derive(Default)]
pub struct Received {
    call: Call<FileRef, Ucred>,
    /// Whether the socket had SO_PASSCRED when the receive last asked for bytes
    passcred: bool,
}

impl Received {
    /// Whether the receive stopped at control data: it has taken descriptors, or it stopped before
    /// the bytes of another sender
    pub fn stopped_at_control(&self) -> bool {
        self.call.is_over()
    }

    /// Writes the control messages of a receive into the buffer of the program, as Linux does:
    /// the credentials if the socket has SO_PASSCRED, and the files, which get new descriptors in
    /// the current process. What does not fit, or has no buffer, is dropped. Returns the length of
    /// the control messages, and whether any were dropped (MSG_CTRUNC).
    pub fn write_control(self, control: Option<&mut [u8]>, cloexec: bool) -> (usize, bool) {
        let Received { call, passcred } = self;
        let fds = call.fds;
        let control = match control {
            Some(control) => control,
            None => return (0, passcred || !fds.is_empty()),
        };
        let mut writer = CmsgWriter {
            buf: control,
            len: 0,
            truncated: false,
        };
        if passcred {
            let cred = match call.cred {
                Some(Some(cred)) => cred,
                Some(None) => Ucred::UNKNOWN,
                None => Ucred::ZERO,
            };
            writer.put(libc::SOL_SOCKET, SCM_CREDENTIALS, &cred.to_bytes());
        }
        if !fds.is_empty() {
            writer.put_files(fds, cloexec);
        }
        (writer.len, writer.truncated)
    }
}

/// Writes control messages like put_cmsg() and scm_detach_fds() of Linux
struct CmsgWriter<'a> {
    buf: &'a mut [u8],
    /// The bytes that are used
    len: usize,
    truncated: bool,
}

impl CmsgWriter<'_> {
    /// Writes a control message, or as much of it as fits
    fn put(&mut self, level: i32, type_: i32, data: &[u8]) {
        let room = self.buf.len() - self.len;
        if room < CMSG_HDR_LEN {
            self.truncated = true;
            return;
        }
        let mut cmsg_len = CMSG_HDR_LEN + data.len();
        if room < cmsg_len {
            self.truncated = true;
            cmsg_len = room;
        }
        let at = self.len;
        self.buf[at..at + 8].copy_from_slice(&cmsg_len.to_ne_bytes());
        self.buf[at + 8..at + 12].copy_from_slice(&level.to_ne_bytes());
        self.buf[at + 12..at + 16].copy_from_slice(&type_.to_ne_bytes());
        self.buf[at + CMSG_HDR_LEN..at + cmsg_len]
            .copy_from_slice(&data[..cmsg_len - CMSG_HDR_LEN]);
        self.len += cmsg_align(CMSG_HDR_LEN + data.len()).min(room);
    }

    /// Writes the files that fit as descriptors of the current process in one control message
    fn put_files(&mut self, files: Vec<FileRef>, cloexec: bool) {
        let room = self.buf.len() - self.len;
        let max = if room <= CMSG_HDR_LEN {
            0
        } else {
            (room - CMSG_HDR_LEN) / size_of::<i32>()
        };
        if files.len() > max {
            self.truncated = true;
        }
        let mut data = Vec::new();
        for file in files.into_iter().take(max) {
            let fd = current!().add_file(file, cloexec);
            data.extend_from_slice(&(fd as i32).to_ne_bytes());
        }
        if !data.is_empty() {
            self.put(libc::SOL_SOCKET, SCM_RIGHTS, &data);
        }
    }
}

fn cmsg_align(len: usize) -> usize {
    (len + size_of::<usize>() - 1) & !(size_of::<usize>() - 1)
}

/// The control data that a send attaches to its bytes: the files and the credentials that its
/// control messages name
#[derive(Default)]
pub struct Outgoing {
    msg: Message<FileRef, Ucred>,
}

impl Outgoing {
    /// Whether the send has nothing to tell the receiver
    pub fn is_empty(&self) -> bool {
        self.msg.fds.is_empty() && self.msg.cred.is_none()
    }

    /// Reads the control messages of a send, and takes the files of the descriptors, which must be
    /// open in the current process, as the sender may close them as soon as the call returns.
    /// Like Linux, it fails for a control message that has an invalid length (EINVAL), a
    /// descriptor that is not open (EBADF), too many descriptors in all (EINVAL), credentials of
    /// another process (EPERM), and a control message of SOL_SOCKET that is neither of the two
    /// (EINVAL). It ignores the control messages of other levels.
    pub fn parse(control: &[u8]) -> Result<Self> {
        let mut msg = Message::default();
        let mut offset = 0;
        // As CMSG_FIRSTHDR and CMSG_NXTHDR do: the header of the next control message must fit
        while offset + CMSG_HDR_LEN <= control.len() {
            let hdr = &control[offset..];
            let cmsg_len = usize::from_ne_bytes(hdr[0..8].try_into().unwrap());
            let level = i32::from_ne_bytes(hdr[8..12].try_into().unwrap());
            let type_ = i32::from_ne_bytes(hdr[12..16].try_into().unwrap());
            if cmsg_len < CMSG_HDR_LEN || cmsg_len > hdr.len() {
                return_errno!(EINVAL, "the length of the control message is invalid");
            }
            let data = &hdr[CMSG_HDR_LEN..cmsg_len];
            if level == libc::SOL_SOCKET {
                match type_ {
                    SCM_RIGHTS => Self::parse_rights(data, &mut msg.fds)?,
                    SCM_CREDENTIALS => msg.cred = Some(Self::parse_credentials(data)?),
                    _ => return_errno!(EINVAL, "the type of the control message is unknown"),
                }
            }
            offset += cmsg_align(cmsg_len);
        }
        Ok(Self { msg })
    }

    fn parse_rights(data: &[u8], files: &mut Vec<FileRef>) -> Result<()> {
        let num = data.len() / size_of::<i32>();
        if num > MAX_FDS || files.len() + num > MAX_FDS {
            return_errno!(EINVAL, "too many file descriptors");
        }
        for fd_bytes in data.chunks_exact(size_of::<i32>()) {
            let fd = i32::from_ne_bytes(fd_bytes.try_into().unwrap());
            if fd < 0 {
                return_errno!(EBADF, "not a file descriptor");
            }
            files.push(current!().file(fd as FileDesc)?);
        }
        Ok(())
    }

    /// A process may name its own credentials only: in this single-user LibOS, nobody may be
    /// anybody else
    fn parse_credentials(data: &[u8]) -> Result<Ucred> {
        if data.len() != size_of::<libc::ucred>() {
            return_errno!(EINVAL, "the credentials have a wrong length");
        }
        let cred = Ucred::from_bytes(data);
        if cred.uid == u32::MAX || cred.gid == u32::MAX {
            return_errno!(EINVAL, "the user or group is invalid");
        }
        if cred != Ucred::of_current() {
            return_errno!(EPERM, "the credentials are those of another process");
        }
        Ok(cred)
    }
}

/// Tells the queue of the connection about the bytes of a send, which `Producer` calls with the
/// producer locked (see `PushMark`)
pub struct Mark<'a> {
    queue: &'a ControlQueue,
    msg: Message<FileRef, Ucred>,
    /// The credentials of the current process, as soon as a send has to tell them
    own: Option<Ucred>,
}

impl<'a> Mark<'a> {
    pub fn new(queue: &'a ControlQueue, outgoing: Outgoing) -> Self {
        Self {
            queue,
            msg: outgoing.msg,
            own: None,
        }
    }
}

impl PushMark for Mark<'_> {
    fn before(&mut self, at: usize, room: usize) -> bool {
        let mut entries = self.queue.entries().lock().unwrap();
        // The receiver is closed: the bytes go nowhere, nor do the files
        if self.queue.closed.load(Ordering::Acquire) {
            return true;
        }
        // The credentials that the send names, or else its own, if a side wants them
        let mut msg = Message {
            cred: self.msg.cred,
            fds: std::mem::take(&mut self.msg.fds),
        };
        if msg.cred.is_none() && self.queue.records_creds() {
            msg.cred = Some(*self.own.get_or_insert_with(Ucred::of_current));
        }
        let registered = entries.register(at, room, &mut msg);
        // The files stay with the send if the entries are full, for the next attempt
        self.msg.fds = msg.fds;
        self.queue.len.store(entries.len(), Ordering::Release);
        registered
    }

    fn after(&mut self, pushed: usize) {
        let mut entries = self.queue.entries().lock().unwrap();
        // The descriptors of a send that pushed no bytes are dropped with the send, not here. If
        // the receiver is closed, they are still in the send, and stay there.
        let back = entries.pushed(pushed);
        if !back.is_empty() {
            self.msg.fds = back;
        }
        self.queue.len.store(entries.len(), Ordering::Release);
    }
}
