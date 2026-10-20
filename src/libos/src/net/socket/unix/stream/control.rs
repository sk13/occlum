//! The control messages of a unix stream socket: the file descriptors of SCM_RIGHTS.
//!
//! They do not travel in the bytes of the stream, but next to them, in a queue per direction of
//! the connection, and the offsets of the bytes tell which of them a message belongs to (see
//! `entries.rs` for the rules, which are those of Linux). The sender puts the files into the
//! queue when it sends, so the receiver does not depend on the descriptors or the thread of the
//! sender any more: the files stay open as long as they are queued, also if the sender closes its
//! descriptors or exits, and they are released when the receiver takes them or is closed.
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
use std::cell::RefCell;
use std::mem::size_of;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

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
/// The most descriptors that a message holds, in all its control messages (SCM_MAX_FD)
const MAX_FDS: usize = 253;

/// The control data has no credentials yet
type Queue = Entries<FileRef, ()>;

/// The control messages that travel in one direction of a connection
pub struct ControlQueue {
    /// The number of entries, which a receive reads without the lock: if it is zero, the bytes
    /// that it takes have no control data. A sender changes it before the bytes that its entry
    /// belongs to become visible, and the receiver reads it after it has seen the bytes.
    len: AtomicUsize,
    /// Whether the receiver is closed, so that nobody looks at the entries any more
    closed: AtomicBool,
    entries: SgxMutex<Queue>,
}

impl ControlQueue {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            len: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
            entries: SgxMutex::new(Queue::default()),
        })
    }

    /// Drops the queued files, for the receiver is closed. A sender that is still there does not
    /// queue any more.
    pub fn close(&self) {
        let fds = {
            let mut entries = self.entries.lock().unwrap();
            self.closed.store(true, Ordering::Release);
            self.len.store(0, Ordering::Release);
            entries.drain_fds()
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
            // Nothing is queued after the bytes that the receive sees now, as an entry is queued
            // before its bytes
            if received.call.is_new() && self.len.load(Ordering::Acquire) == 0 {
                return (queued.min(want), false);
            }
            let mut entries = self.entries.lock().unwrap();
            let result = entries.take(at, queued, want, false, peek, &mut received.call);
            self.len.store(entries.len(), Ordering::Release);
            result
        }
    }
}

/// The control data that a receive has taken
#[derive(Default)]
pub struct Received {
    call: Call<FileRef, ()>,
}

impl Received {
    /// Writes the control messages of a receive into the buffer of the program, as Linux does: the
    /// files get new descriptors in the current process. What does not fit, or has no buffer, is
    /// dropped. Returns the length of the control messages, and whether any were dropped
    /// (MSG_CTRUNC).
    pub fn write_control(self, control: Option<&mut [u8]>, cloexec: bool) -> (usize, bool) {
        let fds = self.call.fds;
        let control = match control {
            Some(control) => control,
            None => return (0, !fds.is_empty()),
        };
        let mut writer = CmsgWriter {
            buf: control,
            len: 0,
            truncated: false,
        };
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

/// The control data that a send attaches to its bytes: the files that its control messages name
#[derive(Default)]
pub struct Outgoing {
    msg: Message<FileRef, ()>,
}

impl Outgoing {
    /// Whether the send has nothing to tell the receiver
    pub fn is_empty(&self) -> bool {
        self.msg.fds.is_empty()
    }

    /// Reads the control messages of a send, and takes the files of the descriptors, which must be
    /// open in the current process, as the sender may close them as soon as the call returns.
    /// Like Linux, it fails for a control message that has an invalid length (EINVAL), a
    /// descriptor that is not open (EBADF), too many descriptors in all (EINVAL), and a control
    /// message of SOL_SOCKET of another type (EINVAL). It ignores the control messages of other
    /// levels.
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
                    // The credentials are not delivered yet
                    libc::SCM_CREDENTIALS => (),
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
}

/// Tells the queue of the connection about the bytes of a send, which `Producer` calls with the
/// producer locked (see `PushMark`)
pub struct Mark<'a> {
    queue: &'a ControlQueue,
    msg: Message<FileRef, ()>,
}

impl<'a> Mark<'a> {
    pub fn new(queue: &'a ControlQueue, outgoing: Outgoing) -> Self {
        Self {
            queue,
            msg: outgoing.msg,
        }
    }
}

impl PushMark for Mark<'_> {
    fn before(&mut self, at: usize, room: usize) -> bool {
        let mut entries = self.queue.entries.lock().unwrap();
        // The receiver is closed: the bytes go nowhere, nor do the files
        if self.queue.closed.load(Ordering::Acquire) {
            return true;
        }
        let mut msg = Message {
            cred: None,
            fds: std::mem::take(&mut self.msg.fds),
        };
        let registered = entries.register(at, room, &mut msg);
        // The files stay with the send if the entries are full, for the next attempt
        self.msg.fds = msg.fds;
        self.queue.len.store(entries.len(), Ordering::Release);
        registered
    }

    fn after(&mut self, pushed: usize) {
        let mut entries = self.queue.entries.lock().unwrap();
        // The descriptors of a send that pushed no bytes are dropped with the send, not here
        self.msg.fds = entries.pushed(pushed);
        self.queue.len.store(entries.len(), Ordering::Release);
    }
}
