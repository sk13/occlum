//! The named pipes (FIFOs) that a directory of the host (HostFS) holds.
//!
//! A named pipe is the way a program in the enclave and a program outside of it, e.g., the one that
//! controls it, exchange bytes through the file system. The FIFO is a FIFO of the host, so both ends
//! see each other. The enclave opens it as a file of the host, like a host socket, and reads and writes
//! it with calls of the host: a read of a blocking file that finds no data blocks in the host and is
//! ended by the interrupts of the LibOS, as the calls of a host socket are.
//!
//! The directories of the enclave's own file systems (SEFS, the root file system) can not hold a named
//! pipe: they have no file type for it, and the other end of it would be in the enclave, too.

use atomic::{Atomic, Ordering};
use std::any::Any;
use std::ffi::CString;
use std::io::SeekFrom;

use super::hostfs::HNode;
use super::*;
use crate::fs::{
    AccessMode, File, GetReadBufLen, HostFd, IoEvents, IoNotifier, IoctlCmd, SetNonBlocking,
    StatusFlags, STATUS_FLAGS_MASK,
};
use crate::interrupt::retry_stale_interrupt;
use rcore_fs_mountfs::MNode;

/// The file of a named pipe at the host.
#[derive(Debug)]
pub struct HostFifo {
    host_fd: HostFd,
    host_events: Atomic<IoEvents>,
    notifier: IoNotifier,
    access_mode: AccessMode,
}

impl HostFifo {
    /// Opens the named pipe that `inode` is, if it is one of the host.
    pub fn open(inode: &Arc<dyn INode>, flags: u32) -> Result<Self> {
        let hnode = host_node(inode)
            .ok_or_else(|| errno!(ENXIO, "the named pipe is not in a directory of the host"))?;
        let access_mode = AccessMode::from_u32(flags)?;
        let status_flags = StatusFlags::from_bits_truncate(flags);

        // The host decides how a blocking open of a named pipe waits for the other end. The close-on-exec
        // flag is of the LibOS, which closes the file, and the path was resolved by the LibOS.
        let host_flags = access_mode.clone() as i32
            | if status_flags.contains(StatusFlags::O_NONBLOCK) {
                libc::O_NONBLOCK
            } else {
                0
            };
        let path = CString::new(hnode.host_path().to_string_lossy().as_bytes())
            .map_err(|_| errno!(EINVAL, "invalid path"))?;
        let raw_host_fd = retry_stale_interrupt(|| {
            Ok(try_libc!(libc::ocall::open64(path.as_ptr(), host_flags, 0)))
        })? as FileDesc;
        Ok(Self {
            host_fd: HostFd::new(raw_host_fd),
            host_events: Atomic::new(IoEvents::empty()),
            notifier: IoNotifier::new(),
            access_mode,
        })
    }

    fn raw_host_fd(&self) -> i32 {
        self.host_fd.to_raw() as i32
    }
}

/// The node of the host that `inode` is, if it is one.
fn host_node(inode: &Arc<dyn INode>) -> Option<&HNode> {
    let inode: &dyn INode = &**inode;
    let inner = match inode.downcast_ref::<MNode>() {
        Some(mnode) => &*mnode.inode,
        None => inode,
    };
    inner.downcast_ref::<HNode>()
}

impl File for HostFifo {
    fn read(&self, buf: &mut [u8]) -> Result<usize> {
        if !self.access_mode.readable() {
            return_errno!(EBADF, "the named pipe is not open for reading");
        }
        if buf.is_empty() {
            return Ok(0);
        }
        let len = retry_stale_interrupt(|| {
            Ok(try_libc!(libc::ocall::read(
                self.raw_host_fd(),
                buf.as_mut_ptr() as *mut libc::c_void,
                buf.len()
            )))
        })?;
        // The host may report more than it was asked for
        if len as usize > buf.len() {
            return_errno!(EINVAL, "the host read more than asked for");
        }
        Ok(len as usize)
    }

    fn write(&self, buf: &[u8]) -> Result<usize> {
        if !self.access_mode.writable() {
            return_errno!(EBADF, "the named pipe is not open for writing");
        }
        if buf.is_empty() {
            return Ok(0);
        }
        let len = retry_stale_interrupt(|| {
            Ok(try_libc_may_epipe!(libc::ocall::write(
                self.raw_host_fd(),
                buf.as_ptr() as *const libc::c_void,
                buf.len()
            )))
        })?;
        if len as usize > buf.len() {
            return_errno!(EINVAL, "the host wrote more than asked for");
        }
        Ok(len as usize)
    }

    fn read_at(&self, offset: usize, buf: &mut [u8]) -> Result<usize> {
        if offset != 0 {
            return_errno!(ESPIPE, "a nonzero position is not supported");
        }
        self.read(buf)
    }

    fn write_at(&self, offset: usize, buf: &[u8]) -> Result<usize> {
        if offset != 0 {
            return_errno!(ESPIPE, "a nonzero position is not supported");
        }
        self.write(buf)
    }

    fn seek(&self, _pos: SeekFrom) -> Result<off_t> {
        return_errno!(ESPIPE, "a named pipe does not support seek")
    }

    fn metadata(&self) -> Result<Metadata> {
        Ok(Metadata {
            dev: 0,
            inode: 0,
            size: 0,
            blk_size: 0,
            blocks: 0,
            atime: Timespec { sec: 0, nsec: 0 },
            mtime: Timespec { sec: 0, nsec: 0 },
            ctime: Timespec { sec: 0, nsec: 0 },
            type_: FileType::NamedPipe,
            mode: 0o600,
            nlinks: 1,
            uid: 0,
            gid: 0,
            rdev: 0,
        })
    }

    fn ioctl(&self, cmd: &mut dyn IoctlCmd) -> Result<()> {
        match_ioctl_cmd_mut!(&mut *cmd, {
            cmd: GetReadBufLen => {
                cmd.execute(self.host_fd.to_raw())?;
            },
            cmd: SetNonBlocking => {
                cmd.execute(self.host_fd.to_raw())?;
            },
            _ => {
                return_errno!(EINVAL, "Not supported yet");
            }
        });
        Ok(())
    }

    fn access_mode(&self) -> Result<AccessMode> {
        Ok(self.access_mode.clone())
    }

    fn status_flags(&self) -> Result<StatusFlags> {
        let ret = try_libc!(libc::ocall::fcntl_arg0(self.raw_host_fd(), libc::F_GETFL));
        Ok(StatusFlags::from_bits_truncate(ret as u32))
    }

    fn set_status_flags(&self, new_status_flags: StatusFlags) -> Result<()> {
        let raw_status_flags = (new_status_flags & STATUS_FLAGS_MASK).bits();
        try_libc!(libc::ocall::fcntl_arg1(
            self.raw_host_fd(),
            libc::F_SETFL,
            raw_status_flags as c_int
        ));
        Ok(())
    }

    fn poll_new(&self) -> IoEvents {
        self.host_events.load(Ordering::Acquire)
    }

    fn host_fd(&self) -> Option<&HostFd> {
        Some(&self.host_fd)
    }

    fn notifier(&self) -> Option<&IoNotifier> {
        Some(&self.notifier)
    }

    fn update_host_events(&self, ready: &IoEvents, mask: &IoEvents, trigger_notifier: bool) {
        self.host_events.update(ready, mask, Ordering::Release);

        if trigger_notifier {
            self.notifier.broadcast(ready);
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// The file that opening the named pipe `inode` gives.
pub fn open_fifo(inode: &Arc<dyn INode>, flags: u32) -> Result<Arc<dyn File>> {
    Ok(Arc::new(HostFifo::open(inode, flags)?))
}

pub trait AsHostFifo {
    fn as_host_fifo(&self) -> Result<&HostFifo>;
}

impl AsHostFifo for FileRef {
    fn as_host_fifo(&self) -> Result<&HostFifo> {
        self.as_any()
            .downcast_ref::<HostFifo>()
            .ok_or_else(|| errno!(EBADF, "not a named pipe"))
    }
}
