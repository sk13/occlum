use super::*;
use process::Process;

pub use self::access::{do_faccessat, AccessibilityCheckMode};
pub use self::chmod::{do_fchmod, do_fchmodat, FileMode};
pub use self::chown::{do_fchown, do_fchownat, ChownFlags};
pub use self::close::do_close;
pub use self::dup::{do_dup, do_dup2, do_dup3};
pub use self::fallocate::{do_fallocate, FallocateFlags};
pub use self::fcntl::{do_fcntl, FcntlCmd};
pub use self::file_flags::{AccessMode, CreationFlags, StatusFlags, STATUS_FLAGS_MASK};
pub use self::flock::do_flock;
pub use self::fspath::{get_abs_path_by_fd, FsPath, AT_FDCWD};
pub use self::fsync::{do_fdatasync, do_fsync};
pub use self::getdents::{do_getdents, do_getdents64};
pub use self::ioctl::{
    do_ioctl, occlum_ocall_ioctl, BuiltinIoctlNum, GetAtMark, GetIfConf, GetIfReqWithRawCmd,
    GetReadBufLen, GetWinSize, GetWriteBufLen, IfConf, IoctlCmd, IoctlRawCmd, NonBuiltinIoctlCmd,
    SetNonBlocking, SetWinSize, StructuredIoctlArgType, StructuredIoctlNum, TcGets, TcSets,
};
pub use self::link::{do_linkat, LinkFlags};
pub use self::lseek::do_lseek;
pub use self::mkdir::do_mkdirat;
pub use self::mknod::do_mknodat;
pub use self::open::do_openat;
pub use self::read::{do_pread, do_preadv, do_read, do_readv};
pub use self::rename::do_renameat;
pub use self::rmdir::do_rmdir;
pub use self::sendfile::do_sendfile;
pub use self::stat::{do_fstat, do_fstatat, Stat, StatFlags};
pub use self::symlink::{do_readlinkat, do_symlinkat};
pub use self::truncate::{do_ftruncate, do_truncate};
pub use self::unlink::{do_unlinkat, UnlinkFlags};
pub use self::utimes::{
    do_utimes_fd, do_utimes_path, get_utimes, utimbuf_t, Utime, UtimeFlags, UTIME_OMIT,
};
pub use self::write::{do_pwrite, do_pwritev, do_write, do_writev};

mod access;
mod chmod;
mod chown;
mod close;
mod dup;
mod fallocate;
mod fcntl;
mod file_flags;
mod flock;
mod fspath;
mod fsync;
mod getdents;
mod ioctl;
mod link;
mod lseek;
mod mkdir;
mod mknod;
mod open;
mod read;
mod rename;
mod rmdir;
mod sendfile;
mod stat;
mod symlink;
mod truncate;
mod unlink;
mod utimes;
mod write;

/// The SDK's libc wrappers of the OCALLs that read or write (read, pread, readv, preadv, write, pwrite, writev, pwritev, ...)
/// keep the data in a buffer on the stack of the host when the count is at most this (MAX_OCALL_ALLOC_SIZE of sgx_libc): a
/// read copies it to the caller's buffer after the OCALL, a write fills it from the caller's buffer before. If a page of
/// the caller's buffer is not committed yet (EDMM commits it on the first access), the #PF of that copy is delivered on the
/// host stack too and overwrites the data of the buffer: a read returns garbage after the page boundary, a write puts
/// garbage into the file. So a buffer this small is committed before the call. A larger one goes through a buffer on the
/// heap of the enclave, which a #PF does not touch.
const OCALL_STACK_BUFFER_MAX: usize = 0x4000;

pub(super) fn commit_for_ocall(buf: &[u8]) {
    if buf.is_empty() || buf.len() > OCALL_STACK_BUFFER_MAX {
        return;
    }
    // Best effort: a buffer that is not mapped makes the read fail by itself.
    let _ = current!().vm().commit_user_range(buf.as_ptr() as usize, buf.len());
}
