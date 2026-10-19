use super::*;
use crate::fs::FileId;
use std::path::{Path, PathBuf};
use std::{cmp, mem, slice, str};

const SUN_FAMILY_LEN: usize = mem::size_of::<libc::sa_family_t>();

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UnixAddr {
    Unnamed,
    File(Option<FileId>, UnixPath), // An optional file and path. Use the file if there is one.
    Abstract(Vec<u8>),              // The name after the leading NUL. It can have any bytes.
}

impl UnixAddr {
    /// Construct a unix address from its C counterpart.
    ///
    /// The argument `c_len` specifies the length of the valid part in the given
    /// C address.
    pub fn from_c(c_addr: &libc::sockaddr_un, c_len: usize) -> Result<Self> {
        // Sanity checks
        if c_addr.sun_family != libc::AF_UNIX as libc::sa_family_t {
            return_errno!(EINVAL, "an unix address is expected");
        }
        if c_len < SUN_FAMILY_LEN {
            return_errno!(EINVAL, "the length of the address is too small");
        } else if c_len > std::mem::size_of::<libc::sockaddr_un>() {
            return_errno!(EINVAL, "the length of the address is too large");
        }

        if c_len == SUN_FAMILY_LEN {
            return Ok(Self::Unnamed);
        }

        // At most the 108 bytes of sun_path, as c_len is checked above
        let path_len = c_len - SUN_FAMILY_LEN;
        // Safety. Converting from &[c_char] to &[u8] is harmless.
        let path: &[u8] =
            unsafe { slice::from_raw_parts(c_addr.sun_path.as_ptr() as *const u8, path_len) };

        // A pathname address
        if path[0] != 0 {
            // Like Linux, the pathname ends with the first NUL, or with the
            // address if there is none: a program may count the NUL in the
            // length of the address, or not.
            let end = path.iter().position(|byte| *byte == 0).unwrap_or(path_len);
            let pathname =
                str::from_utf8(&path[..end]).map_err(|_| errno!(EINVAL, "path is not UTF8"))?;

            Ok(Self::File(None, UnixPath::new(pathname)))
        }
        // An abstract address, whose name is made of all the bytes after the
        // first NUL, the NULs at the end included
        else {
            Ok(Self::Abstract(path[1..].to_vec()))
        }
    }

    pub fn from_c_storage(c_addr: &libc::sockaddr_storage, c_addr_len: usize) -> Result<Self> {
        if (c_addr_len) > std::mem::size_of::<libc::sockaddr_storage>() {
            return_errno!(EINVAL, "address length is too large");
        }
        // Safety. Convert from sockaddr_storage to sockaddr_un is harmless.
        let c_addr = unsafe { std::mem::transmute(c_addr) };
        unsafe { Self::from_c(c_addr, c_addr_len) }
    }

    pub fn copy_to_slice(&self, dst: &mut [u8]) -> usize {
        let src = self.to_bytes();
        let copied = std::cmp::min(dst.len(), src.len());
        dst[..copied].copy_from_slice(&src[..copied]);
        copied
    }

    pub fn raw_len(&self) -> usize {
        self.to_bytes().len()
    }

    /// Returns the path of a pathname address, or the name of an abstract
    /// address (without the leading NUL).
    pub fn path_bytes(&self) -> Result<&[u8]> {
        match self {
            Self::File(_, unix_path) => Ok(unix_path.path_str().as_bytes()),
            Self::Abstract(name) => Ok(name),
            Self::Unnamed => return_errno!(EINVAL, "can't get path name for unnamed socket"),
        }
    }

    pub fn to_c_storage(&self) -> (libc::sockaddr_storage, usize) {
        let bytes = self.to_bytes();
        let mut c_storage = unsafe { mem::zeroed::<libc::sockaddr_storage>() };
        // Safety. Converting from sockaddr_storage to &mut [u8] is harmless.
        let dst = unsafe {
            slice::from_raw_parts_mut(
                &mut c_storage as *mut _ as *mut u8,
                mem::size_of::<libc::sockaddr_storage>(),
            )
        };
        let len = cmp::min(bytes.len(), dst.len());
        dst[..len].copy_from_slice(&bytes[..len]);
        (c_storage, len)
    }

    pub fn to_raw(&self) -> SockAddr {
        let (storage, addr_len) = self.to_c_storage();
        SockAddr::from_c_storage(&storage, addr_len)
    }

    /// The bytes of the C address: the family, and the path with its NUL, or
    /// the name with its leading NUL. Like in Linux, the length of a pathname
    /// address counts the NUL, which does not fit into `sockaddr_un` if the
    /// path has 108 characters.
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = (Domain::LOCAL as libc::sa_family_t).to_ne_bytes().to_vec();
        match self {
            Self::Unnamed => (),
            Self::File(_, unix_path) => {
                bytes.extend_from_slice(unix_path.path_str().as_bytes());
                bytes.push(0);
            }
            Self::Abstract(name) => {
                bytes.push(0);
                bytes.extend_from_slice(name);
            }
        }
        bytes
    }
}

impl Default for UnixAddr {
    fn default() -> Self {
        UnixAddr::Unnamed
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnixPath {
    inner: PathBuf,
    /// Holds the cwd when a relative path is created
    cwd: Option<String>,
}

impl UnixPath {
    pub fn new(path: &str) -> Self {
        let inner = PathBuf::from(path);
        let is_absolute = inner.is_absolute();
        Self {
            inner: inner,
            cwd: if is_absolute {
                None
            } else {
                let thread = current!();
                let fs = thread.fs().read().unwrap();
                let cwd = fs.cwd().to_owned();

                Some(cwd)
            },
        }
    }

    pub fn absolute(&self) -> String {
        let path_str = self.path_str();
        if self.inner.is_absolute() {
            path_str.to_string()
        } else {
            let mut prefix = self.cwd.as_ref().unwrap().clone();
            if prefix.ends_with("/") {
                prefix.push_str(path_str);
            } else {
                prefix.push_str("/");
                prefix.push_str(path_str);
            }
            prefix
        }
    }

    pub fn path_str(&self) -> &str {
        self.inner.to_str().unwrap()
    }
}
