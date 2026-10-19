use super::endpoint::Endpoint;
use super::endpoint::RelayNotifier;
use super::listener::Listener;
use super::*;
use misc::get_random;
use std::collections::btree_map::BTreeMap;
use std::time::Duration;

lazy_static! {
    pub(super) static ref ADDRESS_SPACE: AddressSpace = AddressSpace::new();
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub enum AddressSpaceKey {
    FileKey(usize),
    AbstrKey(Vec<u8>),
}

impl AddressSpaceKey {
    pub fn from_inode(inode: usize) -> Self {
        AddressSpaceKey::FileKey(inode)
    }

    pub fn from_name(name: Vec<u8>) -> Self {
        AddressSpaceKey::AbstrKey(name)
    }
}

/// What a socket bound to a name: the address as it was bound, which the peers
/// of the socket show whatever name they used to connect, and the listener
/// once the socket is listening.
struct Binding {
    addr: UnixAddr,
    listener: Option<Arc<Listener>>,
}

pub struct AddressSpace {
    // For "file", use inode number as "key" instead of path string so that listeners can still
    // be reached even if the socket file is moved or renamed.
    file: SgxMutex<BTreeMap<AddressSpaceKey, Binding>>,
    abstr: SgxMutex<BTreeMap<AddressSpaceKey, Binding>>,
}

impl AddressSpace {
    pub fn new() -> Self {
        Self {
            file: SgxMutex::new(BTreeMap::new()),
            abstr: SgxMutex::new(BTreeMap::new()),
        }
    }

    /// Binds the address, which is automatically chosen if it is unnamed.
    /// Returns the address that was bound.
    pub fn add_binder(&self, addr: &UnixAddr) -> Result<UnixAddr> {
        if let UnixAddr::Unnamed = addr {
            return self.add_autobinder();
        }

        let key = Self::get_key(addr)?;
        let mut space = self.get_space(addr)?;
        if space.contains_key(&key) {
            return_errno!(EADDRINUSE, "the addr is already bound");
        } else {
            space.insert(
                key,
                Binding {
                    addr: addr.clone(),
                    listener: None,
                },
            );
            Ok(addr.clone())
        }
    }

    /// Binds an abstract name of the form that Linux chooses for a socket that
    /// does not ask for one: a NUL and five hexadecimal digits.
    fn add_autobinder(&self) -> Result<UnixAddr> {
        const NAMES: u32 = 0x100000;

        let mut first = [0; 4];
        // The names do not have to be random
        let _ = get_random(&mut first);
        let first = u32::from_ne_bytes(first);

        let mut space = self.abstr.lock().unwrap();
        for i in 0..NAMES {
            let name = format!("{:05x}", first.wrapping_add(i) % NAMES);
            let addr = UnixAddr::Abstract(name.into_bytes());
            let key = Self::get_key(&addr)?;
            if !space.contains_key(&key) {
                space.insert(
                    key,
                    Binding {
                        addr: addr.clone(),
                        listener: None,
                    },
                );
                return Ok(addr);
            }
        }
        return_errno!(ENOSPC, "all the names of the autobind are in use");
    }

    pub(super) fn add_listener(
        &self,
        addr: &UnixAddr,
        backlog: usize,
        nonblocking: bool,
        notifier: Arc<RelayNotifier>,
    ) -> Result<()> {
        let key = Self::get_key(addr).map_err(|_| errno!(EINVAL, "the socket is not bound"))?;
        let mut space = self.get_space(addr)?;

        if let Some(binding) = space.get_mut(&key) {
            if binding.listener.is_none() {
                binding.listener = Some(Arc::new(Listener::new(backlog, nonblocking, notifier)));
                Ok(())
            } else {
                return_errno!(EINVAL, "the socket is already listened");
            }
        } else {
            return_errno!(EINVAL, "the socket is not bound");
        }
    }

    pub fn set_backlog(&self, addr: &UnixAddr, backlog: usize) -> Result<()> {
        self.get_listener_ref(addr)
            .ok_or_else(|| errno!(EINVAL, "the socket is not listening"))?
            .set_backlog(backlog);
        Ok(())
    }

    /// Returns the listener that connecting to the address reaches, and the
    /// address that its socket bound. The errors are those of Linux: a path
    /// that cannot be resolved is ENOENT or ENOTDIR, while a name that no one
    /// listens on, be it the path of a file that is no socket or has no
    /// listener any more, is ECONNREFUSED.
    pub fn find_listener(&self, addr: &UnixAddr) -> Result<(Arc<Listener>, UnixAddr)> {
        let key = Self::get_key(addr)?;
        let space = self.get_space(addr)?;
        match space.get(&key) {
            Some(Binding {
                addr,
                listener: Some(listener),
            }) => Ok((listener.clone(), addr.clone())),
            _ => return_errno!(ECONNREFUSED, "no one's listening on the remote address"),
        }
    }

    pub fn pop_incoming(&self, addr: &UnixAddr, timeout: Option<Duration>) -> Result<Endpoint> {
        self.get_listener_ref(addr)
            .ok_or_else(|| errno!(EINVAL, "the socket is not listening"))?
            .pop_incoming(timeout)
    }

    pub fn get_listener_ref(&self, addr: &UnixAddr) -> Option<Arc<Listener>> {
        let key = Self::get_key(addr).ok()?;
        let space = self.get_space(addr).ok()?;
        space.get(&key)?.listener.clone()
    }

    pub fn remove_addr(&self, addr: &UnixAddr) {
        let key = Self::get_key(addr);
        if let Ok(key) = key {
            let mut space = self.get_space(addr).unwrap();
            space.remove(&key);
        } else {
            warn!("address space key not exit: {:?}", addr);
        }
    }

    fn get_space(
        &self,
        addr: &UnixAddr,
    ) -> Result<SgxMutexGuard<'_, BTreeMap<AddressSpaceKey, Binding>>> {
        match addr {
            UnixAddr::File(_, _) => Ok(self.file.lock().unwrap()),
            UnixAddr::Abstract(_) => Ok(self.abstr.lock().unwrap()),
            UnixAddr::Unnamed => return_errno!(EINVAL, "can't get path name for unnamed socket"),
        }
    }

    fn get_key(addr: &UnixAddr) -> Result<AddressSpaceKey> {
        trace!("addr = {:?}", addr);
        match addr {
            UnixAddr::File(Some(inode_num), _) => Ok(AddressSpaceKey::from_inode(*inode_num)),
            UnixAddr::File(None, unix_path) => {
                // The errors of the lookup are those of connect()
                let inode = {
                    let file_path = unix_path.absolute();
                    let current = current!();
                    let fs = current.fs().read().unwrap();
                    fs.lookup_inode(&file_path)?
                };
                Ok(AddressSpaceKey::from_inode(inode.metadata()?.inode))
            }
            UnixAddr::Abstract(name) => Ok(AddressSpaceKey::from_name(name.clone())),
            UnixAddr::Unnamed => return_errno!(EINVAL, "an unnamed address has no key"),
        }
    }
}
