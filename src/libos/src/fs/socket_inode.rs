//! The files that name the Unix sockets which are bound to a path.

use super::*;

/// Identifies a file by its file system and its inode number, as every file system numbers its own
/// files.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct FileId {
    // The address of the file system, which no other file system has while it is mounted, or
    // while a file of it is kept (an inode keeps its file system)
    fs: usize,
    ino: usize,
}

impl FileId {
    fn new(inode: &Arc<dyn INode>, metadata: &Metadata) -> Self {
        Self {
            fs: Arc::as_ptr(&inode.fs()) as *const () as usize,
            ino: metadata.inode,
        }
    }

    /// Returns the identity of the file at the path, which a connect looks up. The symbolic
    /// links are followed.
    pub fn at(fs: &FsView, path: &str) -> Result<Self> {
        let inode = fs.lookup_inode(path)?;
        Ok(Self::new(&inode, &inode.metadata()?))
    }
}

/// The file that a Unix socket binds to. It is a file of the type socket, which the bind creates,
/// unless the file system cannot have such files (HostFS), where it is a regular file.
///
/// The file that a bind has created is unlinked again if the bind fails, unless `keep` is called.
/// The socket that is bound keeps the file, as Linux keeps the inode: no other file of its file
/// system has the same inode number while the socket is bound, even if the file is unlinked.
pub struct SocketInode {
    inode: Arc<dyn INode>,
    id: FileId,
    // The directory and the name of the file if the bind created it
    created: Option<(Arc<dyn INode>, String)>,
}

impl SocketInode {
    /// Returns the file for a socket that binds to the path: a new file with the mode, or the file
    /// that exists if it is a socket file. As in Linux, any other file that exists is in use
    /// (EADDRINUSE), a symbolic link included, which is not followed. Whether a socket is bound
    /// to a socket file is for the address space to tell: unlike Linux, which keeps the file in
    /// use until it is unlinked, Occlum lets a socket take the file of a socket that is closed.
    pub fn bind(fs: &FsView, path: &str, mode: FileMode) -> Result<Self> {
        // A file that exists is found with slashes at the end of its path, too, as in Linux
        let existing = match path.trim_end_matches('/') {
            "" => path,
            existing => existing,
        };
        match fs.lookup_inode_no_follow(existing) {
            Ok(inode) => {
                let metadata = inode.metadata()?;
                if metadata.type_ != FileType::Socket {
                    return_errno!(EADDRINUSE, "the file exists");
                }
                Ok(Self {
                    id: FileId::new(&inode, &metadata),
                    inode,
                    created: None,
                })
            }
            Err(e) if e.errno() == ENOENT => Self::create(fs, path, mode),
            Err(e) => Err(e),
        }
    }

    fn create(fs: &FsView, path: &str, mode: FileMode) -> Result<Self> {
        let (dir_path, file_name) = split_path(path);
        // A slash at the end of the path names a directory, which does not exist
        if file_name.ends_with("/") {
            return_errno!(ENOENT, "the path is a directory that does not exist");
        }
        let dir_inode = fs.lookup_inode(dir_path)?;
        if !dir_inode.allow_write()? {
            return_errno!(EPERM, "file cannot be created");
        }
        let inode = match dir_inode.create(file_name, FileType::Socket, mode.bits()) {
            Err(FsError::NotSupported | FsError::PermError | FsError::InvalidParam) => {
                dir_inode.create(file_name, FileType::File, mode.bits())
            }
            result => result,
        };
        let inode = match inode {
            Ok(inode) => inode,
            // Another thread has created the file meanwhile
            Err(FsError::EntryExist) => return_errno!(EADDRINUSE, "the file exists"),
            Err(e) => return Err(e.into()),
        };
        Ok(Self {
            id: FileId::new(&inode, &inode.metadata()?),
            inode,
            created: Some((dir_inode, file_name.to_owned())),
        })
    }

    pub fn id(&self) -> FileId {
        self.id
    }

    /// Ends the bind: the file stays. Returns the inode, which the socket keeps.
    pub fn keep(mut self) -> Arc<dyn INode> {
        self.created = None;
        self.inode.clone()
    }
}

impl Drop for SocketInode {
    fn drop(&mut self) {
        if let Some((dir_inode, file_name)) = self.created.take() {
            let _ = dir_inode.unlink(&file_name);
        }
    }
}
