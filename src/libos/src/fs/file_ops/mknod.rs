use super::*;

/// Creates a file of the given type: a regular file, or a named pipe, which only a directory of the host
/// can hold (HostFS, see fifo.rs). The other types need devices or sockets that the LibOS does not have.
pub fn do_mknodat(fs_path: &FsPath, mode: u32, dev: u64) -> Result<()> {
    debug!(
        "mknodat: fs_path: {:?}, mode: {:#o}, dev: {:#x}",
        fs_path, mode, dev
    );

    let file_type = match mode & libc::S_IFMT {
        0 | libc::S_IFREG => FileType::File,
        libc::S_IFIFO => FileType::NamedPipe,
        _ => return_errno!(EPERM, "the type of file is not supported"),
    };

    let path = fs_path.to_abs_path()?;
    if path.ends_with("/") {
        return_errno!(ENOENT, "the path ends with a slash");
    }
    let (dir_path, file_name) = split_path(&path);
    let current = current!();
    let dir_inode = {
        let fs = current.fs().read().unwrap();
        fs.lookup_inode(dir_path)?
    };
    if dir_inode.find(file_name).is_ok() {
        return_errno!(EEXIST, "");
    }
    if !dir_inode.allow_write()? {
        return_errno!(EPERM, "file cannot be created");
    }
    let permissions = FileMode::from_bits_truncate((mode & 0o7777) as u16);
    let masked_mode = permissions & !current.process().umask();
    match dir_inode.create(file_name, file_type, masked_mode.bits()) {
        Err(FsError::NotSupported | FsError::PermError | FsError::InvalidParam)
            if file_type == FileType::NamedPipe =>
        {
            return_errno!(EPERM, "the file system does not support named pipes")
        }
        result => result?,
    };
    Ok(())
}
