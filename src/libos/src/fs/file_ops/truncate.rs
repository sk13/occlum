use super::*;

pub fn do_truncate(path: &str, len: usize) -> Result<()> {
    debug!("truncate: path: {:?}, len: {}", path, len);
    let inode = {
        let current = current!();
        let fs = current.fs().read().unwrap();
        fs.lookup_inode(&path)?
    };
    // Like in Linux, the file of a socket cannot be truncated
    if inode.metadata()?.type_ == FileType::Socket {
        return_errno!(EINVAL, "the file is a socket");
    }
    inode.resize(len)?;
    Ok(())
}

pub fn do_ftruncate(fd: FileDesc, len: usize) -> Result<()> {
    debug!("ftruncate: fd: {}, len: {}", fd, len);
    let file_ref = current!().file(fd)?;
    file_ref.set_len(len as u64)?;
    Ok(())
}
