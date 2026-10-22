use crate::{fs::IoctlCmd, prelude::*};
use libc::ocall::setsockopt as do_setsockopt;

#[derive(Debug)]
pub struct SetSockOptRawCmd<'a> {
    level: i32,
    optname: i32,
    optval: &'a [u8],
    result: i32,
}

impl IoctlCmd for SetSockOptRawCmd<'static> {}

impl<'a> SetSockOptRawCmd<'a> {
    pub fn new(level: i32, optname: i32, optval: &'a [u8]) -> SetSockOptRawCmd<'a> {
        Self {
            level,
            optname,
            optval,
            result: 0,
        }
    }

    pub fn execute(&mut self, fd: FileDesc) -> Result<()> {
        self.result = setsockopt_by_host(fd, self.level, self.optname, &self.optval)?;
        Ok(())
    }

    /// The result of the host, which is 0 for almost all options. For SCTP_SOCKOPT_CONNECTX, it is
    /// the ID of the association, which setsockopt gives back to the program.
    pub fn result(&self) -> i32 {
        self.result
    }
}

pub fn setsockopt_by_host(fd: FileDesc, level: i32, optname: i32, optval: &[u8]) -> Result<i32> {
    let result = try_libc!(do_setsockopt(
        fd as _,
        level as _,
        optname as _,
        optval.as_ptr() as _,
        optval.len() as _
    ));
    Ok(result as i32)
}
