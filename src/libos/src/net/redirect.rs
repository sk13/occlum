//! Redirects TCP sockets to Unix sockets, as the redirects of the network
//! policy say (see `policy`).
//!
//! When a TCP socket of a program binds or connects to an address that a
//! redirect of the program's rules matches, the LibOS replaces the socket at
//! the file descriptor with a Unix socket that is bound or connected to the
//! Unix address of the redirect, and closes the TCP socket. This is what
//! ip2unix does with LD_PRELOAD, but for all programs, including those that
//! make system calls without libc, and with the redirects in the enclave
//! configuration.
//!
//! The Unix socket shows IP addresses to the program: its local address is the
//! address bound to, or 127.0.0.1 (::1 for IPv6) with an ephemeral port after
//! connect(), and its peer address the address connected to, or 127.0.0.1
//! (::1) with an ephemeral port for an accepted socket. Its domain and protocol
//! are those of TCP, it accepts the options of the IP levels and returns zeros
//! for them.
//!
//! The epoll files that monitor the file descriptor go on monitoring it, with
//! the Unix socket, and the Unix socket is nonblocking and close-on-spawn if
//! the TCP socket was. What else the program did with the TCP socket before
//! bind() or connect() does not carry over: its options, and other file
//! descriptors for it, e.g., from dup().

use std::sync::atomic::{AtomicU16, Ordering};

use super::policy::{self, RedirectOp};
use super::socket::{Ipv4Addr, Ipv4SocketAddr, Ipv6Addr, Ipv6SocketAddr, SocketFlags};
use super::*;
use crate::fs::{FileDesc, FileRef, StatusFlags};

/// Replaces the socket at `fd` with a Unix socket bound to the Unix address to
/// which the network policy redirects binding the socket to `addr`, if the
/// socket is a TCP socket and there is such a redirect. Returns whether the
/// socket was replaced.
pub fn redirect_bind(fd: FileDesc, file_ref: &FileRef, addr: &AnyAddr) -> Result<bool> {
    let unix_addr = match redirect_target(file_ref, RedirectOp::Bind, addr)? {
        Some(unix_addr) => unix_addr,
        None => return Ok(false),
    };
    let stream = new_stream(file_ref)?;
    stream.bind(&unix_addr)?;
    stream.set_inet_view(InetView {
        local: addr.clone(),
        peer: None,
    });
    replace(fd, file_ref, stream)?;
    Ok(true)
}

/// Replaces the socket at `fd` with a Unix socket connected to the Unix
/// address to which the network policy redirects connecting the socket to
/// `addr`, if the socket is a TCP socket and there is such a redirect. Returns
/// whether the socket was replaced. If the connection fails, e.g., with
/// ECONNREFUSED as nothing listens on the Unix address, the socket is not
/// replaced.
pub fn redirect_connect(fd: FileDesc, file_ref: &FileRef, addr: &AnyAddr) -> Result<bool> {
    let unix_addr = match redirect_target(file_ref, RedirectOp::Connect, addr)? {
        Some(unix_addr) => unix_addr,
        None => return Ok(false),
    };
    let stream = new_stream(file_ref)?;
    stream.connect(&unix_addr)?;
    stream.set_inet_view(InetView {
        local: loopback_addr(addr),
        peer: Some(addr.clone()),
    });
    replace(fd, file_ref, stream)?;
    Ok(true)
}

/// Returns the IP addresses of a socket that a listening socket with the IP
/// addresses `listener` accepted.
pub fn accepted_view(listener: &InetView) -> InetView {
    InetView {
        local: listener.local.clone(),
        peer: Some(loopback_addr(&listener.local)),
    }
}

/// Returns the Unix address of the redirect for the socket operation, if the
/// socket is a TCP socket and the network policy has such a redirect.
fn redirect_target(file_ref: &FileRef, op: RedirectOp, addr: &AnyAddr) -> Result<Option<UnixAddr>> {
    let unix_addr = match policy::redirect(op, addr)? {
        Some(unix_addr) => unix_addr,
        None => return Ok(None),
    };
    let is_tcp = if let Ok(host_socket) = file_ref.as_host_socket() {
        host_socket.is_tcp()
    } else if let Ok(uring_socket) = file_ref.as_uring_socket() {
        // The io_uring sockets of type SOCK_STREAM are those of TCP
        uring_socket.get_type() == SocketType::STREAM
    } else {
        false
    };
    Ok(if is_tcp { Some(unix_addr) } else { None })
}

/// Returns a Unix socket that is nonblocking if the socket is.
fn new_stream(file_ref: &FileRef) -> Result<UnixStream> {
    let flags = if file_ref.status_flags()?.contains(StatusFlags::O_NONBLOCK) {
        SocketFlags::SOCK_NONBLOCK
    } else {
        SocketFlags::empty()
    };
    unix_socket(SocketType::STREAM, flags, 0)
}

/// Puts the Unix socket at `fd` in place of the socket, keeping close-on-spawn
/// and the monitoring of the fd by epoll files.
fn replace(fd: FileDesc, file_ref: &FileRef, stream: UnixStream) -> Result<()> {
    let current = current!();
    let old_file = {
        let mut files = current.files().lock();
        // Another thread may have closed or replaced the socket meanwhile
        let entry = files.get_entry(fd)?;
        if Arc::as_ptr(entry.get_file()) as *const () != Arc::as_ptr(file_ref) as *const () {
            return_errno!(EBADF, "the file descriptor no longer refers to the socket");
        }
        files.replace(fd, Arc::new(stream))?
    };
    // The TCP socket is closed with the last reference, after the lock
    drop(old_file);
    Ok(())
}

/// Returns 127.0.0.1 or ::1, as the address is IPv4 or IPv6, with an ephemeral
/// port.
fn loopback_addr(addr: &AnyAddr) -> AnyAddr {
    let port = ephemeral_port();
    match addr {
        AnyAddr::Ipv6(_) => AnyAddr::Ipv6(Ipv6SocketAddr::new(Ipv6Addr::LOCALHOST, port, 0, 0)),
        _ => AnyAddr::Ipv4(Ipv4SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1), port)),
    }
}

/// Returns the next port of the ephemeral range of Linux, 32768 to 60999.
fn ephemeral_port() -> u16 {
    const FIRST: u16 = 32768;
    const LAST: u16 = 60999;
    static NEXT: AtomicU16 = AtomicU16::new(FIRST);
    let port = NEXT
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |port| {
            Some(if port >= LAST { FIRST } else { port + 1 })
        })
        .unwrap();
    port
}
