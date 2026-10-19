//! The options of a Unix stream socket at the level `SOL_SOCKET`.
//!
//! Like Linux, the socket stores the options that a program sets and returns
//! them to it: programs read them back and check the errors of setsockopt().
//! Most options change nothing for a Unix socket. These are the options for
//! which the LibOS has to do more than to store the value:
//!
//! * `SO_RCVTIMEO` and `SO_SNDTIMEO` are the timeouts of the blocking calls,
//!   see `SockTimeouts` and `Stream::timeouts`.
//! * `SO_SNDBUF` and `SO_RCVBUF` are the sizes of the buffers, see
//!   `Stream::send_buf_size` and `Stream::recv_buf_size`.
//! * `SO_PASSCRED` is whether the socket receives the credentials of the
//!   sender, see `Stream::passcred`. An accepted socket takes it, and
//!   `SO_PASSSEC`, over from the listening socket, like in Linux.
//!
//! The options are the same for all file descriptors of the socket, as they
//! share the file description.
//!
//! A socket that replaces a TCP socket (see `net::redirect`) takes the options
//! of all levels silently, and stores those of `SOL_SOCKET`. It has the
//! timeouts of the TCP socket, and the sockets that it accepts have all its
//! options, as with TCP.

use super::endpoint::DEFAULT_BUF_SIZE;
use super::stream::Status;
use super::*;
use bitflags::bitflags;
use std::convert::{TryFrom, TryInto};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// The timeouts that `SO_RCVTIMEO` and `SO_SNDTIMEO` set.
///
/// A blocking call that has waited for its timeout without making progress
/// fails with EAGAIN. `None`, the default, is no timeout: the call waits as
/// long as it takes. This is what a timeout of zero seconds and zero
/// microseconds sets. `Some(Duration::ZERO)`, which a negative number of
/// seconds sets, is a timeout that has already passed: the call does not wait
/// at all. getsockopt() returns zero for both.
///
/// Linux rounds a timeout up to the tick of its clock (1 to 10 ms). The LibOS
/// keeps the value as it is. It takes a timeout of more than
/// `MAX_TIMEOUT_SECS` as no timeout, so that it can be added to a time without
/// an overflow.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SockTimeouts {
    /// The timeout of reading, receiving and accepting (`SO_RCVTIMEO`)
    pub recv: Option<Duration>,
    /// The timeout of writing, sending and connecting (`SO_SNDTIMEO`)
    pub send: Option<Duration>,
}

/// The timeouts of a socket in atomics. A call that reads or writes needs them
/// every time, and a lock costs about as much as the rest of a call that does
/// not wait, so they are also kept here, in nanoseconds, for the calls to read
/// without a lock. They are always those of the options: `Stream::setsockopt`
/// stores them when the program sets one, and a socket that accept() returns
/// gets those of its options (see `AtomicTimeouts::of`), which are those of the
/// listening socket if that replaces a TCP socket.
#[derive(Debug)]
pub struct AtomicTimeouts {
    recv: AtomicU64,
    send: AtomicU64,
}

/// The value of an atomic timeout that is no timeout. The longest timeout is
/// shorter, see `MAX_TIMEOUT_SECS`.
const NO_TIMEOUT_NANOS: u64 = u64::MAX;

impl AtomicTimeouts {
    /// Returns the timeouts of the options
    pub fn of(options: &SockOptions) -> Self {
        let timeouts = Self::default();
        timeouts.store(options.timeouts);
        timeouts
    }

    fn store(&self, timeouts: SockTimeouts) {
        self.recv.store(to_nanos(timeouts.recv), Ordering::Release);
        self.send.store(to_nanos(timeouts.send), Ordering::Release);
    }

    pub fn recv(&self) -> Option<Duration> {
        from_nanos(self.recv.load(Ordering::Acquire))
    }

    pub fn send(&self) -> Option<Duration> {
        from_nanos(self.send.load(Ordering::Acquire))
    }
}

impl Default for AtomicTimeouts {
    fn default() -> Self {
        Self {
            recv: AtomicU64::new(NO_TIMEOUT_NANOS),
            send: AtomicU64::new(NO_TIMEOUT_NANOS),
        }
    }
}

fn to_nanos(timeout: Option<Duration>) -> u64 {
    timeout.map_or(NO_TIMEOUT_NANOS, |timeout| timeout.as_nanos() as u64)
}

fn from_nanos(nanos: u64) -> Option<Duration> {
    if nanos == NO_TIMEOUT_NANOS {
        None
    } else {
        Some(Duration::from_nanos(nanos))
    }
}

/// About 292 years, the longest timeout that fits in nanoseconds of an i64
/// (Linux takes timeouts that are much longer, of more than 10^15 seconds, as no
/// timeout, too)
const MAX_TIMEOUT_SECS: i64 = i64::MAX / 1_000_000_000 - 1;

/// The option that returns a file descriptor for the process of the peer. It is
/// not in `SockOptName`.
const SO_PEERPIDFD: i32 = 77;

/// The size of the buffers of a new socket (`net.core.wmem_default` and
/// `rmem_default` in Linux)
const DEFAULT_SOCK_BUF_SIZE: i32 = DEFAULT_BUF_SIZE as i32;
/// The least size of the buffer of a socket that is set (`SOCK_MIN_SNDBUF` and
/// `SOCK_MIN_RCVBUF` in Linux on x86-64). The buffer has twice the size that a
/// program sets, but not less than this.
const MIN_SOCK_SNDBUF: i32 = 4608;
const MIN_SOCK_RCVBUF: i32 = 2304;
/// The greatest size that a program sets the buffer to, unless it forces it
/// (`net.core.wmem_max` and `rmem_max` of Linux, which are 4 MiB on tuned
/// systems, but 208 KiB by default)
const MAX_SOCK_BUF_SIZE: u32 = 4 * 1024 * 1024;

bitflags! {
    /// The options that are on or off
    struct Flags: u32 {
        const DEBUG              = 1 << 0;
        const REUSEADDR          = 1 << 1;
        const REUSEPORT          = 1 << 2;
        const DONTROUTE          = 1 << 3;
        const BROADCAST          = 1 << 4;
        const KEEPALIVE          = 1 << 5;
        const OOBINLINE          = 1 << 6;
        const NO_CHECK           = 1 << 7;
        const PASSCRED           = 1 << 8;
        const PASSSEC            = 1 << 9;
        const RXQ_OVFL           = 1 << 10;
        const WIFI_STATUS        = 1 << 11;
        const NOFCS              = 1 << 12;
        const SELECT_ERR_QUEUE   = 1 << 13;
        const LOCK_FILTER        = 1 << 14;
        // SO_TIMESTAMP, SO_TIMESTAMPNS (the option is the combination)
        const RCVTSTAMP          = 1 << 15;
        const RCVTSTAMPNS        = 1 << 16;
        const TSTAMP_NEW         = 1 << 17;
    }
}

impl Flags {
    /// Returns the flag of an option that is on or off, and nothing else
    fn of_option(opt: SockOptName) -> Option<Self> {
        Some(match opt {
            SockOptName::SO_DEBUG => Flags::DEBUG,
            SockOptName::SO_REUSEADDR => Flags::REUSEADDR,
            SockOptName::SO_REUSEPORT => Flags::REUSEPORT,
            SockOptName::SO_DONTROUTE => Flags::DONTROUTE,
            SockOptName::SO_BROADCAST => Flags::BROADCAST,
            SockOptName::SO_KEEPALIVE => Flags::KEEPALIVE,
            SockOptName::SO_OOBINLINE => Flags::OOBINLINE,
            SockOptName::SO_NO_CHECK => Flags::NO_CHECK,
            SockOptName::SO_PASSCRED => Flags::PASSCRED,
            SockOptName::SO_PASSSEC => Flags::PASSSEC,
            SockOptName::SO_RXQ_OVFL => Flags::RXQ_OVFL,
            SockOptName::SO_WIFI_STATUS => Flags::WIFI_STATUS,
            SockOptName::SO_NOFCS => Flags::NOFCS,
            SockOptName::SO_SELECT_ERR_QUEUE => Flags::SELECT_ERR_QUEUE,
            SockOptName::SO_LOCK_FILTER => Flags::LOCK_FILTER,
            _ => return None,
        })
    }
}

/// The options that a socket stores
#[derive(Clone, Debug)]
pub struct SockOptions {
    flags: Flags,
    priority: i32,
    mark: i32,
    busy_poll: i32,
    incoming_cpu: i32,
    peek_off: i32,
    rcvlowat: i32,
    // The sizes that getsockopt() returns, which are twice those that are set
    sndbuf: i32,
    rcvbuf: i32,
    // Linux keeps the time of SO_LINGER when it is turned off
    linger_on: bool,
    linger_secs: i32,
    timeouts: SockTimeouts,
}

impl Default for SockOptions {
    fn default() -> Self {
        Self {
            flags: Flags::empty(),
            priority: 0,
            mark: 0,
            busy_poll: 0,
            incoming_cpu: -1,
            peek_off: -1,
            rcvlowat: 1,
            sndbuf: DEFAULT_SOCK_BUF_SIZE,
            rcvbuf: DEFAULT_SOCK_BUF_SIZE,
            linger_on: false,
            linger_secs: 0,
            timeouts: SockTimeouts::default(),
        }
    }
}

impl SockOptions {
    /// Returns the options of a socket that a listening socket with these
    /// options accepts. In Linux, it takes over SO_PASSCRED and SO_PASSSEC,
    /// and no other option.
    pub fn of_accepted(&self) -> Self {
        Self {
            flags: self.flags & (Flags::PASSCRED | Flags::PASSSEC),
            ..Self::default()
        }
    }

    /// Sets an option. The errors are those of Linux. `tcp` is whether the
    /// socket shows to be a TCP socket, which may set SO_REUSEPORT.
    fn set(&mut self, optname: i32, optval: &[u8], tcp: bool) -> Result<()> {
        let opt = SockOptName::try_from(optname).ok();
        // The options with a value that is not an int
        match opt {
            Some(SockOptName::SO_LINGER) => {
                let (onoff, secs) = match (optval.get(0..4), optval.get(4..8)) {
                    (Some(onoff), Some(secs)) => (to_i32(onoff), to_i32(secs)),
                    _ => return_errno!(EINVAL, "the value is shorter than struct linger"),
                };
                if onoff != 0 {
                    self.linger_on = true;
                    self.linger_secs = secs;
                } else {
                    self.linger_on = false;
                }
                return Ok(());
            }
            Some(SockOptName::SO_RCVTIMEO_OLD) | Some(SockOptName::SO_RCVTIMEO_NEW) => {
                self.timeouts.recv = parse_timeout(optval)?;
                return Ok(());
            }
            Some(SockOptName::SO_SNDTIMEO_OLD) | Some(SockOptName::SO_SNDTIMEO_NEW) => {
                self.timeouts.send = parse_timeout(optval)?;
                return Ok(());
            }
            // The options that Linux takes other values for (filters, a device
            // name, ...), which the LibOS accepts and ignores, as it did before
            // it stored options
            Some(opt) if is_ignored(opt) => return Ok(()),
            _ => (),
        }

        // The other options have an int as value. Bytes after it are ignored.
        let val = match optval.get(0..4) {
            Some(val) => to_i32(val),
            None => return_errno!(EINVAL, "the value is shorter than an int"),
        };
        let opt = opt.ok_or_else(|| errno!(ENOPROTOOPT, "unknown option"))?;
        if let Some(flag) = Flags::of_option(opt) {
            match opt {
                SockOptName::SO_REUSEPORT if val != 0 && !tcp => {
                    return_errno!(EOPNOTSUPP, "a Unix socket has no port to reuse")
                }
                // Once locked, the filter stays locked
                SockOptName::SO_LOCK_FILTER if val == 0 && self.flags.contains(flag) => {
                    return_errno!(EPERM, "the filter is locked")
                }
                _ => self.flags.set(flag, val != 0),
            }
            return Ok(());
        }
        match opt {
            // All processes run as root, who may set these
            SockOptName::SO_PRIORITY => self.priority = val,
            SockOptName::SO_MARK => self.mark = val,
            SockOptName::SO_INCOMING_CPU => self.incoming_cpu = val,
            SockOptName::SO_PEEK_OFF => self.peek_off = val,
            SockOptName::SO_BUSY_POLL => {
                if val < 0 {
                    return_errno!(EINVAL, "the time is negative");
                }
                self.busy_poll = val;
            }
            // Zero is the least, and a negative value the greatest
            SockOptName::SO_RCVLOWAT => {
                self.rcvlowat = match val {
                    0 => 1,
                    val if val < 0 => i32::MAX,
                    val => val,
                }
            }
            // The size is not greater than the limit. A negative size is a
            // great one, as Linux takes it as an unsigned number.
            SockOptName::SO_SNDBUF => {
                self.sndbuf = buf_size((val as u32).min(MAX_SOCK_BUF_SIZE) as i32, MIN_SOCK_SNDBUF)
            }
            SockOptName::SO_RCVBUF => {
                self.rcvbuf = buf_size((val as u32).min(MAX_SOCK_BUF_SIZE) as i32, MIN_SOCK_RCVBUF)
            }
            // The size is not limited, as root forces it
            SockOptName::SO_SNDBUFFORCE => self.sndbuf = buf_size(val, MIN_SOCK_SNDBUF),
            SockOptName::SO_RCVBUFFORCE => self.rcvbuf = buf_size(val, MIN_SOCK_RCVBUF),
            // SO_TIMESTAMP and SO_TIMESTAMPNS, of which a socket has one
            SockOptName::SO_TIMESTAMP_OLD => self.set_timestamp(val != 0, false, false),
            SockOptName::SO_TIMESTAMP_NEW => self.set_timestamp(val != 0, true, false),
            SockOptName::SO_TIMESTAMPNS_OLD => self.set_timestamp(val != 0, false, true),
            SockOptName::SO_TIMESTAMPNS_NEW => self.set_timestamp(val != 0, true, true),
            // The other options of Linux that the LibOS does not store
            opt if is_read_only(opt) => {
                return_errno!(ENOPROTOOPT, "the option is read-only or not supported")
            }
            _ => (),
        }
        Ok(())
    }

    fn set_timestamp(&mut self, on: bool, new: bool, ns: bool) {
        if on {
            self.flags.set(Flags::TSTAMP_NEW, new);
            self.flags.set(Flags::RCVTSTAMPNS, ns);
            self.flags.insert(Flags::RCVTSTAMP);
        } else {
            self.flags.remove(Flags::RCVTSTAMP | Flags::RCVTSTAMPNS);
        }
    }

    /// Returns the value of an option that the socket stores. The other options
    /// are not known here.
    fn get(&self, opt: SockOptName) -> Option<Vec<u8>> {
        let int = |value: i32| Some(value.to_ne_bytes().to_vec());
        if let Some(flag) = Flags::of_option(opt) {
            return int(self.flags.contains(flag) as i32);
        }
        let has = |flag: Flags| self.flags.contains(flag);
        match opt {
            SockOptName::SO_PRIORITY => int(self.priority),
            SockOptName::SO_MARK => int(self.mark),
            SockOptName::SO_INCOMING_CPU => int(self.incoming_cpu),
            SockOptName::SO_PEEK_OFF => int(self.peek_off),
            SockOptName::SO_BUSY_POLL => int(self.busy_poll),
            SockOptName::SO_RCVLOWAT => int(self.rcvlowat),
            SockOptName::SO_SNDBUF => int(self.sndbuf),
            SockOptName::SO_RCVBUF => int(self.rcvbuf),
            SockOptName::SO_LINGER => Some(
                [
                    (self.linger_on as i32).to_ne_bytes(),
                    self.linger_secs.to_ne_bytes(),
                ]
                .concat(),
            ),
            SockOptName::SO_RCVTIMEO_OLD | SockOptName::SO_RCVTIMEO_NEW => {
                Some(timeval_bytes(self.timeouts.recv))
            }
            SockOptName::SO_SNDTIMEO_OLD | SockOptName::SO_SNDTIMEO_NEW => {
                Some(timeval_bytes(self.timeouts.send))
            }
            // The old options do not show what the new ones set, and the one
            // without NS not what the one with NS set
            SockOptName::SO_TIMESTAMP_OLD => int((has(Flags::RCVTSTAMP)
                && !has(Flags::TSTAMP_NEW)
                && !has(Flags::RCVTSTAMPNS))
                as i32),
            SockOptName::SO_TIMESTAMPNS_OLD => {
                int((has(Flags::RCVTSTAMPNS) && !has(Flags::TSTAMP_NEW)) as i32)
            }
            SockOptName::SO_TIMESTAMP_NEW => {
                int((has(Flags::RCVTSTAMP) && has(Flags::TSTAMP_NEW)) as i32)
            }
            SockOptName::SO_TIMESTAMPNS_NEW => {
                int((has(Flags::RCVTSTAMPNS) && has(Flags::TSTAMP_NEW)) as i32)
            }
            _ => None,
        }
    }
}

fn to_i32(bytes: &[u8]) -> i32 {
    i32::from_ne_bytes(bytes.try_into().unwrap())
}

/// Returns the size of the buffer that is set to `size`, which is twice the
/// size (to allow for the overhead of the kernel), and not less than `min`.
fn buf_size(size: i32, min: i32) -> i32 {
    size.min(i32::MAX / 2).saturating_mul(2).max(min)
}

/// Parses a struct timeval, which SO_RCVTIMEO and SO_SNDTIMEO have as value.
fn parse_timeout(optval: &[u8]) -> Result<Option<Duration>> {
    let (secs, usecs) = match (optval.get(0..8), optval.get(8..16)) {
        (Some(secs), Some(usecs)) => (
            i64::from_ne_bytes(secs.try_into().unwrap()),
            i64::from_ne_bytes(usecs.try_into().unwrap()),
        ),
        _ => return_errno!(EINVAL, "the value is shorter than struct timeval"),
    };
    if usecs < 0 || usecs >= 1_000_000 {
        return_errno!(EDOM, "the microseconds are out of range");
    }
    Ok(if secs < 0 {
        Some(Duration::ZERO)
    } else if (secs == 0 && usecs == 0) || secs > MAX_TIMEOUT_SECS {
        None
    } else {
        Some(Duration::new(secs as u64, usecs as u32 * 1000))
    })
}

/// Returns a struct timeval of a timeout
fn timeval_bytes(timeout: Option<Duration>) -> Vec<u8> {
    let timeout = timeout.unwrap_or_default();
    [
        (timeout.as_secs() as i64).to_ne_bytes(),
        (timeout.subsec_micros() as i64).to_ne_bytes(),
    ]
    .concat()
}

/// Whether Linux does not allow to set the option, or does not have it for
/// Unix sockets
fn is_read_only(opt: SockOptName) -> bool {
    matches!(
        opt,
        SockOptName::SO_TYPE
            | SockOptName::SO_ERROR
            | SockOptName::SO_PEERCRED
            | SockOptName::SO_SNDLOWAT
            | SockOptName::SO_SECURITY_AUTHENTICATION
            | SockOptName::SO_SECURITY_ENCRYPTION_TRANSPORT
            | SockOptName::SO_SECURITY_ENCRYPTION_NETWORK
            | SockOptName::SO_PEERNAME
            | SockOptName::SO_ACCEPTCONN
            | SockOptName::SO_PEERSEC
            | SockOptName::SO_PROTOCOL
            | SockOptName::SO_DOMAIN
            | SockOptName::SO_BPF_EXTENSIONS
            | SockOptName::SCM_TIMESTAMPING_OPT_STATS
            | SockOptName::SO_MEMINFO
            | SockOptName::SO_INCOMING_NAPI_ID
            | SockOptName::SO_COOKIE
            | SockOptName::SCM_TIMESTAMPING_PKTINFO
            | SockOptName::SO_PEERGROUPS
    )
}

/// Whether Linux does not return the option, or does not have it for Unix
/// sockets: it fails with ENOPROTOOPT
fn is_write_only(opt: SockOptName) -> bool {
    matches!(
        opt,
        SockOptName::SO_SECURITY_AUTHENTICATION
            | SockOptName::SO_SECURITY_ENCRYPTION_TRANSPORT
            | SockOptName::SO_SECURITY_ENCRYPTION_NETWORK
            | SockOptName::SO_DETACH_FILTER
            | SockOptName::SO_SNDBUFFORCE
            | SockOptName::SO_RCVBUFFORCE
            | SockOptName::SO_ATTACH_BPF
            | SockOptName::SO_ATTACH_REUSEPORT_CBPF
            | SockOptName::SO_ATTACH_REUSEPORT_EBPF
            | SockOptName::SO_CNX_ADVICE
            | SockOptName::SCM_TIMESTAMPING_OPT_STATS
            | SockOptName::SCM_TIMESTAMPING_PKTINFO
            | SockOptName::SO_DETACH_REUSEPORT_BPF
    )
}

/// Whether Linux takes another value than an int for the option, which the
/// LibOS accepts and ignores
fn is_ignored(opt: SockOptName) -> bool {
    matches!(
        opt,
        SockOptName::SO_BINDTODEVICE
            | SockOptName::SO_ATTACH_FILTER
            | SockOptName::SO_DETACH_FILTER
            | SockOptName::SO_ATTACH_BPF
            | SockOptName::SO_ATTACH_REUSEPORT_CBPF
            | SockOptName::SO_ATTACH_REUSEPORT_EBPF
            | SockOptName::SO_DETACH_REUSEPORT_BPF
            | SockOptName::SO_TIMESTAMPING_OLD
            | SockOptName::SO_TIMESTAMPING_NEW
            | SockOptName::SO_MAX_PACING_RATE
            | SockOptName::SO_TXTIME
    )
}

impl Stream {
    /// Returns the value of a socket option for a buffer of `*optlen` bytes. If
    /// the buffer is too small for the groups of the peer (SO_PEERGROUPS), sets
    /// `*optlen` to the size that they need, and fails with ERANGE.
    ///
    /// A socket that replaces a TCP socket has the domain and protocol of that
    /// socket, and zeros for the options of the IP levels.
    pub fn getsockopt(&self, level: i32, optname: i32, optlen: &mut u32) -> Result<Vec<u8>> {
        let inet = self.inet_view();
        if let Some(inet) = &inet {
            match level {
                libc::IPPROTO_TCP | libc::IPPROTO_IP | libc::IPPROTO_IPV6 => {
                    return Ok(vec![0; (*optlen as usize).min(256)]);
                }
                _ => (),
            }
        }
        if level != libc::SOL_SOCKET {
            return_errno!(EOPNOTSUPP, "unix sockets have no options at this level");
        }
        if optname == SO_PEERPIDFD {
            // The LibOS has no pidfd to return for the peer
            if !matches!(&*self.inner(), Status::Connected(_)) {
                return_errno!(ENODATA, "the socket is not connected");
            }
            return_errno!(ENOPROTOOPT, "the LibOS has no pidfd");
        }
        let opt = SockOptName::try_from(optname)
            .map_err(|_| errno!(ENOPROTOOPT, "Not a valid optname"))?;
        let int = |value: i32| value.to_ne_bytes().to_vec();

        if is_write_only(opt) {
            return_errno!(ENOPROTOOPT, "the option cannot be read");
        }
        if let Some(value) = self.options.lock().unwrap().get(opt) {
            return Ok(value);
        }
        Ok(match opt {
            SockOptName::SO_TYPE => int(libc::SOCK_STREAM),
            SockOptName::SO_DOMAIN => int(inet.as_ref().map_or(libc::AF_UNIX, InetView::family)),
            SockOptName::SO_PROTOCOL => int(inet.as_ref().map_or(0, |_| libc::IPPROTO_TCP)),
            // Connecting never fails asynchronously, see connect()
            SockOptName::SO_ERROR => int(0),
            SockOptName::SO_ACCEPTCONN => {
                int(matches!(&*self.inner(), Status::Listening(_)) as i32)
            }
            SockOptName::SO_SNDLOWAT => int(1),
            SockOptName::SO_PEERCRED => {
                // The process that connected, for an accepted socket, and the
                // one that accepted, for a connected one. The process that
                // created the socket pair, for both ends of a socket pair. All
                // processes run as root. Like Linux, pid 0 and uid/gid -1 if
                // unknown.
                let cred = match self.peer_ancillary() {
                    Some(peer) => libc::ucred {
                        pid: peer.pid() as libc::pid_t,
                        uid: 0,
                        gid: 0,
                    },
                    None => libc::ucred {
                        pid: 0,
                        uid: u32::MAX,
                        gid: u32::MAX,
                    },
                };
                [
                    cred.pid.to_ne_bytes(),
                    cred.uid.to_ne_bytes(),
                    cred.gid.to_ne_bytes(),
                ]
                .concat()
            }
            SockOptName::SO_PEERGROUPS => {
                // Like Linux: no peer, no groups, and the size of the groups
                // if the buffer is too small. All processes are in group 0.
                if !matches!(&*self.inner(), Status::Connected(_)) {
                    return_errno!(ENODATA, "the socket is not connected");
                }
                let groups = 0u32.to_ne_bytes().to_vec();
                if (*optlen as usize) < groups.len() {
                    *optlen = groups.len() as u32;
                    return_errno!(ERANGE, "the buffer is too small for the groups");
                }
                groups
            }
            SockOptName::SO_PEERNAME => {
                let (storage, len) = match inet.as_ref().map(|inet| inet.peer.as_ref()) {
                    Some(Some(peer)) => peer.to_c_storage(),
                    Some(None) => return_errno!(ENOTCONN, "the socket is not connected"),
                    None => self.peer_addr()?.to_c_storage(),
                };
                // Like Linux, which does not truncate the address
                if len < *optlen as usize {
                    return_errno!(EINVAL, "the address is shorter than the buffer");
                }
                let bytes =
                    unsafe { std::slice::from_raw_parts(&storage as *const _ as *const u8, len) };
                bytes.to_vec()
            }
            SockOptName::SO_PEERSEC => return_errno!(ENOPROTOOPT, "no security context"),
            // No device
            SockOptName::SO_BINDTODEVICE => Vec::new(),
            _ => int(0),
        })
    }

    /// Returns the options of a socket that accept() returns. A Unix socket
    /// passes on the credentials only, see `SockOptions::of_accepted`. A socket
    /// that replaces a TCP socket passes on all, as a TCP socket does: what it
    /// accepts has its timeouts, for one.
    pub(super) fn options_of_accepted(&self) -> SockOptions {
        let tcp = self.inet_view().is_some();
        let options = self.options.lock().unwrap();
        if tcp {
            options.clone()
        } else {
            options.of_accepted()
        }
    }

    /// Sets a socket option, which is `optval`.
    pub fn setsockopt(&self, level: i32, optname: i32, optval: &[u8]) -> Result<()> {
        let tcp = self.inet_view().is_some();
        if level != libc::SOL_SOCKET {
            // A socket that replaces a TCP socket takes the options silently
            if tcp {
                return Ok(());
            }
            return_errno!(EOPNOTSUPP, "unix sockets have no options at this level");
        }
        let result = {
            let mut options = self.options.lock().unwrap();
            let result = options.set(optname, optval, tcp);
            self.timeouts.store(options.timeouts);
            result
        };
        if tcp {
            return Ok(());
        }
        result
    }

    /// Returns the timeouts that the program set to read, accept, write and
    /// connect. The wait of such a call has to end with EAGAIN when the timeout
    /// has passed, see `SockTimeouts`. The timeout of accept() is that of the
    /// listening socket, and a socket that it accepts has none of its own, as in
    /// Linux, but for a socket that replaces a TCP socket, which passes its
    /// timeouts on (see `options_of_accepted`).
    pub fn timeouts(&self) -> SockTimeouts {
        SockTimeouts {
            recv: self.timeouts.recv(),
            send: self.timeouts.send(),
        }
    }

    /// Returns the timeout of reading, receiving and accepting
    pub fn recv_timeout(&self) -> Option<Duration> {
        self.timeouts.recv()
    }

    /// Returns the timeout of writing, sending and connecting
    pub fn send_timeout(&self) -> Option<Duration> {
        self.timeouts.send()
    }

    /// Returns the size of the buffer for sending, which is the number of bytes
    /// that the socket may have in flight to its peer. This is what getsockopt()
    /// returns for SO_SNDBUF, which is twice the size that was set.
    ///
    /// The LibOS stores the size, but does not use it to limit the buffer yet. The
    /// buffer is of `DEFAULT_BUF_SIZE` bytes. To limit it, the ring buffer of the
    /// connection needs a limit that can change while the socket is in use (also
    /// for a socket that is connected before it is accepted, which does not have
    /// the options of its end yet), and the size that is greater than the buffer
    /// needs a buffer that grows with the data.
    pub fn send_buf_size(&self) -> usize {
        self.options.lock().unwrap().sndbuf as usize
    }

    /// Returns the size of the buffer for receiving (SO_RCVBUF), which is twice
    /// the size that was set. Linux does not limit what a Unix socket
    /// receives with it, but what the sending socket has in flight with its
    /// `send_buf_size`.
    pub fn recv_buf_size(&self) -> usize {
        self.options.lock().unwrap().rcvbuf as usize
    }

    /// Returns whether the socket receives the credentials of the sender
    /// (SO_PASSCRED). The LibOS does not send them yet.
    pub fn passcred(&self) -> bool {
        self.options.lock().unwrap().flags.contains(Flags::PASSCRED)
    }
}
