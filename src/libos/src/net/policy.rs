//! The network policy of the enclave.
//!
//! In Occlum, all sockets except those of AF_UNIX are sockets of the host, so
//! the traffic of IP sockets leaves the enclave. A network policy, set with
//! `network_policy` in Occlum.json, restricts which programs may use which
//! addresses, e.g., so that only a TLS proxy communicates outside the enclave.
//! It is part of the enclave configuration, which the LibOS verifies, so that
//! the host cannot change it.
//!
//! Without a network policy, all socket operations are allowed. With one, a
//! socket operation fails with EACCES unless a rule of the current program
//! allows it:
//!
//! * Binding an IP socket (with bind(), sctp_bindx(), or implicitly at listen())
//!   needs a `bind` pattern that matches the local address.
//! * Sending to an IP address (with connect(), sctp_connectx(), sendto(),
//!   sendmsg() or sendmmsg()) needs a `connect` pattern that matches the
//!   destination.
//! * Creating a socket whose destinations the addresses do not determine, i.e.,
//!   a raw IP socket, an AF_PACKET socket or a socket of a domain other than
//!   AF_UNIX, AF_NETLINK, AF_INET and AF_INET6, needs a rule with `raw`.
//!
//! An address whose IP address or port the policy cannot determine, e.g., that
//! of IP source routing or of an AF_UNSPEC address that Linux interprets as an
//! IPv4 address, only matches the patterns "*" for the IP address or port.
//! AF_UNIX sockets stay in the LibOS, and AF_NETLINK sockets reach the kernel
//! of the host, but not the network; neither is restricted.
//!
//! The rules that name a program apply to the processes that run it, i.e.,
//! whose executable (or the interpreter of whose script) has that absolute path,
//! with "." and ".." resolved, but not symbolic links. The rules without a
//! program apply to the programs that no rule names.
//!
//! The host sees all data sent with host sockets and decides where the packets
//! go. So the policy restricts which programs hand which data to the host, and
//! for which destinations, but not where the host delivers the packets.
//!
//! A rule may also redirect TCP sockets to AF_UNIX sockets, so that a program
//! that uses TCP, e.g., for a proxy in the same enclave, does not use host
//! sockets for it: a `redirect` of a rule has a pattern for `bind` or for
//! `connect` and a Unix socket address `to` (a path, or an abstract name after
//! "@"), in which "%a" stands for the IP address, "%p" for the port and "%%"
//! for "%". When a TCP socket of the program binds or connects to an address
//! that the pattern matches, the LibOS replaces it with a Unix socket bound or
//! connected to that Unix address (see `net::redirect`). The first matching
//! redirect of the program's rules applies, and a redirected address needs no
//! `bind` or `connect` pattern, as the data stays in the enclave.

use std::fmt;
use std::ops::RangeInclusive;

use super::{AnyAddr, Domain, SockAddr, SocketType, UnixAddr, UnixPath};
use crate::config::{
    InputConfigNetworkPolicy, InputConfigNetworkRedirect, InputConfigNetworkRule, LIBOS_CONFIG,
};
use crate::fs::normalize_abs_path;
use crate::prelude::*;

#[derive(Debug)]
pub struct NetworkPolicy {
    rules: Vec<Rule>,
}

#[derive(Debug)]
struct Rule {
    program: Option<String>,
    bind: Vec<AddrPattern>,
    connect: Vec<AddrPattern>,
    raw: bool,
    redirects: Vec<Redirect>,
}

/// The socket operation that a redirect applies to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedirectOp {
    Bind,
    Connect,
}

/// A redirect of the TCP sockets that bind or connect to the addresses of a
/// pattern to a Unix socket address, e.g., from "*:8080" to "/run/nf.sock".
#[derive(Debug)]
struct Redirect {
    op: RedirectOp,
    pattern: AddrPattern,
    to: String,
}

/// The longest path or abstract name of a Unix socket address, without the
/// null byte that ends a path or starts an abstract name
const MAX_UNIX_NAME_LEN: usize = 107;

/// A pattern of IP socket addresses, e.g., "*:8443", "10.0.0.0/8:80-89" or
/// "[::1]:*".
#[derive(Debug)]
struct AddrPattern {
    ip: IpPattern,
    ports: RangeInclusive<u16>,
}

#[derive(Debug)]
enum IpPattern {
    Any,
    V4 { network: u32, prefix_len: u32 },
    V6 { network: u128, prefix_len: u32 },
}

/// An IP socket address to check, whose IP address or port is None if the
/// policy cannot determine it.
#[derive(Debug)]
struct Target {
    ip: Option<Ip>,
    port: Option<u16>,
}

#[derive(Debug, Clone, Copy)]
enum Ip {
    V4(u32),
    V6(u128),
}

impl Ip {
    fn from_v6(ip: u128) -> Self {
        // An IPv4-mapped IPv6 address reaches the IPv4 address
        if ip >> 32 == 0xffff {
            Ip::V4(ip as u32)
        } else {
            Ip::V6(ip)
        }
    }
}

impl NetworkPolicy {
    pub fn from_input(input: &InputConfigNetworkPolicy) -> Result<Self> {
        let rules = input
            .rules
            .iter()
            .map(Rule::from_input)
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { rules })
    }

    /// Returns the rules that apply to the given program.
    fn rules_of<'a>(&'a self, program: &'a str) -> impl Iterator<Item = &'a Rule> + 'a {
        let is_named = self
            .rules
            .iter()
            .any(|rule| rule.program.as_deref() == Some(program));
        self.rules.iter().filter(move |rule| match &rule.program {
            Some(rule_program) => rule_program == program,
            None => !is_named,
        })
    }

    fn check<F, D>(&self, is_allowed: F, describe: D) -> Result<()>
    where
        F: Fn(&Rule) -> bool,
        D: FnOnce() -> String,
    {
        let current = current!();
        let process = current.process();
        let program = process.exec_path();
        if self.rules_of(program).any(|rule| is_allowed(rule)) {
            return Ok(());
        }
        warn!(
            "network policy: {} (pid {}) may not {}",
            program,
            process.pid(),
            describe()
        );
        return_errno!(EACCES, "denied by the network policy");
    }
}

impl Rule {
    fn from_input(input: &InputConfigNetworkRule) -> Result<Self> {
        let program = match &input.program {
            Some(program) if program.starts_with('/') => Some(normalize_abs_path(program)),
            Some(program) => {
                eprintln!(
                    "network_policy: the program {:?} is not an absolute path",
                    program
                );
                return_errno!(EINVAL, "invalid program in the network policy");
            }
            None => None,
        };
        let parse_patterns = |patterns: &Vec<String>| {
            patterns
                .iter()
                .map(|pattern| AddrPattern::parse(pattern))
                .collect::<Result<Vec<_>>>()
        };
        Ok(Self {
            program,
            bind: parse_patterns(&input.bind)?,
            connect: parse_patterns(&input.connect)?,
            raw: input.raw,
            redirects: input
                .redirect
                .iter()
                .map(Redirect::from_input)
                .collect::<Result<Vec<_>>>()?,
        })
    }
}

impl Redirect {
    fn from_input(input: &InputConfigNetworkRedirect) -> Result<Self> {
        let (op, pattern) = match (&input.bind, &input.connect) {
            (Some(pattern), None) => (RedirectOp::Bind, pattern),
            (None, Some(pattern)) => (RedirectOp::Connect, pattern),
            _ => {
                eprintln!(
                    "network_policy: a redirect needs either \"bind\" or \"connect\": {:?}",
                    input
                );
                return_errno!(EINVAL, "invalid redirect in the network policy");
            }
        };
        let pattern = AddrPattern::parse(pattern)?;
        if let Err(reason) = Self::check_to(&input.to) {
            eprintln!(
                "network_policy: invalid redirect target {:?}: {}",
                input.to, reason
            );
            return_errno!(EINVAL, "invalid redirect target in the network policy");
        }
        Ok(Self {
            op,
            pattern,
            to: input.to.clone(),
        })
    }

    fn check_to(to: &str) -> std::result::Result<(), &'static str> {
        let name = if let Some(name) = to.strip_prefix('@') {
            name
        } else if to.starts_with('/') {
            to
        } else {
            return Err("expected an absolute path or \"@\" and an abstract name");
        };
        if name.is_empty() {
            return Err("empty abstract name");
        }
        let mut chars = name.chars();
        while let Some(c) = chars.next() {
            match c {
                '\0' => return Err("null byte"),
                '%' => match chars.next() {
                    Some('a') | Some('p') | Some('%') => (),
                    _ => return Err("\"%\" must be followed by \"a\", \"p\" or \"%\""),
                },
                _ => (),
            }
        }
        if Self::expand(name, "", "").len() > MAX_UNIX_NAME_LEN {
            return Err("too long");
        }
        Ok(())
    }

    fn expand(template: &str, ip: &str, port: &str) -> String {
        let mut expanded = String::with_capacity(template.len());
        let mut chars = template.chars();
        while let Some(c) = chars.next() {
            match (c, chars.clone().next()) {
                ('%', Some('a')) => expanded.push_str(ip),
                ('%', Some('p')) => expanded.push_str(port),
                ('%', Some('%')) => expanded.push('%'),
                _ => {
                    expanded.push(c);
                    continue;
                }
            }
            chars.next();
        }
        expanded
    }

    /// Returns the Unix socket address for an IP address and port.
    fn unix_addr(&self, ip: Ip, port: u16) -> Result<UnixAddr> {
        let ip = match ip {
            Ip::V4(ip) => std::net::Ipv4Addr::from(ip).to_string(),
            Ip::V6(ip) => std::net::Ipv6Addr::from(ip).to_string(),
        };
        let port = port.to_string();
        let addr = match self.to.strip_prefix('@') {
            Some(name) => UnixAddr::Abstract(Self::expand(name, &ip, &port)),
            None => UnixAddr::File(None, UnixPath::new(&Self::expand(&self.to, &ip, &port))),
        };
        if addr.path_str()?.len() > MAX_UNIX_NAME_LEN {
            return_errno!(ENAMETOOLONG, "the redirect target is too long");
        }
        Ok(addr)
    }
}

impl AddrPattern {
    fn parse(pattern: &str) -> Result<Self> {
        Self::try_parse(pattern).map_err(|reason| {
            eprintln!(
                "network_policy: invalid address pattern {:?}: {}",
                pattern, reason
            );
            errno!(EINVAL, "invalid address pattern in the network policy")
        })
    }

    fn try_parse(pattern: &str) -> std::result::Result<Self, &'static str> {
        let (ip, ports) = pattern
            .rsplit_once(':')
            .ok_or("expected <address>:<port>")?;

        let ip = if ip == "*" {
            IpPattern::Any
        } else if let Some(ip) = ip.strip_prefix('[').and_then(|ip| ip.strip_suffix(']')) {
            let (addr, prefix_len) = split_prefix_len(ip, 128)?;
            let addr = addr
                .parse::<std::net::Ipv6Addr>()
                .map_err(|_| "invalid IPv6 address")?;
            let network = u128::from_be_bytes(addr.octets());
            if network & !mask_u128(prefix_len) != 0 {
                return Err("the IPv6 address has bits set outside of its prefix");
            }
            IpPattern::V6 {
                network,
                prefix_len,
            }
        } else {
            let (addr, prefix_len) = split_prefix_len(ip, 32)?;
            let addr = addr
                .parse::<std::net::Ipv4Addr>()
                .map_err(|_| "invalid IPv4 address (IPv6 addresses need brackets)")?;
            let network = u32::from_be_bytes(addr.octets());
            if network & !mask_u32(prefix_len) != 0 {
                return Err("the IPv4 address has bits set outside of its prefix");
            }
            IpPattern::V4 {
                network,
                prefix_len,
            }
        };

        let parse_port = |port: &str| port.parse::<u16>().map_err(|_| "invalid port");
        let ports = if ports == "*" {
            0..=u16::MAX
        } else if let Some((first, last)) = ports.split_once('-') {
            let (first, last) = (parse_port(first)?, parse_port(last)?);
            if first > last {
                return Err("empty port range");
            }
            first..=last
        } else {
            let port = parse_port(ports)?;
            port..=port
        };

        Ok(Self { ip, ports })
    }

    fn matches(&self, target: &Target) -> bool {
        let ip_matches = match (&self.ip, target.ip) {
            (IpPattern::Any, _) => true,
            (
                IpPattern::V4 {
                    network,
                    prefix_len,
                },
                Some(Ip::V4(ip)),
            ) => ip & mask_u32(*prefix_len) == *network,
            (
                IpPattern::V6 {
                    network,
                    prefix_len,
                },
                Some(Ip::V6(ip)),
            ) => ip & mask_u128(*prefix_len) == *network,
            _ => false,
        };
        let port_matches = match target.port {
            Some(port) => self.ports.contains(&port),
            None => self.ports == (0..=u16::MAX),
        };
        ip_matches && port_matches
    }
}

fn split_prefix_len(ip: &str, max_len: u32) -> std::result::Result<(&str, u32), &'static str> {
    match ip.split_once('/') {
        Some((addr, prefix_len)) => {
            let prefix_len = prefix_len
                .parse::<u32>()
                .ok()
                .filter(|len| *len <= max_len)
                .ok_or("invalid prefix length")?;
            Ok((addr, prefix_len))
        }
        None => Ok((ip, max_len)),
    }
}

fn mask_u32(prefix_len: u32) -> u32 {
    u32::MAX.checked_shl(32 - prefix_len).unwrap_or(0)
}

fn mask_u128(prefix_len: u32) -> u128 {
    u128::MAX.checked_shl(128 - prefix_len).unwrap_or(0)
}

impl Target {
    /// Returns the target of an address, or None if the address is not
    /// restricted by the policy, e.g., an AF_UNIX address.
    fn from_addr(addr: &AnyAddr) -> Option<Self> {
        let target = match addr {
            AnyAddr::Ipv4(addr) => Self {
                ip: Some(Ip::V4(u32::from_be_bytes(*addr.ip().octets()))),
                port: Some(addr.port()),
            },
            AnyAddr::Ipv6(addr) => Self {
                ip: Some(Ip::from_v6(u128::from_be_bytes(addr.ip().octets()))),
                port: Some(addr.port()),
            },
            // Linux takes an AF_UNSPEC address of an IPv4 socket for an IPv4
            // address in bind() and sendto(), so the destination is unknown
            AnyAddr::Unspec => Self::unknown(),
            _ => return None,
        };
        Some(target)
    }

    fn unknown() -> Self {
        Self {
            ip: None,
            port: None,
        }
    }
}

impl fmt::Display for Target {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.ip {
            Some(Ip::V4(ip)) => write!(f, "{}", std::net::Ipv4Addr::from(ip))?,
            Some(Ip::V6(ip)) => write!(f, "[{}]", std::net::Ipv6Addr::from(ip))?,
            None => write!(f, "?")?,
        }
        match self.port {
            Some(port) => write!(f, ":{}", port),
            None => write!(f, ":?"),
        }
    }
}

/// Returns the Unix socket address to which the network policy redirects a TCP
/// socket of the current program that binds or connects to the address, if
/// any.
pub fn redirect(op: RedirectOp, addr: &AnyAddr) -> Result<Option<UnixAddr>> {
    let policy = match policy() {
        Some(policy) => policy,
        None => return Ok(None),
    };
    let (ip, port) = match Target::from_addr(addr) {
        Some(Target {
            ip: Some(ip),
            port: Some(port),
        }) => (ip, port),
        _ => return Ok(None),
    };
    let target = Target {
        ip: Some(ip),
        port: Some(port),
    };
    let current = current!();
    let process = current.process();
    let redirect = policy
        .rules_of(process.exec_path())
        .flat_map(|rule| rule.redirects.iter())
        .find(|redirect| redirect.op == op && redirect.pattern.matches(&target));
    match redirect {
        Some(redirect) => {
            let unix_addr = redirect.unix_addr(ip, port)?;
            debug!(
                "network policy: {:?} of {} to {} redirected to {:?}",
                op,
                process.exec_path(),
                target,
                unix_addr
            );
            Ok(Some(unix_addr))
        }
        None => Ok(None),
    }
}

fn policy() -> Option<&'static NetworkPolicy> {
    LIBOS_CONFIG.network_policy.as_ref()
}

/// Returns whether the enclave has a network policy.
pub fn is_enabled() -> bool {
    policy().is_some()
}

/// Checks whether the current program may create a socket of the domain and type.
pub fn check_socket(domain: Domain, socket_type: SocketType) -> Result<()> {
    let policy = match policy() {
        Some(policy) => policy,
        None => return Ok(()),
    };
    let needs_raw = match domain {
        Domain::LOCAL | Domain::NETLINK => false,
        Domain::INET | Domain::INET6 => {
            matches!(socket_type, SocketType::RAW | SocketType::PACKET)
        }
        _ => true,
    };
    if !needs_raw {
        return Ok(());
    }
    policy.check(
        |rule| rule.raw,
        || format!("create a {:?} socket of domain {:?}", socket_type, domain),
    )
}

/// Checks whether the current program may bind a socket to the address.
pub fn check_bind(addr: &AnyAddr) -> Result<()> {
    match (policy(), Target::from_addr(addr)) {
        (Some(policy), Some(target)) => check_bind_target(policy, &target),
        _ => Ok(()),
    }
}

/// Checks whether the current program may listen on a host socket with the
/// local address.
pub fn check_listen(addr: &SockAddr) -> Result<()> {
    check_bind(&any_addr_of(addr)?)
}

/// Checks whether the current program may connect a socket to the address.
pub fn check_connect(addr: &AnyAddr) -> Result<()> {
    match (policy(), addr) {
        // Connecting to an AF_UNSPEC address dissolves the association
        (_, AnyAddr::Unspec) | (None, _) => Ok(()),
        (Some(policy), addr) => match Target::from_addr(addr) {
            Some(target) => check_connect_target(policy, &target),
            None => Ok(()),
        },
    }
}

/// Checks whether the current program may connect a host socket to the address.
pub fn check_connect_raw(addr: &SockAddr) -> Result<()> {
    if !is_enabled() {
        return Ok(());
    }
    check_connect(&any_addr_of(addr)?)
}

/// Returns the address of a host socket.
fn any_addr_of(addr: &SockAddr) -> Result<AnyAddr> {
    let (mut storage, mut len) = addr.to_c_storage();
    // Like Linux, SockAddr accepts IPv6 socket addresses without the scope ID,
    // which the policy does not need
    let in6_len = std::mem::size_of::<libc::sockaddr_in6>();
    if storage.ss_family as i32 == libc::AF_INET6 && len < in6_len {
        let bytes =
            unsafe { std::slice::from_raw_parts_mut(&mut storage as *mut _ as *mut u8, in6_len) };
        bytes[len..].fill(0);
        len = in6_len;
    }
    AnyAddr::from_c_storage(&storage, len)
}

/// Checks whether the current program may send a message with the destination
/// address and control messages.
pub fn check_send(addr: Option<&AnyAddr>, control: Option<&[u8]>) -> Result<()> {
    let policy = match policy() {
        Some(policy) => policy,
        None => return Ok(()),
    };
    let dest = match addr {
        Some(addr) => Target::from_addr(addr),
        None => None,
    };
    if let Some(dest) = &dest {
        check_connect_target(policy, dest)?;
    }
    if let Some(control) = control {
        for (level, type_, data) in cmsgs(control)? {
            match (level, type_) {
                // SCTP_DSTADDRV4 and SCTP_DSTADDRV6: additional addresses of a
                // new association, with the port of the destination address
                (IPPROTO_SCTP, 7) | (IPPROTO_SCTP, 8) => {
                    let ip = match (type_, data.len()) {
                        (7, 4) => Ip::V4(u32::from_be_bytes(data.try_into().unwrap())),
                        (8, 16) => Ip::from_v6(u128::from_be_bytes(data.try_into().unwrap())),
                        _ => return_errno!(EINVAL, "invalid SCTP destination address"),
                    };
                    let target = Target {
                        ip: Some(ip),
                        port: dest.as_ref().and_then(|dest| dest.port),
                    };
                    check_connect_target(policy, &target)?;
                }
                // IP_RETOPTS (IP options, which may route through other
                // addresses), IPV6_2292RTHDR and IPV6_RTHDR (routing headers)
                (libc::IPPROTO_IP, 7) | (libc::IPPROTO_IPV6, 5) | (libc::IPPROTO_IPV6, 57) => {
                    check_connect_target(policy, &Target::unknown())?;
                }
                _ => (),
            }
        }
    }
    Ok(())
}

/// Checks whether the current program may set the socket option. Returns a
/// copy of the option value if the check depends on it, which must then be
/// used instead of the value in user memory.
pub fn check_setsockopt(level: i32, optname: i32, optval: &[u8]) -> Result<Option<Vec<u8>>> {
    let policy = match policy() {
        Some(policy) => policy,
        None => return Ok(None),
    };
    match (level, optname) {
        // SCTP_SOCKOPT_BINDX_ADD, SCTP_SOCKOPT_CONNECTX_OLD, SCTP_SOCKOPT_CONNECTX:
        // packed arrays of IPv4 and IPv6 socket addresses
        (IPPROTO_SCTP, 100) | (IPPROTO_SCTP, 107) | (IPPROTO_SCTP, 110) => {
            let optval = optval.to_vec();
            let mut offset = 0;
            while offset < optval.len() {
                let rest = &optval[offset..];
                let family = rest
                    .get(0..2)
                    .map(|family| u16::from_ne_bytes(family.try_into().unwrap()) as i32);
                let len = match family {
                    Some(libc::AF_INET) => std::mem::size_of::<libc::sockaddr_in>(),
                    Some(libc::AF_INET6) => std::mem::size_of::<libc::sockaddr_in6>(),
                    _ => return_errno!(EINVAL, "invalid SCTP address"),
                };
                if rest.len() < len {
                    return_errno!(EINVAL, "invalid SCTP address");
                }
                let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        rest.as_ptr(),
                        &mut storage as *mut _ as *mut u8,
                        len,
                    );
                }
                let target = Target::from_addr(&AnyAddr::from_c_storage(&storage, len)?).unwrap();
                if optname == 100 {
                    check_bind_target(policy, &target)?;
                } else {
                    check_connect_target(policy, &target)?;
                }
                offset += len;
            }
            Ok(Some(optval))
        }
        // IP_OPTIONS (which may route through other addresses), IPV6_2292RTHDR
        // and IPV6_RTHDR (routing headers)
        (libc::IPPROTO_IP, IP_OPTIONS) | (libc::IPPROTO_IPV6, 5) | (libc::IPPROTO_IPV6, 57)
            if !optval.is_empty() =>
        {
            check_connect_target(policy, &Target::unknown())?;
            Ok(None)
        }
        _ => Ok(None),
    }
}

/// Checks whether the current program may get the socket option.
pub fn check_getsockopt(level: i32, optname: i32) -> Result<()> {
    // SCTP_SOCKOPT_CONNECTX3, which connects to addresses in the option value.
    // Fail with ENOPROTOOPT, so that libsctp falls back to SCTP_SOCKOPT_CONNECTX,
    // whose addresses are checked.
    if is_enabled() && level == IPPROTO_SCTP && optname == 111 {
        return_errno!(ENOPROTOOPT, "SCTP_SOCKOPT_CONNECTX3 with a network policy");
    }
    Ok(())
}

fn check_bind_target(policy: &NetworkPolicy, target: &Target) -> Result<()> {
    policy.check(
        |rule| rule.bind.iter().any(|pattern| pattern.matches(target)),
        || format!("bind to {}", target),
    )
}

fn check_connect_target(policy: &NetworkPolicy, target: &Target) -> Result<()> {
    policy.check(
        |rule| rule.connect.iter().any(|pattern| pattern.matches(target)),
        || format!("connect or send to {}", target),
    )
}

const IPPROTO_SCTP: i32 = 132;
const IP_OPTIONS: i32 = 4;

/// Returns the level, type and data of the control messages in the buffer,
/// which Linux parses the same way.
fn cmsgs(control: &[u8]) -> Result<Vec<(i32, i32, &[u8])>> {
    const HDR_LEN: usize = std::mem::size_of::<libc::cmsghdr>();
    const ALIGN: usize = std::mem::size_of::<usize>();
    let mut cmsgs = Vec::new();
    let mut offset = 0;
    while control.len().saturating_sub(offset) >= HDR_LEN {
        let rest = &control[offset..];
        let hdr: libc::cmsghdr = unsafe { std::ptr::read_unaligned(rest.as_ptr() as *const _) };
        let len = hdr.cmsg_len as usize;
        if len < HDR_LEN || len > rest.len() {
            return_errno!(EINVAL, "invalid control message");
        }
        cmsgs.push((hdr.cmsg_level, hdr.cmsg_type, &rest[HDR_LEN..len]));
        offset += (len + ALIGN - 1) & !(ALIGN - 1);
    }
    Ok(cmsgs)
}
