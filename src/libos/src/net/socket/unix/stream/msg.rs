//! The sending and receiving of messages with control data, `sendmsg` and `recvmsg`: the file
//! descriptors that SCM_RIGHTS sends (see `control.rs`).

use super::control::{Outgoing, Received};
use super::*;

impl Stream {
    /// Sends the bytes of the buffers with the control messages, which are validated first: the
    /// call fails before it sends anything if one of them is invalid. A socket that replaces a
    /// TCP socket has no control messages to send, as a TCP socket ignores them.
    pub fn sendmsg(
        &self,
        bufs: &[&[u8]],
        flags: SendFlags,
        addr: Option<&AnyAddr>,
        control: Option<&[u8]>,
    ) -> Result<usize> {
        self.check_no_addr(addr.is_some())?;
        let control = match control {
            Some(control) if self.inet_view().is_none() => {
                Some(Outgoing::parse(control)?).filter(|control| !control.is_empty())
            }
            _ => None,
        };
        self.send_with(bufs, flags, control)
    }

    /// Receives bytes into the buffers, and the control messages that belong to them into the
    /// buffer of the control messages: the files of SCM_RIGHTS get new file descriptors. A control
    /// message that does not fit, or that has no buffer, is dropped, and MSG_CTRUNC tells so.
    pub fn recvmsg(
        &self,
        bufs: &mut [&mut [u8]],
        flags: RecvFlags,
        control: Option<&mut [u8]>,
    ) -> Result<(usize, Option<AnyAddr>, MsgFlags, usize)> {
        let mut received = Received::default();
        let data_len = self.recv_with(bufs, flags, &mut received)?;

        // The name of the peer is the only other field that is supported
        let cloexec = flags.contains(RecvFlags::MSG_CMSG_CLOEXEC);
        let (control_len, truncated) = received.write_control(control, cloexec);
        let mut msg_flags = MsgFlags::empty();
        if truncated {
            msg_flags |= MsgFlags::MSG_CTRUNC;
        }

        Ok((
            data_len,
            self.recv_addr().map(AnyAddr::Unix),
            msg_flags,
            control_len,
        ))
    }
}
