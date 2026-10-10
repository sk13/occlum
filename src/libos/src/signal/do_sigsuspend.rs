use super::constants::*;
use super::do_sigreturn::replace_sig_mask_until_sysret;
use super::do_sigtimedwait::PendingSigWaiter;
use super::{sigset_t, MaskOp, SigNum, SigSet, Signal};
use crate::prelude::*;

pub fn do_sigsuspend(mask: &SigSet) -> Result<()> {
    debug!("do_sigsuspend: mask: {:?}", mask);

    let thread = current!();
    let process = thread.process().clone();

    // Set signal mask
    let update_mask = {
        let mut set = *mask;
        // According to man pages, "it is not possible to block SIGKILL or SIGSTOP.
        // Attempts to do so are silently ignored."
        set -= SIGKILL;
        set -= SIGSTOP;
        set
    };

    // The original mask is restored after the handler of the signal that ends the
    // suspension has run, with the new mask in effect
    replace_sig_mask_until_sysret(&thread, update_mask);

    // Suspend for interest signal
    let interest = !update_mask;
    let pending_sig_waiter = PendingSigWaiter::new(thread.clone(), process, interest);

    let err = match pending_sig_waiter.suspend() {
        Ok(_) => {
            errno!(EINTR, "Wait for EINTR signal successfully")
        }
        // The thread is interrupted because it has to exit or to stop
        Err(e) => e,
    };

    Err(err)
}
