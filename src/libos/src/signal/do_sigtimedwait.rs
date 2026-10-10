use std::sync::Weak;
use std::time::Duration;

use super::{siginfo_t, SigNum, SigSet, Signal};
use crate::events::{Observer, Waiter, WaiterQueueObserver};
use crate::prelude::*;
use crate::process::{ProcessRef, TermStatus, ThreadRef};

pub fn do_sigtimedwait(interest: SigSet, timeout: Option<&Duration>) -> Result<siginfo_t> {
    debug!(
        "do_rt_sigtimedwait: interest: {:?}, timeout: {:?}",
        interest, timeout,
    );

    let thread = current!();
    let process = thread.process().clone();

    // Interesting, blocked signals
    let interest = {
        let blocked = thread.sig_mask().read().unwrap();
        *blocked & interest
    };

    let signal = {
        let pending_sig_waiter = PendingSigWaiter::new(thread, process, interest);
        pending_sig_waiter.wait(timeout).map_err(|e| {
            if e.errno() == Errno::EINTR {
                return e;
            }
            errno!(EAGAIN, "no interesting, pending signal")
        })?
    };

    let siginfo = signal.to_info();
    Ok(siginfo)
}

pub struct PendingSigWaiter {
    thread: ThreadRef,
    process: ProcessRef,
    interest: SigSet,
    observer: Arc<WaiterQueueObserver<SigNum>>,
}

impl PendingSigWaiter {
    pub fn new(thread: ThreadRef, process: ProcessRef, interest: SigSet) -> Arc<Self> {
        let observer = WaiterQueueObserver::new();

        let weak_observer = Arc::downgrade(&observer) as Weak<dyn Observer<_>>;
        thread.sig_queues().read().unwrap().notifier().register(
            weak_observer.clone(),
            Some(interest),
            None,
        );
        process.sig_queues().read().unwrap().notifier().register(
            weak_observer,
            Some(interest),
            None,
        );

        Arc::new(Self {
            thread,
            process,
            interest,
            observer,
        })
    }

    pub fn wait(&self, timeout: Option<&Duration>) -> Result<Box<dyn Signal>> {
        let waiter_queue = self.observer.waiter_queue();
        let waiter = Waiter::new();
        loop {
            // Try to dequeue a pending signal from the current process or thread
            if let Some(signal) =
                dequeue_pending_signal(&self.interest, &self.thread, &self.process)
            {
                return Ok(signal);
            }

            // If the timeout is zero and if no pending signals, return immediately with an error.
            if let Some(duration) = timeout {
                if *duration == Duration::new(0, 0) {
                    return_errno!(ETIMEDOUT, "timeout");
                }
            }

            // Enqueue the waiter so that it can be waken up by the queue later.
            waiter_queue.reset_and_enqueue(&waiter);

            // As there is no intersting signal to dequeue right now, let's wait
            // some time to try again later. Most likely, the waiter will keep
            // waiting until being waken up by the waiter queue, which means
            // the arrival of an interesting signal.
            let res = waiter.wait(timeout);

            // Do not try again if some error is encountered. There are only
            // two possible errors: ETIMEDOUT or EINTR.
            if let Err(e) = res {
                // When interrupted or timeout is reached, it is possible that the interrupting signal happens
                // to be an interesting and pending signal. So we attempt to dequeue again.
                if e.errno() == Errno::EINTR || e.errno() == Errno::ETIMEDOUT {
                    if let Some(signal) =
                        dequeue_pending_signal(&self.interest, &self.thread, &self.process)
                    {
                        return Ok(signal);
                    }
                }
                return Err(e);
            }
        }
    }

    pub fn suspend(&self) -> Result<()> {
        let waiter_queue = self.observer.waiter_queue();
        let waiter = Waiter::new();
        // As EINTR may occur even if there are no interesting signals
        loop {
            // Try to search for an interesting signal from the current process or thread
            if has_interest_signal(&self.interest, &self.thread, &self.process) {
                return Ok(());
            }

            // Enqueue the waiter so that it can be waken up by the queue later.
            waiter_queue.reset_and_enqueue(&waiter);

            // As there is no intersting signal right now, let's wait for
            // the arrival of an interesting signal.
            let res = waiter.wait(None);

            // The only possible error is EINTR, as there is no timeout.
            if let Err(e) = res {
                // The queue has not woken up the waiter, so it is still in the queue.
                // Dequeue it as it is enqueued again if we wait again.
                waiter_queue.dequeue(&waiter);

                // The host interrupts a thread if it has a pending signal that it does not
                // block, or if its process is forced to exit or the thread is forced to
                // stop. The interrupt arrives some time after the host has looked for the
                // pending signal, so the signal may have been delivered by then, e.g., if
                // it has ended the previous sigsuspend of the thread and the thread has
                // called sigsuspend again. Then the interrupt is not for this call, and we
                // wait again, unless the thread has to exit or to stop.
                if e.errno() == Errno::EINTR && !self.is_forced_to_return() {
                    continue;
                }
                return Err(e);
            }
        }
    }

    // Returns whether the thread must leave its call when it is interrupted, even if
    // it has no signal to deliver: its process is forced to exit or it is forced to stop.
    fn is_forced_to_return(&self) -> bool {
        self.process.is_forced_to_exit() || self.thread.is_forced_to_stop()
    }
}

impl Drop for PendingSigWaiter {
    fn drop(&mut self) {
        let weak_observer = Arc::downgrade(&self.observer) as Weak<dyn Observer<_>>;
        self.thread
            .sig_queues()
            .read()
            .unwrap()
            .notifier()
            .unregister(&weak_observer);
        self.process
            .sig_queues()
            .read()
            .unwrap()
            .notifier()
            .unregister(&weak_observer);
    }
}

fn has_interest_signal(interest: &SigSet, thread: &ThreadRef, process: &ProcessRef) -> bool {
    let pending = (process.sig_queues().read().unwrap().pending()
        | thread.sig_queues().read().unwrap().pending())
        & *interest;

    !pending.empty()
}

fn dequeue_pending_signal(
    interest: &SigSet,
    thread: &ThreadRef,
    process: &ProcessRef,
) -> Option<Box<dyn Signal>> {
    dequeue_process_pending_signal(process, interest)
        .or_else(|| dequeue_thread_pending_signal(thread, interest))
}

fn dequeue_process_pending_signal(
    process: &ProcessRef,
    interest: &SigSet,
) -> Option<Box<dyn Signal>> {
    let blocked = !*interest;
    process.sig_queues().write().unwrap().dequeue(&blocked)
}

fn dequeue_thread_pending_signal(thread: &ThreadRef, interest: &SigSet) -> Option<Box<dyn Signal>> {
    let blocked = !*interest;
    thread.sig_queues().write().unwrap().dequeue(&blocked)
}
