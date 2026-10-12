use std::time::Duration;

use super::host_file_epoller::HostFileEpoller;
use crate::events::{poll_with_eventfd, Waiter};
use crate::prelude::*;

/// A waiter that is suitable for epoll.
pub struct EpollWaiter {
    waiter: Waiter,
    host_epoll_fd: FileDesc,
}

impl EpollWaiter {
    pub fn new(host_file_epoller: &HostFileEpoller) -> Self {
        Self {
            waiter: Waiter::new(),
            host_epoll_fd: host_file_epoller.host_fd().to_raw(),
        }
    }

    /// Wait until the waiter is waken or the host epoll file has any
    /// events or the method call is timeout or interrupted.
    pub fn wait_mut(&self, mut timeout: Option<&mut Duration>) -> Result<()> {
        const ZERO: Duration = Duration::from_secs(0);
        if let Some(timeout) = timeout.as_ref() {
            if **timeout == ZERO {
                return_errno!(ETIMEDOUT, "should return immediately");
            }
        }

        let host_eventfd = libc::pollfd {
            fd: self.waiter.host_eventfd().host_fd() as i32,
            events: libc::POLLIN,
            revents: 0,
        };
        let host_epf = libc::pollfd {
            fd: self.host_epoll_fd as i32,
            events: libc::POLLIN,
            revents: 0,
        };
        let mut pollfds = [host_eventfd, host_epf];
        let host_eventfd_idx = 0;

        let num_events = poll_with_eventfd(&mut pollfds, &mut timeout, host_eventfd_idx)?;

        // Poll syscall does not treat timeout as error. So we need
        // to distinguish the case by ourselves.
        if let Some(timeout) = timeout.as_mut() {
            if num_events == 0 {
                **timeout = ZERO;
                return_errno!(ETIMEDOUT, "no results and the time is up");
            }
        }

        Ok(())
    }
}

impl AsRef<Waiter> for EpollWaiter {
    fn as_ref(&self) -> &Waiter {
        &self.waiter
    }
}
