use super::thread::sgx_thread_get_self;
use super::untrusted_event::{set_event, wait_event};
/// A wait/wakeup mechanism that connects wait4 and exit system calls.
use crate::prelude::*;

#[derive(Debug)]
pub struct Waiter<D, R>
where
    D: Sized + Copy,
    R: Sized + Copy,
{
    inner: Arc<SgxMutex<WaiterInner<D, R>>>,
    thread: *const c_void,
}

unsafe impl<D, R> Send for Waiter<D, R>
where
    D: Sized + Copy,
    R: Sized + Copy,
{
}

#[derive(Debug)]
struct WaiterInner<D, R>
where
    D: Sized + Copy,
    R: Sized + Copy,
{
    is_woken: bool,
    data: D,
    result: Option<R>,
}

impl<D, R> Waiter<D, R>
where
    D: Sized + Copy,
    R: Sized + Copy,
{
    pub fn new(data: &D) -> Waiter<D, R> {
        Waiter {
            thread: unsafe { sgx_thread_get_self() },
            inner: Arc::new(SgxMutex::new(WaiterInner {
                is_woken: false,
                data: *data,
                result: None,
            })),
        }
    }

    pub fn get_data(&self) -> D {
        self.inner.lock().unwrap().data
    }

    pub fn sleep_until_woken_with_result(self) -> Option<R> {
        while !self.inner.lock().unwrap().is_woken {
            unsafe {
                wait_event(self.thread);
            }
        }

        self.inner.lock().unwrap().result
    }

    /// Sleep until woken, or until the current thread is interrupted: its process
    /// is forced to exit, or it has a signal to deliver.
    ///
    /// Returns whether the waiter has been woken. An interrupted waiter remains in
    /// its wait queue; the caller removes it with WaitQueue::del_waiter, and then
    /// checks with result() whether it has been woken in the meantime.
    pub fn sleep_until_woken_or_interrupted(&self) -> bool {
        let thread = current!();
        let process = thread.process();
        loop {
            if self.inner.lock().unwrap().is_woken {
                return true;
            }
            if process.is_forced_to_exit() || crate::signal::has_signal_to_deliver(&thread, process)
            {
                return false;
            }
            // The wait also returns when the host interrupts it with a signal, which
            // it does for threads with pending signals, see broadcast_interrupts
            wait_event(self.thread);
        }
    }

    /// Returns the result of the wakeup, if the waiter has been woken.
    pub fn result(&self) -> Option<R> {
        self.inner.lock().unwrap().result
    }
}

#[derive(Debug)]
pub struct WaitQueue<D, R>
where
    D: Sized + Copy,
    R: Sized + Copy,
{
    waiters: Vec<Waiter<D, R>>,
}

impl<D, R> WaitQueue<D, R>
where
    D: Sized + Copy,
    R: Sized + Copy,
{
    pub fn new() -> WaitQueue<D, R> {
        WaitQueue {
            waiters: Vec::new(),
        }
    }

    pub fn add_waiter(&mut self, waiter: &Waiter<D, R>) -> () {
        self.waiters.push(Waiter {
            thread: waiter.thread,
            inner: waiter.inner.clone(),
        });
    }

    /// Remove the waiter from the queue, if it has not been woken yet.
    pub fn del_waiter(&mut self, waiter: &Waiter<D, R>) {
        self.waiters
            .retain(|queued| !Arc::ptr_eq(&queued.inner, &waiter.inner));
    }

    pub fn del_and_wake_one_waiter<F>(&mut self, cond: F) -> usize
    where
        F: Fn(&D) -> Option<R>,
    {
        let mut waiters = &mut self.waiters;
        let del_waiter_i = {
            let waiter_i = waiters.iter().position(|waiter| {
                let mut waiter_inner = waiter.inner.lock().unwrap();
                if let Some(waiter_result) = cond(&waiter_inner.data) {
                    waiter_inner.is_woken = true;
                    waiter_inner.result = Some(waiter_result);
                    true
                } else {
                    false
                }
            });
            if waiter_i.is_none() {
                return 0;
            }
            waiter_i.unwrap()
        };
        let del_waiter = waiters.swap_remove(del_waiter_i);
        set_event(del_waiter.thread);
        1
    }

    pub fn del_and_wake_all_waiters(&mut self) -> usize {
        let mut waiters = &mut self.waiters;
        let ret = waiters.len();
        waiters.drain(..).for_each(|waiter| {
            let mut waiter_inner = waiter.inner.lock().unwrap();
            waiter_inner.is_woken = true;
            waiter_inner.result = None;
            set_event(waiter.thread);
        });

        ret
    }
}
