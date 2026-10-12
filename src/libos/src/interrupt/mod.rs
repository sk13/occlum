pub use self::sgx::sgx_interrupt_info_t;
use crate::prelude::*;
use crate::process::ThreadRef;
use crate::syscall::exception_interrupt_syscall_c_abi;
use crate::syscall::{CpuContext, ExtraContext, SyscallNum};

mod sgx;

pub fn init() {
    unsafe {
        let status = sgx::sgx_interrupt_init(handle_interrupt);
        assert!(status == sgx_status_t::SGX_SUCCESS);
    }
}

extern "C" fn handle_interrupt(info: *mut sgx_interrupt_info_t) -> i32 {
    unsafe {
        exception_interrupt_syscall_c_abi(SyscallNum::HandleInterrupt as u32, info as *mut _)
    };
    unreachable!();
}

pub fn do_handle_interrupt(
    info: *mut sgx_interrupt_info_t,
    cpu_context: *mut CpuContext,
) -> Result<isize> {
    let info = unsafe { &mut *info };
    let context = unsafe { &mut *cpu_context };
    // The cpu context is overriden so that it is as if the syscall is called from where the
    // interrupt happened
    *context = CpuContext::from_sgx(&info.cpu_context);
    context.extra_context = ExtraContext::XsaveOnStack;
    context.extra_context_ptr = info.xsave_area.as_mut_ptr();
    context.extra_context_size = info.xsave_size;
    Ok(0)
}

/// Broadcast interrupts to threads by sending POSIX signals.
///
/// A thread is interrupted only if it has a reason to leave its blocking call, see
/// `has_reason_to_interrupt`.
pub fn broadcast_interrupts() -> Result<usize> {
    let num_signaled_threads = crate::process::table::get_all_threads()
        .iter()
        .filter(|thread| has_reason_to_interrupt(thread))
        .map(|thread| {
            let host_tid = {
                let sched = thread.sched().lock().unwrap();
                match sched.host_tid() {
                    None => return false,
                    Some(host_tid) => host_tid,
                }
            };
            let signum = 64; // real-time signal 64 is used to notify interrupts
            let is_signaled = unsafe {
                let mut retval = 0;
                let status = occlum_ocall_tkill(&mut retval, host_tid, signum);
                assert!(status == sgx_status_t::SGX_SUCCESS);
                if retval == 0 {
                    true
                } else {
                    false
                }
            };
            is_signaled
        })
        .filter(|&is_signaled| is_signaled)
        .count();
    Ok(num_signaled_threads)
}

/// Returns whether the thread has to leave the blocking call that it is in, if it is in
/// one: its process is forced to exit, or it is forced to stop (another thread of its
/// process calls vfork), or it has a signal to deliver.
///
/// A signal that `deliver_signal` discards, e.g., one that is ignored, is no reason: the
/// call has to go on, as Linux fails a blocking call with EINTR only if a signal handler
/// has run for it. Such a signal stays in the queue of a thread that is blocked, as the
/// thread discards it on its way back to user space, so it must not interrupt the thread
/// at every tick until then.
fn has_reason_to_interrupt(thread: &ThreadRef) -> bool {
    let process = thread.process();
    process.is_forced_to_exit()
        || thread.is_forced_to_stop()
        || crate::signal::has_signal_to_deliver(thread, process)
}

extern "C" {
    fn occlum_ocall_tkill(retval: &mut i32, host_tid: pid_t, signum: i32) -> sgx_status_t;
}

pub fn enable_current_thread() {
    // Interruptible range
    let (addr, size) = {
        let thread = current!();
        let vm = thread.vm();
        let range = vm.get_process_range();
        (range.start(), range.size())
    };
    unsafe {
        let status = sgx::sgx_interrupt_enable(addr, size);
        assert!(status == sgx_status_t::SGX_SUCCESS);
    }
}

pub fn disable_current_thread() {
    unsafe {
        let status = sgx::sgx_interrupt_disable();
        assert!(status == sgx_status_t::SGX_SUCCESS);
    }
}
