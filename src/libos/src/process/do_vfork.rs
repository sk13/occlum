use super::thread::close_files;
use super::untrusted_event::{set_event, wait_event};
use super::{ProcessFilter, ProcessRef, ProcessStatus, TermStatus, ThreadId, ThreadRef};
use crate::fs::FileTable;
use crate::interrupt::broadcast_interrupts;
use crate::prelude::*;
use crate::syscall::{BoxXsaveArea, CpuContext, ExtraContext};
use std::collections::HashMap;
use std::mem;

// From Man page: The calling thread is suspended until the child terminates (either normally, by calling
// _exit(2), or abnormally, after delivery of a fatal signal), or it makes a call to execve(2).
// Until that point, the child shares all memory with its parent, including the stack.
//
// Thus in this implementation, the main idea is to let child use parent's task until exit or execve.
//
// Limitation:
// 1. The child process will not have a complete process structure before execve. Thus during the time from vfork
// to new child process execve or exit, the child process just reuse the parent process's everything, including
// task, pid and etc. And also the log of child process will not start from the point that vfork returns but the
// point that execve returns.
// 2. When vfork is called and the current process has other running child threads, for Linux, the other threads remain
// running. For Occlum, this behavior is different. All the other threads will be frozen until the vfork returns or
// execve is called in the child process. The reason is that since Occlum doesn't support fork, many applications will
// use vfork to replace fork. For multi-threaded applications, if vfork doesn't stop other child threads, the application
// will be more likely to fail because the child process directly uses the VM and the file table of the parent process.
// 3. A vforked child can call vfork, too, and so can its child. Every vfork is a level (VforkLevel) that holds what its child
// needs to return to its parent: the pid of the child and the context and the file table of the parent. The levels are a stack,
// as the innermost child has to exit or call execve before its parent continues. All the children run on the thread of the
// outermost parent, so the stack is in the TLS. A level that returns restores the context and the file table of its parent,
// which is the vforked child of the level before. The process is stopped, and the other threads are frozen, from the vfork of the
// outermost child until it returns, not until the innermost one does. Like the outermost child, all of them have the pid of the
// process (see limitation 1). The process that a child creates with execve is a child of the process, not of the vforked child
// that has no process structure, so the process can wait for it, too. If nobody waits for it, it stays a zombie until the
// process ends. The levels cost memory of the LibOS, so there are at most MAX_VFORK_DEPTH of them. A child that a signal
// kills never returns to its parent: its thread ends, and with it the process (see exit_thread, which frees the levels and
// resumes the other threads, so that they can end, too).
// 4. A vforked child ends with exit_group or exit (see do_exit_group and do_exit), e.g., with syscall(SYS_exit), which the Go
// runtime makes in the child when its execve fails. Both return to the parent: the child has no thread to end, it runs on the
// thread of its parent, and the clear_child_tid and the robust list of the thread are those of the parent, too. What libc does
// before it makes the system call is not for the LibOS to undo: pthread_exit runs the destructors of the thread and unwinds its
// stack, which are those of the parent, so the parent is not as it was when it returns (glibc aborts with a smashed stack if
// the parent has no other thread, musl hangs when the program ends). As in Linux, nothing but _exit and execve is safe in a
// vforked child. The signal handlers of the parent can run in the child, as the child runs on the thread of the parent. A
// handler that ends the child does not return (no rt_sigreturn), so the signal mask and the saved context of the handler stay on
// the thread when it returns to the parent. After a number of such children (the nested handlers are limited) the enclave aborts.
// 5. The threads that a vforked child creates are threads of the process, which the vfork does not freeze. They run on when the
// child has returned to its parent, also after exit_group, which ends them in Linux, and wait4 does not wait for them. An
// exit_group of one of them ends the process, with the parent. Creating a thread in the child with clone works, but not
// necessarily with libc: musl's pthread_create has been seen to hang in a vforked child (not investigated; the other threads
// are frozen wherever they were, e.g., while they hold a lock of libc).

// The exit status of the child process which directly calls exit after vfork.
struct ChildExitStatus {
    pid: pid_t,
    status: TermStatus,
}

// A level of vfork: what the vforked child needs to return to its parent.
struct VforkLevel {
    // The pid of the child
    child_pid: pid_t,
    // The cpu context of the parent, with the heap copy of its xsave area (see save_parent_context)
    parent_context: CpuContext,
    // The file table of the parent. It will be recovered when the child exits or has its own task.
    parent_file_table: FileTable,
}

lazy_static! {
    // Store all the child process's exit status which are created with vfork and directly exit without calling execve. Because
    // these children process are only allocated with a pid, they are not managed by the usual way including exit and wait. Use
    // this special structure to record these children.
    // K: parent pid, V: exit children created with vfork. The parent is the process, or the vforked child that called vfork
    // (see vfork_parent_pid).
    static ref EXIT_CHILDREN_STATUS: SgxMutex<HashMap<pid_t, Vec<ChildExitStatus>>> = SgxMutex::new(HashMap::new());
}

thread_local! {
    // Store the levels of vfork that the current thread runs the children of, the innermost last. A parent only has one vforked
    // child at a time, but the child can call vfork, too. There is no level if the thread is not a vforked child.
    static VFORK_LEVELS: RefCell<Vec<VforkLevel>> = Default::default();
}

// The deepest nesting of vfork: every level keeps a context, a file table and, if the parent used the syscall instruction,
// a copy of its xsave area (a few KB) in the heap of the LibOS. A runaway recursion must fail with an error, as the LibOS
// aborts if it runs out of heap.
const MAX_VFORK_DEPTH: usize = 1024;

pub fn do_vfork(mut context: *mut CpuContext) -> Result<isize> {
    // Fail before anything has changed
    if VFORK_LEVELS.with(|levels| levels.borrow().len()) >= MAX_VFORK_DEPTH {
        return_errno!(EAGAIN, "too many nested vforks");
    }
    let current = current!();
    trace!("vfork parent process pid = {:?}", current.process().pid());

    // Force stop all child threads
    // To prevent multiple threads do vfork simultaneously and force stop each other, the thread must change the process status at first.
    // A vforked child that calls vfork runs in a process that is stopped already, by the vfork of the outermost child. It
    // is this thread that resumes it, so the thread must not wait for it.
    if !is_vforked_child_process() {
        loop {
            let mut process_inner = current.process().inner();
            if process_inner.status() == ProcessStatus::Stopped {
                trace!("process is doing vfork, current thread handle force stop");
                drop(process_inner);
                handle_force_stop();
                continue;
            } else {
                trace!("current thread start vfork");
                process_inner.stop();
                break;
            }
        }
    }

    // Stop all other child threads
    vfork_stop_all_child_thread(&current);

    // Generate a new pid for child process
    let child_pid = {
        let new_tid = ThreadId::new();
        new_tid.as_u32() as pid_t
    };

    // Save parent's file table.
    let parent_file_table = vfork_save_file_table(&current);

    // Save parent's context in TLS. This is done last, so that no error return can leave
    // the heap copy of its xsave area behind. Both are saved in the level of the child.
    let parent_context = save_parent_context(unsafe { &*context });
    VFORK_LEVELS.with(|levels| {
        levels.borrow_mut().push(VforkLevel {
            child_pid,
            parent_context,
            parent_file_table,
        });
    });

    // This is the first time return and will return as child.
    // The second time return will return as parent in vfork_return_to_parent.
    info!("vfork child pid = {:?}", child_pid);
    return Ok(0 as isize);
}

// Make a copy of the parent's context that stays valid while the child runs on the same thread.
//
// If the parent called vfork with the emulated "syscall" instruction, its vector registers are
// in the xsave area of the exception handling of the SDK (ExtraContext::XsaveOnStack). That
// memory is on the stack and is overwritten by the next exception of this thread, e.g., by the
// emulated system calls of the child. Restoring the parent's registers from it would give the
// parent garbage. So the xsave area is copied to the heap, like do_sigreturn does when it must
// keep one, and the saved context refers to the copy (ExtraContext::XsaveOnHeap).
//
// The saved context owns the copy, as the copy has no other owner. restore_parent_process
// moves the ownership to the context that it restores. do_sysret restores from the copy and
// hands it to the PendingFpAreas of the thread, which frees it before the next restore (or when
// the thread is dropped). A saved context that is not restored is freed by
// free_xsave_area_of_saved_context.
fn save_parent_context(context: &CpuContext) -> CpuContext {
    let mut saved_context = *context;
    if saved_context.extra_context_ptr.is_null() {
        // vfork through the Occlum entry: the registers of the parent are not in an extra context
        return saved_context;
    }
    match saved_context.extra_context {
        ExtraContext::XsaveOnStack => {
            let xsave_size = saved_context.extra_context_size as usize;
            assert!(xsave_size != 0, "no xsave area size");
            let xsave_area = BoxXsaveArea::new_with_slice(unsafe {
                std::slice::from_raw_parts(saved_context.extra_context_ptr, xsave_size)
            });
            let (xsave_ptr, xsave_size) = xsave_area.into_raw();
            saved_context.extra_context = ExtraContext::XsaveOnHeap;
            saved_context.extra_context_ptr = xsave_ptr;
            saved_context.extra_context_size = xsave_size as u64;
        }
        // The context of a system call comes from the Occlum entry (no extra context) or from
        // an exception (XsaveOnStack). An extra context on the heap only exists while a system
        // call returns to the user space, and is owned by the context that is returned with.
        // A copy of that context would own it, too.
        ExtraContext::Fpregs | ExtraContext::XsaveOnHeap => {
            unreachable!("the context of a vfork system call has an extra context on the heap")
        }
    }
    saved_context
}

// Free the heap copy of the xsave area in a context that save_parent_context returned, if the
// context is not going to be restored.
fn free_xsave_area_of_saved_context(context: &CpuContext) {
    if context.extra_context_ptr.is_null() {
        return;
    }
    debug_assert!(matches!(context.extra_context, ExtraContext::XsaveOnHeap));
    drop(unsafe {
        BoxXsaveArea::from_raw(
            context.extra_context_ptr,
            context.extra_context_size as usize,
        )
    });
}

// Forget the vfork state of a LibOS thread that ends. A vforked child that does not leave with
// exit_group or execve, e.g., because a signal kills it, never returns to its parent. Its saved
// contexts would stay in the TLS of the host thread, and the next LibOS thread on that host thread
// would take itself for a vforked child, e.g., in its execve.
//
// Nobody will use the saved file tables of the parents, either, as the process ends with the child.
// They hold the original files of the process, so close them like close_all_files does with the
// file table in use. Otherwise, e.g., the write end of a pipe would stay open for ever.
//
// Returns whether the thread was a vforked child. Then the other threads of the process are frozen
// still (see resume_frozen_threads).
pub fn reset_vfork_context() -> bool {
    let levels = VFORK_LEVELS.with(|levels| levels.take());
    let was_vforked_child = !levels.is_empty();
    for mut level in levels {
        free_xsave_area_of_saved_context(&level.parent_context);
        close_files(level.parent_file_table.del_all());
        EXIT_CHILDREN_STATUS
            .lock()
            .unwrap()
            .remove(&level.child_pid);
    }
    was_vforked_child
}

// Check if the calling process is a vforked child process that reuse parent's task and pid.
pub fn is_vforked_child_process() -> bool {
    VFORK_LEVELS.with(|levels| !levels.borrow().is_empty())
}

// The pid of the parent of the children that the caller creates with vfork. It is the pid of the
// innermost vforked child, if the caller is one, and otherwise the pid of the process. The vforked
// child runs in the process of its parent and has no process structure of its own, but the exit
// statuses of its children must not be mixed up with those of its parent.
fn vfork_parent_pid(process_pid: pid_t) -> pid_t {
    VFORK_LEVELS.with(|levels| {
        levels
            .borrow()
            .last()
            .map_or(process_pid, |level| level.child_pid)
    })
}

// Replace the file table of the current thread with a clone for the child, and return the original file table.
fn vfork_save_file_table(current: &ThreadRef) -> FileTable {
    let mut current_file_table = current.files().lock();
    let new_file_table = current_file_table.clone();
    // FileTable contains non-cloned struct, so here we do a memory replacement to use new
    // file table in child and store the original file table in TLS.
    mem::replace(&mut *current_file_table, new_file_table)
}

fn vfork_stop_all_child_thread(current: &ThreadRef) {
    // stop all other child threads
    loop {
        let child_threads = current.process().threads();
        let running_thread_num = child_threads
            .iter()
            .filter(|thread| !thread.is_stopped() && thread.tid() != current.tid())
            .map(|thread| {
                thread.force_stop();
                thread
            })
            .count();

        trace!("running threads num: {:?}", running_thread_num);

        if running_thread_num == 0 {
            trace!("all other threads are stopped");
            break;
        }

        // Don't hesitate. Interrupt all threads right now to stop child threads.
        broadcast_interrupts();
    }
}

// Return to parent process to continue executing
pub fn vfork_return_to_parent(
    mut context: *mut CpuContext,
    current_ref: &ThreadRef,
    child_exit_status: Option<TermStatus>, // If the child process exits, the exit status should be specified.
) -> Result<isize> {
    let child_pid = restore_parent_process(context, current_ref)?;

    if let Some(term_status) = child_exit_status {
        record_exit_child(
            vfork_parent_pid(current_ref.process().pid()),
            child_pid as pid_t,
            term_status,
        );
    }

    // The parent is a vforked child, too, which still runs in the stopped process: the other threads stay frozen
    // until the outermost child returns.
    if is_vforked_child_process() {
        return Ok(child_pid);
    }

    resume_frozen_threads(&current!());

    Ok(child_pid)
}

// Wake the threads that the vfork of the outermost child has frozen, and let the process run again. The caller is the
// thread of the child, which is the last one to run in the process: it returns to its parent, or it ends.
pub fn resume_frozen_threads(current: &ThreadRef) {
    // Wake parent's child thread which are all sleeping
    // Hold the process inner lock during the wake process to avoid other threads do vfork again and try to stop the thread
    let mut process_inner = current.process().inner();
    // The process is gone if the other threads have ended meanwhile (they can, if the child has created them)
    if let Some(child_threads) = process_inner.threads() {
        child_threads.iter().for_each(|thread| {
            // Only the threads that the vfork has frozen wait for the event. A thread that the child has created since is
            // running, or has not started yet, and then it has no event to wake it (set_event fails, which ends the enclave).
            if !thread.is_stopped() {
                return;
            }
            thread.resume();
            let thread_ptr = thread.raw_ptr();
            if current.raw_ptr() != thread_ptr {
                set_event(thread_ptr as *const c_void);
                info!("Thread 0x{:x} is waken", thread_ptr);
            }
        });
        process_inner.resume();
    }
}

fn record_exit_child(parent_pid: pid_t, child_pid: pid_t, child_exit_status: TermStatus) {
    let child_exit_status = ChildExitStatus::new(child_pid, child_exit_status);

    let mut children_status = EXIT_CHILDREN_STATUS.lock().unwrap();
    // The child is gone, and with it the statuses of its children that nobody has waited for: as it has no
    // process structure, nobody would adopt them.
    children_status.remove(&child_pid);
    if let Some(children) = children_status.get_mut(&parent_pid) {
        children.push(child_exit_status);
    } else {
        children_status.insert(parent_pid, vec![child_exit_status]);
    }
}

fn restore_parent_process(mut context: *mut CpuContext, current_ref: &ThreadRef) -> Result<isize> {
    let current_thread = current!();

    // Leave the innermost level. Nothing can fail after this, so the level is not lost half restored.
    let level = VFORK_LEVELS.with(|levels| levels.borrow_mut().pop());
    let level = match level {
        Some(level) => level,
        None => return_errno!(EFAULT, "couldn't find the vfork level to return from"),
    };
    let parent_file_table = level.parent_file_table;

    // Close all child opened files
    close_files_opened_by_child(current_ref, &parent_file_table);

    // Restore parent file table
    let mut current_file_table = current_ref.files().lock();
    *current_file_table = parent_file_table;

    // Restore CpuContext. The context of the child owns no extra context
    // (see save_parent_context). The restored one owns the heap copy of the xsave area of the
    // parent, if there is one, which do_sysret restores and hands to the PendingFpAreas.
    unsafe { *context = level.parent_context };

    // Set return value to child_pid
    // This will be the second time return
    Ok(level.child_pid as isize)
}

pub fn check_vfork_for_exec(current_ref: &ThreadRef) -> Option<(ThreadId, Option<ProcessRef>)> {
    let current_pid = current_ref.process().pid();
    if is_vforked_child_process() {
        let mut child_pid = 0;
        VFORK_LEVELS.with(|levels| {
            // The innermost child is the one that calls execve
            child_pid = levels.borrow().last().unwrap().child_pid;
        });
        return Some((
            // Reuse tid which was generated when do_vfork
            ThreadId {
                tid: child_pid as u32,
            },
            // By default, use current process as parent
            None,
        ));
    } else {
        None
    }
}

fn close_files_opened_by_child(current: &ThreadRef, parent_file_table: &FileTable) {
    let current_file_table = current.files().lock();
    let child_open_fds: Vec<FileDesc> = current_file_table
        .table()
        .iter()
        .enumerate()
        .filter(|(fd, _entry)| {
            // Entry is only shown in the child file table
            _entry.is_some() && parent_file_table.get_entry(*fd as FileDesc).is_err()
        })
        .map(|(fd, entry)| fd as FileDesc)
        .collect();

    drop(current_file_table);

    child_open_fds
        .iter()
        .for_each(|&fd| current.close_file(fd).expect("close child file error"));
}

pub fn handle_force_stop() {
    let current = current!();
    if current.is_forced_to_stop() {
        let current_thread_ptr = current.raw_ptr();
        info!(
            "Thread 0x{:x} is forced to stop ...",
            current_thread_ptr as usize
        );

        current.inner().stop();
        while current.is_stopped() {
            wait_event(current_thread_ptr as *const c_void);
        }
    }
}

// Wait4 unwaited child which are created with vfork and directly exit without calling execve.
pub fn wait4_exit_child_created_with_vfork(
    process_pid: pid_t,
    child_filter: &ProcessFilter,
) -> Option<(pid_t, i32)> {
    let parent_pid = vfork_parent_pid(process_pid);
    let mut children_status = EXIT_CHILDREN_STATUS.lock().unwrap();
    if let Some(children) = children_status.get_mut(&parent_pid) {
        let unwaited_child_idx = children.iter().position(|child| match child_filter {
            ProcessFilter::WithAnyPid => true,
            ProcessFilter::WithPid(pid) => pid == child.pid(),
            // The children are in the process group of the process
            ProcessFilter::WithPgid(pgid) => *pgid == current!().process().pgid(),
        });

        if let Some(child_idx) = unwaited_child_idx {
            let child = children.remove(child_idx);
            if children.is_empty() {
                children_status.remove(&parent_pid);
            }
            return Some((*child.pid(), child.status().as_u32() as i32));
        }
    }

    None
}

// Reap all unwaited child which are created with vfork and directly exit without calling execve.
pub fn reap_zombie_child_created_with_vfork(parent_pid: pid_t) -> Option<Vec<pid_t>> {
    let mut children_status = EXIT_CHILDREN_STATUS.lock().unwrap();

    let children = children_status.remove(&parent_pid);
    if children.is_none() {
        warn!("no vforked children found");
        return None;
    }

    Some(
        children
            .unwrap()
            .into_iter()
            .map(|child| child.pid)
            .collect(),
    )
}

impl ChildExitStatus {
    fn new(child_pid: pid_t, status: TermStatus) -> Self {
        Self {
            pid: child_pid,
            status,
        }
    }

    fn pid(&self) -> &pid_t {
        &self.pid
    }

    fn status(&self) -> &TermStatus {
        &self.status
    }
}
