use super::*;

use crate::events::{Observer, Waiter, WaiterQueue};
use crate::process::{do_getegid, do_geteuid, gid_t, pid_t, uid_t, ThreadRef};
use crate::time::{do_gettimeofday, time_t};
use crate::util::mem_util::from_user;
use alloc::vec::Vec;
use bitflags::bitflags;
use core::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, Ordering};
use intrusive_collections::LinkedList;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::time::Duration;
use std::{cmp, time};

#[allow(non_camel_case_types)]
pub type key_t = u32;
pub type SemId = u32;
pub type CmdId = u32;

const IPC_PRIVATE: key_t = 0;

// Maximum number of semaphore sets
const SEMMNI: SemId = 128;
// Maximum semaphores per set
const SEMMSL: usize = 250;
// Maximum semaphores system-wide
const SEMMNS: usize = SEMMNI as usize * SEMMSL;
// Maximum operations per semop call
const SEMOPM: usize = 32;
// Maximum semaphore value
const SEMVMX: usize = 32767;
// Maximum adjustment of a semaphore on exit of a process, in both directions
const SEMAEM: usize = SEMVMX;

const IPC_RMID: CmdId = 0; // Remove semaphore set
const IPC_SET: CmdId = 1; // Set semaphore set parameters
const IPC_STAT: CmdId = 2; // Get semaphore set status
const IPC_INFO: CmdId = 3; // Get system-wide semaphore information

// Semaphore operation command constants (following System V specifications)
const SEM_GETPID: CmdId = 11; // Get PID of last operation
const SEM_GETVAL: CmdId = 12; // Get semaphore value
const SEM_GETALL: CmdId = 13; // Get all semaphore values in set
const SEM_GETNCNT: CmdId = 14; // Get count of processes waiting for value > current
const SEM_GETZCNT: CmdId = 15; // Get count of processes waiting for value = 0
const SEM_SETVAL: CmdId = 16; // Set semaphore value
const SEM_SETALL: CmdId = 17; // Set all semaphore values in set
const SEM_STAT: CmdId = 18; // Get status by semid
const SEM_INFO: CmdId = 19; // Get extended semaphore information
const SEM_STAT_ANY: CmdId = 20; // Get status by semid, without permission check

bitflags! {
    pub struct SemFlags: u32 {
        // IPC operation flags
        const IPC_CREAT = 0o1000;    // Create if not exists
        const IPC_EXCL = 0o2000;     // Fail if exists (with IPC_CREAT)
        const IPC_NOWAIT = 0o4000;   // Non-blocking mode

        // Semaphore-specific flags
        const SEM_UNDO = 0o10000;    // Auto-undo operations on process exit

        // Permission flags
        const S_IRUSR = 0o400;       // Owner read permission
        const S_IWUSR = 0o200;       // Owner write permission
        const S_IXUSR = 0o100;       // Owner execute permission

        const S_IRGRP = 0o040;       // Group read permission
        const S_IWGRP = 0o020;       // Group write permission
        const S_IXGRP = 0o010;       // Group execute permission

        const S_IROTH = 0o004;       // Others read permission
        const S_IWOTH = 0o002;       // Others write permission
        const S_IXOTH = 0o001;       // Others execute permission
    }
}

#[allow(non_camel_case_types)]
#[derive(Debug)]
#[repr(C)]
pub struct semids_t {
    sem_perm: ipc_perm_t,
    sem_otime: time_t,
    sem_otime_high: time_t,
    sem_ctime: time_t,
    sem_ctime_high: time_t,
    sem_nsems: u64,
    unused1: u64,
    unused2: u64,
}

/// Structure for storing semaphore system limits (used with IPC_INFO)
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, Copy)]
#[repr(C)]
struct seminfo_t {
    semmap: u32, // Number of entries in semaphore map
    semmni: u32, // Maximum number of semaphore sets
    semmns: u32, // Maximum number of semaphores in system
    semmnu: u32, // System-wide maximum number of undo structures
    semmsl: u32, // Maximum number of semaphores per set
    semopm: u32, // Maximum number of operations per semop call
    semume: u32, // Maximum number of undo entries per process
    semusz: u32, // Size in bytes of undo structure
    semvmx: u32, // Maximum semaphore value
    semaem: u32, // Adjust on exit max value
}

/// Structure for extended semaphore system information (used with SEM_INFO)
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, Copy)]
#[repr(C)]
struct seminfo_ext_t {
    sem_info: seminfo_t, // Basic limit information
    semusz: u32,         // Size of sem_undo structure
    semaem: u32,         // Max adjust on exit value
    sem_nsems: u32,      // Current number of semaphores in system
    sem_nsets: u32,      // Current number of semaphore sets in system
    sem_largest_id: u32, // Largest semaphore set identifier
}

#[allow(non_camel_case_types)]
#[derive(Debug, Clone, Copy)]
#[repr(C)]
struct ipc_perm_t {
    key: key_t,
    uid: uid_t,
    gid: gid_t,
    cuid: uid_t,
    cgid: gid_t,
    mode: u16,
    pad1: u16,
    seq: u16,
    pad2: u16,
    unused1: u64,
    unused2: u64,
}

#[allow(non_camel_case_types)]
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct sembuf_t {
    sem_num: u16, // Semaphore number
    sem_op: i16,  // Operation to perform
    sem_flg: i16, // Operation flags
}

struct Semaphore {
    count: i32,
    ncnt: usize,     // Number of threads waiting to acquire (count > 0)
    zcnt: usize,     // Number of threads waiting for count = 0
    last_pid: pid_t, // PID of last process that modified this semaphore
    // What each process has to add to count when it exits: the sum of its operations with
    // SEM_UNDO, negated. Processes without adjustment have no entry
    semadj: HashMap<pid_t, i32>,
}

impl Semaphore {
    fn new(initial_value: i32) -> Self {
        Semaphore {
            count: initial_value,
            ncnt: 0,
            zcnt: 0,
            last_pid: current!().process().pid(),
            semadj: HashMap::new(),
        }
    }

    fn get_semadj(&self, pid: pid_t) -> i32 {
        self.semadj.get(&pid).copied().unwrap_or(0)
    }

    fn set_semadj(&mut self, pid: pid_t, adj: i32) {
        if adj == 0 {
            self.semadj.remove(&pid);
        } else {
            self.semadj.insert(pid, adj);
        }
    }

    /// Executes the operation if it can complete immediately. If undo_pid is given, the
    /// operation is recorded as an operation with SEM_UNDO of this process
    /// Returns false if the operation would block, or an error if the count or the
    /// adjustment of the process would exceed the maximum
    fn try_op(&mut self, op: i32, undo_pid: Option<pid_t>) -> Result<bool> {
        // Zero operation: wait if count > 0
        if op == 0 {
            return Ok(self.count == 0);
        }

        // Negative operation: wait if insufficient count
        let new_count = self.count + op;
        if new_count < 0 {
            return Ok(false);
        }

        // Positive operation: allowed unless it exceeds the maximum
        if new_count > SEMVMX as i32 {
            return_errno!(ERANGE, "semaphore count exceeds maximum value");
        }

        if let Some(pid) = undo_pid {
            let adj = self.get_semadj(pid) - op;
            if adj < -(SEMAEM as i32) - 1 || adj > SEMAEM as i32 {
                return_errno!(ERANGE, "semaphore adjustment exceeds maximum value");
            }
            self.set_semadj(pid, adj);
        }
        self.count = new_count;
        Ok(true)
    }

    /// Reverts an operation that try_op has executed
    fn revert_op(&mut self, op: i32, undo_pid: Option<pid_t>) {
        self.count -= op;
        if let Some(pid) = undo_pid {
            self.set_semadj(pid, self.get_semadj(pid) + op);
        }
    }

    /// Returns number of threads waiting for count > current value
    fn get_ncnt(&self) -> usize {
        self.ncnt
    }

    /// Returns number of threads waiting for count = 0
    fn get_zcnt(&self) -> usize {
        self.zcnt
    }
}

struct SemSet {
    semid: SemId,
    nsems: usize,            // Number of semaphores in this set
    perm: Mutex<ipc_perm_t>, // Permission structure
    sem_otime: AtomicI64,    // Last operation time
    sem_ctime: AtomicI64,    // Creation/modification time

    sems: Mutex<Vec<Semaphore>>,          // The semaphores in this set
    attached_pids: Mutex<HashSet<pid_t>>, // Processes attached to this set
    waiter_queue: Mutex<WaiterQueue>,     // Queue for waiting processes

    is_removed: AtomicBool, // Set to true when semaphore set is removed
    marked_for_removal: AtomicBool, // Set when removal is requested but processes are still attached
}

impl SemSet {
    /// Creates a new semaphore set
    fn new(semid: SemId, key: key_t, nsems: usize, mode: u16) -> Result<Self> {
        info!(
            "New Semset Created: semid: {}, key: {}, nsems: {}, mode: {:o}",
            semid, key, nsems, mode
        );

        // Validate number of semaphores
        if nsems == 0 || nsems > SEMMSL {
            return_errno!(EINVAL, "invalid number of semaphores");
        }

        // Initialize semaphores with value 0
        let sems = (0..nsems).map(|_| Semaphore::new(0)).collect();

        Ok(SemSet {
            semid,
            nsems,
            perm: Mutex::new(ipc_perm_t {
                key,
                uid: 0,
                gid: 0,
                cuid: 0,
                cgid: 0,
                mode,
                pad1: 0,
                seq: 0,
                pad2: 0,
                unused1: 0,
                unused2: 0,
            }),
            sem_otime: AtomicI64::new(0),
            sem_ctime: AtomicI64::new(SemManager::current_time()),
            sems: Mutex::new(sems),
            attached_pids: Mutex::new(HashSet::new()),
            waiter_queue: Mutex::new(WaiterQueue::new()),
            is_removed: AtomicBool::new(false),
            marked_for_removal: AtomicBool::new(false),
        })
    }

    /// Marks semaphore set as removed and wakes all waiting processes
    fn mark_removed_and_wake(&self) {
        self.is_removed.store(true, Ordering::Relaxed);
        let mut waiter_queue = self.waiter_queue.lock();
        waiter_queue.dequeue_and_wake_all();
    }

    fn get_key(&self) -> key_t {
        let perm = self.perm.lock();
        perm.key
    }

    /// Updates permission structure and modification time
    fn set_perm(&self, perm: &ipc_perm_t) {
        let mut current_perm = self.perm.lock();
        *current_perm = *perm;
        self.sem_ctime
            .store(SemManager::current_time(), Ordering::Relaxed);
    }

    fn get_perm(&self) -> ipc_perm_t {
        let perm = self.perm.lock();
        *perm
    }

    /// Records that a process is using this semaphore set
    fn attach_pid(&self, pid: pid_t) {
        let mut pids = self.attached_pids.lock();
        pids.insert(pid);
    }

    /// Removes a process from the attached list
    fn detach_pid(&self, pid: &pid_t) {
        let mut pids = self.attached_pids.lock();
        pids.remove(pid);
    }

    /// Executes the operations one after the other, so that an operation sees the result of the
    /// operations before it. Either all of them are executed or none: if an operation can't
    /// complete, the operations before it are reverted.
    /// Returns the operation that would block, or None if all operations are executed
    fn perform_ops<'a>(
        sems: &mut [Semaphore],
        sops: &'a [sembuf_t],
        pid: pid_t,
    ) -> Result<Option<&'a sembuf_t>> {
        let undo_pid = |sop: &sembuf_t| {
            let flags = SemFlags::from_bits_truncate(sop.sem_flg as u32);
            if flags.contains(SemFlags::SEM_UNDO) {
                Some(pid)
            } else {
                None
            }
        };

        for (i, sop) in sops.iter().enumerate() {
            let performed = sems[sop.sem_num as usize].try_op(sop.sem_op as i32, undo_pid(sop));
            if let Ok(true) = performed {
                continue;
            }

            for done in sops[..i].iter().rev() {
                sems[done.sem_num as usize].revert_op(done.sem_op as i32, undo_pid(done));
            }
            return performed.map(|_| Some(sop));
        }

        for sop in sops {
            sems[sop.sem_num as usize].last_pid = pid;
        }
        Ok(None)
    }

    /// Executes a series of semaphore operations
    fn do_semop(&self, sops: &[sembuf_t], mut timeout: Option<Duration>) -> Result<()> {
        let pid = current!().process().pid();
        let waiter = Waiter::new();

        // Validate semaphore numbers before any operation is executed
        if sops.iter().any(|sop| sop.sem_num as usize >= self.nsems) {
            return_errno!(EFBIG, "semaphore number out of range");
        }

        loop {
            let mut sems = self.sems.lock();
            let mut waiter_queue = self.waiter_queue.lock();

            // Check if semaphore set was removed. This needs the lock of the queue, as the
            // removal wakes up the waiters of the queue after it marks the set as removed
            if self.is_removed.load(Ordering::Relaxed) {
                return_errno!(EIDRM, "semaphore set removed");
            }

            // Execute operations if all can proceed
            let waiting_sop = match Self::perform_ops(&mut sems, sops, pid)? {
                Some(sop) => sop,
                None => {
                    // Update operation time and wake waiting processes
                    self.sem_otime
                        .store(SemManager::current_time(), Ordering::Relaxed);
                    waiter_queue.dequeue_and_wake_all();
                    return Ok(());
                }
            };

            // Fail immediately if NOWAIT flag is set
            let flags = SemFlags::from_bits_truncate(waiting_sop.sem_flg as u32);
            if flags.contains(SemFlags::IPC_NOWAIT) {
                return_errno!(EAGAIN, "semaphore operation would block");
            }

            // Operations can't proceed - add to wait queue and block
            let sem_num = waiting_sop.sem_num as usize;
            let waits_for_zero = waiting_sop.sem_op == 0;
            if waits_for_zero {
                sems[sem_num].zcnt += 1;
            } else {
                sems[sem_num].ncnt += 1;
            }
            waiter_queue.reset_and_enqueue(&waiter);
            drop(sems);
            drop(waiter_queue);

            // Wait for notification or timeout
            let res = waiter.wait_mut(timeout.as_mut());

            // The thread doesn't wait any more
            let mut sems = self.sems.lock();
            if waits_for_zero {
                sems[sem_num].zcnt -= 1;
            } else {
                sems[sem_num].ncnt -= 1;
            }
            if res.is_err() {
                // The queue did not wake up the waiter, which would stay in the
                // queue until the next successful operation on this set
                self.waiter_queue.lock().dequeue(&waiter);
            }
            drop(sems);

            match res {
                Ok(()) => continue,
                Err(e) if e.errno() == Errno::ETIMEDOUT => {
                    return_errno!(EAGAIN, "semaphore operation timed out");
                }
                // Handle signal interrupt
                Err(e) if e.errno() == Errno::EINTR => {
                    return_errno!(EINTR, "semaphore operation interrupted by signal");
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Retrieves the current value of a specific semaphore
    fn getval(&self, sem_num: usize) -> Result<i32> {
        if self.is_removed.load(Ordering::Relaxed) {
            return_errno!(EIDRM, "semaphore set removed");
        }
        if sem_num >= self.nsems {
            return_errno!(ERANGE, "semaphore number out of range");
        }
        let sems = self.sems.lock();
        Ok(sems[sem_num].count)
    }

    /// Sets the value of a specific semaphore
    fn setval(&self, sem_num: usize, count: i32) -> Result<()> {
        if self.is_removed.load(Ordering::Relaxed) {
            return_errno!(EIDRM, "semaphore set removed");
        }
        let mut sems = self.sems.lock();

        // Validate parameters
        if sem_num >= self.nsems {
            return_errno!(ERANGE, "semaphore number out of range");
        }
        if count < 0 {
            return_errno!(ERANGE, "semaphore count cannot be negative");
        }
        if count > SEMVMX as i32 {
            return_errno!(ERANGE, "semaphore count exceeds maximum value");
        }

        // Update value and modification time, which clear the adjustments of the processes
        sems[sem_num].count = count;
        sems[sem_num].semadj.clear();
        self.sem_ctime
            .store(SemManager::current_time(), Ordering::Relaxed);

        // Wake up waiting processes, which can proceed with the new value
        self.waiter_queue.lock().dequeue_and_wake_all();
        Ok(())
    }

    /// Returns number of processes waiting for this semaphore's value to increase
    fn get_ncnt(&self, sem_num: usize) -> Result<usize> {
        if self.is_removed.load(Ordering::Relaxed) {
            return_errno!(EIDRM, "semaphore set removed");
        }
        if sem_num >= self.nsems {
            return_errno!(ERANGE, "semaphore number out of range");
        }
        let sems = self.sems.lock();
        Ok(sems[sem_num].get_ncnt())
    }

    /// Returns number of processes waiting for this semaphore's value to reach zero
    fn get_zcnt(&self, sem_num: usize) -> Result<usize> {
        if self.is_removed.load(Ordering::Relaxed) {
            return_errno!(EIDRM, "semaphore set removed");
        }
        if sem_num >= self.nsems {
            return_errno!(ERANGE, "semaphore number out of range");
        }
        let sems = self.sems.lock();
        Ok(sems[sem_num].get_zcnt())
    }

    /// Applies the undo adjustments of a process, which exits, and wakes up the waiting
    /// processes, for which the counts may have changed
    fn apply_semadj(&self, pid: pid_t) {
        let mut sems = self.sems.lock();
        let mut adjusted = false;
        for sem in sems.iter_mut() {
            if let Some(adj) = sem.semadj.remove(&pid) {
                // Like Linux, don't let the count leave the range of the count
                sem.count = (sem.count + adj).clamp(0, SEMVMX as i32);
                sem.last_pid = pid;
                adjusted = true;
            }
        }
        if adjusted {
            self.waiter_queue.lock().dequeue_and_wake_all();
        }
    }
}

#[derive(Debug)]
struct SemIdManager {
    used_id: HashSet<SemId>, // Track allocated semaphore IDs
    free_num: u32,           // Number of available IDs
    last_alloc_id: SemId,    // Last allocated ID (for efficient allocation)
}

impl SemIdManager {
    fn new() -> Self {
        SemIdManager {
            used_id: HashSet::new(),
            free_num: SEMMNI as u32,
            last_alloc_id: SEMMNI - 1,
        }
    }

    /// Allocates a new unique semaphore ID
    fn get_new_semid(&mut self) -> Result<SemId> {
        // Check if maximum sets reached
        if self.free_num == 0 {
            return_errno!(ENOSPC, "all possible semaphore IDs have been taken");
        }
        self.free_num -= 1;

        // Find next available ID (wrapping around if necessary)
        let mut id = self.last_alloc_id + 1;
        loop {
            if id == SEMMNI {
                id = 0;
            }
            if !self.used_id.contains(&id) {
                break;
            }
            id += 1;
        }

        self.used_id.insert(id);
        self.last_alloc_id = id;
        Ok(id)
    }

    /// Releases a semaphore ID back to the pool
    fn free_semid(&mut self, shmid: &SemId) -> Result<()> {
        self.free_num += 1;
        self.used_id.remove(shmid);
        Ok(())
    }
}

lazy_static! {
    pub static ref SYSTEM_V_SEM_MANAGER: SemManager = SemManager::new();
}

pub struct SemManager {
    sem_sets: RwLock<HashMap<SemId, Arc<SemSet>>>, // All active semaphore sets
    semid_manager: RwLock<SemIdManager>,           // Manages semaphore ID allocation
}

impl SemManager {
    fn new() -> Self {
        SemManager {
            sem_sets: RwLock::new(HashMap::new()),
            semid_manager: RwLock::new(SemIdManager::new()),
        }
    }

    /// Gets current time for timestamping operations
    fn current_time() -> time_t {
        do_gettimeofday().sec()
    }

    /// Allocates a new semaphore ID through the ID manager
    fn get_new_semid(&self) -> Result<SemId> {
        let mut semid_manager = self.semid_manager.write().unwrap();
        semid_manager.get_new_semid()
    }

    /// Retrieves a semaphore set by ID
    fn get_semset(&self, semid: &SemId) -> Result<Arc<SemSet>> {
        let sem_sets = self.sem_sets.read().unwrap();
        let semset = sem_sets
            .get(semid)
            .ok_or_else(|| errno!(EINVAL, "invalid semid"))?
            .clone();
        Ok(semset)
    }

    /// Releases a semaphore ID
    fn free_semid(&self, semid: &SemId) -> Result<()> {
        let mut semid_manager = self.semid_manager.write().unwrap();
        semid_manager.free_semid(semid)
    }

    /// Returns total number of semaphores across all sets
    fn get_total_semaphores(&self) -> usize {
        let sem_sets = self.sem_sets.read().unwrap();
        sem_sets.values().map(|set| set.nsems).sum()
    }

    /// Returns current number of semaphore sets
    fn get_semset_count(&self) -> usize {
        let sem_sets = self.sem_sets.read().unwrap();
        sem_sets.len()
    }

    /// Returns the largest currently allocated semaphore ID
    fn get_largest_semid(&self) -> SemId {
        let sem_sets = self.sem_sets.read().unwrap();
        sem_sets.keys().max().copied().unwrap_or(0)
    }

    /// Implements semget: creates or retrieves a semaphore set
    pub fn do_semget(&self, key: key_t, nsems: usize, semflg: SemFlags) -> Result<SemId> {
        let mut sem_sets = self.sem_sets.write().unwrap();

        let mode = semflg.bits() as u16 & 0o777;
        let semid = if key == IPC_PRIVATE {
            // Create private semaphore set (always new)
            let semid = self.get_new_semid()?;
            let sem_set = Arc::new(SemSet::new(semid, key, nsems, mode)?);
            sem_sets.insert(sem_set.semid, sem_set);
            semid
        } else {
            // Look for existing set with this key
            let sem_set = sem_sets.values().find(|&set| set.get_key() == key);

            match sem_set {
                Some(set) => {
                    // Handle existing set
                    if semflg.contains(SemFlags::IPC_CREAT) && semflg.contains(SemFlags::IPC_EXCL) {
                        return_errno!(EEXIST, "semaphore set already exists");
                    }
                    if nsems > 0 && nsems != set.nsems {
                        return_errno!(EINVAL, "nsems does not match existing set");
                    }
                    set.semid
                }
                None => {
                    // No existing set - create if requested
                    if !semflg.contains(SemFlags::IPC_CREAT) {
                        return_errno!(ENOENT, "no semaphore set exists for key");
                    }
                    if nsems == 0 || nsems > SEMMSL {
                        return_errno!(EINVAL, "invalid nsems");
                    }

                    // Create new semaphore set
                    let semid = self.get_new_semid()?;
                    let sem_set = Arc::new(SemSet::new(semid, key, nsems, mode)?);
                    sem_sets.insert(sem_set.semid, sem_set);
                    semid
                }
            }
        };
        Ok(semid)
    }

    /// Implements semop: performs semaphore operations
    pub fn do_semop(
        &self,
        semid: SemId,
        sops_ptr: *const sembuf_t,
        nsops: usize,
        mut timeout: Option<Duration>,
    ) -> Result<()> {
        // Validate number of operations
        if nsops == 0 || nsops > SEMOPM {
            return_errno!(E2BIG, "too many operations");
        }

        // Copy the operations from user space. Another thread of the process can change them
        // while this call waits, as the call looks at them again after each wake-up
        let sops = from_user::make_slice(sops_ptr, nsops)?.to_vec();
        let pid = current!().process().pid();

        // Get semaphore set and verify it exists
        let sem_set = self.get_semset(&semid)?;

        // Check for race condition (set removed after getting reference)
        if sem_set.is_removed.load(Ordering::Relaxed) {
            return_errno!(EIDRM, "semaphore set removed");
        }

        // Attach process to the set and perform operations
        sem_set.attach_pid(pid);
        let result = sem_set.do_semop(&sops, timeout);

        // Detach process after operations complete
        sem_set.detach_pid(&pid);
        result
    }

    /// Implements semctl: performs control operations on semaphores
    pub fn do_semctl(&self, semid: SemId, semnum: usize, cmd: CmdId, arg: usize) -> Result<usize> {
        info!(
            "do_semctl: semid: {:?}, semnum: {:?}, cmd: {:?}, arg: {:?}",
            semid, semnum, cmd, arg
        );

        // Handle IPC_RMID (remove semaphore set)
        if cmd == IPC_RMID {
            let sem_set = self.get_semset(&semid)?;
            // Mark as removed and wake waiting processes
            sem_set.mark_removed_and_wake();

            sem_set.marked_for_removal.store(true, Ordering::Relaxed);

            // Remove immediately if no processes are attached
            let is_empty = {
                let pids = sem_set.attached_pids.lock();
                pids.is_empty()
            };
            if is_empty {
                self.free_semid(&semid)?;
                let mut sem_sets = self.sem_sets.write().unwrap();
                sem_sets.remove(&semid);
            }
            return Ok(0);
        }

        // Handle commands that don't require a specific semaphore set
        match cmd {
            IPC_INFO => {
                // Fill system semaphore limits
                let info_ptr = arg as *mut seminfo_t;
                let info = unsafe {
                    info_ptr
                        .as_mut()
                        .ok_or_else(|| errno!(EFAULT, "invalid pointer"))?
                };

                *info = seminfo_t {
                    semmap: 0,
                    semmni: SEMMNI,
                    semmns: SEMMNS as u32,
                    semmnu: 0,
                    semmsl: SEMMSL as u32,
                    semopm: SEMOPM as u32,
                    semume: 0,
                    semusz: 0,
                    semvmx: SEMVMX as u32,
                    semaem: 0,
                };

                return Ok(SEMMNI as usize);
            }

            SEM_INFO => {
                // Fill extended semaphore information
                let info_ptr = arg as *mut seminfo_ext_t;
                let info = unsafe {
                    info_ptr
                        .as_mut()
                        .ok_or_else(|| errno!(EFAULT, "invalid pointer"))?
                };

                // Base limit information
                let base_info = seminfo_t {
                    semmap: 0,
                    semmni: SEMMNI,
                    semmns: SEMMNS as u32,
                    semmnu: 0,
                    semmsl: SEMMSL as u32,
                    semopm: SEMOPM as u32,
                    semume: 0,
                    semusz: 0,
                    semvmx: SEMVMX as u32,
                    semaem: 0,
                };

                // Current system status
                *info = seminfo_ext_t {
                    sem_info: base_info,
                    semusz: 0,
                    semaem: 0,
                    sem_nsems: self.get_total_semaphores() as u32,
                    sem_nsets: self.get_semset_count() as u32,
                    sem_largest_id: self.get_largest_semid(),
                };

                return Ok(self.get_largest_semid() as usize);
            }

            _ => {} // Other commands require a semaphore set
        }

        // Get semaphore set for remaining commands
        let sem_set = self.get_semset(&semid)?;

        // Check if set was removed
        if sem_set.is_removed.load(Ordering::Relaxed) {
            return_errno!(EIDRM, "semaphore set removed");
        }

        // Handle set-specific commands
        match cmd {
            IPC_SET => {
                // Update permission structure
                let perm = arg as *const ipc_perm_t;
                let perm = unsafe {
                    perm.as_ref()
                        .ok_or_else(|| errno!(EFAULT, "invalid perm"))?
                };
                sem_set.set_perm(perm);
                Ok(0)
            }
            IPC_STAT => {
                // Retrieve status information
                let buf_ptr = arg as *mut semids_t;
                let buf = unsafe {
                    buf_ptr
                        .as_mut()
                        .ok_or_else(|| errno!(EFAULT, "invalid buf"))?
                };
                *buf = semids_t {
                    sem_perm: sem_set.get_perm(),
                    sem_otime: sem_set.sem_otime.load(Ordering::Relaxed),
                    sem_otime_high: 0,
                    sem_ctime: sem_set.sem_ctime.load(Ordering::Relaxed),
                    sem_ctime_high: 0,
                    sem_nsems: sem_set.nsems as u64,
                    unused1: 0,
                    unused2: 0,
                };
                Ok(0)
            }
            SEM_GETPID => {
                // Get PID of last operation
                if semnum >= sem_set.nsems {
                    return_errno!(ERANGE, "semaphore number out of range");
                }
                let sems = sem_set.sems.lock();
                Ok(sems[semnum].last_pid as usize)
            }
            SEM_GETVAL => {
                // Get current semaphore value
                let value = sem_set.getval(semnum)?;
                Ok(value as usize)
            }
            SEM_GETALL => {
                // Get all semaphore values in set
                let vals_ptr = arg as *mut u16;
                if vals_ptr.is_null() {
                    return_errno!(EFAULT, "null pointer");
                }

                let vals = from_user::make_mut_slice(vals_ptr, sem_set.nsems)?;
                for i in 0..sem_set.nsems {
                    vals[i] = sem_set.getval(i)? as u16;
                }
                Ok(0)
            }
            SEM_GETNCNT => {
                // Get count of processes waiting for higher value
                let ncnt = sem_set.get_ncnt(semnum)?;
                Ok(ncnt)
            }
            SEM_GETZCNT => {
                // Get count of processes waiting for zero
                let zcnt = sem_set.get_zcnt(semnum)?;
                Ok(zcnt)
            }
            SEM_SETVAL => {
                // Set individual semaphore value
                let value = arg as i32;
                sem_set.setval(semnum, value)?;
                Ok(0)
            }
            SEM_SETALL => {
                // Set all semaphore values in set
                let vals_ptr = arg as *const u16;
                if vals_ptr.is_null() {
                    return_errno!(EFAULT, "null pointer");
                }

                let vals = from_user::make_slice(vals_ptr, sem_set.nsems)?;
                for i in 0..sem_set.nsems {
                    let value = vals[i] as i32;
                    sem_set.setval(i, value)?;
                }
                Ok(0)
            }
            SEM_STAT | SEM_STAT_ANY => {
                // Get status by semid
                let buf_ptr = arg as *mut semids_t;
                let buf = unsafe {
                    buf_ptr
                        .as_mut()
                        .ok_or_else(|| errno!(EFAULT, "invalid buf"))?
                };

                *buf = semids_t {
                    sem_perm: sem_set.get_perm(),
                    sem_otime: sem_set.sem_otime.load(Ordering::Relaxed),
                    sem_otime_high: 0,
                    sem_ctime: sem_set.sem_ctime.load(Ordering::Relaxed),
                    sem_ctime_high: 0,
                    sem_nsems: sem_set.nsems as u64,
                    unused1: 0,
                    unused2: 0,
                };
                Ok(sem_set.semid as usize)
            }
            _ => return_errno!(EINVAL, "unsupported command"),
        }
    }

    /// Cleans up semaphore resources when a process exits
    pub fn detach_sem_when_process_exit(&self, thread: &ThreadRef) {
        let pid = thread.process().pid();

        // Apply any pending undo operations, and detach process from all semaphore sets
        let mut sem_sets = self.sem_sets.write().unwrap();
        for (_, sem_set) in sem_sets.iter_mut() {
            sem_set.apply_semadj(pid);
            sem_set.detach_pid(&pid);
        }
    }

    /// Cleans up all semaphore resources on system exit
    pub fn clean_when_libos_exit(&self) {
        let mut sem_sets = self.sem_sets.write().unwrap();
        for (semid, _) in sem_sets.drain() {
            self.free_semid(&semid);
        }
    }
}
