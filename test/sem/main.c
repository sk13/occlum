#define _GNU_SOURCE
#include <sys/ipc.h>
#include <sys/sem.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <signal.h>
#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <spawn.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include "test.h"
#include "kernel_heap.h"

// ============================================================================
// Global Definitions (Semaphore Test Specific)
// ============================================================================

// Permission macro (same as shm, ensures user read/write permissions)
#define S_IRWUSER   (S_IRUSR | S_IWUSR)

// Test type identifiers
#define TEST_GET_SEMID_BY_KEY   0   // Get semid by key
#define TEST_PROCESS_SYNC       1   // Inter-process semaphore synchronization
#define TEST_IMMEDIATELY_RMSEM  2   // Verify immediate semaphore removal
#define TEST_OPERATE_DESTOYED   3   // Operate on destroyed semaphore
#define TEST_NO_RMSEM           4   // Do not remove semaphore (memory leak detection)
#define TEST_UNDO_MERGED        5   // Child: many operations with SEM_UNDO, then exit
#define TEST_UNDO_HOLD          6   // Child: one operation with SEM_UNDO, wait to be told to exit
#define TEST_UNDO_FAILED        7   // Child: operations with SEM_UNDO that fail, then exit

// Number of parameters for each test case (child process argv length)
#define TEST_GET_SEMID_BY_KEY_ARGC  5
#define TEST_PROCESS_SYNC_ARGC     4
#define TEST_OPERATE_DESTOYED_ARGC  5
#define TEST_UNDO_MERGED_ARGC       4
#define TEST_UNDO_HOLD_ARGC         6
#define TEST_UNDO_FAILED_ARGC       3
#define MAX_CHILD_ARGS              4

// General macro definitions
#define ARG_BUF_SZ  64              // Parameter buffer size
#define SEM_SET_SIZE 1              // Semaphore set size (using 1 semaphore)
#define SEM_INIT_VAL 1              // Default initial semaphore value
#define SUCCESS     1
#define FAIL        (-1)

const char prog_name[] = "/bin/sem";

// ============================================================================
// Helper Macros and Functions (Reusing shm test logic, adapted for semaphores)
// ============================================================================

// Log printing macro (includes file/line/function name for easier troubleshooting)
#define INFO(fmt, ...)   do { \
    printf("\t\t[SEM TEST] [file: %s, line: %d, func: %s] " fmt, \
    __FILE__, __LINE__, __func__, ##__VA_ARGS__); \
} while (0)

// Spawn child process to execute test and check exit status
// Returns SUCCESS if child test passes; FAIL if child test fails
static int execute_in_child(char **const child_argv) {
    int ret;
    pid_t child_pid;
    int child_status;

    // Create child process
    ret = posix_spawn(&child_pid, prog_name, NULL, NULL, (char **const)child_argv, NULL);
    if (ret < 0) {
        THROW_ERROR("posix_spawn() failed (errno: %d)", errno);
    }

    // Wait for child process to exit
    ret = waitpid(child_pid, &child_status, 0);
    if (ret < 0) {
        THROW_ERROR("waitpid() failed (errno: %d)", errno);
    }

    // Check child process exit status (normal exit with code 0)
    if (!WIFEXITED(child_status) || WEXITSTATUS(child_status) != 0) {
        INFO("Child process test failed (exit code: %d)\n", WEXITSTATUS(child_status));
        return FAIL;
    }

    return SUCCESS;
}

// Perform a system call and fail the test unless it ends as expected: with the return
// value 0 if the expected errno is 0, or else with -1 and that errno
#define EXPECT_CALL(call, expected_errno) do { \
    errno = 0; \
    long ret__ = (call); \
    if (!((expected_errno) == 0 ? ret__ == 0 : (ret__ == -1 && errno == (expected_errno)))) { \
        INFO("%s returned %ld with errno %d (%s), expected %s\n", #call, ret__, errno, \
             strerror(errno), (expected_errno) == 0 ? "0" : strerror(expected_errno)); \
        return FAIL; \
    } \
} while (0)

// Wrappers that make the system calls directly, as the tests do not depend on the libc
static int sem_create(int nsems) {
    return syscall(SYS_semget, IPC_PRIVATE, nsems, IPC_CREAT | IPC_EXCL | S_IRWUSER);
}

static long sem_op(int semid, int sem_num, int op, int flags) {
    struct sembuf sop = { sem_num, op, flags };
    return syscall(SYS_semop, semid, &sop, 1);
}

static long sem_getval(int semid, int sem_num) {
    return syscall(SYS_semctl, semid, sem_num, GETVAL);
}

// Check the value of a semaphore
static int check_val(int semid, int sem_num, int expected) {
    long val = sem_getval(semid, sem_num);
    if (val != expected) {
        INFO("the value of semaphore %d is %ld, expected %d\n", sem_num, val, expected);
        return FAIL;
    }
    return SUCCESS;
}

// The time in milliseconds of a clock that does not jump
static long now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

// A thread that makes a semaphore operation, which has to wait, and ends with its result. The
// operation is kept here, so that a test can change it while the thread waits
typedef struct {
    int semid;
    struct sembuf sop;
    int timeout_ms;
    long ret;
    int err;
    pthread_t thread;
} waiter_t;

static void *waiter_main(void *arg) {
    waiter_t *waiter = arg;
    struct timespec timeout = { waiter->timeout_ms / 1000, (waiter->timeout_ms % 1000) * 1000000L };
    waiter->ret = syscall(SYS_semtimedop, waiter->semid, &waiter->sop, 1, &timeout);
    waiter->err = errno;
    return NULL;
}

// The timeout keeps a waiter that is not woken up from blocking the test for long
#define WAITER_TIMEOUT_MS   5000

static int waiter_start(waiter_t *waiter, int semid, int sem_num, int op) {
    waiter->semid = semid;
    waiter->sop = (struct sembuf) { sem_num, op, 0 };
    waiter->timeout_ms = WAITER_TIMEOUT_MS;
    if (pthread_create(&waiter->thread, NULL, waiter_main, waiter) != 0) {
        THROW_ERROR("pthread_create() failed");
    }
    return SUCCESS;
}

// Wait for the waiter to end, and check that it was woken up and performed its operation
static int waiter_finish(waiter_t *waiter) {
    pthread_join(waiter->thread, NULL);
    if (waiter->ret != 0) {
        INFO("the operation of the waiter returned %ld with errno %d (%s), expected 0\n",
             waiter->ret, waiter->err, strerror(waiter->err));
        return FAIL;
    }
    return SUCCESS;
}

// Wait for the waiter to end, and check that it was woken up because the semaphore set was removed
static int waiter_finish_removed(waiter_t *waiter) {
    pthread_join(waiter->thread, NULL);
    if (waiter->ret != -1 || waiter->err != EIDRM) {
        INFO("the operation of the waiter returned %ld with errno %d (%s), expected EIDRM\n",
             waiter->ret, waiter->err, strerror(waiter->err));
        return FAIL;
    }
    return SUCCESS;
}

// Wait until the number of the waiters of a semaphore (GETNCNT or GETZCNT) is as expected
static int wait_for_count(int semid, int sem_num, int cmd, int expected) {
    long count = -1;
    for (int i = 0; i < WAITER_TIMEOUT_MS; i++) {
        count = syscall(SYS_semctl, semid, sem_num, cmd);
        if (count == expected) {
            return SUCCESS;
        }
        usleep(1000);
    }
    INFO("the number of waiters (%s) of semaphore %d is %ld, expected %d\n",
         cmd == GETNCNT ? "GETNCNT" : "GETZCNT", sem_num, count, expected);
    return FAIL;
}

// Spawn a child process that runs a child test, which takes the integers as arguments
static int spawn_child(pid_t *pid, int test_type, int arg_count, const int args[]) {
    char arg_bufs[2 + MAX_CHILD_ARGS][ARG_BUF_SZ];
    char *child_argv[2 + MAX_CHILD_ARGS + 1];
    int argc = 0;

    snprintf(arg_bufs[argc], ARG_BUF_SZ, "%s", prog_name);
    child_argv[argc] = arg_bufs[argc];
    argc++;
    snprintf(arg_bufs[argc], ARG_BUF_SZ, "%d", test_type);
    child_argv[argc] = arg_bufs[argc];
    argc++;
    for (int i = 0; i < arg_count; i++) {
        snprintf(arg_bufs[argc], ARG_BUF_SZ, "%d", args[i]);
        child_argv[argc] = arg_bufs[argc];
        argc++;
    }
    child_argv[argc] = NULL;

    int err = posix_spawn(pid, prog_name, NULL, NULL, child_argv, NULL);
    if (err != 0) {
        THROW_ERROR("posix_spawn() failed (error: %d)", err);
    }
    return SUCCESS;
}

// Wait for a child process to exit, and check that its test passed
static int wait_for_child(pid_t pid) {
    int status;
    if (waitpid(pid, &status, 0) < 0) {
        THROW_ERROR("waitpid() failed (errno: %d)", errno);
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        INFO("Child process test failed (status: %d)\n", status);
        return FAIL;
    }
    return SUCCESS;
}

// Wait until a semaphore has the expected value, which a child process changes
static int wait_for_val(int semid, int sem_num, int expected) {
    for (int i = 0; i < 5000; i++) {
        if (sem_getval(semid, sem_num) == expected) {
            return SUCCESS;
        }
        usleep(1000);
    }
    INFO("the value of semaphore %d is still %ld, expected %d\n", sem_num,
         sem_getval(semid, sem_num), expected);
    return FAIL;
}

// ============================================================================
// Core Semaphore Test Cases
// ============================================================================

/**
 * Test 1: Create/get semid by key (basic functionality verification)
 * Scenarios:
 * 1. Get non-existent semaphore → should return ENOENT
 * 2. Create new semaphore (IPC_CREAT|IPC_EXCL) → should successfully get semid
 * 3. Get existing semaphore by key in same process → semid should match
 * 4. Attempt to recreate with IPC_EXCL → should return EEXIST
 * 5. Child process gets semaphore by key → semid should match parent's
 */
static int test_semget_semid_from_key(void) {
    int ret, semid;
    key_t key;
    char *child_argv[TEST_GET_SEMID_BY_KEY_ARGC + 1]; // +1 for NULL terminator

    // Generate random key (avoid conflict with existing semaphores)
    srand(time(NULL));
    key = random();
    INFO("Test key: %d\n", key);

    // Scenario 1: Get non-existent semaphore → expect ENOENT
    ret = syscall(SYS_semget, key, SEM_SET_SIZE, S_IRWUSER);
    if (ret != -1 || errno != ENOENT) {
        INFO("semget() failed: expected ENOENT, actual ret=%d, errno=%d\n", ret, errno);
        return FAIL;
    }

    // Scenario 2: Create new semaphore (IPC_CREAT|IPC_EXCL) → should succeed
    semid = syscall(SYS_semget, key, SEM_SET_SIZE, IPC_CREAT | IPC_EXCL | S_IRWUSER);
    if (semid < 0) {
        THROW_ERROR("semget() create failed (errno: %d)", errno);
    }
    INFO("Created semid: %d\n", semid);

    // Scenario 3: Set initial semaphore value (undefined after semget, needs explicit setting)
    ret = syscall(SYS_semctl, semid, 0, SETVAL, SEM_INIT_VAL);
    if (ret != 0) {
        THROW_ERROR("semctl(SETVAL) failed (errno: %d)", errno);
    }


    // Scenario 4: Get existing semaphore in same process → verify semid and value match
    ret = syscall(SYS_semget, key, SEM_SET_SIZE, S_IRWUSER);
    if (ret < 0) {
        THROW_ERROR("semget() get existing failed (errno: %d)", errno);
    }
    if (ret != semid) {
        INFO("semid mismatch: expected %d, actual %d\n", semid, ret);
        return FAIL;
    }
    ret = syscall(SYS_semctl, semid, 0, GETVAL);
    if (ret != SEM_INIT_VAL) {
        INFO("sem val mismatch: expected %d, actual %d\n", SEM_INIT_VAL, ret);
        return FAIL;
    }

    // Scenario 5: Attempt to recreate with IPC_EXCL → expect EEXIST
    ret = syscall(SYS_semget, key, SEM_SET_SIZE, IPC_CREAT | IPC_EXCL | S_IRWUSER);
    if (ret != -1 || errno != EEXIST) {
        INFO("semget() failed: expected EEXIST, actual ret=%d, errno=%d\n", ret, errno);
        return FAIL;
    }

    // Scenario 6: Child process gets semid by key → verify consistency
    // Initialize child process arguments (argv[0]=program name, argv[1]=test type, argv[2]=key, argv[3]=semid, argv[4]=initial value)
    for (int i = 0; i < TEST_GET_SEMID_BY_KEY_ARGC; i++) {
        child_argv[i] = (char *)malloc(ARG_BUF_SZ);
        if (child_argv[i] == NULL) {
            THROW_ERROR("malloc() failed for child argv");
        }
    }
    snprintf(child_argv[0], ARG_BUF_SZ, "%s", prog_name);
    snprintf(child_argv[1], ARG_BUF_SZ, "%d", TEST_GET_SEMID_BY_KEY);
    snprintf(child_argv[2], ARG_BUF_SZ, "%d", key);
    snprintf(child_argv[3], ARG_BUF_SZ, "%d", semid);
    snprintf(child_argv[4], ARG_BUF_SZ, "%d", SEM_INIT_VAL);
    child_argv[TEST_GET_SEMID_BY_KEY_ARGC] = NULL; // Terminator

    // Execute child process test
    if ((ret = execute_in_child(child_argv)) != SUCCESS) {
        INFO("Child test (GET_SEMID_BY_KEY) failed\n");
        return FAIL;
    }

    // Cleanup resources
    for (int i = 0; i < TEST_GET_SEMID_BY_KEY_ARGC; i++) {
        free(child_argv[i]);
    }

    // Delete semaphore
    ret = syscall(SYS_semctl, semid, 0, IPC_RMID);
    if (ret != 0) {
        THROW_ERROR("semctl(IPC_RMID) failed (errno: %d)", errno);
    }

    INFO("Test semget_semid_from_key passed\n");
    return SUCCESS;
}

/**
 * Test 2: Inter-process semaphore synchronization (core functionality verification)
 * Logic:
 * 1. Parent process creates semaphore (initial value 1), performs P operation (value→0)
 * 2. Child process performs V operation (value→1)
 * 3. Parent process verifies semaphore value returns to 1 → synchronization works
 */
static int test_process_sync(void) {
    int ret, semid;
    int sem_val;
    char *child_argv[TEST_PROCESS_SYNC_ARGC + 1];
    // Semaphore operation structure (P operation: sem_op=-1, V operation: sem_op=1)
    struct sembuf p_op = {0, -1, 0}; // sem_num=0 (first semaphore), sem_flg=0 (blocking)

    // Step 1: Create anonymous semaphore (IPC_PRIVATE, visible only to parent and child)
    semid = syscall(SYS_semget, IPC_PRIVATE, SEM_SET_SIZE, IPC_CREAT | IPC_EXCL | S_IRWUSER);
    if (semid < 0) {
        THROW_ERROR("semget() create private failed (errno: %d)", errno);
    }

    // Step 2: Set initial value to 1
    ret = syscall(SYS_semctl, semid, 0, SETVAL, SEM_INIT_VAL);
    if (ret != 0) {
        THROW_ERROR("semctl(SETVAL) failed (errno: %d)", errno);
    }

    // Step 3: Parent performs P operation (value from 1→0)
    ret = syscall(SYS_semop, semid, &p_op, 1); // 1=number of operations
    if (ret != 0) {
        THROW_ERROR("semop(P) failed (errno: %d)", errno);
    }
    // Verify value is 0 after P operation
    sem_val = syscall(SYS_semctl, semid, 0, GETVAL);
    if (sem_val != 0) {
        INFO("P operation failed: expected val=0, actual=%d\n", sem_val);
        return FAIL;
    }

    // Step 4: Create child process to perform V operation
    // Initialize child process arguments (argv[0]=program name, argv[1]=test type, argv[2]=semid, argv[3]=initial value)
    for (int i = 0; i < TEST_PROCESS_SYNC_ARGC; i++) {
        child_argv[i] = (char *)malloc(ARG_BUF_SZ);
        if (child_argv[i] == NULL) {
            THROW_ERROR("malloc() failed for child argv");
        }
    }
    snprintf(child_argv[0], ARG_BUF_SZ, "%s", prog_name);
    snprintf(child_argv[1], ARG_BUF_SZ, "%d", TEST_PROCESS_SYNC);
    snprintf(child_argv[2], ARG_BUF_SZ, "%d", semid);
    snprintf(child_argv[3], ARG_BUF_SZ, "%d", SEM_INIT_VAL);
    child_argv[TEST_PROCESS_SYNC_ARGC] = NULL;

    // Execute child process V operation
    if ((ret = execute_in_child(child_argv)) != SUCCESS) {
        INFO("Child test (PROCESS_SYNC) failed\n");
        return FAIL;
    }

    // Step 5: Verify semaphore value returns to 1 (child's V operation worked)
    sem_val = syscall(SYS_semctl, semid, 0, GETVAL);
    if (sem_val != 1) {
        INFO("Sync failed: expected val=1, actual=%d\n", sem_val);
        return FAIL;
    }

    // Cleanup resources
    for (int i = 0; i < TEST_PROCESS_SYNC_ARGC; i++) {
        free(child_argv[i]);
    }

    // Delete semaphore
    ret = syscall(SYS_semctl, semid, 0, IPC_RMID);
    if (ret != 0) {
        THROW_ERROR("semctl(IPC_RMID) failed (errno: %d)", errno);
    }

    INFO("Test process_sync passed\n");
    return SUCCESS;
}

/**
 * Test 3: Verify immediate semaphore removal (abnormal scenario)
 * Logic: Delete semaphore immediately after creation, subsequent stat operations return EINVAL
 */
static int test_immediately_rmsem(void) {
    int ret, semid;
    struct semid_ds sem_buf; // Semaphore status buffer

    // Step 1: Create semaphore
    semid = syscall(SYS_semget, IPC_PRIVATE, SEM_SET_SIZE, IPC_CREAT | IPC_EXCL | S_IRWUSER);
    if (semid < 0) {
        THROW_ERROR("semget() create failed (errno: %d)", errno);
    }

    // Step 2: Delete semaphore immediately
    ret = syscall(SYS_semctl, semid, 0, IPC_RMID);
    if (ret != 0) {
        THROW_ERROR("semctl(IPC_RMID) failed (errno: %d)", errno);
    }

    // Step 3: Verify stat operation returns EINVAL after deletion (empty buffer)
    ret = syscall(SYS_semctl, semid, 0, IPC_STAT, NULL);
    if (ret != -1 || errno != EINVAL) {
        INFO("semctl(IPC_STAT) failed: expected EINVAL, actual ret=%d, errno=%d\n", ret, errno);
        return FAIL;
    }

    // Step 4: Verify stat operation returns EINVAL after deletion (non-empty buffer)
    ret = syscall(SYS_semctl, semid, 0, IPC_STAT, &sem_buf);
    if (ret != -1 || errno != EINVAL) {
        INFO("semctl(IPC_STAT) failed: expected EINVAL, actual ret=%d, errno=%d\n", ret, errno);
        return FAIL;
    }

    INFO("Test immediately_rmsem passed\n");
    return SUCCESS;
}

/**
 * Test 4: Operate on destroyed semaphore (abnormal scenario)
 * Logic:
 * 1. Parent creates semaphore then immediately deletes it
 * 2. Child attempts to get by key → returns ENOENT
 * 3. Child attempts to operate on deleted semid → returns EINVAL
 */
static int test_operate_destroyed_sem(void) {
    int ret, semid;
    key_t key;
    char *child_argv[TEST_OPERATE_DESTOYED_ARGC + 1];

    // Generate random key
    srand(time(NULL));
    key = random();
    INFO("Test destroyed key: %d\n", key);

    // Step 1: Create semaphore and set initial value
    semid = syscall(SYS_semget, key, SEM_SET_SIZE, IPC_CREAT | IPC_EXCL | S_IRWUSER);
    if (semid < 0) {
        THROW_ERROR("semget() create failed (errno: %d)", errno);
    }
    ret = syscall(SYS_semctl, semid, 0, SETVAL, SEM_INIT_VAL);
    if (ret != 0) {
        THROW_ERROR("semctl(SETVAL) failed (errno: %d)", errno);
    }

    // Step 2: Delete semaphore immediately
    ret = syscall(SYS_semctl, semid, 0, IPC_RMID);
    if (ret != 0) {
        THROW_ERROR("semctl(IPC_RMID) failed (errno: %d)", errno);
    }

    // Step 3: Child tests operations on destroyed semaphore
    // Initialize child process arguments (argv[0]=program name, argv[1]=test type, argv[2]=key, argv[3]=semid, argv[4]=initial value)
    for (int i = 0; i < TEST_OPERATE_DESTOYED_ARGC; i++) {
        child_argv[i] = (char *)malloc(ARG_BUF_SZ);
        if (child_argv[i] == NULL) {
            THROW_ERROR("malloc() failed for child argv");
        }
    }
    snprintf(child_argv[0], ARG_BUF_SZ, "%s", prog_name);
    snprintf(child_argv[1], ARG_BUF_SZ, "%d", TEST_OPERATE_DESTOYED);
    snprintf(child_argv[2], ARG_BUF_SZ, "%d", key);
    snprintf(child_argv[3], ARG_BUF_SZ, "%d", semid);
    snprintf(child_argv[4], ARG_BUF_SZ, "%d", SEM_INIT_VAL);
    child_argv[TEST_OPERATE_DESTOYED_ARGC] = NULL;

    // Execute child process test
    if ((ret = execute_in_child(child_argv)) != SUCCESS) {
        INFO("Child test (OPERATE_DESTROYED) failed\n");
        return FAIL;
    }

    // Cleanup resources
    for (int i = 0; i < TEST_OPERATE_DESTOYED_ARGC; i++) {
        free(child_argv[i]);
    }

    INFO("Test operate_destroyed_sem passed\n");
    return SUCCESS;
}

/**
 * Test 5: Do not delete semaphore (memory leak detection)
 * Note: As the last test case, verifies if LibOS reclaims unexplicitly deleted semaphore resources on exit
 * Logic: Only create semaphore, do not perform IPC_RMID
 */
static int test_no_rmsem(void) {
    int semid;

    // Create semaphore and set initial value (do not delete)
    semid = syscall(SYS_semget, IPC_PRIVATE, SEM_SET_SIZE, IPC_CREAT | IPC_EXCL | S_IRWUSER);
    if (semid < 0) {
        THROW_ERROR("semget() create failed (errno: %d)", errno);
    }
    int ret = syscall(SYS_semctl, semid, 0, SETVAL, SEM_INIT_VAL);
    if (ret != 0) {
        THROW_ERROR("semctl(SETVAL) failed (errno: %d)", errno);
    }

    INFO("Test no_rmsem passed (wait for resource recycle)\n");
    return SUCCESS;
}

/**
 * Test 6: Return values of semop() and semtimedop()
 * Both return 0 if the operations have been performed
 */
static int test_semop_return_value(void) {
    struct sembuf up = {0, 1, 0};
    struct sembuf down = {0, -1, 0};
    struct sembuf both[2] = {{0, 2, 0}, {1, 1, 0}};
    struct timespec timeout = {1, 0};
    int semid = sem_create(2);
    if (semid < 0) {
        THROW_ERROR("semget() create failed (errno: %d)", errno);
    }

    EXPECT_CALL(syscall(SYS_semop, semid, &up, 1), 0);
    EXPECT_CALL(syscall(SYS_semop, semid, both, 2), 0);
    EXPECT_CALL(syscall(SYS_semtimedop, semid, &down, 1, NULL), 0);
    EXPECT_CALL(syscall(SYS_semtimedop, semid, &up, 1, &timeout), 0);
    EXPECT_CALL(syscall(SYS_semtimedop, semid, both, 2, &timeout), 0);
    // Semaphore 0: 1 + 2 - 1 + 1 + 2 = 5, semaphore 1: 1 + 1 = 2
    if (check_val(semid, 0, 5) != SUCCESS || check_val(semid, 1, 2) != SUCCESS) {
        return FAIL;
    }

    EXPECT_CALL(syscall(SYS_semctl, semid, 0, IPC_RMID), 0);
    INFO("Test semop_return_value passed\n");
    return SUCCESS;
}

/**
 * Test 7: Errors of semop() and semtimedop() that are reported before any operation is performed
 * 1. No operations (nsops is 0) → EINVAL
 * 2. Too many operations → E2BIG
 * 3. Semaphore number out of range → EFBIG
 * 4. Semaphore set does not exist → EINVAL
 */
static int test_semop_errors(void) {
    static struct sembuf many[1000];
    struct sembuf up = {0, 1, 0};
    struct sembuf bad_num = {1, 1, 0};
    struct timespec timeout = {1, 0};
    int semid = sem_create(1);
    if (semid < 0) {
        THROW_ERROR("semget() create failed (errno: %d)", errno);
    }

    EXPECT_CALL(syscall(SYS_semop, semid, &up, 0), EINVAL);
    EXPECT_CALL(syscall(SYS_semtimedop, semid, &up, 0, NULL), EINVAL);
    EXPECT_CALL(syscall(SYS_semtimedop, semid, &up, 0, &timeout), EINVAL);

    EXPECT_CALL(syscall(SYS_semop, semid, many, 1000), E2BIG);
    EXPECT_CALL(syscall(SYS_semtimedop, semid, many, 1000, &timeout), E2BIG);

    EXPECT_CALL(syscall(SYS_semop, semid, &bad_num, 1), EFBIG);
    EXPECT_CALL(syscall(SYS_semtimedop, semid, &bad_num, 1, &timeout), EFBIG);

    EXPECT_CALL(syscall(SYS_semop, -1, &up, 1), EINVAL);
    EXPECT_CALL(syscall(SYS_semop, 100000, &up, 1), EINVAL);

    // None of the calls has changed the semaphore
    if (check_val(semid, 0, 0) != SUCCESS) {
        return FAIL;
    }

    EXPECT_CALL(syscall(SYS_semctl, semid, 0, IPC_RMID), 0);
    INFO("Test semop_errors passed\n");
    return SUCCESS;
}

/**
 * Test 8: Timeout of semtimedop()
 * 1. The operation cannot be performed in time → EAGAIN after the timeout
 * 2. Zero timeout → EAGAIN at once
 * 3. IPC_NOWAIT → EAGAIN at once, whatever the timeout is
 * 4. Invalid timeout → EINVAL
 * 5. The operation that can be performed does not wait
 */
static int test_semtimedop_timeout(void) {
    struct sembuf down = {0, -1, 0};
    struct sembuf down_nowait = {0, -1, IPC_NOWAIT};
    struct sembuf wait_zero = {0, 0, 0};
    struct timespec timeout = {0, 200 * 1000 * 1000};
    struct timespec long_timeout = {2, 0};
    struct timespec zero = {0, 0};
    struct timespec bad_nsec = {0, 1000 * 1000 * 1000};
    struct timespec bad_sec = {-1, 0};
    long start, elapsed;
    int semid = sem_create(1);
    if (semid < 0) {
        THROW_ERROR("semget() create failed (errno: %d)", errno);
    }

    // Scenario 1: The semaphore is 0, so the P operation has to wait for the timeout
    start = now_ms();
    EXPECT_CALL(syscall(SYS_semtimedop, semid, &down, 1, &timeout), EAGAIN);
    elapsed = now_ms() - start;
    if (elapsed < 180 || elapsed > 5000) {
        INFO("semtimedop() took %ld ms, expected about 200 ms\n", elapsed);
        return FAIL;
    }

    // Scenario 2: Zero timeout
    EXPECT_CALL(syscall(SYS_semtimedop, semid, &down, 1, &zero), EAGAIN);

    // Scenario 3: IPC_NOWAIT does not wait for the timeout
    start = now_ms();
    EXPECT_CALL(syscall(SYS_semtimedop, semid, &down_nowait, 1, &long_timeout), EAGAIN);
    EXPECT_CALL(syscall(SYS_semop, semid, &down_nowait, 1), EAGAIN);
    elapsed = now_ms() - start;
    if (elapsed > 1000) {
        INFO("semtimedop() with IPC_NOWAIT took %ld ms\n", elapsed);
        return FAIL;
    }

    // Scenario 4: Invalid timeouts
    EXPECT_CALL(syscall(SYS_semtimedop, semid, &down, 1, &bad_nsec), EINVAL);
    EXPECT_CALL(syscall(SYS_semtimedop, semid, &down, 1, &bad_sec), EINVAL);

    // Scenario 5: The Z operation waits for the semaphore to become 0 ...
    EXPECT_CALL(sem_op(semid, 0, 1, 0), 0);
    start = now_ms();
    EXPECT_CALL(syscall(SYS_semtimedop, semid, &wait_zero, 1, &timeout), EAGAIN);
    elapsed = now_ms() - start;
    if (elapsed < 180 || elapsed > 5000) {
        INFO("semtimedop() of the Z operation took %ld ms, expected about 200 ms\n", elapsed);
        return FAIL;
    }
    // ... and the P operation, which can be performed, does not wait for the timeout
    start = now_ms();
    EXPECT_CALL(syscall(SYS_semtimedop, semid, &down, 1, &long_timeout), 0);
    elapsed = now_ms() - start;
    if (elapsed > 1000) {
        INFO("semtimedop() that can be performed took %ld ms\n", elapsed);
        return FAIL;
    }
    if (check_val(semid, 0, 0) != SUCCESS) {
        return FAIL;
    }

    EXPECT_CALL(syscall(SYS_semctl, semid, 0, IPC_RMID), 0);
    INFO("Test semtimedop_timeout passed\n");
    return SUCCESS;
}

/**
 * Test 9: The SEM_UNDO operations do not leak memory
 * The P and V operations with SEM_UNDO undo each other, so there is nothing left to undo
 * when the process exits. The kernel heap must not grow with their number: it used to
 * grow by 32 bytes per pair, about 1 MB in the test.
 */
#define UNDO_PAIRS      20000
#define HEAP_LIMIT_KB   64

static int test_sem_undo_no_growth(void) {
    struct sembuf up = {0, 1, SEM_UNDO};
    struct sembuf down = {0, -1, SEM_UNDO};
    int semid = sem_create(2);
    if (semid < 0) {
        THROW_ERROR("semget() create failed (errno: %d)", errno);
    }

    for (int i = 0; i < 100; i++) {
        EXPECT_CALL(syscall(SYS_semop, semid, &up, 1), 0);
        EXPECT_CALL(syscall(SYS_semop, semid, &down, 1), 0);
    }
    long before = kernel_heap_in_use();
    if (before == -2) {
        INFO("SKIPPED: the kernel heap monitor is not enabled\n");
        goto out;
    }
    if (before < 0) {
        THROW_ERROR("failed to get the kernel heap in use");
    }
    for (int i = 0; i < UNDO_PAIRS / 2; i++) {
        EXPECT_CALL(syscall(SYS_semop, semid, &up, 1), 0);
        EXPECT_CALL(syscall(SYS_semop, semid, &down, 1), 0);
    }
    // The same with two semaphores, so that the operations that undo each other are not next to
    // each other
    for (int i = 0; i < UNDO_PAIRS / 4; i++) {
        EXPECT_CALL(sem_op(semid, 0, 1, SEM_UNDO), 0);
        EXPECT_CALL(sem_op(semid, 1, 1, SEM_UNDO), 0);
        EXPECT_CALL(sem_op(semid, 0, -1, SEM_UNDO), 0);
        EXPECT_CALL(sem_op(semid, 1, -1, SEM_UNDO), 0);
    }
    long after = kernel_heap_in_use();
    if (after - before > HEAP_LIMIT_KB) {
        INFO("the kernel heap grew by %ld kB in %d pairs of operations (limit %d kB)\n",
             after - before, UNDO_PAIRS, HEAP_LIMIT_KB);
        return FAIL;
    }

out:
    EXPECT_CALL(syscall(SYS_semctl, semid, 0, IPC_RMID), 0);
    INFO("Test sem_undo_no_growth passed\n");
    return SUCCESS;
}

/**
 * Test 10: A process that exits undoes its SEM_UNDO operations
 * Many operations on a semaphore, in many calls, in one call and in two threads, are undone
 * as the sum of them
 */
static int test_sem_undo_at_exit(void) {
    pid_t pid;
    int set_a = sem_create(2);
    int set_b = sem_create(2);
    if (set_a < 0 || set_b < 0) {
        THROW_ERROR("semget() create failed (errno: %d)", errno);
    }
    EXPECT_CALL(syscall(SYS_semctl, set_a, 0, SETVAL, 10), 0);
    EXPECT_CALL(syscall(SYS_semctl, set_a, 1, SETVAL, 20), 0);
    EXPECT_CALL(syscall(SYS_semctl, set_b, 0, SETVAL, 7), 0);
    EXPECT_CALL(syscall(SYS_semctl, set_b, 1, SETVAL, 3), 0);

    int args[] = {set_a, set_b};
    if (spawn_child(&pid, TEST_UNDO_MERGED, 2, args) != SUCCESS ||
            wait_for_child(pid) != SUCCESS) {
        return FAIL;
    }
    if (check_val(set_a, 0, 10) != SUCCESS || check_val(set_a, 1, 20) != SUCCESS ||
            check_val(set_b, 0, 7) != SUCCESS || check_val(set_b, 1, 3) != SUCCESS) {
        return FAIL;
    }

    EXPECT_CALL(syscall(SYS_semctl, set_a, 0, IPC_RMID), 0);
    EXPECT_CALL(syscall(SYS_semctl, set_b, 0, IPC_RMID), 0);
    INFO("Test sem_undo_at_exit passed\n");
    return SUCCESS;
}

/**
 * Test 11: Every process undoes its own SEM_UNDO operations
 * Two processes operate on one semaphore, and exit one after the other
 */
static int test_sem_undo_per_process(void) {
    pid_t pid_x, pid_y;
    int semid = sem_create(1);
    int go_x = sem_create(1);
    int go_y = sem_create(1);
    if (semid < 0 || go_x < 0 || go_y < 0) {
        THROW_ERROR("semget() create failed (errno: %d)", errno);
    }
    EXPECT_CALL(syscall(SYS_semctl, semid, 0, SETVAL, 10), 0);

    int args_x[] = {semid, 0, 3, go_x};
    int args_y[] = {semid, 0, 5, go_y};
    if (spawn_child(&pid_x, TEST_UNDO_HOLD, 4, args_x) != SUCCESS ||
            spawn_child(&pid_y, TEST_UNDO_HOLD, 4, args_y) != SUCCESS ||
            wait_for_val(semid, 0, 18) != SUCCESS) {
        return FAIL;
    }

    // The first process exits and subtracts its 3, the second one subtracts its 5 later
    EXPECT_CALL(sem_op(go_x, 0, 1, 0), 0);
    if (wait_for_child(pid_x) != SUCCESS || check_val(semid, 0, 15) != SUCCESS) {
        return FAIL;
    }
    EXPECT_CALL(sem_op(go_y, 0, 1, 0), 0);
    if (wait_for_child(pid_y) != SUCCESS || check_val(semid, 0, 10) != SUCCESS) {
        return FAIL;
    }

    EXPECT_CALL(syscall(SYS_semctl, semid, 0, IPC_RMID), 0);
    EXPECT_CALL(syscall(SYS_semctl, go_x, 0, IPC_RMID), 0);
    EXPECT_CALL(syscall(SYS_semctl, go_y, 0, IPC_RMID), 0);
    INFO("Test sem_undo_per_process passed\n");
    return SUCCESS;
}

/**
 * Test 12: The SEM_UNDO operations are limited
 * What a process has to undo for a semaphore is in the range of -32768 to 32767 (ERANGE),
 * and the operations of one call count together
 */
#define SEMVMX_VAL      32767

static int test_sem_undo_range(void) {
    struct sembuf two_downs[2] = {{2, -1, SEM_UNDO}, {2, -1, SEM_UNDO}};
    struct sembuf out_and_back[2] = {{0, -1, SEM_UNDO}, {0, 1, SEM_UNDO}};
    int semid = sem_create(3);
    if (semid < 0) {
        THROW_ERROR("semget() create failed (errno: %d)", errno);
    }

    // Semaphore 0: the process has to add 32767 to the semaphore, which is the limit
    EXPECT_CALL(syscall(SYS_semctl, semid, 0, SETVAL, SEMVMX_VAL), 0);
    EXPECT_CALL(sem_op(semid, 0, -SEMVMX_VAL, SEM_UNDO), 0);
    EXPECT_CALL(sem_op(semid, 0, SEMVMX_VAL, 0), 0);
    EXPECT_CALL(sem_op(semid, 0, -1, SEM_UNDO), ERANGE);
    // The range is checked after each operation: the first operation of this call leaves it,
    // although the second one would bring the adjustment back
    EXPECT_CALL(syscall(SYS_semop, semid, out_and_back, 2), ERANGE);
    if (check_val(semid, 0, SEMVMX_VAL) != SUCCESS) {
        return FAIL;
    }

    // Semaphore 1: the process has to subtract 32768 from the semaphore, which is the limit
    EXPECT_CALL(sem_op(semid, 1, SEMVMX_VAL, SEM_UNDO), 0);
    EXPECT_CALL(sem_op(semid, 1, -SEMVMX_VAL, 0), 0);
    EXPECT_CALL(sem_op(semid, 1, 1, SEM_UNDO), 0);
    EXPECT_CALL(sem_op(semid, 1, 1, SEM_UNDO), ERANGE);
    if (check_val(semid, 1, 1) != SUCCESS) {
        return FAIL;
    }

    // Semaphore 2: two operations of one call together would leave the range, so the call
    // fails and changes nothing, and one of the operations alone works
    EXPECT_CALL(syscall(SYS_semctl, semid, 2, SETVAL, SEMVMX_VAL), 0);
    EXPECT_CALL(sem_op(semid, 2, -(SEMVMX_VAL - 1), SEM_UNDO), 0);
    EXPECT_CALL(sem_op(semid, 2, SEMVMX_VAL - 1, 0), 0);
    EXPECT_CALL(syscall(SYS_semop, semid, two_downs, 2), ERANGE);
    if (check_val(semid, 2, SEMVMX_VAL) != SUCCESS) {
        return FAIL;
    }
    EXPECT_CALL(sem_op(semid, 2, -1, SEM_UNDO), 0);
    EXPECT_CALL(sem_op(semid, 2, -1, SEM_UNDO), ERANGE);
    if (check_val(semid, 2, SEMVMX_VAL - 1) != SUCCESS) {
        return FAIL;
    }

    EXPECT_CALL(syscall(SYS_semctl, semid, 0, IPC_RMID), 0);

    // A call that fails undoes the adjustments of its operations: the first operation of this
    // call fails with ERANGE because of the second, and the same operation then works alone
    semid = sem_create(2);
    if (semid < 0) {
        THROW_ERROR("semget() create failed (errno: %d)", errno);
    }
    EXPECT_CALL(syscall(SYS_semctl, semid, 0, SETVAL, SEMVMX_VAL), 0);
    EXPECT_CALL(syscall(SYS_semctl, semid, 1, SETVAL, SEMVMX_VAL), 0);
    struct sembuf undo_then_max[2] = {{0, -SEMVMX_VAL, SEM_UNDO}, {1, 1, 0}};
    EXPECT_CALL(syscall(SYS_semop, semid, undo_then_max, 2), ERANGE);
    EXPECT_CALL(sem_op(semid, 0, -SEMVMX_VAL, SEM_UNDO), 0);

    EXPECT_CALL(syscall(SYS_semctl, semid, 0, IPC_RMID), 0);
    INFO("Test sem_undo_range passed\n");
    return SUCCESS;
}

/**
 * Test 13: The SEM_UNDO operations on a removed semaphore set are dropped
 * The semid of a removed set is allocated again after a while. The process that exits
 * must not undo its operations on the removed set for the new set.
 */
static int test_sem_undo_removed_set(void) {
    pid_t pid;
    int old_semid = sem_create(1);
    int go_semid = sem_create(1);
    if (old_semid < 0 || go_semid < 0) {
        THROW_ERROR("semget() create failed (errno: %d)", errno);
    }

    // The child adds 1 to the semaphore and has to subtract it when it exits
    int args[] = {old_semid, 0, 1, go_semid};
    if (spawn_child(&pid, TEST_UNDO_HOLD, 4, args) != SUCCESS ||
            wait_for_val(old_semid, 0, 1) != SUCCESS) {
        return FAIL;
    }
    EXPECT_CALL(syscall(SYS_semctl, old_semid, 0, IPC_RMID), 0);

    // Create and remove sets until the semid is allocated again
    int new_semid = -1;
    for (int i = 0; i < 300; i++) {
        int semid = sem_create(1);
        if (semid < 0) {
            THROW_ERROR("semget() create failed (errno: %d)", errno);
        }
        if (semid == old_semid) {
            new_semid = semid;
            break;
        }
        EXPECT_CALL(syscall(SYS_semctl, semid, 0, IPC_RMID), 0);
    }
    if (new_semid < 0) {
        INFO("the semid is not allocated again, there is nothing to test\n");
    } else {
        EXPECT_CALL(syscall(SYS_semctl, new_semid, 0, SETVAL, 5), 0);
    }

    // Tell the child to exit
    EXPECT_CALL(sem_op(go_semid, 0, 1, 0), 0);
    if (wait_for_child(pid) != SUCCESS) {
        return FAIL;
    }
    if (new_semid >= 0) {
        if (check_val(new_semid, 0, 5) != SUCCESS) {
            return FAIL;
        }
        EXPECT_CALL(syscall(SYS_semctl, new_semid, 0, IPC_RMID), 0);
    }

    EXPECT_CALL(syscall(SYS_semctl, go_semid, 0, IPC_RMID), 0);
    INFO("Test sem_undo_removed_set passed\n");
    return SUCCESS;
}

/**
 * Test 14: The SEM_UNDO operations that fail are not undone
 * An operation that times out or would have to wait does not change the semaphore, so the
 * process that exits has nothing to undo
 */
static int test_sem_undo_failed_ops(void) {
    pid_t pid;
    int semid = sem_create(1);
    if (semid < 0) {
        THROW_ERROR("semget() create failed (errno: %d)", errno);
    }

    int args[] = {semid};
    if (spawn_child(&pid, TEST_UNDO_FAILED, 1, args) != SUCCESS ||
            wait_for_child(pid) != SUCCESS) {
        return FAIL;
    }
    if (check_val(semid, 0, 0) != SUCCESS) {
        return FAIL;
    }

    EXPECT_CALL(syscall(SYS_semctl, semid, 0, IPC_RMID), 0);
    INFO("Test sem_undo_failed_ops passed\n");
    return SUCCESS;
}

// A thread that changes the semaphore number of an operation again and again
typedef struct {
    volatile struct sembuf sop;
    volatile int stop;
} flipper_t;

#define FLIPPER_CALLS   5000

static void *flipper_main(void *arg) {
    flipper_t *flipper = arg;
    while (!flipper->stop) {
        flipper->sop.sem_num = 200;
        flipper->sop.sem_num = 0;
    }
    return NULL;
}

/**
 * Test 15: The operations of one semop() are performed one after the other, all or none of them
 * 1. An operation sees the result of the operations before it
 * 2. The semaphore does not become negative, even if the operations would make it so together
 * 3. The semaphore does not exceed the maximum value (ERANGE), and the call changes nothing
 * 4. The semaphore number is checked first (EFBIG), before an operation fails (EAGAIN)
 * 5. IPC_NOWAIT only counts for the operation that has to wait first
 * 6. The operations are those that the call got when it started, also if another thread changes
 *    them while the call waits, or while it does not wait
 */
static int test_semop_one_after_the_other(void) {
    struct sembuf v_then_p[2] = {{0, 1, 0}, {0, -1, IPC_NOWAIT}};
    struct sembuf p_then_p[2] = {{0, -1, 0}, {0, -1, IPC_NOWAIT}};
    struct sembuf v_then_max[2] = {{0, 1, 0}, {1, 1, 0}};
    struct sembuf two_vs[2] = {{2, SEMVMX_VAL, 0}, {2, 1, 0}};
    struct sembuf to_max[2] = {{2, SEMVMX_VAL - 1, 0}, {2, 1, 0}};
    struct sembuf nowait_then_bad[2] = {{0, -2, IPC_NOWAIT}, {3, 1, 0}};
    struct sembuf wait_then_nowait[2] = {{0, -2, 0}, {1, -1, IPC_NOWAIT}};
    struct timespec timeout = {0, 200 * 1000 * 1000};
    waiter_t waiter;
    flipper_t flipper = { .sop = {0, 0, IPC_NOWAIT} };
    pthread_t flipper_thread;
    long start, elapsed;
    int semid = sem_create(3);
    if (semid < 0) {
        THROW_ERROR("semget() create failed (errno: %d)", errno);
    }

    // Scenario 1: The semaphore is 0, but the P operation comes after the V operation
    EXPECT_CALL(syscall(SYS_semop, semid, v_then_p, 2), 0);
    if (check_val(semid, 0, 0) != SUCCESS) {
        return FAIL;
    }

    // Scenario 2: The semaphore is 1, so each P operation could be performed, but not both
    EXPECT_CALL(syscall(SYS_semctl, semid, 0, SETVAL, 1), 0);
    EXPECT_CALL(syscall(SYS_semop, semid, p_then_p, 2), EAGAIN);
    if (check_val(semid, 0, 1) != SUCCESS) {
        return FAIL;
    }

    // Scenario 3: The semaphore 1 has the maximum value, and the call undoes its first operation
    EXPECT_CALL(syscall(SYS_semctl, semid, 1, SETVAL, SEMVMX_VAL), 0);
    EXPECT_CALL(sem_op(semid, 1, 1, 0), ERANGE);
    EXPECT_CALL(syscall(SYS_semop, semid, v_then_max, 2), ERANGE);
    // The operations together exceed the maximum value, but each one does not
    EXPECT_CALL(syscall(SYS_semop, semid, two_vs, 2), ERANGE);
    if (check_val(semid, 0, 1) != SUCCESS || check_val(semid, 1, SEMVMX_VAL) != SUCCESS ||
            check_val(semid, 2, 0) != SUCCESS) {
        return FAIL;
    }
    // The maximum value can be reached
    EXPECT_CALL(syscall(SYS_semop, semid, to_max, 2), 0);
    if (check_val(semid, 2, SEMVMX_VAL) != SUCCESS) {
        return FAIL;
    }

    // Scenario 4: The semaphore 0 is 1, so the first operation fails, but the semaphore number
    // of the second one is out of range
    EXPECT_CALL(syscall(SYS_semop, semid, nowait_then_bad, 2), EFBIG);

    // Scenario 5: The semaphore 0 is 1 and the semaphore 1 is 0, so both operations would have
    // to wait. The first one has no IPC_NOWAIT, so the call waits until the timeout
    EXPECT_CALL(syscall(SYS_semctl, semid, 1, SETVAL, 0), 0);
    start = now_ms();
    EXPECT_CALL(syscall(SYS_semtimedop, semid, wait_then_nowait, 2, &timeout), EAGAIN);
    elapsed = now_ms() - start;
    if (elapsed < 180 || elapsed > 5000) {
        INFO("semtimedop() took %ld ms, expected about 200 ms\n", elapsed);
        return FAIL;
    }

    // Scenario 6: A thread waits for a P operation on semaphore 0, and another thread changes the
    // operation in the memory that they share. The call must go on with the operation that it
    // got, when the other thread wakes it up with a V operation on semaphore 0 ...
    EXPECT_CALL(syscall(SYS_semctl, semid, 0, SETVAL, 0), 0);
    if (waiter_start(&waiter, semid, 0, -1) != SUCCESS ||
            wait_for_count(semid, 0, GETNCNT, 1) != SUCCESS) {
        return FAIL;
    }
    // ... also if the new operation is a V operation on another semaphore ...
    waiter.sop.sem_num = 1;
    waiter.sop.sem_op = 5;
    EXPECT_CALL(sem_op(semid, 0, 1, 0), 0);
    if (waiter_finish(&waiter) != SUCCESS) {
        return FAIL;
    }
    if (check_val(semid, 0, 0) != SUCCESS || check_val(semid, 1, 0) != SUCCESS) {
        return FAIL;
    }
    // ... or an operation on a semaphore that is not in the set
    if (waiter_start(&waiter, semid, 0, -1) != SUCCESS ||
            wait_for_count(semid, 0, GETNCNT, 1) != SUCCESS) {
        return FAIL;
    }
    waiter.sop.sem_num = 200;
    EXPECT_CALL(sem_op(semid, 0, 1, 0), 0);
    if (waiter_finish(&waiter) != SUCCESS) {
        return FAIL;
    }
    if (check_val(semid, 0, 0) != SUCCESS) {
        return FAIL;
    }

    // Scenario 7: The same for calls that do not wait. A thread changes the semaphore number of a Z
    // operation on semaphore 0, which is 0, between 0 and a number that is not in the set. Each
    // call either performs the operation or fails with EFBIG, according to the number that it got
    if (pthread_create(&flipper_thread, NULL, flipper_main, &flipper) != 0) {
        THROW_ERROR("pthread_create() failed");
    }
    for (int i = 0; i < FLIPPER_CALLS; i++) {
        errno = 0;
        long ret = syscall(SYS_semop, semid, (struct sembuf *)&flipper.sop, 1);
        if (ret != 0 && !(ret == -1 && errno == EFBIG)) {
            flipper.stop = 1;
            pthread_join(flipper_thread, NULL);
            INFO("semop() returned %ld with errno %d (%s), expected 0 or EFBIG\n", ret, errno,
                 strerror(errno));
            return FAIL;
        }
    }
    flipper.stop = 1;
    pthread_join(flipper_thread, NULL);

    EXPECT_CALL(syscall(SYS_semctl, semid, 0, IPC_RMID), 0);
    INFO("Test semop_one_after_the_other passed\n");
    return SUCCESS;
}

/**
 * Test 16: Number of waiting threads (GETNCNT and GETZCNT)
 * 1. A call that does not wait, or that ends without being woken up, leaves no waiter
 * 2. Every thread that waits for the semaphore to increase counts for GETNCNT, until it is woken up
 * 3. Every thread that waits for the semaphore to become 0 counts for GETZCNT
 */
static int test_semctl_wait_counts(void) {
    struct sembuf down = {0, -1, 0};
    struct sembuf down_nowait = {1, -1, IPC_NOWAIT};
    struct timespec timeout = {0, 10 * 1000 * 1000};
    waiter_t waiters[2];
    int semid = sem_create(3);
    if (semid < 0) {
        THROW_ERROR("semget() create failed (errno: %d)", errno);
    }

    // Scenario 1: Neither a timeout nor IPC_NOWAIT leaves a waiter
    EXPECT_CALL(syscall(SYS_semtimedop, semid, &down, 1, &timeout), EAGAIN);
    EXPECT_CALL(syscall(SYS_semop, semid, &down_nowait, 1), EAGAIN);
    EXPECT_CALL(syscall(SYS_semctl, semid, 0, GETNCNT), 0);
    EXPECT_CALL(syscall(SYS_semctl, semid, 1, GETNCNT), 0);

    // Scenario 2: Two threads wait for semaphore 0 to increase
    for (int i = 0; i < 2; i++) {
        if (waiter_start(&waiters[i], semid, 0, -1) != SUCCESS) {
            return FAIL;
        }
    }
    if (wait_for_count(semid, 0, GETNCNT, 2) != SUCCESS) {
        return FAIL;
    }
    EXPECT_CALL(syscall(SYS_semctl, semid, 0, GETZCNT), 0);
    // Both can proceed
    EXPECT_CALL(sem_op(semid, 0, 2, 0), 0);
    for (int i = 0; i < 2; i++) {
        if (waiter_finish(&waiters[i]) != SUCCESS) {
            return FAIL;
        }
    }
    EXPECT_CALL(syscall(SYS_semctl, semid, 0, GETNCNT), 0);

    // Scenario 3: A thread waits for semaphore 2 to become 0
    EXPECT_CALL(syscall(SYS_semctl, semid, 2, SETVAL, 1), 0);
    if (waiter_start(&waiters[0], semid, 2, 0) != SUCCESS) {
        return FAIL;
    }
    if (wait_for_count(semid, 2, GETZCNT, 1) != SUCCESS) {
        return FAIL;
    }
    EXPECT_CALL(syscall(SYS_semctl, semid, 2, GETNCNT), 0);
    EXPECT_CALL(sem_op(semid, 2, -1, 0), 0);
    if (waiter_finish(&waiters[0]) != SUCCESS) {
        return FAIL;
    }
    EXPECT_CALL(syscall(SYS_semctl, semid, 2, GETZCNT), 0);

    EXPECT_CALL(syscall(SYS_semctl, semid, 0, IPC_RMID), 0);
    INFO("Test semctl_wait_counts passed\n");
    return SUCCESS;
}

/**
 * Test 17: SEM_STAT_ANY and SEM_STAT get the status of the set (the number of the command
 * used to clear the SEM_UNDO operations of the process)
 */
#ifndef SEM_STAT_ANY
#define SEM_STAT_ANY    20
#endif

static int test_semctl_stat_any(void) {
    struct semid_ds ds;
    int semid = sem_create(3);
    if (semid < 0) {
        THROW_ERROR("semget() create failed (errno: %d)", errno);
    }

    EXPECT_CALL(sem_op(semid, 0, 1, SEM_UNDO), 0);
    memset(&ds, 0, sizeof(ds));
    EXPECT_CALL(syscall(SYS_semctl, semid, 0, SEM_STAT_ANY, &ds) - semid, 0);
    if (ds.sem_nsems != 3) {
        INFO("SEM_STAT_ANY got %lu semaphores, expected 3\n", (unsigned long)ds.sem_nsems);
        return FAIL;
    }
    memset(&ds, 0, sizeof(ds));
    EXPECT_CALL(syscall(SYS_semctl, semid, 0, SEM_STAT, &ds) - semid, 0);
    if (ds.sem_nsems != 3) {
        INFO("SEM_STAT got %lu semaphores, expected 3\n", (unsigned long)ds.sem_nsems);
        return FAIL;
    }

    EXPECT_CALL(syscall(SYS_semctl, semid, 0, IPC_RMID), 0);
    INFO("Test semctl_stat_any passed\n");
    return SUCCESS;
}

/**
 * Test 18: A process that exits wakes up the processes that wait for the semaphores that it undoes
 */
static int test_sem_undo_wakes_waiter(void) {
    struct sembuf down = {0, -1, 0};
    struct timespec timeout = {5, 0};
    sigset_t sigchld, old_set;
    pid_t pid;
    int semid = sem_create(1);
    if (semid < 0) {
        THROW_ERROR("semget() create failed (errno: %d)", errno);
    }
    EXPECT_CALL(syscall(SYS_semctl, semid, 0, SETVAL, 1), 0);

    // The exit of the child must not interrupt the call that waits for it
    sigemptyset(&sigchld);
    sigaddset(&sigchld, SIGCHLD);
    sigprocmask(SIG_BLOCK, &sigchld, &old_set);

    // The child takes the semaphore, and gives it back when it exits after a while
    int args[] = {semid, 0, -1, -1};
    if (spawn_child(&pid, TEST_UNDO_HOLD, 4, args) != SUCCESS ||
            wait_for_val(semid, 0, 0) != SUCCESS) {
        return FAIL;
    }
    EXPECT_CALL(syscall(SYS_semtimedop, semid, &down, 1, &timeout), 0);
    if (wait_for_child(pid) != SUCCESS) {
        return FAIL;
    }
    sigprocmask(SIG_SETMASK, &old_set, NULL);

    EXPECT_CALL(syscall(SYS_semctl, semid, 0, IPC_RMID), 0);
    INFO("Test sem_undo_wakes_waiter passed\n");
    return SUCCESS;
}

/**
 * Test 19: A process that exits does not make a semaphore negative or larger than the maximum
 * It undoes its operations as far as the semaphore allows
 */
static int test_sem_undo_clamp(void) {
    pid_t pid;
    int semid = sem_create(1);
    int go_semid = sem_create(1);
    if (semid < 0 || go_semid < 0) {
        THROW_ERROR("semget() create failed (errno: %d)", errno);
    }

    // The child adds 1 to the semaphore, which is 0, and has to subtract it when it exits.
    // But this process takes it, so the semaphore stays 0
    int args[] = {semid, 0, 1, go_semid};
    if (spawn_child(&pid, TEST_UNDO_HOLD, 4, args) != SUCCESS ||
            wait_for_val(semid, 0, 1) != SUCCESS) {
        return FAIL;
    }
    EXPECT_CALL(sem_op(semid, 0, -1, 0), 0);
    EXPECT_CALL(sem_op(go_semid, 0, 1, 0), 0);
    if (wait_for_child(pid) != SUCCESS || check_val(semid, 0, 0) != SUCCESS) {
        return FAIL;
    }

    // The child subtracts 32767 from the semaphore, which is 32767, and has to add it when
    // it exits. But this process adds it, so the semaphore stays 32767
    EXPECT_CALL(syscall(SYS_semctl, semid, 0, SETVAL, SEMVMX_VAL), 0);
    args[2] = -SEMVMX_VAL;
    if (spawn_child(&pid, TEST_UNDO_HOLD, 4, args) != SUCCESS ||
            wait_for_val(semid, 0, 0) != SUCCESS) {
        return FAIL;
    }
    EXPECT_CALL(sem_op(semid, 0, SEMVMX_VAL, 0), 0);
    EXPECT_CALL(sem_op(go_semid, 0, 1, 0), 0);
    if (wait_for_child(pid) != SUCCESS || check_val(semid, 0, SEMVMX_VAL) != SUCCESS) {
        return FAIL;
    }

    EXPECT_CALL(syscall(SYS_semctl, semid, 0, IPC_RMID), 0);
    EXPECT_CALL(syscall(SYS_semctl, go_semid, 0, IPC_RMID), 0);
    INFO("Test sem_undo_clamp passed\n");
    return SUCCESS;
}

/**
 * Test 20: SETVAL and SETALL clear what the processes have to undo for the semaphores
 * The processes that exit must not undo their operations on the old value for the new one
 */
static int test_sem_undo_cleared_by_set(void) {
    unsigned short vals[2] = {3, 7};
    pid_t pid;
    int semid = sem_create(2);
    int go_semid = sem_create(1);
    if (semid < 0 || go_semid < 0) {
        THROW_ERROR("semget() create failed (errno: %d)", errno);
    }
    EXPECT_CALL(syscall(SYS_semctl, semid, 0, SETVAL, 1), 0);
    EXPECT_CALL(syscall(SYS_semctl, semid, 1, SETVAL, 1), 0);

    // SETVAL: The child subtracts 1 from the semaphore 0, and has to add it when it exits
    int args[] = {semid, 0, -1, go_semid};
    if (spawn_child(&pid, TEST_UNDO_HOLD, 4, args) != SUCCESS ||
            wait_for_val(semid, 0, 0) != SUCCESS) {
        return FAIL;
    }
    EXPECT_CALL(syscall(SYS_semctl, semid, 0, SETVAL, 5), 0);
    EXPECT_CALL(sem_op(go_semid, 0, 1, 0), 0);
    if (wait_for_child(pid) != SUCCESS || check_val(semid, 0, 5) != SUCCESS) {
        return FAIL;
    }

    // SETALL: The child does the same with the semaphore 1
    args[1] = 1;
    if (spawn_child(&pid, TEST_UNDO_HOLD, 4, args) != SUCCESS ||
            wait_for_val(semid, 1, 0) != SUCCESS) {
        return FAIL;
    }
    EXPECT_CALL(syscall(SYS_semctl, semid, 0, SETALL, vals), 0);
    EXPECT_CALL(sem_op(go_semid, 0, 1, 0), 0);
    if (wait_for_child(pid) != SUCCESS || check_val(semid, 0, 3) != SUCCESS ||
            check_val(semid, 1, 7) != SUCCESS) {
        return FAIL;
    }

    EXPECT_CALL(syscall(SYS_semctl, semid, 0, IPC_RMID), 0);
    EXPECT_CALL(syscall(SYS_semctl, go_semid, 0, IPC_RMID), 0);
    INFO("Test sem_undo_cleared_by_set passed\n");
    return SUCCESS;
}

/**
 * Test 21: SETVAL and SETALL wake up the threads that wait for the new value
 * 1. A thread that waits for the semaphore to increase, and SETVAL
 * 2. A thread that waits for the semaphore to become 0, and SETVAL
 * 3. A thread that waits for the semaphore to increase, and SETALL
 */
static int test_semctl_set_wakes_waiter(void) {
    unsigned short vals[2] = {0, 1};
    waiter_t waiter;
    int semid = sem_create(2);
    if (semid < 0) {
        THROW_ERROR("semget() create failed (errno: %d)", errno);
    }

    // Scenario 1
    if (waiter_start(&waiter, semid, 0, -1) != SUCCESS ||
            wait_for_count(semid, 0, GETNCNT, 1) != SUCCESS) {
        return FAIL;
    }
    EXPECT_CALL(syscall(SYS_semctl, semid, 0, SETVAL, 1), 0);
    if (waiter_finish(&waiter) != SUCCESS || check_val(semid, 0, 0) != SUCCESS) {
        return FAIL;
    }

    // Scenario 2
    EXPECT_CALL(syscall(SYS_semctl, semid, 0, SETVAL, 1), 0);
    if (waiter_start(&waiter, semid, 0, 0) != SUCCESS ||
            wait_for_count(semid, 0, GETZCNT, 1) != SUCCESS) {
        return FAIL;
    }
    EXPECT_CALL(syscall(SYS_semctl, semid, 0, SETVAL, 0), 0);
    if (waiter_finish(&waiter) != SUCCESS) {
        return FAIL;
    }

    // Scenario 3
    if (waiter_start(&waiter, semid, 1, -1) != SUCCESS ||
            wait_for_count(semid, 1, GETNCNT, 1) != SUCCESS) {
        return FAIL;
    }
    EXPECT_CALL(syscall(SYS_semctl, semid, 0, SETALL, vals), 0);
    if (waiter_finish(&waiter) != SUCCESS || check_val(semid, 1, 0) != SUCCESS) {
        return FAIL;
    }

    EXPECT_CALL(syscall(SYS_semctl, semid, 0, IPC_RMID), 0);
    INFO("Test semctl_set_wakes_waiter passed\n");
    return SUCCESS;
}

/**
 * Test 22: A semaphore set that is removed while a thread waits for it is removed at once
 * 1. The thread that waits fails with EIDRM
 * 2. The semid is invalid (EINVAL) and the key is free (ENOENT), a new set can be created with it
 */
static int test_semctl_rmid_waiter(void) {
    waiter_t waiter;
    key_t key;
    int semid, new_semid;

    srand(time(NULL));
    key = random();
    semid = syscall(SYS_semget, key, 1, IPC_CREAT | IPC_EXCL | S_IRWUSER);
    if (semid < 0) {
        THROW_ERROR("semget() create failed (errno: %d)", errno);
    }
    if (waiter_start(&waiter, semid, 0, -1) != SUCCESS ||
            wait_for_count(semid, 0, GETNCNT, 1) != SUCCESS) {
        return FAIL;
    }
    EXPECT_CALL(syscall(SYS_semctl, semid, 0, IPC_RMID), 0);
    if (waiter_finish_removed(&waiter) != SUCCESS) {
        return FAIL;
    }

    EXPECT_CALL(sem_op(semid, 0, 1, 0), EINVAL);
    EXPECT_CALL(syscall(SYS_semctl, semid, 0, GETVAL), EINVAL);
    EXPECT_CALL(syscall(SYS_semget, key, 1, S_IRWUSER), ENOENT);
    new_semid = syscall(SYS_semget, key, 1, IPC_CREAT | IPC_EXCL | S_IRWUSER);
    if (new_semid < 0) {
        THROW_ERROR("semget() create the removed set again failed (errno: %d)", errno);
    }
    if (sem_op(new_semid, 0, 1, 0) != 0) {
        THROW_ERROR("semop() on the new set failed (errno: %d)", errno);
    }

    EXPECT_CALL(syscall(SYS_semctl, new_semid, 0, IPC_RMID), 0);
    INFO("Test semctl_rmid_waiter passed\n");
    return SUCCESS;
}

/**
 * Test 23: The removal of a semaphore set that a thread waits for does not leak its semid
 * There are only 128 semids, and none of them must be lost
 */
static int test_semctl_rmid_waiter_semid(void) {
    waiter_t waiter;

    for (int i = 0; i < 200; i++) {
        int semid = sem_create(1);
        if (semid < 0) {
            THROW_ERROR("semget() create failed in round %d (errno: %d)", i, errno);
        }
        if (waiter_start(&waiter, semid, 0, -1) != SUCCESS ||
                wait_for_count(semid, 0, GETNCNT, 1) != SUCCESS) {
            return FAIL;
        }
        EXPECT_CALL(syscall(SYS_semctl, semid, 0, IPC_RMID), 0);
        if (waiter_finish_removed(&waiter) != SUCCESS) {
            return FAIL;
        }
    }

    INFO("Test semctl_rmid_waiter_semid passed\n");
    return SUCCESS;
}

/**
 * Test 24: A semaphore set that cannot be created does not take a semid
 * There are only 128 semids, and none of them must be lost
 */
static int test_semget_invalid_nsems(void) {
    for (int i = 0; i < 200; i++) {
        EXPECT_CALL(syscall(SYS_semget, IPC_PRIVATE, 100000, IPC_CREAT | S_IRWUSER), EINVAL);
        EXPECT_CALL(syscall(SYS_semget, IPC_PRIVATE, 0, IPC_CREAT | S_IRWUSER), EINVAL);
    }

    int semid = sem_create(1);
    if (semid < 0) {
        THROW_ERROR("semget() create failed (errno: %d)", errno);
    }
    EXPECT_CALL(syscall(SYS_semctl, semid, 0, IPC_RMID), 0);
    INFO("Test semget_invalid_nsems passed\n");
    return SUCCESS;
}

/**
 * Test 25: Errors of semctl() for the semaphores of the set
 * 1. The semaphore number is not in the set → EINVAL
 * 2. The value is not in the range of the semaphore values → ERANGE
 */
static int test_semctl_errors(void) {
    int semid = sem_create(2);
    if (semid < 0) {
        THROW_ERROR("semget() create failed (errno: %d)", errno);
    }

    for (int sem_num = 2; sem_num >= -1; sem_num -= 3) {
        EXPECT_CALL(syscall(SYS_semctl, semid, sem_num, GETVAL), EINVAL);
        EXPECT_CALL(syscall(SYS_semctl, semid, sem_num, SETVAL, 1), EINVAL);
        EXPECT_CALL(syscall(SYS_semctl, semid, sem_num, GETNCNT), EINVAL);
        EXPECT_CALL(syscall(SYS_semctl, semid, sem_num, GETZCNT), EINVAL);
        EXPECT_CALL(syscall(SYS_semctl, semid, sem_num, GETPID), EINVAL);
    }

    EXPECT_CALL(syscall(SYS_semctl, semid, 0, SETVAL, -1), ERANGE);
    EXPECT_CALL(syscall(SYS_semctl, semid, 0, SETVAL, SEMVMX_VAL + 1), ERANGE);
    EXPECT_CALL(syscall(SYS_semctl, semid, 0, SETVAL, SEMVMX_VAL), 0);
    if (check_val(semid, 0, SEMVMX_VAL) != SUCCESS) {
        return FAIL;
    }

    EXPECT_CALL(syscall(SYS_semctl, semid, 0, IPC_RMID), 0);
    INFO("Test semctl_errors passed\n");
    return SUCCESS;
}

// ============================================================================
// Child Process Test Logic (Corresponding to parent process test types)
// ============================================================================

/**
 * Child process: Verify getting semid by key
 */
static int child_test_get_semid_by_key(int argc, const char *argv[]) {
    key_t key;
    int ret, semid, expected_semid, init_val;
    int actual_val;

    // Check parameter count
    if (argc != TEST_GET_SEMID_BY_KEY_ARGC) {
        INFO("Invalid argc: expected %d, actual %d\n", TEST_GET_SEMID_BY_KEY_ARGC, argc);
        return FAIL;
    }

    // Parse parameters
    key = atoi(argv[2]);
    expected_semid = atoi(argv[3]);
    init_val = atoi(argv[4]);
    INFO("Child get key: %d, expected semid: %d\n", key, expected_semid);

    // Get semid by key
    semid = syscall(SYS_semget, key, SEM_SET_SIZE, S_IRWUSER);
    if (semid < 0) {
        THROW_ERROR("Child semget() failed (errno: %d)", errno);
    }

    // Verify semid matches parent's
    if (semid != expected_semid) {
        INFO("Child semid mismatch: expected %d, actual %d\n", expected_semid, semid);
        return FAIL;
    }

    // Verify initial semaphore value is correct
    actual_val = syscall(SYS_semctl, semid, 0, GETVAL);
    if (actual_val != init_val) {
        INFO("Child sem val mismatch: expected %d, actual %d\n", init_val, actual_val);
        return FAIL;
    }

    return SUCCESS;
}

/**
 * Child process: Perform V operation (synchronize with parent)
 */
static int child_test_process_sync(int argc, const char *argv[]) {
    int semid, init_val;
    int ret;
    struct sembuf v_op = {0, 1, 0}; // V operation: sem_op=1

    // Check parameter count
    if (argc != TEST_PROCESS_SYNC_ARGC) {
        INFO("Invalid argc: expected %d, actual %d\n", TEST_PROCESS_SYNC_ARGC, argc);
        return FAIL;
    }

    // Parse parameters
    semid = atoi(argv[2]);
    init_val = atoi(argv[3]);
    INFO("Child sync semid: %d, init val: %d\n", semid, init_val);

    // Perform V operation (value from 0→1)
    ret = syscall(SYS_semop, semid, &v_op, 1);
    if (ret != 0) {
        THROW_ERROR("Child semop(V) failed (errno: %d)", errno);
    }

    // Verify value is 1 after V operation
    int sem_val = syscall(SYS_semctl, semid, 0, GETVAL);
    if (sem_val != 1) {
        INFO("Child V operation failed: expected val=1, actual=%d\n", sem_val);
        return FAIL;
    }

    return SUCCESS;
}

/**
 * Child process: Verify operations on destroyed semaphore
 */
static int child_test_operate_destroyed_sem(int argc, const char *argv[]) {
    key_t key;
    int semid, init_val;
    int ret;
    struct sembuf p_op = {0, -1, 0}; // Attempt P operation

    // Check parameter count
    if (argc != TEST_OPERATE_DESTOYED_ARGC) {
        INFO("Invalid argc: expected %d, actual %d\n", TEST_OPERATE_DESTOYED_ARGC, argc);
        return FAIL;
    }

    // Parse parameters
    key = atoi(argv[2]);
    semid = atoi(argv[3]);
    init_val = atoi(argv[4]);
    INFO("Child destroy test: key=%d, semid=%d\n", key, semid);

    // Scenario 1: Get deleted semaphore by key → expect ENOENT
    ret = syscall(SYS_semget, key, SEM_SET_SIZE, S_IRWUSER);
    if (ret != -1 || errno != ENOENT) {
        INFO("Child semget() failed: expected ENOENT, actual ret=%d, errno=%d\n", ret, errno);
        return FAIL;
    }

    // Scenario 2: Operate on deleted semid → expect EINVAL
    ret = syscall(SYS_semop, semid, &p_op, 1);
    if (ret != -1 || errno != EINVAL) {
        INFO("Child semop() failed: expected EINVAL, actual ret=%d, errno=%d\n", ret, errno);
        return FAIL;
    }

    return SUCCESS;
}

/**
 * Child process thread: Operate on a semaphore with SEM_UNDO
 */
static void *child_undo_thread(void *arg) {
    int semid = *(int *)arg;
    return (void *)sem_op(semid, 0, 6, SEM_UNDO);
}

/**
 * Child process: Operate on the semaphores with SEM_UNDO, in many calls, in one call and
 * in a thread
 */
static int child_test_undo_merged(int argc, const char *argv[]) {
    int set_a, set_b;
    struct sembuf ops[2];
    pthread_t thread;
    void *thread_ret;

    if (argc != TEST_UNDO_MERGED_ARGC) {
        INFO("Invalid argc: expected %d, actual %d\n", TEST_UNDO_MERGED_ARGC, argc);
        return FAIL;
    }
    set_a = atoi(argv[2]);
    set_b = atoi(argv[3]);

    // Semaphore 0 of set A: +3, -2, +5, -1, and +4, -1 in one call
    if (sem_op(set_a, 0, 3, SEM_UNDO) != 0 || sem_op(set_a, 0, -2, SEM_UNDO) != 0 ||
            sem_op(set_a, 0, 5, SEM_UNDO) != 0 || sem_op(set_a, 0, -1, SEM_UNDO) != 0) {
        THROW_ERROR("Child semop() on semaphore 0 failed (errno: %d)", errno);
    }
    ops[0] = (struct sembuf) {0, 4, SEM_UNDO};
    ops[1] = (struct sembuf) {0, -1, SEM_UNDO};
    if (syscall(SYS_semop, set_a, ops, 2) != 0) {
        THROW_ERROR("Child semop() with two operations failed (errno: %d)", errno);
    }

    // Semaphore 1 of set A: -3, +1, -2
    if (sem_op(set_a, 1, -3, SEM_UNDO) != 0 || sem_op(set_a, 1, 1, SEM_UNDO) != 0 ||
            sem_op(set_a, 1, -2, SEM_UNDO) != 0) {
        THROW_ERROR("Child semop() on semaphore 1 failed (errno: %d)", errno);
    }

    // Semaphore 0 of set B: +2, +2, -1, and +6 in another thread
    if (sem_op(set_b, 0, 2, SEM_UNDO) != 0 || sem_op(set_b, 0, 2, SEM_UNDO) != 0 ||
            sem_op(set_b, 0, -1, SEM_UNDO) != 0) {
        THROW_ERROR("Child semop() on set B failed (errno: %d)", errno);
    }
    if (pthread_create(&thread, NULL, child_undo_thread, &set_b) != 0 ||
            pthread_join(thread, &thread_ret) != 0 || thread_ret != NULL) {
        THROW_ERROR("The thread of the child failed");
    }

    // Semaphore 1 of set B: +4, -4, which leave nothing to undo
    if (sem_op(set_b, 1, 4, SEM_UNDO) != 0 || sem_op(set_b, 1, -4, SEM_UNDO) != 0) {
        THROW_ERROR("Child semop() on semaphore 1 of set B failed (errno: %d)", errno);
    }

    // The semaphores have changed until the child exits
    if (sem_getval(set_a, 0) != 18 || sem_getval(set_a, 1) != 16 ||
            sem_getval(set_b, 0) != 16 || sem_getval(set_b, 1) != 3) {
        INFO("Child: unexpected values %ld, %ld, %ld and %ld\n", sem_getval(set_a, 0),
             sem_getval(set_a, 1), sem_getval(set_b, 0), sem_getval(set_b, 1));
        return FAIL;
    }
    return SUCCESS;
}

/**
 * Child process: Operate on a semaphore with SEM_UNDO, then wait until the parent process
 * increments the "go" semaphore (or sleep if there is none), and exit
 */
static int child_test_undo_hold(int argc, const char *argv[]) {
    int semid, sem_num, op, go_semid;
    struct sembuf go = {0, -1, 0};
    struct timespec timeout = {10, 0};

    if (argc != TEST_UNDO_HOLD_ARGC) {
        INFO("Invalid argc: expected %d, actual %d\n", TEST_UNDO_HOLD_ARGC, argc);
        return FAIL;
    }
    semid = atoi(argv[2]);
    sem_num = atoi(argv[3]);
    op = atoi(argv[4]);
    go_semid = atoi(argv[5]);

    if (sem_op(semid, sem_num, op, SEM_UNDO) != 0) {
        THROW_ERROR("Child semop() failed (errno: %d)", errno);
    }
    if (go_semid < 0) {
        usleep(500 * 1000);
    } else if (syscall(SYS_semtimedop, go_semid, &go, 1, &timeout) != 0) {
        THROW_ERROR("Child semop(go) failed (errno: %d)", errno);
    }
    return SUCCESS;
}

/**
 * Child process: Operate on a semaphore of 0 with SEM_UNDO, the operations fail
 */
static int child_test_undo_failed(int argc, const char *argv[]) {
    struct sembuf down = {0, -1, SEM_UNDO};
    struct sembuf down_nowait = {0, -1, SEM_UNDO | IPC_NOWAIT};
    struct timespec timeout = {0, 20 * 1000 * 1000};

    if (argc != TEST_UNDO_FAILED_ARGC) {
        INFO("Invalid argc: expected %d, actual %d\n", TEST_UNDO_FAILED_ARGC, argc);
        return FAIL;
    }
    int semid = atoi(argv[2]);

    EXPECT_CALL(syscall(SYS_semtimedop, semid, &down, 1, &timeout), EAGAIN);
    EXPECT_CALL(syscall(SYS_semop, semid, &down_nowait, 1), EAGAIN);
    return SUCCESS;
}

// ============================================================================
// Test Suite Entry
// ============================================================================

// Test case list (test_no_rmsem must be last for memory leak detection)
static test_case_t test_cases[] = {
    TEST_CASE(test_semget_semid_from_key),
    TEST_CASE(test_process_sync),
    TEST_CASE(test_immediately_rmsem),
    TEST_CASE(test_operate_destroyed_sem),
    TEST_CASE(test_semop_return_value),
    TEST_CASE(test_semop_errors),
    TEST_CASE(test_semtimedop_timeout),
    TEST_CASE(test_sem_undo_no_growth),
    TEST_CASE(test_sem_undo_at_exit),
    TEST_CASE(test_sem_undo_per_process),
    TEST_CASE(test_sem_undo_range),
    TEST_CASE(test_sem_undo_removed_set),
    TEST_CASE(test_sem_undo_failed_ops),
    TEST_CASE(test_semop_one_after_the_other),
    TEST_CASE(test_semctl_wait_counts),
    TEST_CASE(test_semctl_stat_any),
    TEST_CASE(test_sem_undo_wakes_waiter),
    TEST_CASE(test_sem_undo_clamp),
    TEST_CASE(test_sem_undo_cleared_by_set),
    TEST_CASE(test_semctl_set_wakes_waiter),
    TEST_CASE(test_semctl_rmid_waiter),
    TEST_CASE(test_semctl_rmid_waiter_semid),
    TEST_CASE(test_semget_invalid_nsems),
    TEST_CASE(test_semctl_errors),
    TEST_CASE(test_no_rmsem),
};

int main(int argc, const char *argv[]) {
    // Parent process: Run test suite
    if (argc == 1) {
        INFO("Start sem test suite (total cases: %d)\n", ARRAY_SIZE(test_cases));
        return test_suite_run(test_cases, ARRAY_SIZE(test_cases));
    }
    // Child process: Execute specified test logic
    else {
        int option = atoi(argv[1]);
        int ret = FAIL;

        switch (option) {
            case TEST_GET_SEMID_BY_KEY:
                ret = child_test_get_semid_by_key(argc, argv);
                break;
            case TEST_PROCESS_SYNC:
                ret = child_test_process_sync(argc, argv);
                break;
            case TEST_OPERATE_DESTOYED:
                ret = child_test_operate_destroyed_sem(argc, argv);
                break;
            case TEST_UNDO_MERGED:
                ret = child_test_undo_merged(argc, argv);
                break;
            case TEST_UNDO_HOLD:
                ret = child_test_undo_hold(argc, argv);
                break;
            case TEST_UNDO_FAILED:
                ret = child_test_undo_failed(argc, argv);
                break;
            default:
                INFO("Invalid test option: %d\n", option);
                ret = FAIL;
        }

        // Child process returns 0 for success, -1 for failure
        return (ret == SUCCESS) ? 0 : -1;
    }
}
