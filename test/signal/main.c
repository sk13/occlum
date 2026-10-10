#define _GNU_SOURCE
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <sched.h>
#include <unistd.h>
#include <ucontext.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <spawn.h>
#include <assert.h>
#include <string.h>
#include <fcntl.h>
#include <signal.h>
#include <pthread.h>
#include <errno.h>
#include <limits.h>
#include <time.h>
#include "test.h"

// ============================================================================
// Helper macros
// ============================================================================


// ============================================================================
// Helper functions
// ============================================================================


// ============================================================================
// Test sigprocmask
// ============================================================================

// Add a new macro to compare two sigset. Returns 0 iff the two sigset are equal.
// Musl libc defines sigset_t to 16 bytes, but on x86 only the first 8 bytes are
// meaningful. So this comparison only takes the first 8 bytes into account.
#define sigcmpset(a, b) memcmp((a), (b), 8)

int test_sigprocmask() {
    int ret;
    sigset_t new, old;
    sigset_t expected_old;

    // Check sigmask == []
    if ((ret = sigprocmask(0, NULL, &old)) < 0) {
        THROW_ERROR("sigprocmask failed unexpectedly");
    }
    sigemptyset(&expected_old);
    if (sigcmpset(&old, &expected_old) != 0) {
        THROW_ERROR("unexpected old sigset");
    }

    // SIG_BLOCK: [] --> [SIGSEGV]
    sigemptyset(&new);
    sigaddset(&new, SIGSEGV);
    if ((ret = sigprocmask(SIG_BLOCK, &new, &old)) < 0) {
        THROW_ERROR("sigprocmask failed unexpectedly");
    }
    sigemptyset(&expected_old);
    if (sigcmpset(&old, &expected_old) != 0) {
        THROW_ERROR("unexpected old sigset");
    }

    // SIG_SETMASK: [SIGSEGV] --> [SIGIO]
    sigemptyset(&new);
    sigaddset(&new, SIGIO);
    if ((ret = sigprocmask(SIG_SETMASK, &new, &old)) < 0) {
        THROW_ERROR("sigprocmask failed unexpectedly");
    }
    sigemptyset(&expected_old);
    sigaddset(&expected_old, SIGSEGV);
    if (sigcmpset(&old, &expected_old) != 0) {
        THROW_ERROR("unexpected old sigset");
    }

    // SIG_UNBLOCK: [SIGIO] -> []
    if ((ret = sigprocmask(SIG_UNBLOCK, &new, &old)) < 0) {
        THROW_ERROR("sigprocmask failed unexpectedly");
    }
    sigemptyset(&expected_old);
    sigaddset(&expected_old, SIGIO);
    if (sigcmpset(&old, &expected_old) != 0) {
        THROW_ERROR("unexpected old sigset");
    }

    // Check sigmask == []
    if ((ret = sigprocmask(0, NULL, &old)) < 0) {
        THROW_ERROR("sigprocmask failed unexpectedly");
    }
    sigemptyset(&expected_old);
    if (sigcmpset(&old, &expected_old) != 0) {
        THROW_ERROR("unexpected old sigset");
    }

    return 0;
}

// ============================================================================
// Test raise syscall and user-registered signal handlers
// ============================================================================

#define MAX_RECURSION_LEVEL     3

static void handle_sigio(int num, siginfo_t *info, void *context) {
    static volatile int recursion_level = 0;
    printf("Hello from SIGIO signal handler (recursion_level = %d)!\n", recursion_level);

    recursion_level++;
    if (recursion_level <= MAX_RECURSION_LEVEL) {
        raise(SIGIO);
    }
    recursion_level--;
}

int test_raise() {
    struct sigaction new_action, old_action;
    memset(&new_action, 0, sizeof(struct sigaction));
    memset(&old_action, 0, sizeof(struct sigaction));
    new_action.sa_sigaction = handle_sigio;
    new_action.sa_flags = SA_SIGINFO | SA_NODEFER;
    if (sigaction(SIGIO, &new_action, &old_action) < 0) {
        THROW_ERROR("registering new signal handler failed");
    }
    if (old_action.sa_handler != SIG_DFL) {
        THROW_ERROR("unexpected old sig handler");
    }

    raise(SIGIO);

    if (sigaction(SIGIO, &old_action, NULL) < 0) {
        THROW_ERROR("restoring old signal handler failed");
    }
    return 0;
}

// ============================================================================
// Test abort, which uses SIGABRT behind the scene
// ============================================================================

int test_abort() {
    pid_t child_pid;
    char *child_argv[] = {"signal", "aborted_child", NULL};
    int ret;
    int status;

    // Repeat multiple times to check that the resources of the killed child
    // processes are indeed freed by the LibOS
    for (int i = 0; i < 3; i++) {
        ret = posix_spawn(&child_pid, "/bin/signal", NULL, NULL, child_argv, NULL);
        if (ret < 0) {
            THROW_ERROR("failed to spawn a child process\n");
        }

        ret = wait4(-1, &status, 0, NULL);
        if (ret < 0) {
            THROW_ERROR("failed to wait4 the child process\n");
        }
        if (!WIFSIGNALED(status) || WTERMSIG(status) != SIGABRT) {
            THROW_ERROR("child process is expected to be killed by SIGILL\n");
        }
    }
    return 0;
}

static int aborted_child() {
    while (1) {
        abort();
    }
    return 0;
}

// ============================================================================
// Test kill by sending SIGKILL to another process
// ============================================================================

int test_kill() {
    pid_t child_pid;
    char *child_argv[] = {"signal", "killed_child", NULL};
    int ret;
    int status;

    // Repeat multiple times to check that the resources of the killed child
    // processes are indeed freed by the LibOS
    for (int i = 0; i < 3; i++) {
        ret = posix_spawn(&child_pid, "/bin/signal", NULL, NULL, child_argv, NULL);
        if (ret < 0) {
            THROW_ERROR("failed to spawn a child process\n");
        }

        kill(child_pid, SIGKILL);

        ret = wait4(-1, &status, 0, NULL);
        if (ret < 0) {
            THROW_ERROR("failed to wait4 the child process\n");
        }
        if (!WIFSIGNALED(status) || WTERMSIG(status) != SIGKILL) {
            THROW_ERROR("child process is expected to be killed by SIGILL\n");
        }
    }
    return 0;
}

// TODO: remove the use of getpid when we can deliver signals through interrupt
static int killed_child() {
    while (1) {
        getpid();
    }
    return 0;
}

// ============================================================================
// Test catching and handling hardware exception
// ============================================================================
static int SIGFPE_RESURSIVE_LEVEL = 0;

#define fxsave(addr) __asm __volatile("fxsave %0" : "=m" (*(addr)))

// Note: this function is fragile in the sense that compiler may not always
// emit the instruction pattern that triggers divide-by-zero as we expect.
// TODO: rewrite this in assembly
int div_maybe_zero(int x, int y) {
    return x / y;
}

static void handle_sigfpe(int num, siginfo_t *info, void *_context) {
    printf("SIGFPE Caught\n");
    assert(num == SIGFPE);
    assert(info->si_signo == SIGFPE);
    char x[512] __attribute__((aligned(16))) = {};
    char y[512] __attribute__((aligned(16))) = {};
    SIGFPE_RESURSIVE_LEVEL ++;

    ucontext_t *ucontext = _context;
    mcontext_t *mcontext = &ucontext->uc_mcontext;
    // The faulty instruction should be `idiv %esi` (f7 fe)
    mcontext->gregs[REG_RIP] += 2;
    fxsave(x);

    // Try modify the floating point register
    float a = 3.00001234567890123 / 2.001;
    printf("a = %f\n", a);
    float value = 3.14f;
    __asm__ volatile (
        "movss %[newval], %%xmm0"   // Move new value to xmm0 register
        :
        : [newval] "m" (value)       // Input constraint: value is a memory operand
        : "%xmm0"                    // Use xmm0 register
    );

    fxsave(y);

    // Make sure the floating point registers are modified
    if (memcmp(x, y, 512) == 0) {
        printf("floating point registers is not modified\n");
        abort();
    }

    // Recursively trigger exception
    if (SIGFPE_RESURSIVE_LEVEL < 4) {
        volatile int c;
        // Trigger divide-by-zero exception
        int a = 1;
        int b = 0;
        c = div_maybe_zero(a, b);
    }

    return;
}

int test_handle_sigfpe() {
    char *stack = malloc(SIGSTKSZ * 4);
    if (stack == NULL) {
        THROW_ERROR("stack allocation failed");
    }
    stack_t expected_ss = {
        .ss_size = SIGSTKSZ * 4,
        .ss_sp = stack,
        .ss_flags = 0,
    };
    if (sigaltstack(&expected_ss, NULL) < 0) {
        free(stack);
        THROW_ERROR("failed to call sigaltstack");
    }

    // Set up a signal handler that handles divide-by-zero exception
    struct sigaction new_action, old_action;
    memset(&new_action, 0, sizeof(struct sigaction));
    memset(&old_action, 0, sizeof(struct sigaction));
    new_action.sa_sigaction = handle_sigfpe;
    new_action.sa_flags = SA_SIGINFO | SA_NODEFER | SA_ONSTACK;
    if (sigaction(SIGFPE, &new_action, &old_action) < 0) {
        free(stack);
        THROW_ERROR("registering new signal handler failed");
    }
    if (old_action.sa_handler != SIG_DFL) {
        free(stack);
        THROW_ERROR("unexpected old sig handler");
    }

    char x[512] __attribute__((aligned(16))) = {};
    char y[512] __attribute__((aligned(16))) = {};

    // Trigger divide-by-zero exception
    int a = 1;
    int b = 0;
    // Use volatile to prevent compiler optimization
    volatile int c;
    fxsave(x);
    c = div_maybe_zero(a, b);
    fxsave(y);

    if (memcmp(x, y, 512) != 0) {
        free(stack);
        THROW_ERROR("floating point registers are modified");
    }

    printf("Signal handler successfully jumped over the divide-by-zero instruction\n");

    if (sigaction(SIGFPE, &old_action, NULL) < 0) {
        free(stack);
        THROW_ERROR("restoring old signal handler failed");
    }
    free(stack);
    return 0;
}

// TODO: rewrite this in assembly
int read_maybe_null(int *p) {
    return *p;
}

static void handle_sigsegv(int num, siginfo_t *info, void *_context) {
    printf("SIGSEGV Caught\n");
    assert(num == SIGSEGV);
    assert(info->si_signo == SIGSEGV);

    ucontext_t *ucontext = _context;
    mcontext_t *mcontext = &ucontext->uc_mcontext;
    // TODO: how long is the instruction?
    // The faulty instruction should be `idiv %esi` (f7 fe)
    mcontext->gregs[REG_RIP] += 2;

    return;
}


int test_handle_sigsegv() {
    // Set up a signal handler that handles divide-by-zero exception
    struct sigaction new_action, old_action;
    memset(&new_action, 0, sizeof(struct sigaction));
    memset(&old_action, 0, sizeof(struct sigaction));
    new_action.sa_sigaction = handle_sigsegv;
    new_action.sa_flags = SA_SIGINFO;
    if (sigaction(SIGSEGV, &new_action, &old_action) < 0) {
        THROW_ERROR("registering new signal handler failed");
    }
    if (old_action.sa_handler != SIG_DFL) {
        THROW_ERROR("unexpected old sig handler");
    }

    int *addr = NULL;
    volatile int val = read_maybe_null(addr);
    (void)val; // to suppress "unused variables" warning

    printf("Signal handler successfully jumped over a null-dereferencing instruction\n");

    void *ptr = mmap(NULL, 8192, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
    if (ptr == NULL) {
        THROW_ERROR("mmap failure");
    }

    int ret = mprotect(ptr, 8192, PROT_NONE);
    if (ret < 0) {
        THROW_ERROR("mprotect failure");
    }

    val = read_maybe_null(ptr);
    (void)val; // to suppress "unused variables" warning

    printf("Signal handler successfully jumped over a PROT_NONE-visit instruction\n");

    if (sigaction(SIGSEGV, &old_action, NULL) < 0) {
        THROW_ERROR("restoring old signal handler failed");
    }
    return 0;
}

// ============================================================================
// Test handle signal on alternate signal stack
// ============================================================================

#define MAX_ALTSTACK_RECURSION_LEVEL    2

stack_t g_old_ss;

static void handle_sigpipe(int num, siginfo_t *info, void *context) {
    static volatile int recursion_level = 0;
    printf("Hello from SIGPIPE signal handler on the alternate signal stack (recursion_level = %d)\n",
           recursion_level);

    // save old_ss to check if we are on stack
    stack_t old_ss;
    sigaltstack(NULL, &old_ss);
    g_old_ss = old_ss;

    recursion_level++;
    if (recursion_level <= MAX_ALTSTACK_RECURSION_LEVEL) {
        raise(SIGPIPE);
    }
    recursion_level--;
}

int test_sigaltstack() {
    char *stack = malloc(SIGSTKSZ);
    if (stack == NULL) {
        THROW_ERROR("stack allocation failed");
    }
    stack_t expected_ss = {
        .ss_size = SIGSTKSZ,
        .ss_sp = stack,
        .ss_flags = 0,
    };
    if (sigaltstack(&expected_ss, NULL) < 0) {
        free(stack);
        THROW_ERROR("failed to call sigaltstack");
    }
    stack_t actual_ss;
    if (sigaltstack(NULL, &actual_ss) < 0) {
        free(stack);
        THROW_ERROR("failed to call sigaltstack");
    }
    if (actual_ss.ss_size != expected_ss.ss_size
            || actual_ss.ss_sp != expected_ss.ss_sp
            || actual_ss.ss_flags != expected_ss.ss_flags) {
        free(stack);
        THROW_ERROR("failed to check the signal stack after set");
    }

    struct sigaction new_action, old_action;
    memset(&new_action, 0, sizeof(struct sigaction));
    memset(&old_action, 0, sizeof(struct sigaction));
    new_action.sa_sigaction = handle_sigpipe;
    new_action.sa_flags = SA_SIGINFO | SA_NODEFER | SA_ONSTACK;
    if (sigaction(SIGPIPE, &new_action, &old_action) < 0) {
        free(stack);
        THROW_ERROR("registering new signal handler failed");
    }
    if (old_action.sa_handler != SIG_DFL) {
        free(stack);
        THROW_ERROR("unexpected old sig handler");
    }

    raise(SIGPIPE);
    if (g_old_ss.ss_flags != SS_ONSTACK) {
        free(stack);
        THROW_ERROR("check stack flags failed");
    }

    if (sigaction(SIGPIPE, &old_action, NULL) < 0) {
        free(stack);
        THROW_ERROR("restoring old signal handler failed");
    }
    free(stack);
    return 0;
}

// ============================================================================
// Test SIGCHLD signal
// ============================================================================
int sigchld = 0;

void proc_exit() {
    sigchld = 1;
}

int test_sigchld() {
    signal(SIGCHLD, proc_exit);

    int ret, child_pid;
    printf("Run a parent process has pid = %d and ppid = %d\n", getpid(), getppid());

    ret = posix_spawn(&child_pid, "/bin/getpid", NULL, NULL, NULL, NULL);
    if (ret < 0) {
        printf("ERROR: failed to spawn a child process\n");
        return -1;
    }
    printf("Spawn a new proces successfully (pid = %d)\n", child_pid);

    wait(NULL);
    if (sigchld == 0) { THROW_ERROR("Did not receive SIGCHLD"); }

    return 0;
}

// ============================================================================
// Test sigtimedwait syscall
// ============================================================================

struct send_signal_data {
    pthread_t           target;
    int                 signum;
    struct timespec     delay;
};

static void *send_signal_with_delay(void *_data) {
    int ret;
    struct send_signal_data *data = _data;

    // Ensure data->delay time elapsed
    while ((ret = nanosleep(&data->delay, NULL) < 0 && errno == EINTR)) ;
    // Send the signal to the target thread
    pthread_kill(data->target, data->signum);

    free(data);

    return NULL;
}

// Raise a signal for the current thread asynchronously by spawning another
// thread to send the signal to the parent thread after some specified delay.
// The delay is meant to ensure some operation in the parent thread is
// completed by the time the signal is sent.
static pthread_t raise_async(int signum, const struct timespec *delay) {
    pthread_t thread;
    int ret;
    struct send_signal_data *data = malloc(sizeof(*data));
    data->target = pthread_self();
    data->signum = signum;
    data->delay = *delay;
    if ((ret = pthread_create(&thread, NULL, send_signal_with_delay, (void *)data)) < 0) {
        printf("ERROR: pthread_create failed unexpectedly\n");
        abort();
    }

    return thread;
}

int test_sigtimedwait() {
    int ret;
    siginfo_t info;
    sigset_t new_mask, old_mask;
    struct timespec delay, timeout;

    // Update signal mask to block SIGIO
    sigemptyset(&new_mask);
    sigaddset(&new_mask, SIGIO);
    if ((ret = sigprocmask(SIG_BLOCK, &new_mask, &old_mask)) < 0) {
        THROW_ERROR("sigprocmask failed unexpectedly");
    }

    timeout.tv_sec = 1;
    timeout.tv_nsec = 0;

    // There is no pending signal, yet; so the syscall must return EAGAIN error
    ret = sigtimedwait(&new_mask, &info, &timeout);
    if (ret == 0 || (ret < 0 && errno != EAGAIN)) {
        THROW_ERROR("sigprocmask must return with EAGAIN error");
    }

    // Let's generate a pending signal and then get it
    raise(SIGIO);
    if ((ret = sigtimedwait(&new_mask, &info, NULL)) != SIGIO || info.si_signo != SIGIO) {
        THROW_ERROR("sigtimedwait should return the SIGIO");
    }

    // The same for a process-directed signal
    kill(getpid(), SIGIO);
    if ((ret = sigtimedwait(&new_mask, &info, NULL)) != SIGIO || info.si_signo != SIGIO) {
        THROW_ERROR("sigtimedwait should return the process-directed SIGIO");
    }

    // The info argument is optional
    raise(SIGIO);
    if ((ret = sigtimedwait(&new_mask, NULL, NULL)) != SIGIO) {
        THROW_ERROR("sigtimedwait without info should return the SIGIO");
    }
    kill(getpid(), SIGIO);
    if ((ret = sigtimedwait(&new_mask, NULL, NULL)) != SIGIO) {
        THROW_ERROR("sigtimedwait without info should return the process-directed SIGIO");
    }

    // Now let's generate a pending signal in an async way. The pending signal
    // does not exist yet at the time when sigtimedwait is called. So the
    // current thread will be put to sleep and waken up until the
    // asynchronously raised signal is sent to the current thread and becomes
    // pending.
    delay.tv_sec = 0;
    delay.tv_nsec = 10 * 1000 * 1000; // 10ms
    pthread_t thread = raise_async(SIGIO, &delay);

    timeout.tv_sec = 0;
    timeout.tv_nsec = 2 * delay.tv_nsec;

    if ((ret = sigtimedwait(&new_mask, &info, &timeout)) != SIGIO || info.si_signo != SIGIO) {
        THROW_ERROR("sigtimedwait should return the SIGIO");
    }

    // Restore the signal mask
    if ((ret = sigprocmask(SIG_SETMASK, &old_mask, NULL)) < 0) {
        THROW_ERROR("sigprocmask failed unexpectedly");
    }

    if (pthread_join(thread, NULL) != 0) {
        THROW_ERROR("failed to join the thread");
    }
    return 0;
}

// ============================================================================
// Test sigtimedwait that is interrupted by a stream of signals
// ============================================================================

// The LibOS asks the host to interrupt the threads that have a pending signal
// that they do not block, so that a thread that is blocked in a call notices
// the signal. The interrupt arrives some time after the LibOS has found the
// pending signal, and by then the signal may have been delivered to the thread
// already, e.g., when the signal has just ended a sigtimedwait together with the
// signal that the thread waits for, and the thread has called sigtimedwait again.
// sigtimedwait must go on waiting in that case. It used to fail with EINTR
// although no signal handler had run.
//
// The interrupt of a thread is sent after the LibOS has taken the scheduler lock
// of the thread, and sched_setaffinity holds that lock while it calls the host.
// So two threads that call sched_setaffinity on the main thread in a loop widen
// the time between the decision to interrupt the thread and the interrupt, and
// the situation above occurs many times per second instead of once in a while.
//
// The cases below send the main thread SIGUSR2, which it handles or ignores, and
// SIGUSR1, which it blocks and waits for with sigtimedwait.

#define STREAM_DURATION_MSEC    3000
#define STREAM_INTERVAL_NSEC    200000
#define NUM_AFFINITY_THREADS    2
#define KERNEL_SIGSET_SIZE      8

static volatile int g_stop;
static volatile unsigned long g_num_handled;
static pid_t g_main_tid;

static void count_signal(int sig) {
    g_num_handled++;
}

static long elapsed_msec(const struct timespec *start) {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return (now.tv_sec - start->tv_sec) * 1000 + (now.tv_nsec - start->tv_nsec) / 1000000;
}

// Set the action of SIGUSR2. The handler runs with SIGUSR1 blocked, as the main thread
// blocks SIGUSR1 to wait for it: SIGUSR1 would terminate the process if it was delivered
// while the handler runs.
static int set_sigusr2_action(void (*handler)(int)) {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = handler;
    sigemptyset(&sa.sa_mask);
    sigaddset(&sa.sa_mask, SIGUSR1);
    if (sigaction(SIGUSR2, &sa, NULL) < 0) {
        THROW_ERROR("failed to set the signal action");
    }
    return 0;
}

static int block_sigusr1(sigset_t *old_mask) {
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGUSR1);
    return sigprocmask(SIG_BLOCK, &set, old_mask);
}

// Make the system call. The sigtimedwait of musl calls it again if it fails with EINTR,
// which hides the failures that the cases below look for.
static long sigtimedwait_sigusr1(const struct timespec *timeout) {
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGUSR1);
    siginfo_t info;
    return syscall(SYS_rt_sigtimedwait, &set, &info, timeout, KERNEL_SIGSET_SIZE);
}

// Take the pending SIGUSR1 before it is unblocked, as it would terminate the process
static int unblock_sigusr1(const sigset_t *old_mask) {
    struct timespec no_timeout = { 0, 0 };
    while (sigtimedwait_sigusr1(&no_timeout) > 0) {
    }
    return sigprocmask(SIG_SETMASK, old_mask, NULL);
}

// Send the main thread SIGUSR2, which it handles, and SIGUSR1, which it waits for
static void *send_signals(void *arg) {
    struct timespec interval = { 0, STREAM_INTERVAL_NSEC };
    while (!g_stop) {
        nanosleep(&interval, NULL);
        syscall(SYS_tgkill, getpid(), g_main_tid, SIGUSR2);
        syscall(SYS_tgkill, getpid(), g_main_tid, SIGUSR1);
    }
    return NULL;
}

static void *set_affinity(void *arg) {
    cpu_set_t cpu_set;
    if (sched_getaffinity(g_main_tid, sizeof(cpu_set), &cpu_set) < 0) {
        return NULL;
    }
    while (!g_stop) {
        sched_setaffinity(g_main_tid, sizeof(cpu_set), &cpu_set);
    }
    return NULL;
}

int test_sigtimedwait_with_stream_of_signals() {
    sigset_t old_mask;
    if (set_sigusr2_action(count_signal) < 0) {
        return -1;
    }
    if (block_sigusr1(&old_mask) < 0) {
        THROW_ERROR("failed to block SIGUSR1");
    }

    g_stop = 0;
    g_main_tid = syscall(SYS_gettid);
    pthread_t threads[1 + NUM_AFFINITY_THREADS];
    int num_threads = 0;
    while (num_threads < 1 + NUM_AFFINITY_THREADS) {
        void *(*thread_func)(void *) = num_threads == 0 ? send_signals : set_affinity;
        if (pthread_create(&threads[num_threads], NULL, thread_func, NULL) != 0) {
            break;
        }
        num_threads++;
    }

    int ret = 0;
    if (num_threads == 1 + NUM_AFFINITY_THREADS) {
        struct timespec start;
        clock_gettime(CLOCK_MONOTONIC, &start);
        unsigned long num_calls = 0;
        do {
            unsigned long num_handled_before = g_num_handled;
            struct timespec timeout = { 10, 0 };
            long sigtimedwait_ret = sigtimedwait_sigusr1(&timeout);
            int sigtimedwait_errno = errno;
            num_calls++;
            // The call may fail with EINTR if a signal handler has run
            if (sigtimedwait_ret == -1 && sigtimedwait_errno == EINTR &&
                    g_num_handled != num_handled_before) {
                continue;
            }
            if (sigtimedwait_ret != SIGUSR1) {
                printf("\t\tERROR: sigtimedwait returned %ld with errno %d (%s) in call %lu\n",
                       sigtimedwait_ret, sigtimedwait_errno, strerror(sigtimedwait_errno),
                       num_calls);
                ret = -1;
            }
        } while (ret == 0 && elapsed_msec(&start) < STREAM_DURATION_MSEC);
    } else {
        printf("\t\tERROR: failed to create the threads\n");
        ret = -1;
    }

    g_stop = 1;
    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    if (unblock_sigusr1(&old_mask) < 0) {
        THROW_ERROR("failed to restore the signal mask");
    }
    return ret;
}

// ============================================================================
// Test sigtimedwait with signals that arrive while it waits
// ============================================================================

// The host also interrupts a thread for the signals that the thread ignores. They must
// not end sigtimedwait or shorten its timeout, and a handled signal must end it with EINTR.
// A thread sends the main thread a signal every interval_msec, and SIGUSR1 once when the
// duration is over, unless it has been told to stop. So a sigtimedwait that does not end as
// it should returns when the duration is over, instead of waiting for its timeout.

struct signal_stream {
    int signum;
    int interval_msec;
    int duration_msec;
};

static void *send_signal_stream(void *arg) {
    const struct signal_stream *stream = arg;
    struct timespec interval = { 0, stream->interval_msec * 1000000L };
    struct timespec start;
    clock_gettime(CLOCK_MONOTONIC, &start);
    while (!g_stop) {
        if (elapsed_msec(&start) >= stream->duration_msec) {
            syscall(SYS_tgkill, getpid(), g_main_tid, SIGUSR1);
            break;
        }
        nanosleep(&interval, NULL);
        syscall(SYS_tgkill, getpid(), g_main_tid, stream->signum);
    }
    return NULL;
}

struct wait_result {
    long ret;
    int err;
    long elapsed_msec;
    unsigned long num_handled;
};

// Call sigtimedwait for SIGUSR1 while a thread sends the stream of signals to the main
// thread, which handles SIGUSR2 with the given handler (or ignores it)
static int sigtimedwait_during_stream(const struct signal_stream *stream,
                                      void (*sigusr2_handler)(int),
                                      const struct timespec *timeout,
                                      struct wait_result *result) {
    sigset_t old_mask;
    if (set_sigusr2_action(sigusr2_handler) < 0) {
        return -1;
    }
    if (block_sigusr1(&old_mask) < 0) {
        THROW_ERROR("failed to block SIGUSR1");
    }

    g_stop = 0;
    g_main_tid = syscall(SYS_gettid);
    pthread_t sender;
    if (pthread_create(&sender, NULL, send_signal_stream, (void *)stream) != 0) {
        THROW_ERROR("failed to create the thread");
    }

    struct timespec start;
    clock_gettime(CLOCK_MONOTONIC, &start);
    unsigned long num_handled_before = g_num_handled;
    result->ret = sigtimedwait_sigusr1(timeout);
    result->err = errno;
    result->elapsed_msec = elapsed_msec(&start);
    result->num_handled = g_num_handled - num_handled_before;

    g_stop = 1;
    pthread_join(sender, NULL);
    if (unblock_sigusr1(&old_mask) < 0) {
        THROW_ERROR("failed to restore the signal mask");
    }
    return 0;
}

#define IGNORED_TIMEOUT_MSEC    300
// Allow for the rounding of the milliseconds, and for a slow call
#define TIMEOUT_EARLY_MSEC      10
#define TIMEOUT_LATE_MSEC       500

// The timeout is not shortened by the ignored signals, and it is not made longer
int test_sigtimedwait_with_ignored_signals() {
    struct signal_stream stream = { SIGUSR2, 5, 1500 };
    struct timespec timeout = { 0, IGNORED_TIMEOUT_MSEC * 1000000L };
    struct wait_result result;
    if (sigtimedwait_during_stream(&stream, SIG_IGN, &timeout, &result) < 0) {
        return -1;
    }
    if (result.ret != -1 || result.err != EAGAIN) {
        THROW_ERROR("sigtimedwait returned %ld with errno %d (%s) instead of failing with EAGAIN",
                    result.ret, result.err, strerror(result.err));
    }
    if (result.elapsed_msec < IGNORED_TIMEOUT_MSEC - TIMEOUT_EARLY_MSEC) {
        THROW_ERROR("sigtimedwait failed after %ld ms, which is before its timeout of %d ms",
                    result.elapsed_msec, IGNORED_TIMEOUT_MSEC);
    }
    if (result.elapsed_msec > IGNORED_TIMEOUT_MSEC + TIMEOUT_LATE_MSEC) {
        THROW_ERROR("sigtimedwait failed after %ld ms, which is long after its timeout of %d ms",
                    result.elapsed_msec, IGNORED_TIMEOUT_MSEC);
    }
    return 0;
}

// The timeout is so long that sigtimedwait would wait for ever if a handled signal did
// not end it
int test_sigtimedwait_with_handled_signal() {
    struct signal_stream stream = { SIGUSR2, 200, 1000 };
    struct timespec timeout = { LONG_MAX, 0 };
    struct wait_result result;
    if (sigtimedwait_during_stream(&stream, count_signal, &timeout, &result) < 0) {
        return -1;
    }
    if (result.ret != -1 || result.err != EINTR) {
        THROW_ERROR("sigtimedwait returned %ld with errno %d (%s) instead of failing with EINTR",
                    result.ret, result.err, strerror(result.err));
    }
    if (result.num_handled == 0) {
        THROW_ERROR("sigtimedwait failed with EINTR, but no signal handler has run");
    }
    return 0;
}

// ============================================================================
// Test sigtimedwait in a process that exits or that is stopped by vfork
// ============================================================================

// The host also interrupts a thread if its process is forced to exit, or if the thread is
// forced to stop because another thread of the process calls vfork. A thread that waits in
// sigtimedwait without a timeout must leave the call then, or the process would never exit
// or never return from vfork. The processes below hang if it does not, so the main process
// kills them after some time.

#define CHILD_TIMEOUT_MSEC      10000
#define CHILD_EXIT_STATUS       7

static void *wait_for_sigusr1(void *arg) {
    sigtimedwait_sigusr1(NULL);
    return NULL;
}

// The child process starts a thread that waits for SIGUSR1, which nobody sends, and gives
// it the time to call sigtimedwait
static int start_waiting_thread() {
    sigset_t old_mask;
    pthread_t thread;
    if (block_sigusr1(&old_mask) < 0 ||
            pthread_create(&thread, NULL, wait_for_sigusr1, NULL) != 0) {
        return -1;
    }
    usleep(200 * 1000);
    return 0;
}

// The child process exits with CHILD_EXIT_STATUS while the thread waits
static int exiting_child() {
    if (start_waiting_thread() < 0) {
        return EXIT_FAILURE;
    }
    exit(CHILD_EXIT_STATUS);
}

// The same, but the child process calls vfork before it exits
static int vforking_child() {
    if (start_waiting_thread() < 0) {
        return EXIT_FAILURE;
    }
    pid_t pid = vfork();
    if (pid == 0) {
        _exit(CHILD_EXIT_STATUS);
    }
    int status = 0;
    if (pid < 0 || waitpid(pid, &status, 0) != pid || !WIFEXITED(status) ||
            WEXITSTATUS(status) != CHILD_EXIT_STATUS) {
        return EXIT_FAILURE;
    }
    exit(CHILD_EXIT_STATUS);
}

// Run the child process with the given command, and get its wait status. The child is
// killed if it does not exit in time.
static int run_child_and_wait(const char *cmd, int *status) {
    pid_t child;
    char *child_argv[] = {"signal", (char *)cmd, NULL};
    if (posix_spawn(&child, "/bin/signal", NULL, NULL, child_argv, NULL) != 0) {
        THROW_ERROR("failed to spawn a child process");
    }
    int is_exited = 0;
    for (int i = 0; i < CHILD_TIMEOUT_MSEC / 10 && !is_exited; i++) {
        pid_t ret = waitpid(child, status, WNOHANG);
        if (ret < 0) {
            THROW_ERROR("failed to wait for the child process");
        }
        is_exited = ret == child;
        if (!is_exited) {
            usleep(10 * 1000);
        }
    }
    if (!is_exited) {
        kill(child, SIGKILL);
        waitpid(child, NULL, 0);
        THROW_ERROR("the child process did not exit in %d ms", CHILD_TIMEOUT_MSEC);
    }
    return 0;
}

int test_sigtimedwait_process_exits() {
    int status;
    if (run_child_and_wait("exiting_child", &status) < 0) {
        return -1;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != CHILD_EXIT_STATUS) {
        THROW_ERROR("the child process exited with unexpected status 0x%x", status);
    }
    return 0;
}

int test_sigtimedwait_vfork() {
#ifdef SGX_MODE_SIM
    // In the simulation mode, a vfork now and then crashes the LibOS when another thread of
    // the process is blocked in a call of the host, with sigtimedwait as well as with nanosleep
    printf("\t\tskipped, vfork may crash the LibOS in the simulation mode\n");
    return 0;
#endif
    int status;
    if (run_child_and_wait("vforking_child", &status) < 0) {
        return -1;
    }
    // The child fails if vfork does not work. Its exit status is otherwise not checked, as
    // it is 0 instead of CHILD_EXIT_STATUS now and then when a thread that vfork has
    // stopped is blocked in a call, whether it is sigtimedwait or not.
    if (!WIFEXITED(status) || WEXITSTATUS(status) == EXIT_FAILURE) {
        THROW_ERROR("the child process failed with status 0x%x", status);
    }
    return 0;
}

// ============================================================================
// Test suite main
// ============================================================================

static test_case_t test_cases[] = {
    TEST_CASE(test_sigprocmask),
    TEST_CASE(test_raise),
    TEST_CASE(test_abort),
    TEST_CASE(test_kill),
    TEST_CASE(test_handle_sigfpe),
    TEST_CASE(test_handle_sigsegv),
    TEST_CASE(test_sigaltstack),
    TEST_CASE(test_sigchld),
    TEST_CASE(test_sigtimedwait),
    TEST_CASE(test_sigtimedwait_with_stream_of_signals),
    TEST_CASE(test_sigtimedwait_with_ignored_signals),
    TEST_CASE(test_sigtimedwait_with_handled_signal),
    TEST_CASE(test_sigtimedwait_process_exits),
    TEST_CASE(test_sigtimedwait_vfork),
};

int main(int argc, const char *argv[]) {
    if (argc > 1) {
        const char *cmd = argv[1];
        if (strcmp(cmd, "aborted_child") == 0) {
            return aborted_child();
        } else if (strcmp(cmd, "killed_child") == 0) {
            return killed_child();
        } else if (strcmp(cmd, "exiting_child") == 0) {
            return exiting_child();
        } else if (strcmp(cmd, "vforking_child") == 0) {
            return vforking_child();
        } else {
            fprintf(stderr, "ERROR: unknown command: %s\n", cmd);
            return EXIT_FAILURE;
        }
    }

    return test_suite_run(test_cases, ARRAY_SIZE(test_cases));
}
