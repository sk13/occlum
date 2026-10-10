#define _GNU_SOURCE
#include <sys/select.h>
#include <sys/syscall.h>
#include <sys/timerfd.h>
#include <sys/wait.h>
#include <sched.h>
#include <signal.h>
#include <spawn.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <stdint.h> // for uint64_t
#include <pthread.h>
#include <time.h>
#include "test.h"

// ============================================================================
// Test sigsuspend with a signal that arrives while it waits
// ============================================================================

// Signal handler for SIGUSR1
void sigusr_handler(int sig) {
    printf("Received signals: %d. ", sig);
}

void *send_signal(void *arg) {
    pthread_t main_thread_id = *(pthread_t *)arg;
    sleep(1);
    pthread_kill(main_thread_id, SIGUSR1);
    sleep(1);
    pthread_kill(main_thread_id, SIGUSR2);
    return NULL;
}

int test_sigsuspend_signal_arrives() {
    // Set SIGUSR1 signal action
    struct sigaction sa1;
    sa1.sa_handler = sigusr_handler;
    sigemptyset(&sa1.sa_mask);
    sa1.sa_flags = 0;
    sigaction(SIGUSR1, &sa1, NULL);

    // Set SIGUSR2 signal action
    struct sigaction sa2;
    sa2.sa_handler = sigusr_handler;
    sigemptyset(&sa2.sa_mask);
    sa2.sa_flags = 0;
    sigaction(SIGUSR2, &sa2, NULL);

    // Mask for blocking SIGUSR1 signal
    sigset_t sigmask;
    sigemptyset(&sigmask);
    sigaddset(&sigmask, SIGUSR1);

    // Access pthread id
    pthread_t main_thread_id = pthread_self();

    // Spawn new thread for sending signal when call pselect syscall
    pthread_t signal_thread;
    if (pthread_create(&signal_thread, NULL, send_signal, &main_thread_id) != 0) {
        THROW_ERROR("failed to create pthread");
        return 1;
    }

    int ret = sigsuspend(&sigmask);
    if (ret == -1) {
        printf("Signal received, the rt_sigsuspend syscall returns successfully\n");
    } else {
        THROW_ERROR("failed to call rt_sigsuspend syscall");
    }

    pthread_join(signal_thread, NULL);
    return 0;
}

// ============================================================================
// Test sigsuspend that is interrupted by a stream of signals
// ============================================================================

// The LibOS asks the host to interrupt the threads that have a pending signal,
// so that a thread that is blocked in a call notices the signal. The interrupt
// arrives some time after the LibOS has found the pending signal, and by then
// the signal may have been delivered to the thread already, e.g. when the
// signal has just ended a sigsuspend and the thread has called sigsuspend again.
// sigsuspend must wait for the next signal in that case. It used to fail with
// EFAULT, as it found no signal that had interrupted it.
//
// The interrupt of a thread is sent after the LibOS has taken the scheduler lock
// of the thread, and sched_setaffinity holds that lock while it calls the host.
// So two threads that call sched_setaffinity on the main thread in a loop widen
// the time between the decision to interrupt the thread and the interrupt, and
// the situation above occurs many times per second instead of once in a while.

#define TEST_DURATION_MSEC      3000
#define SIGNAL_INTERVAL_NSEC    200000
#define NUM_AFFINITY_THREADS    2

static volatile int g_stop;
static volatile unsigned long g_num_handled;
static pid_t g_main_tid;

static void count_signal(int sig) {
    g_num_handled++;
}

static void *send_signals(void *arg) {
    struct timespec interval = { 0, SIGNAL_INTERVAL_NSEC };
    while (!g_stop) {
        nanosleep(&interval, NULL);
        syscall(SYS_tgkill, getpid(), g_main_tid, SIGUSR2);
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

static long elapsed_msec(const struct timespec *start) {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return (now.tv_sec - start->tv_sec) * 1000 + (now.tv_nsec - start->tv_nsec) / 1000000;
}

int test_sigsuspend_eintr_with_stream_of_signals() {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = count_signal;
    if (sigaction(SIGUSR2, &sa, NULL) < 0) {
        THROW_ERROR("failed to set the signal action");
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
        sigset_t empty_mask;
        sigemptyset(&empty_mask);
        struct timespec start;
        clock_gettime(CLOCK_MONOTONIC, &start);
        unsigned long num_calls = 0;
        do {
            unsigned long num_handled_before = g_num_handled;
            int sigsuspend_ret = sigsuspend(&empty_mask);
            int sigsuspend_errno = errno;
            num_calls++;
            if (sigsuspend_ret != -1 || sigsuspend_errno != EINTR) {
                printf("\t\tERROR: sigsuspend returned %d with errno %d (%s) in call %lu\n",
                       sigsuspend_ret, sigsuspend_errno, strerror(sigsuspend_errno), num_calls);
                ret = -1;
            } else if (g_num_handled == num_handled_before) {
                printf("\t\tERROR: sigsuspend returned before a signal handler ran in call %lu\n",
                       num_calls);
                ret = -1;
            }
        } while (ret == 0 && elapsed_msec(&start) < TEST_DURATION_MSEC);
    } else {
        printf("\t\tERROR: failed to create the threads\n");
        ret = -1;
    }

    g_stop = 1;
    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }
    return ret;
}

// ============================================================================
// Test sigsuspend that is ended by a signal that the original mask blocks
// ============================================================================

// sigsuspend replaces the signal mask while it waits, and a signal that the original
// mask blocks but the new mask does not ends it. The handler of the signal must have
// run when sigsuspend returns, and the original mask must be in effect again.

#define MAX_SIGSUSPEND_CALLS    1000

static volatile sig_atomic_t g_num_sigusr1;
static volatile sig_atomic_t g_is_sigusr2_blocked_in_handler;

static void count_sigusr1(int sig) {
    sigset_t mask;
    sigprocmask(SIG_BLOCK, NULL, &mask);
    g_is_sigusr2_blocked_in_handler = sigismember(&mask, SIGUSR2);
    g_num_sigusr1++;
}

// The signal action of SIGUSR1: the handler runs with SIGUSR2 blocked
static int set_sigusr1_action() {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = count_sigusr1;
    sigemptyset(&sa.sa_mask);
    sigaddset(&sa.sa_mask, SIGUSR2);
    if (sigaction(SIGUSR1, &sa, NULL) < 0) {
        THROW_ERROR("failed to set the signal action");
    }
    return 0;
}

static void *send_sigusr1(void *arg) {
    pthread_t target = *(pthread_t *)arg;
    usleep(100 * 1000);
    pthread_kill(target, SIGUSR1);
    return NULL;
}

static int is_sigusr1_blocked() {
    sigset_t mask;
    sigprocmask(SIG_BLOCK, NULL, &mask);
    return sigismember(&mask, SIGUSR1);
}

static int check_sigsuspend_ended_by_sigusr1(int sigsuspend_ret, int sigsuspend_errno) {
    if (sigsuspend_ret != -1 || sigsuspend_errno != EINTR) {
        THROW_ERROR("sigsuspend returned %d with errno %d instead of failing with EINTR",
                    sigsuspend_ret, sigsuspend_errno);
    }
    if (g_num_sigusr1 != 1) {
        THROW_ERROR("the signal handler ran %d times when sigsuspend returned instead of once",
                    (int)g_num_sigusr1);
    }
    if (!g_is_sigusr2_blocked_in_handler) {
        THROW_ERROR("SIGUSR2, which the signal action blocks, was not blocked in the handler");
    }
    if (!is_sigusr1_blocked()) {
        THROW_ERROR("the original signal mask is not in effect after sigsuspend");
    }
    return 0;
}

int test_sigsuspend_runs_handler_of_blocked_signal() {
    if (set_sigusr1_action() < 0) {
        return -1;
    }
    sigset_t sigusr1_mask, empty_mask, old_mask;
    sigemptyset(&sigusr1_mask);
    sigaddset(&sigusr1_mask, SIGUSR1);
    sigemptyset(&empty_mask);
    if (sigprocmask(SIG_BLOCK, &sigusr1_mask, &old_mask) < 0) {
        THROW_ERROR("failed to block SIGUSR1");
    }

    // The signal is pending when sigsuspend is called
    g_num_sigusr1 = 0;
    if (raise(SIGUSR1) != 0) {
        THROW_ERROR("failed to raise SIGUSR1");
    }
    int ret = sigsuspend(&empty_mask);
    if (check_sigsuspend_ended_by_sigusr1(ret, errno) < 0) {
        return -1;
    }

    // The signal arrives while sigsuspend waits
    g_num_sigusr1 = 0;
    pthread_t main_thread = pthread_self();
    pthread_t sender;
    if (pthread_create(&sender, NULL, send_sigusr1, &main_thread) != 0) {
        THROW_ERROR("failed to create the thread");
    }
    ret = sigsuspend(&empty_mask);
    int sigsuspend_errno = errno;
    pthread_join(sender, NULL);
    if (check_sigsuspend_ended_by_sigusr1(ret, sigsuspend_errno) < 0) {
        return -1;
    }

    if (sigprocmask(SIG_SETMASK, &old_mask, NULL) < 0) {
        THROW_ERROR("failed to restore the signal mask");
    }
    return 0;
}

// The usual way to wait for a signal without a race: block the signal, and
// call sigsuspend until the handler of the signal has run
int test_sigsuspend_in_loop_until_signal_handled() {
    if (set_sigusr1_action() < 0) {
        return -1;
    }
    sigset_t sigusr1_mask, empty_mask, old_mask;
    sigemptyset(&sigusr1_mask);
    sigaddset(&sigusr1_mask, SIGUSR1);
    sigemptyset(&empty_mask);
    if (sigprocmask(SIG_BLOCK, &sigusr1_mask, &old_mask) < 0) {
        THROW_ERROR("failed to block SIGUSR1");
    }

    g_num_sigusr1 = 0;
    pthread_t main_thread = pthread_self();
    pthread_t sender;
    if (pthread_create(&sender, NULL, send_sigusr1, &main_thread) != 0) {
        THROW_ERROR("failed to create the thread");
    }
    int num_calls = 0;
    while (g_num_sigusr1 == 0 && num_calls < MAX_SIGSUSPEND_CALLS) {
        sigsuspend(&empty_mask);
        num_calls++;
    }
    pthread_join(sender, NULL);
    if (g_num_sigusr1 != 1) {
        THROW_ERROR("the signal handler did not run in %d calls of sigsuspend", num_calls);
    }

    if (sigprocmask(SIG_SETMASK, &old_mask, NULL) < 0) {
        THROW_ERROR("failed to restore the signal mask");
    }
    return 0;
}

// ============================================================================
// Test sigsuspend in a process that exits or that is stopped by vfork
// ============================================================================

// The host also interrupts a thread if its process is forced to exit, or if the
// thread is forced to stop because another thread of the process calls vfork. The
// thread must leave sigsuspend then, or the process would never exit or never
// return from vfork. The processes below hang if it does not, so the main process
// kills them after some time.

#define CHILD_TIMEOUT_MSEC      10000
#define CHILD_EXIT_STATUS       7

static const char *g_self_path = "/bin/sigsuspend";

static void *suspend_forever(void *arg) {
    sigset_t empty_mask;
    sigemptyset(&empty_mask);
    sigsuspend(&empty_mask);
    return NULL;
}

static volatile int g_suspend_done;
static volatile int g_suspend_ret;
static volatile int g_suspend_errno;
static volatile int g_suspend_is_sigusr1_blocked;

// Call sigsuspend with an empty mask while SIGUSR1 is blocked, and record how it ends
static void *suspend_with_blocked_sigusr1(void *arg) {
    sigset_t sigusr1_mask, empty_mask, mask;
    sigemptyset(&sigusr1_mask);
    sigaddset(&sigusr1_mask, SIGUSR1);
    sigemptyset(&empty_mask);
    pthread_sigmask(SIG_BLOCK, &sigusr1_mask, NULL);
    g_suspend_ret = sigsuspend(&empty_mask);
    g_suspend_errno = errno;
    pthread_sigmask(SIG_BLOCK, NULL, &mask);
    g_suspend_is_sigusr1_blocked = sigismember(&mask, SIGUSR1);
    g_suspend_done = 1;
    return NULL;
}

// The child process starts a thread that calls sigsuspend, and exits with CHILD_EXIT_STATUS
// after the thread has had the time to call it
static int exit_while_suspended() {
    pthread_t thread;
    if (pthread_create(&thread, NULL, suspend_forever, NULL) != 0) {
        return EXIT_FAILURE;
    }
    usleep(200 * 1000);
    exit(CHILD_EXIT_STATUS);
}

// The same, but the child process calls vfork before it exits. The thread blocks SIGUSR1 before
// it calls sigsuspend, and its original mask must be in effect when sigsuspend returns.
static int vfork_while_suspended() {
    pthread_t thread;
    if (set_sigusr1_action() < 0 ||
            pthread_create(&thread, NULL, suspend_with_blocked_sigusr1, NULL) != 0) {
        return EXIT_FAILURE;
    }
    usleep(200 * 1000);
    pid_t pid = vfork();
    if (pid == 0) {
        _exit(CHILD_EXIT_STATUS);
    }
    int status = 0;
    if (pid < 0 || waitpid(pid, &status, 0) != pid || !WIFEXITED(status) ||
            WEXITSTATUS(status) != CHILD_EXIT_STATUS) {
        return EXIT_FAILURE;
    }
    // The LibOS ends the sigsuspend of the thread, as it stops the thread for the vfork
    // (no signal handler runs then). Linux does not, and SIGUSR1 ends it.
    usleep(200 * 1000);
    if (!g_suspend_done) {
        pthread_kill(thread, SIGUSR1);
    }
    pthread_join(thread, NULL);
    if (g_suspend_ret != -1 || g_suspend_errno != EINTR) {
        printf("\t\tERROR: sigsuspend returned %d with errno %d instead of failing with EINTR\n",
               g_suspend_ret, g_suspend_errno);
        return EXIT_FAILURE;
    }
    if (!g_suspend_is_sigusr1_blocked) {
        printf("\t\tERROR: the original signal mask is not in effect after sigsuspend\n");
        return EXIT_FAILURE;
    }
    exit(CHILD_EXIT_STATUS);
}

// Run the child process in the given mode, and get its wait status. The child is killed if
// it does not exit in time.
static int run_child_and_wait(const char *mode, int *status) {
    pid_t child;
    char *argv[] = { (char *)g_self_path, (char *)mode, NULL };
    if (posix_spawn(&child, g_self_path, NULL, NULL, argv, NULL) != 0) {
        THROW_ERROR("failed to spawn the child");
    }
    int is_exited = 0;
    for (int i = 0; i < CHILD_TIMEOUT_MSEC / 10 && !is_exited; i++) {
        pid_t ret = waitpid(child, status, WNOHANG);
        if (ret < 0) {
            THROW_ERROR("failed to wait for the child");
        }
        is_exited = ret == child;
        if (!is_exited) {
            usleep(10 * 1000);
        }
    }
    if (!is_exited) {
        kill(child, SIGKILL);
        waitpid(child, NULL, 0);
        THROW_ERROR("the child did not exit in %d ms", CHILD_TIMEOUT_MSEC);
    }
    return 0;
}

int test_sigsuspend_process_exits() {
    int status;
    if (run_child_and_wait("--exit-while-suspended", &status) < 0) {
        return -1;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != CHILD_EXIT_STATUS) {
        THROW_ERROR("the child exited with unexpected status 0x%x", status);
    }
    return 0;
}

int test_sigsuspend_vfork() {
#ifdef SGX_MODE_SIM
    // In the simulation mode, a vfork now and then crashes the LibOS when another thread of
    // the process is blocked in a call of the host, with nanosleep as well as with sigsuspend
    printf("\t\tskipped, vfork may crash the LibOS in the simulation mode\n");
    return 0;
#endif
    int status;
    if (run_child_and_wait("--vfork-while-suspended", &status) < 0) {
        return -1;
    }
    // The child fails if vfork does not work. Its exit status is otherwise not checked, as
    // it is 0 instead of CHILD_EXIT_STATUS now and then when a thread that vfork has
    // stopped is blocked in a call, whether it is sigsuspend or not.
    if (!WIFEXITED(status) || WEXITSTATUS(status) == EXIT_FAILURE) {
        THROW_ERROR("the child failed with status 0x%x", status);
    }
    return 0;
}

// ============================================================================
// Test suite main
// ============================================================================

static test_case_t test_cases[] = {
    TEST_CASE(test_sigsuspend_signal_arrives),
    TEST_CASE(test_sigsuspend_eintr_with_stream_of_signals),
    TEST_CASE(test_sigsuspend_process_exits),
    TEST_CASE(test_sigsuspend_vfork),
    TEST_CASE(test_sigsuspend_runs_handler_of_blocked_signal),
    TEST_CASE(test_sigsuspend_in_loop_until_signal_handled),
};

int main(int argc, const char *argv[]) {
    if (argc > 1) {
        const char *cmd = argv[1];
        if (strcmp(cmd, "--exit-while-suspended") == 0) {
            return exit_while_suspended();
        } else if (strcmp(cmd, "--vfork-while-suspended") == 0) {
            return vfork_while_suspended();
        } else {
            fprintf(stderr, "ERROR: unknown command: %s\n", cmd);
            return EXIT_FAILURE;
        }
    }

    return test_suite_run(test_cases, ARRAY_SIZE(test_cases));
}
