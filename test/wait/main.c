#define _GNU_SOURCE
#include <sys/wait.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <spawn.h>
#include <stdbool.h>
#include <stdlib.h>
#include <time.h>
#include "test.h"

// ============================================================================
// Helper functions
// ============================================================================

// The time for which the children spawned by the tests below run
#define CHILD_RUN_MS        1000
// The time after which the tests below send a signal to a waiting process
#define SIGNAL_DELAY_MS     100

static void sleep_ms(long ms) {
    struct timespec ts = { .tv_sec = ms / 1000, .tv_nsec = (ms % 1000) * 1000 * 1000 };
    while (nanosleep(&ts, &ts) < 0 && errno == EINTR) ;
}

static long ms_since(const struct timespec *start) {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    long ms = (now.tv_sec - start->tv_sec) * 1000;
    return ms + (now.tv_nsec - start->tv_nsec) / (1000 * 1000);
}

// Spawn this program with a command to run as a child, see main()
static int spawn_child(pid_t *child_pid, char *cmd) {
    char *child_argv[] = { "wait", cmd, NULL };
    return posix_spawn(child_pid, "/bin/wait", NULL, NULL, child_argv, NULL);
}

struct send_signal_data {
    bool        to_process; // Send the signal to the process instead of the thread
    pthread_t   thread;
    int         signum;
};

static void *send_signal_with_delay(void *_data) {
    struct send_signal_data *data = _data;
    sleep_ms(SIGNAL_DELAY_MS);
    if (data->to_process) {
        kill(getpid(), data->signum);
    } else {
        pthread_kill(data->thread, data->signum);
    }
    return NULL;
}

static int test_wait_no_children() {
    int status = 0;
    int ret = wait(&status);
    if (ret != -1 || errno != ECHILD) {
        THROW_ERROR("wait no children error");
    }
    return 0;
}

static int test_wait_nohang() {
    int status = 0;
    int ret = waitpid(-1, &status, WNOHANG);
    if (ret != -1 || errno != ECHILD) {
        THROW_ERROR("wait no children with NOHANG error");
    }

    int child_pid = 0;
    // /bin/sleep lasts more than 1 sec
    if (posix_spawn(&child_pid, "/bin/sleep", NULL, NULL, NULL, NULL) < 0) {
        THROW_ERROR("posix_spawn child error");
    }

    ret = waitpid(child_pid, &status, WNOHANG);
    if (ret != 0) {
        THROW_ERROR("wait child with NOHANG error");
    }

    sleep(2);
    // The child process should exit
    ret = waitpid(child_pid, &status, WNOHANG);
    if (ret != child_pid) {
        THROW_ERROR("wait child with NOHANG error");
    }
    return 0;
}

// NOTE: WUNTRACED is same as WSTOPPED
// TODO: Support WUNTRACED and WCONTINUED and enable this test case
static int test_wait_untraced_and_continued() {
    int status = 0;
    int ret = waitpid(-1, &status, WNOHANG);
    if (ret != -1 || errno != ECHILD) {
        THROW_ERROR("wait no children with NOHANG error");
    }

    int child_pid = 0;
    if (posix_spawn(&child_pid, "/bin/sleep", NULL, NULL, NULL, NULL) < 0) {
        THROW_ERROR("posix_spawn child error");
    }

    ret = waitpid(child_pid, &status, WNOHANG);
    if (ret != 0) {
        THROW_ERROR("wait child with NOHANG error");
    }

    kill(child_pid, SIGSTOP);
    // WUNTRACED will get child_pid status
    ret = waitpid(child_pid, &status, WUNTRACED);
    printf("ret = %d, status = %d\n", ret, status);
    if (ret != child_pid || !WIFSTOPPED(status) || WSTOPSIG(status) != SIGSTOP ) {
        THROW_ERROR("wait child status error");
    }

    // Let child get back to running by sending SIGCONT
    kill(child_pid, SIGCONT);
    ret = waitpid(child_pid, &status, WCONTINUED);
    printf("ret = %d, status = %d\n", ret, status);
    if (ret != child_pid || !WIFCONTINUED(status)) {
        THROW_ERROR("wait child status error");
    }

    sleep(2);
    // The child process should exit
    ret = waitpid(child_pid, &status, WNOHANG | WUNTRACED);
    printf("ret = %d, status = %d\n", ret, status);
    if (ret != child_pid || !WIFEXITED(status) ) {
        THROW_ERROR("wait child with NOHANG error");
    }
    return 0;
}

// ============================================================================
// Test the interruption of wait by signals
// ============================================================================

static volatile sig_atomic_t num_handled_sigusr1 = 0;

static void handle_sigusr1(int signum) {
    num_handled_sigusr1++;
}

enum signal_sender {
    THREAD_TO_THREAD,   // Another thread sends the signal to the waiting thread
    THREAD_TO_PROCESS,  // Another thread sends the signal to the process
    CHILD_TO_PARENT,    // The child that the process waits for sends the signal
};

// Wait for a child while a signal with a handler is sent. The handler should
// run and the wait should fail with EINTR, without waiting for the child to exit.
static int wait_interrupted_by_signal(enum signal_sender sender) {
    struct sigaction action = { .sa_handler = handle_sigusr1 };
    struct sigaction old_action;
    sigemptyset(&action.sa_mask);
    if (sigaction(SIGUSR1, &action, &old_action) < 0) {
        THROW_ERROR("sigaction error");
    }
    num_handled_sigusr1 = 0;

    int child_pid = 0;
    if (spawn_child(&child_pid, sender == CHILD_TO_PARENT ? "signal_parent" : "sleep") != 0) {
        THROW_ERROR("posix_spawn child error");
    }

    pthread_t thread;
    struct send_signal_data data = {
        .to_process = sender == THREAD_TO_PROCESS,
        .thread = pthread_self(),
        .signum = SIGUSR1,
    };
    if (sender != CHILD_TO_PARENT &&
            pthread_create(&thread, NULL, send_signal_with_delay, &data) != 0) {
        THROW_ERROR("pthread_create error");
    }

    int status = 0;
    int ret = waitpid(child_pid, &status, 0);
    if (ret != -1 || errno != EINTR) {
        THROW_ERROR("waitpid should be interrupted, but returns %d", ret);
    }
    if (num_handled_sigusr1 != 1) {
        THROW_ERROR("the signal handler should run once, but runs %d times",
                    num_handled_sigusr1);
    }

    // The child is still running
    ret = waitpid(child_pid, &status, 0);
    if (ret != child_pid || !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        THROW_ERROR("wait child error");
    }

    if (sender != CHILD_TO_PARENT && pthread_join(thread, NULL) != 0) {
        THROW_ERROR("pthread_join error");
    }
    if (sigaction(SIGUSR1, &old_action, NULL) < 0) {
        THROW_ERROR("sigaction error");
    }
    return 0;
}

static int test_wait_interrupted_by_thread_signal() {
    return wait_interrupted_by_signal(THREAD_TO_THREAD);
}

static int test_wait_interrupted_by_process_signal() {
    return wait_interrupted_by_signal(THREAD_TO_PROCESS);
}

static int test_wait_interrupted_by_child_signal() {
    return wait_interrupted_by_signal(CHILD_TO_PARENT);
}

// Signals that are ignored, explicitly or by default, should not interrupt a wait
static int test_wait_not_interrupted_by_ignored_signals() {
    if (signal(SIGUSR2, SIG_IGN) == SIG_ERR) {
        THROW_ERROR("signal error");
    }

    int child_pid = 0;
    if (spawn_child(&child_pid, "sleep") != 0) {
        THROW_ERROR("posix_spawn child error");
    }
    // Its SIGCHLD is ignored by default
    int exiting_child_pid = 0;
    if (spawn_child(&exiting_child_pid, "exit") != 0) {
        THROW_ERROR("posix_spawn child error");
    }
    pthread_t thread;
    struct send_signal_data data = { .to_process = true, .signum = SIGUSR2 };
    if (pthread_create(&thread, NULL, send_signal_with_delay, &data) != 0) {
        THROW_ERROR("pthread_create error");
    }

    int status = 0;
    int ret = waitpid(child_pid, &status, 0);
    if (ret != child_pid || !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        THROW_ERROR("wait child error, waitpid returns %d", ret);
    }
    ret = waitpid(exiting_child_pid, &status, 0);
    if (ret != exiting_child_pid || !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        THROW_ERROR("wait child error");
    }

    if (pthread_join(thread, NULL) != 0) {
        THROW_ERROR("pthread_join error");
    }
    if (signal(SIGUSR2, SIG_DFL) == SIG_ERR) {
        THROW_ERROR("signal error");
    }
    return 0;
}

// A signal that terminates a process should terminate it while it waits
static int test_wait_terminated_by_signal() {
    int child_pid = 0;
    if (spawn_child(&child_pid, "wait_for_child") != 0) {
        THROW_ERROR("posix_spawn child error");
    }
    // Let the child start to wait for its child
    sleep_ms(SIGNAL_DELAY_MS);

    struct timespec start;
    clock_gettime(CLOCK_MONOTONIC, &start);
    if (kill(child_pid, SIGTERM) < 0) {
        THROW_ERROR("kill error");
    }
    int status = 0;
    int ret = waitpid(child_pid, &status, 0);
    if (ret != child_pid || !WIFSIGNALED(status) || WTERMSIG(status) != SIGTERM) {
        THROW_ERROR("the child should be terminated by SIGTERM");
    }
    // It should not wait for its own child to exit
    long elapsed_ms = ms_since(&start);
    if (elapsed_ms >= CHILD_RUN_MS / 2) {
        THROW_ERROR("the child is terminated after %ld ms", elapsed_ms);
    }
    return 0;
}

// ============================================================================
// Child commands
// ============================================================================

static int run_child_cmd(const char *cmd) {
    if (strcmp(cmd, "exit") == 0) {
        return 0;
    } else if (strcmp(cmd, "sleep") == 0) {
        sleep_ms(CHILD_RUN_MS);
        return 0;
    } else if (strcmp(cmd, "signal_parent") == 0) {
        sleep_ms(SIGNAL_DELAY_MS);
        kill(getppid(), SIGUSR1);
        sleep_ms(CHILD_RUN_MS);
        return 0;
    } else if (strcmp(cmd, "wait_for_child") == 0) {
        int child_pid = 0;
        if (spawn_child(&child_pid, "sleep") != 0) {
            return EXIT_FAILURE;
        }
        waitpid(child_pid, NULL, 0);
        return 0;
    } else {
        fprintf(stderr, "ERROR: unknown command: %s\n", cmd);
        return EXIT_FAILURE;
    }
}

// ============================================================================
// Test suite main
// ============================================================================

static test_case_t test_cases[] = {
    TEST_CASE(test_wait_no_children),
    TEST_CASE(test_wait_nohang),
    TEST_CASE(test_wait_interrupted_by_thread_signal),
    TEST_CASE(test_wait_interrupted_by_process_signal),
    TEST_CASE(test_wait_interrupted_by_child_signal),
    TEST_CASE(test_wait_not_interrupted_by_ignored_signals),
    TEST_CASE(test_wait_terminated_by_signal),
    // TODO: Enable this test case
    // TEST_CASE(test_wait_untraced_and_continued),
};

int main(int argc, const char *argv[]) {
    if (argc > 1) {
        return run_child_cmd(argv[1]);
    }
    return test_suite_run(test_cases, ARRAY_SIZE(test_cases));
}
