#define _GNU_SOURCE
#include <stdio.h>
#include <fcntl.h>
#include <stdlib.h>
#include <poll.h>
#include <sched.h>
#include <signal.h>
#include <spawn.h>
#include <time.h>
#include <sys/wait.h>
#include <sys/syscall.h>
#include <pthread.h>
#include "test.h"

// Note: This test intends to test the case that child process directly calls _exit()
// after vfork. "exit", "_exit" and returning from main function are different.
// And here the exit function must be "_exit" to prevent undefined bevaviour.
int test_vfork_exit_and_wait() {
    int status = 0;
    pid_t child_pid = vfork();
    if (child_pid == 0) {
        _exit(0);
    } else {
        printf ("Comming back to parent process from child with pid = %d\n", child_pid);

        // vfork again
        pid_t child_pid_2 = vfork();
        if (child_pid_2 == 0) {
            _exit(1);
        } else {
            printf ("Comming back to parent process from child with pid = %d\n", child_pid_2);
            int ret = waitpid(child_pid, &status, WUNTRACED);
            if (ret != child_pid  || !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
                THROW_ERROR("wait child status error");
            }
            ret = waitpid(child_pid_2, &status, WUNTRACED);
            if (ret != child_pid_2  || !WIFEXITED(status) || WEXITSTATUS(status) != 1) {
                THROW_ERROR("wait child status error");
            }
        }
    }
    return 0;
}

int test_multiple_vfork_execve() {
    char **child_argv = calloc(1, sizeof(char *) * 2); // "hello_world", NULL
    child_argv[0] = strdup("naughty_child");
    for (int i = 0; i < 3; i++ ) {
        pid_t child_pid = vfork();
        if (child_pid == 0) {
            int ret = execve("/bin/naughty_child", child_argv, NULL);
            if (ret != 0) {
                printf("child process execve error");
            }
            _exit(1);
        } else {
            printf ("Comming back to parent process from child with pid = %d\n", child_pid);
            int ret = waitpid(child_pid, 0, 0);
            if (ret != child_pid) {
                THROW_ERROR("wait child error, child pid = %d\n", child_pid);
            }
        }
    }
    return 0;
}

// Create a pipe between parent and child and check file status.
int test_vfork_isolate_file_table() {
    int pipe_fds[2];
    if (pipe(pipe_fds) < 0) {
        THROW_ERROR("failed to create a pipe");
    }

    pid_t child_pid = vfork();
    if (child_pid == 0) {
        close(pipe_fds[1]); // close write end
        char **child_argv = calloc(1,
                                   sizeof(char *) * (5 + 1)); // naughty_child -t vfork reader_fd writer_fd
        child_argv[0] = "naughty_child";
        child_argv[1] = "-t";
        child_argv[2] = "vfork";
        if (asprintf(&child_argv[3], "%d", pipe_fds[0]) < 0 ||
                asprintf(&child_argv[4], "%d", pipe_fds[1]) < 0) {
            THROW_ERROR("failed to asprintf");
        }

        int ret = execve("/bin/naughty_child", child_argv, NULL);
        if (ret != 0) {
            printf("child process execve error\n");
        }
        _exit(1);
    } else {
        printf ("Comming back to parent process from child with pid = %d\n", child_pid);
        if (close(pipe_fds[0]) < 0) { // close read end
            printf("close pipe reader error\n");
            goto parent_exit;
        }
        char *greetings = "Hello from parent\n";
        if (write(pipe_fds[1], greetings, strlen(greetings) + 1) < 0) {
            printf("parent write pipe error\n");
            goto parent_exit;
        }
        int ret = waitpid(child_pid, 0, 0);
        if (ret != child_pid) {
            THROW_ERROR("wait child error, child pid = %d\n", child_pid);
        }
    }

    return 0;

parent_exit:
    kill(child_pid, SIGKILL);
    exit(1);
}

volatile static int test_stop_child_flag = 0;

static void *child_thread_routine(void *_arg) {
    printf("Child thread starts\n");
    test_stop_child_flag = 1;

    struct timespec t1, t2;
    if (clock_gettime(CLOCK_REALTIME, &t1)) {
        return (void *) -1;
    }

    int i = 0;
    while (1) {
        i++;
        int ret = sleep(1);
        if (ret == 0 || i >= 10) {
            break;
        } else if (errno == EINTR) {
            // Interrupted, sleep again
            continue;
        }
    }

    if (clock_gettime(CLOCK_REALTIME, &t2)) {
        return (void *) -1;
    }

    // Parent thread vfork and will stop this thread for several seconds
    if (t2.tv_sec - t1.tv_sec <= 1) {
        printf("the thread is not stopped");
        exit(-1);
    }

    printf("child thread exits\n");
    return NULL;
}

// Test the behavior that when vfork is called, the parent process' other child threads are forced to stopped.
//
// This test case has different behaviors for Linux and Occlum
// This limitation is recorded in src/libos/src/process/do_vfork.rs
int test_vfork_stop_child_thread() {
    pthread_t child_thread;
    pid_t child_pid;
    struct timespec ts;
    ts.tv_sec = 3;
    ts.tv_nsec = 0;
    if (pthread_create(&child_thread, NULL, child_thread_routine, NULL) < 0) {
        THROW_ERROR("pthread_create failed\n");
    }

    // Wait for child thread to start
    while (test_stop_child_flag == 0);

    child_pid = vfork();
    if (child_pid == 0) {
        printf("child process created\n");
        char **child_argv = calloc(1, sizeof(char *) * 2);
        child_argv[0] = "getpid";

        // Wait for a few seconds
        while (1) {
            int ret = nanosleep(&ts, &ts);
            if (ret == 0) {
                break;
            }
            if (ret < 0 && errno != EINTR) {
                THROW_ERROR("nanosleep failed");
            }
        }

        printf("child process exec\n");
        int ret = execve("/bin/getpid", child_argv, NULL);
        if (ret != 0) {
            printf("child process execve error\n");
        }
        _exit(1);
    } else {
        printf("return to parent\n");

        pthread_join(child_thread, NULL);
    }

    return 0;
}

#define NUM_THREADS 20
volatile static int test_main_thread_is_ready = 0;

void *child_thread(void *arg) {
    int *number = (int *)arg;

    int repeat = 10;
    if (*number == 3) {
        printf("child thread %d do vfork\n", *number);
        fflush(stdout);
        // This thread will continually vfork and exit
        int i = repeat;
        while (i--) {
            // wait for main thread to be ready for vfork
            while (test_main_thread_is_ready == 0);
            pid_t pid = vfork();
            if (pid == 0) {
                // Child process
                sleep(1);
                _exit(0);
            } else if (pid > 0) {
                // Parent process
                waitpid(pid, NULL, 0);
                printf("child vfork i = %d\n", i);
            } else {
                perror("vfork");
                exit(EXIT_FAILURE);
            }
        }

        return NULL;
    }

    // Other threads do their own work
    for (int i = 5; i < repeat; ++i) {
        printf("Thread %ld doing its work i = %d.\n", pthread_self(), i);
        fflush(stdout);
        sleep(1);
    }

    return NULL;
}

// Test multiple threads of the same process do vfork simultaneously and shouldn't force stop each other to make the process hang.
int test_vfork_multiple_threads() {
    pthread_t threads[NUM_THREADS];
    int ret;
    int test[NUM_THREADS] = {0};

    // Create NUM_THREADS threads
    for (int i = 0; i < NUM_THREADS; ++i) {
        test[i] = i;
        ret = pthread_create(&threads[i], NULL, child_thread, &test[i]);
        if (ret != 0) {
            perror("pthread_create");
            return EXIT_FAILURE;
        }
    }
    printf("create child threads done\n");
    fflush(stdout);

    test_main_thread_is_ready = 1;
    // Main thread does a vfork and exec hello_world
    pid_t pid = vfork();
    if (pid == 0) {
        // Child process
        sleep(1);
        char *args[] = { "/bin/getpid", NULL };
        execv(args[0], args);
        perror("execv");
        _exit(EXIT_FAILURE); // Exit if exec fails
    } else if (pid > 0) {
        // Parent process waits for the child to complete
        waitpid(pid, NULL, 0);
    } else {
        perror("vfork");
        return EXIT_FAILURE;
    }

    // Join the threads
    for (int i = 0; i < NUM_THREADS; ++i) {
        pthread_join(threads[i], NULL);
    }

    return 0;
}

// The test cases below are about a vforked child that calls vfork again (a nested vfork). A vforked
// child shares the memory and the stack of its parent, so it must not return from the function that
// called vfork, and it cannot report a failure with THROW_ERROR. It records the first check that
// failed in g_failed_line (the parent reads it) and leaves with _exit. The other results that the
// children pass to the parent are volatile, as the compiler does not know that they change.
//
// A child whose execve fails exits with EXEC_FAILED, which the parent does not expect.

#define ARG_EXIT        "--exit"
#define ARG_EXEC_EXIT   "--exec-exit"
#define ARG_WAIT_CHILD  "--wait-child"
#define ARG_VICTIM      "--victim"
#define ARG_THREAD      "--thread"
#define ARG_PROCESS_GROUP "--wait-process-group"
#define ARG_SYS_EXIT_THREAD "--sys-exit-thread"
#define ARG_SYS_EXIT_MAIN "--sys-exit-main"
#define EXEC_FAILED     99
// The exit code of a child whose exit system call has returned
#define EXIT_RETURNED   98

// This program, which the test cases execute and spawn (see main)
#define SELF_PATH       "/bin/vfork"

static volatile int g_failed_line;

#define CHILD_CHECK(cond) \
    do { \
        if (!(cond) && g_failed_line == 0) { \
            g_failed_line = __LINE__; \
        } \
    } while (0)

#define EXITED_WITH(status, code)   (WIFEXITED(status) && WEXITSTATUS(status) == (code))

static int is_fd_open(int fd) {
    return fcntl(fd, F_GETFD) >= 0;
}

// waitpid that is not given up by a signal. The SIGCHLD of an exiting child can interrupt it.
static pid_t wait_for_child(pid_t pid, int *status) {
    pid_t ret;
    do {
        ret = waitpid(pid, status, 0);
    } while (ret < 0 && errno == EINTR);
    return ret;
}

// Execute this program, which exits with the code. Returns only if execve fails. The code is a
// string, as a vforked child must not format it (snprintf is not async-signal-safe).
static void exec_exit(char *code) {
    char *argv[] = { SELF_PATH, ARG_EXIT, code, NULL };
    execve(SELF_PATH, argv, NULL);
}

// How a vforked child leaves: with _exit (exit_group), with the execve of this program, or with the
// system call exit, which ends the calling thread only (it is what pthread_exit ends with in a thread
// that is not the last one). The system call is made through libc, or with the "syscall" instruction,
// which an enclave cannot execute: the LibOS emulates it, but only in the HW mode.
#define LEAVE_EXIT          0
#define LEAVE_EXEC          1
#define LEAVE_SYS_EXIT      2
#define LEAVE_SYS_EXIT_INSN 3

#ifdef SGX_MODE_HW
#define SYSCALL_INSN_EMULATED   1
#else
#define SYSCALL_INSN_EMULATED   0
#endif

// End the vforked child with the code (not for LEAVE_EXEC, see exec_exit). It does not return, unless the
// LibOS fails to end the child.
static void __attribute__((noinline, noreturn)) vfork_child_leave(int how, int code) {
    if (how == LEAVE_SYS_EXIT) {
        syscall(SYS_exit, code);
    } else if (how == LEAVE_SYS_EXIT_INSN) {
        __asm__ volatile ("syscall" : : "a"(SYS_exit), "D"(code) : "rcx", "r11", "memory");
    } else {
        _exit(code);
    }
    _exit(EXIT_RETURNED);
}

// The parent vforks a child, which vforks a grandchild. The grandchild leaves with 42 or executes a
// program that exits with it. The child waits for the grandchild, if it is told to, and leaves with 7 or
// executes a program that exits with it. The parent waits for the child. The grandchild and the child
// leave in the way that grandchild_how and child_how give (one of the LEAVE_ values; the test cases that
// are older than LEAVE_SYS_EXIT pass 0 and 1 for LEAVE_EXIT and LEAVE_EXEC).
//
// The files of a vforked child are not those of its parent. When the grandchild returns to the
// child, the child must find the files that it had before the vfork (the fd that the grandchild
// opened is closed, the ones that it opened itself and those of the parent are open), and the same
// holds for the parent when the child returns.
static int run_nested(int grandchild_how, int child_how, int child_waits) {
    g_failed_line = 0;
    volatile int parent_fd = dup(STDOUT_FILENO);
    volatile int child_fd = -1;
    volatile int grandchild_fd = -1;
    volatile pid_t grandchild_pid = -1;
    if (parent_fd < 0) {
        THROW_ERROR("failed to dup");
    }

    pid_t child_pid = vfork();
    if (child_pid == 0) {
        child_fd = dup(STDOUT_FILENO);
        pid_t pid = vfork();
        if (pid == 0) {
            CHILD_CHECK(is_fd_open(parent_fd) && is_fd_open(child_fd));
            grandchild_fd = dup(STDOUT_FILENO);
            // What the grandchild closes stays open for the child and for the parent
            CHILD_CHECK(close(child_fd) == 0 && close(parent_fd) == 0);
            if (grandchild_how == LEAVE_EXEC) {
                exec_exit("42");
                _exit(EXEC_FAILED);
            }
            vfork_child_leave(grandchild_how, 42);
        }
        grandchild_pid = pid;
        CHILD_CHECK(pid > 0);
        CHILD_CHECK(is_fd_open(parent_fd) && is_fd_open(child_fd));
        CHILD_CHECK(grandchild_fd >= 0 && !is_fd_open(grandchild_fd));
        int status = 0;
        if (child_waits) {
            CHILD_CHECK(wait_for_child(pid, &status) == pid && EXITED_WITH(status, 42));
            // A child is waited for once
            CHILD_CHECK(wait_for_child(pid, &status) == -1 && errno == ECHILD);
        }
        // What the child closes stays open for the parent
        CHILD_CHECK(close(parent_fd) == 0);
        if (child_how == LEAVE_EXEC) {
            exec_exit("7");
            _exit(EXEC_FAILED);
        }
        vfork_child_leave(child_how, 7);
    }

    if (child_pid <= 0) {
        THROW_ERROR("vfork failed, returned %d", child_pid);
    }
    if (g_failed_line != 0) {
        THROW_ERROR("a child or a grandchild failed the check at line %d", g_failed_line);
    }
    // The files of the parent are back, and none of the others
    if (!is_fd_open(parent_fd) || child_fd < 0 || is_fd_open(child_fd) ||
            grandchild_fd < 0 || is_fd_open(grandchild_fd)) {
        THROW_ERROR("the files of the parent are not back (parent_fd %d, child_fd %d, "
                    "grandchild_fd %d)", parent_fd, child_fd, grandchild_fd);
    }
    int status = 0;
    if (wait_for_child(child_pid, &status) != child_pid || !EXITED_WITH(status, 7)) {
        THROW_ERROR("wait child status error (status 0x%x)", status);
    }
    // A grandchild that has exited is not a child of the parent, whether or not the child has
    // waited for it. (A program that the grandchild has executed is a child of the process, which
    // the parent can wait for.)
    if (grandchild_pid <= 0 || grandchild_pid == child_pid) {
        THROW_ERROR("wrong pid of the grandchild: %d", grandchild_pid);
    }
    if (grandchild_how != LEAVE_EXEC &&
            (wait_for_child(grandchild_pid, &status) != -1 || errno != ECHILD)) {
        THROW_ERROR("the parent can wait for its grandchild (pid %d)", grandchild_pid);
    }
    close(parent_fd);
    return 0;
}

// The child and the grandchild exit
int test_vfork_nested_exit_and_wait() {
    return run_nested(0, 0, 1);
}

// The grandchild executes a program, which is a child of the child that waits for it
int test_vfork_nested_grandchild_execve() {
    return run_nested(1, 0, 1);
}

// The grandchild exits, the child executes a program
int test_vfork_nested_child_execve() {
    return run_nested(0, 1, 1);
}

// Both the grandchild and the child execute a program
int test_vfork_nested_both_execve() {
    return run_nested(1, 1, 1);
}

// The child does not wait for the grandchild
int test_vfork_nested_without_wait() {
    for (int i = 0; i < 4; i++) {
        if (run_nested(i & 1, (i >> 1) & 1, 0) < 0) {
            THROW_ERROR("nested vfork failed (grandchild executes: %d, child executes: %d)",
                        i & 1, (i >> 1) & 1);
        }
    }
    return 0;
}

// The grandchild exits, and the child executes a program without having waited for the grandchild.
// The program is the same process as the child, so it can wait for the grandchild (see main).
int test_vfork_nested_execve_waits_for_grandchild() {
    g_failed_line = 0;
    pid_t child_pid = vfork();
    if (child_pid == 0) {
        pid_t pid = vfork();
        if (pid == 0) {
            _exit(42);
        }
        CHILD_CHECK(pid > 0);
        char *argv[] = { SELF_PATH, ARG_WAIT_CHILD, "42", NULL };
        execve(SELF_PATH, argv, NULL);
        _exit(EXEC_FAILED);
    }

    if (child_pid <= 0) {
        THROW_ERROR("vfork failed, returned %d", child_pid);
    }
    if (g_failed_line != 0) {
        THROW_ERROR("a child or a grandchild failed the check at line %d", g_failed_line);
    }
    int status = 0;
    if (wait_for_child(child_pid, &status) != child_pid || !EXITED_WITH(status, 0)) {
        THROW_ERROR("the program did not wait for the grandchild (status 0x%x)", status);
    }
    return 0;
}

// The execve of a child or a grandchild fails and the vforked child goes on as before.
int test_vfork_nested_failing_execve() {
    g_failed_line = 0;
    char *argv[] = { "no_such_program", NULL };
    pid_t child_pid = vfork();
    if (child_pid == 0) {
        pid_t pid = vfork();
        if (pid == 0) {
            CHILD_CHECK(execve("/bin/no_such_program", argv, NULL) == -1 && errno == ENOENT);
            _exit(5);
        }
        CHILD_CHECK(pid > 0);
        CHILD_CHECK(execve("/bin/no_such_program", argv, NULL) == -1 && errno == ENOENT);
        int status = 0;
        CHILD_CHECK(wait_for_child(pid, &status) == pid && EXITED_WITH(status, 5));
        _exit(3);
    }

    if (child_pid <= 0) {
        THROW_ERROR("vfork failed, returned %d", child_pid);
    }
    if (g_failed_line != 0) {
        THROW_ERROR("a child or a grandchild failed the check at line %d", g_failed_line);
    }
    int status = 0;
    if (wait_for_child(child_pid, &status) != child_pid || !EXITED_WITH(status, 3)) {
        THROW_ERROR("wait child status error");
    }

    // The state of the vfork is clean: a vfork and execve of the parent works
    child_pid = vfork();
    if (child_pid == 0) {
        exec_exit("9");
        _exit(EXEC_FAILED);
    }
    if (child_pid <= 0 || wait_for_child(child_pid, &status) != child_pid ||
            !EXITED_WITH(status, 9)) {
        THROW_ERROR("vfork and execve after the failing execve failed");
    }
    return 0;
}

#define MAX_LEVELS  8
// The fd that a vforked child at a level has opened (the level of the parent is 0)
static volatile int g_level_fds[MAX_LEVELS + 1];

// The child at the level calls vfork until it is at max_level, and every child exits with its level
// after it has waited for its own child. It does not return.
static void __attribute__((noinline, noreturn)) vfork_chain(int level, int max_level) {
    g_level_fds[level] = dup(STDOUT_FILENO);
    CHILD_CHECK(g_level_fds[level] >= 0);
    for (int i = 0; i < level; i++) {
        CHILD_CHECK(is_fd_open(g_level_fds[i]));
    }
    if (level < max_level) {
        pid_t pid = vfork();
        if (pid == 0) {
            vfork_chain(level + 1, max_level);
        }
        CHILD_CHECK(pid > 0);
        // The files of this level are back, and not the ones that the child opened
        for (int i = 0; i <= level; i++) {
            CHILD_CHECK(is_fd_open(g_level_fds[i]));
        }
        CHILD_CHECK(!is_fd_open(g_level_fds[level + 1]));
        int status = 0;
        CHILD_CHECK(wait_for_child(pid, &status) == pid && EXITED_WITH(status, level + 1));
    }
    _exit(level);
}

// The levels of vfork are nested deeper than two, each of them with its own file table.
int test_vfork_nested_depth() {
    g_failed_line = 0;
    g_level_fds[0] = dup(STDOUT_FILENO);
    if (g_level_fds[0] < 0) {
        THROW_ERROR("failed to dup");
    }

    pid_t child_pid = vfork();
    if (child_pid == 0) {
        vfork_chain(1, MAX_LEVELS);
    }

    if (child_pid <= 0) {
        THROW_ERROR("vfork failed, returned %d", child_pid);
    }
    if (g_failed_line != 0) {
        THROW_ERROR("a child failed the check at line %d", g_failed_line);
    }
    int status = 0;
    if (wait_for_child(child_pid, &status) != child_pid || !EXITED_WITH(status, 1)) {
        THROW_ERROR("wait child status error");
    }
    if (!is_fd_open(g_level_fds[0])) {
        THROW_ERROR("the file of the parent is not back");
    }
    for (int i = 1; i <= MAX_LEVELS; i++) {
        if (is_fd_open(g_level_fds[i])) {
            THROW_ERROR("the file of the level %d is still open", i);
        }
    }
    close(g_level_fds[0]);
    return 0;
}

static volatile unsigned long g_ticks;
static volatile int g_stop_ticks;

static void *ticks_thread(void *_arg) {
    struct timespec ts = { 0, 2 * 1000 * 1000 };
    while (!g_stop_ticks) {
        g_ticks++;
        nanosleep(&ts, NULL);
    }
    return NULL;
}

static void sleep_ms(long ms) {
    struct timespec ts = { 0, ms * 1000 * 1000 };
    nanosleep(&ts, NULL);
}

// The other threads of the process are frozen from the vfork of the child until the child, which
// is the outermost one, exits or executes a program. They must not run in the meantime, in
// particular not when the grandchild returns to the child. The grandchild and the child leave in
// the way that how gives (LEAVE_EXIT or LEAVE_SYS_EXIT).
static int run_nested_stop_child_thread(int how) {
    g_failed_line = 0;
    g_stop_ticks = 0;
    pthread_t thread;
    if (pthread_create(&thread, NULL, ticks_thread, NULL) < 0) {
        THROW_ERROR("pthread_create failed");
    }
    while (g_ticks == 0) {
        sleep_ms(2);
    }

    pid_t child_pid = vfork();
    if (child_pid == 0) {
        unsigned long ticks = g_ticks;
        sleep_ms(100);
        CHILD_CHECK(g_ticks == ticks);
        pid_t pid = vfork();
        if (pid == 0) {
            ticks = g_ticks;
            sleep_ms(100);
            CHILD_CHECK(g_ticks == ticks);
            vfork_child_leave(how, 0);
        }
        CHILD_CHECK(pid > 0);
        ticks = g_ticks;
        sleep_ms(200);
        CHILD_CHECK(g_ticks == ticks);
        vfork_child_leave(how, 0);
    }

    if (child_pid <= 0) {
        THROW_ERROR("vfork failed, returned %d", child_pid);
    }
    int failed_line = g_failed_line;
    // The thread runs again now (give it a second on a busy host)
    unsigned long ticks = g_ticks;
    int is_running = 0;
    for (int i = 0; i < 100 && !is_running; i++) {
        sleep_ms(10);
        is_running = (g_ticks != ticks);
    }
    g_stop_ticks = 1;
    pthread_join(thread, NULL);
    if (failed_line != 0) {
        THROW_ERROR("the other thread ran in a child or a grandchild (line %d)", failed_line);
    }
    if (!is_running) {
        THROW_ERROR("the other thread did not run after the child had exited");
    }
    int status = 0;
    if (wait_for_child(child_pid, &status) != child_pid || !EXITED_WITH(status, 0)) {
        THROW_ERROR("wait child status error");
    }
    return 0;
}

// This test case has different behaviors for Linux and Occlum, see do_vfork.rs
int test_vfork_nested_stop_child_thread() {
    return run_nested_stop_child_thread(LEAVE_EXIT);
}

#define NESTED_ROUNDS   40

// Many nested vforks, with all the ways that the child and the grandchild can leave, and with and
// without a wait of the child for the grandchild. Nothing from one round, e.g., a file of a child or
// the exit status of a grandchild, must be left behind for the next one.
int test_vfork_nested_repeated() {
    for (int i = 0; i < NESTED_ROUNDS; i++) {
        if (run_nested(i & 1, (i >> 1) & 1, !((i >> 2) & 1)) < 0) {
            THROW_ERROR("nested vfork failed in round %d", i);
        }
    }
    return 0;
}

// The process of a program that was started with ARG_VICTIM: the vforked child (and its child, with
// depth 2) is killed by a signal. A fatal signal ends the whole process in Occlum, as the child
// runs on a thread of the process and has the pid of the process. In Linux, only the child ends,
// and the parent has to end the process. With ARG_THREAD, the process has a second thread, which the
// vfork freezes in Occlum.
static int run_victim(int depth, int with_thread) {
    if (with_thread) {
        g_stop_ticks = 0;
        pthread_t thread;
        if (pthread_create(&thread, NULL, ticks_thread, NULL) != 0) {
            return 2;
        }
        while (g_ticks == 0) {
            sleep_ms(2);
        }
    }
    pid_t pid = vfork();
    if (pid == 0) {
        if (depth >= 2) {
            pid_t grandchild_pid = vfork();
            if (grandchild_pid == 0) {
                kill(getpid(), SIGKILL);
                _exit(1);
            }
        }
        kill(getpid(), SIGKILL);
        _exit(1);
    }
    kill(getpid(), SIGKILL);
    return 1;
}

// A process whose vforked child is killed by a signal ends with the child. All its files must be
// closed, including the ones that the parent had before the vfork, which nobody restores. The
// victim, a process that is started by this test, holds the write end of a pipe, which this test
// can only read up to the end if the victim has closed it. If the victim has a second thread, which
// the vfork has frozen, the thread must run again to end, or nothing of the victim is closed at all.
static int test_killed_child(int depth, int with_thread) {
    int pipe_fds[2];
    if (pipe(pipe_fds) < 0) {
        THROW_ERROR("failed to create a pipe");
    }
    char depth_arg[16];
    snprintf(depth_arg, sizeof(depth_arg), "%d", depth);
    char *argv[] = { SELF_PATH, ARG_VICTIM, depth_arg, with_thread ? ARG_THREAD : NULL, NULL };
    pid_t victim_pid;
    if (posix_spawn(&victim_pid, SELF_PATH, NULL, NULL, argv, NULL) != 0) {
        THROW_ERROR("failed to spawn the victim");
    }
    close(pipe_fds[1]);

    // Fail after some seconds, so that this test does not hang if the file is not closed
    struct pollfd poll_fd = { .fd = pipe_fds[0], .events = POLLIN };
    int ret;
    do {
        ret = poll(&poll_fd, 1, 10000);
    } while (ret < 0 && errno == EINTR);
    if (ret != 1 || (poll_fd.revents & POLLHUP) == 0) {
        THROW_ERROR("the write end of the pipe is still open (poll returned %d, events 0x%x)",
                    ret, poll_fd.revents);
    }
    close(pipe_fds[0]);

    int status = 0;
    if (wait_for_child(victim_pid, &status) != victim_pid || !WIFSIGNALED(status) ||
            WTERMSIG(status) != SIGKILL) {
        THROW_ERROR("the victim did not end with SIGKILL (status 0x%x)", status);
    }

    // A program that is started now may run on the host thread of the victim. It must not find the
    // state of the vfork of the victim there: its execve is not that of a vforked child.
    char *exec_argv[] = { SELF_PATH, ARG_EXEC_EXIT, "9", NULL };
    // Which host thread a new program gets is not predictable, so start many of them, one after the other
    for (int i = 0; i < 64; i++) {
        pid_t pid;
        if (posix_spawn(&pid, SELF_PATH, NULL, NULL, exec_argv, NULL) != 0) {
            THROW_ERROR("failed to spawn the program that executes another one");
        }
        if (wait_for_child(pid, &status) != pid || !EXITED_WITH(status, 9)) {
            THROW_ERROR("the program that executes another one failed (status 0x%x)", status);
        }
    }
    return 0;
}

int test_vfork_killed_child() {
    return test_killed_child(1, 0);
}

int test_vfork_nested_killed_child() {
    return test_killed_child(2, 0);
}

int test_vfork_killed_child_with_thread() {
    return test_killed_child(1, 1);
}

int test_vfork_nested_killed_child_with_thread() {
    return test_killed_child(2, 1);
}

// A vfork that nests too deep fails with EAGAIN. Occlum keeps the state of every level in its
// own memory, and a runaway recursion must not exhaust it, as that would end the whole enclave
// (MAX_VFORK_DEPTH in do_vfork.rs). Linux has other limits, e.g., the number of processes.
//
// This test case has different behaviors for Linux and Occlum
#define RUNAWAY_LEVELS  8192
static volatile int g_runaway_level;
static volatile int g_runaway_errno;

// Every child vforks the next one until a vfork fails, then they all exit.
static void __attribute__((noinline, noreturn)) vfork_runaway(int level) {
    if (level >= RUNAWAY_LEVELS) {
        _exit(0);
    }
    pid_t pid = vfork();
    if (pid == 0) {
        vfork_runaway(level + 1);
    }
    if (pid < 0) {
        g_runaway_level = level;
        // The musl of Occlum returns the negative errno itself, not -1 and errno
        g_runaway_errno = (pid == -1) ? errno : -pid;
    }
    _exit(0);
}

int test_vfork_nested_depth_limit() {
    g_runaway_level = 0;
    g_runaway_errno = 0;
    pid_t child_pid = vfork();
    if (child_pid == 0) {
        vfork_runaway(1);
    }
    if (child_pid <= 0) {
        THROW_ERROR("vfork failed, returned %d", child_pid);
    }
    int status = 0;
    if (wait_for_child(child_pid, &status) != child_pid || !EXITED_WITH(status, 0)) {
        THROW_ERROR("wait child status error (status 0x%x)", status);
    }
    if (g_runaway_level == 0) {
        THROW_ERROR("no vfork has failed in %d levels", RUNAWAY_LEVELS);
    }
    if (g_runaway_errno != EAGAIN) {
        THROW_ERROR("vfork failed at level %d with errno %d, not with EAGAIN", g_runaway_level,
                    g_runaway_errno);
    }
    // The limit must leave room for what programs do
    if (g_runaway_level < 64) {
        THROW_ERROR("vfork failed too early, at level %d", g_runaway_level);
    }
    // Nothing is left behind: all the levels have returned, and a nested vfork works
    return run_nested(1, 1, 1);
}

// waitpid(0) and waitpid(-pgid) wait for the children in the process group of the caller, including
// those that have exited after a vfork. They wait for any of them, so the process that does it must
// have no other children. It is not this one, as the programs that some of the other test cases make
// their children execute are children of this process, too.
static int wait_for_process_group() {
    int status = 0;
    pid_t pid = vfork();
    if (pid == 0) {
        _exit(5);
    }
    if (pid <= 0 || wait_for_child(0, &status) != pid || !EXITED_WITH(status, 5)) {
        THROW_ERROR("waitpid(0) failed (pid %d, status 0x%x)", pid, status);
    }
    // A child is waited for once
    if (wait_for_child(0, &status) != -1 || errno != ECHILD) {
        THROW_ERROR("waitpid(0) did not fail with ECHILD");
    }

    pid = vfork();
    if (pid == 0) {
        _exit(6);
    }
    if (pid <= 0) {
        THROW_ERROR("vfork failed, returned %d", pid);
    }
    // There is no child in the other group
    if (wait_for_child(-(getpgrp() + 1), &status) != -1 || errno != ECHILD) {
        THROW_ERROR("waitpid() of another process group did not fail with ECHILD");
    }
    if (wait_for_child(-getpgrp(), &status) != pid || !EXITED_WITH(status, 6)) {
        THROW_ERROR("waitpid(-pgid) failed (pid %d, status 0x%x)", pid, status);
    }

    // The same in a vforked child, for its own child
    g_failed_line = 0;
    pid = vfork();
    if (pid == 0) {
        pid_t grandchild_pid = vfork();
        if (grandchild_pid == 0) {
            _exit(8);
        }
        int child_status = 0;
        CHILD_CHECK(grandchild_pid > 0 && wait_for_child(0, &child_status) == grandchild_pid &&
                    EXITED_WITH(child_status, 8));
        _exit(9);
    }
    if (pid <= 0) {
        THROW_ERROR("vfork failed, returned %d", pid);
    }
    if (g_failed_line != 0) {
        THROW_ERROR("a child failed the check at line %d", g_failed_line);
    }
    if (wait_for_child(0, &status) != pid || !EXITED_WITH(status, 9)) {
        THROW_ERROR("waitpid(0) failed (pid %d, status 0x%x)", pid, status);
    }
    return 0;
}

int test_vfork_wait_for_process_group() {
    char *argv[] = { SELF_PATH, ARG_PROCESS_GROUP, NULL };
    pid_t pid;
    if (posix_spawn(&pid, SELF_PATH, NULL, NULL, argv, NULL) != 0) {
        THROW_ERROR("failed to spawn the program that waits for a process group");
    }
    int status = 0;
    if (wait_for_child(pid, &status) != pid || !EXITED_WITH(status, 0)) {
        THROW_ERROR("the program that waits for a process group failed (status 0x%x)", status);
    }
    return 0;
}

// The test cases below are about a vforked child that ends with the system call exit, not exit_group. exit
// ends the calling thread only. A vforked child has no thread of its own (it runs on the thread of its
// parent), so the LibOS has to return to the parent, as it does for exit_group, and must not end the thread
// of the parent. Programs make this system call in a vforked child without libc, e.g., the Go runtime, if
// the execve of a child fails.
//
// pthread_exit in a vforked child ends with this system call (in a thread that is not the last one), or with
// exit_group (in the last one), but it is not tested: libc runs the destructors of the thread-specific data
// and unwinds the stack of the thread that it runs on, which is the thread of the parent, so what the parent
// does afterwards is undefined (in Linux, too). What the LibOS can do is to return to the parent, and that
// is all that these test cases check. What libc then does with the parent depends on libc (measured with
// the glibc 2.35 and 2.39 and the musl 1.1.24 of Occlum): glibc goes on if the parent has other threads and
// aborts with "stack smashing detected" if it has none, musl hangs at the exit of the program in both cases.

#define TLS_VALUE   0x7157
static __thread int g_tls_value;
// The exit code of the threads that end with the exit system call. It is not 0: the status of the last
// thread is the one of the process in Occlum, so a process that the LibOS ends too early with the end of one
// of these threads must not look like one that has passed.
#define THREAD_EXIT_CODE    42

static pid_t get_tid(void) {
    return syscall(SYS_gettid);
}

// The codes that a child passes to the exit system call, and the exit code that wait4 reports. It is the
// lowest 8 bits of the code, as for exit_group. The first code is not 0: a process that ends with 0 by
// mistake must not look like one that has passed.
static const struct {
    int code;
    int expected;
} SYS_EXIT_CODES[] = {
    { 7, 7 }, { 0, 0 }, { 255, 255 }, { 256, 0 }, { 256 + 3, 3 }, { -1, 255 },
};

// The calling thread vforks a child that leaves with the code, in the way how. The thread must go on as the
// same thread, with its own state and the files that it had before the vfork (the child has opened one),
// and wait4 reports the exit code that is expected.
static int vfork_sys_exit_round(int how, int code, int expected_code) {
    g_failed_line = 0;
    volatile int parent_fd = dup(STDOUT_FILENO);
    volatile int child_fd = -1;
    if (parent_fd < 0) {
        THROW_ERROR("failed to dup");
    }
    pid_t tid = get_tid();
    pthread_t self = pthread_self();
    g_tls_value = TLS_VALUE;

    pid_t child_pid = vfork();
    if (child_pid == 0) {
        child_fd = dup(STDOUT_FILENO);
        CHILD_CHECK(is_fd_open(parent_fd) && is_fd_open(child_fd));
        vfork_child_leave(how, code);
    }

    if (child_pid <= 0) {
        THROW_ERROR("vfork failed, returned %d", child_pid);
    }
    if (g_failed_line != 0) {
        THROW_ERROR("the child failed the check at line %d", g_failed_line);
    }
    if (get_tid() != tid || !pthread_equal(pthread_self(), self) ||
            g_tls_value != TLS_VALUE) {
        THROW_ERROR("the thread that vforked is not the same (tid %d, was %d)", get_tid(), tid);
    }
    if (!is_fd_open(parent_fd) || child_fd < 0 || is_fd_open(child_fd)) {
        THROW_ERROR("the files of the parent are not back (parent_fd %d, child_fd %d)",
                    parent_fd, child_fd);
    }
    int status = 0;
    if (wait_for_child(child_pid, &status) != child_pid ||
            !EXITED_WITH(status, expected_code)) {
        THROW_ERROR("wait child status error (status 0x%x, expected exit code %d)", status,
                    expected_code);
    }
    // A child is waited for once
    if (wait_for_child(child_pid, &status) != -1 || errno != ECHILD) {
        THROW_ERROR("the child can be waited for twice");
    }
    close(parent_fd);
    return 0;
}

#define SYS_EXIT_ROUNDS 50

static int vfork_sys_exit_rounds(int how) {
    for (int i = 0; i < ARRAY_SIZE(SYS_EXIT_CODES); i++) {
        if (vfork_sys_exit_round(how, SYS_EXIT_CODES[i].code, SYS_EXIT_CODES[i].expected) < 0) {
            THROW_ERROR("vfork and exit with %d failed", SYS_EXIT_CODES[i].code);
        }
    }
    // Nothing from one round, e.g., a level of the vfork, must be left behind for the next one
    for (int i = 0; i < SYS_EXIT_ROUNDS; i++) {
        if (vfork_sys_exit_round(how, 7, 7) < 0) {
            THROW_ERROR("vfork and exit failed in round %d", i);
        }
    }
    return 0;
}

// The child makes the exit system call. The parent, which is not told about it, goes on.
int test_vfork_sys_exit_codes() {
    return vfork_sys_exit_rounds(LEAVE_SYS_EXIT);
}

// Like test_vfork_sys_exit_codes, with the "syscall" instruction: the LibOS emulates it, and returns to the
// context that it saved when it emulated the vfork of the child (see test/vector_regs for the registers)
int test_vfork_sys_exit_syscall_insn() {
    if (!SYSCALL_INSN_EMULATED) {
        printf("\t\tskipped, the \"syscall\" instruction is only emulated in the HW mode\n");
        return 0;
    }
    if (vfork_sys_exit_rounds(LEAVE_SYS_EXIT_INSN) < 0) {
        return -1;
    }
    return run_nested(LEAVE_SYS_EXIT_INSN, LEAVE_SYS_EXIT_INSN, 1);
}

// The grandchild and the child end with the exit system call, or one of them does: the exit of the
// grandchild returns to the child, which goes on and leaves; the exit of the child returns to the parent.
int test_vfork_sys_exit_nested() {
    static const int how[][2] = {
        { LEAVE_SYS_EXIT, LEAVE_EXIT },
        { LEAVE_EXIT, LEAVE_SYS_EXIT },
        { LEAVE_SYS_EXIT, LEAVE_SYS_EXIT },
        { LEAVE_SYS_EXIT, LEAVE_EXEC },
        { LEAVE_EXEC, LEAVE_SYS_EXIT },
    };
    for (int i = 0; i < ARRAY_SIZE(how); i++) {
        for (int child_waits = 1; child_waits >= 0; child_waits--) {
            if (run_nested(how[i][0], how[i][1], child_waits) < 0) {
                THROW_ERROR("nested vfork failed (grandchild leaves with %d, child with %d, "
                            "child waits: %d)", how[i][0], how[i][1], child_waits);
            }
        }
    }
    return 0;
}

// The other threads of the process are frozen while the child runs, and run again when it has ended with
// the exit system call. The thread that vforked is not ended: it is still the same thread. The child
// ends as the one of a Go program does, whose execve fails.
//
// This test case has different behaviors for Linux and Occlum, see do_vfork.rs
int test_vfork_sys_exit_with_thread() {
    g_failed_line = 0;
    g_stop_ticks = 0;
    pthread_t thread;
    if (pthread_create(&thread, NULL, ticks_thread, NULL) < 0) {
        THROW_ERROR("pthread_create failed");
    }
    while (g_ticks == 0) {
        sleep_ms(2);
    }
    pid_t tid = get_tid();
    pthread_t self = pthread_self();
    g_tls_value = TLS_VALUE;

    char *argv[] = { "no_such_program", NULL };
    pid_t child_pid = vfork();
    if (child_pid == 0) {
        unsigned long ticks = g_ticks;
        sleep_ms(100);
        CHILD_CHECK(g_ticks == ticks);
        CHILD_CHECK(execve("/bin/no_such_program", argv, NULL) == -1 && errno == ENOENT);
        vfork_child_leave(LEAVE_SYS_EXIT, 253);
    }

    if (child_pid <= 0) {
        THROW_ERROR("vfork failed, returned %d", child_pid);
    }
    int failed_line = g_failed_line;
    // The other thread runs again now (give it a second on a busy host)
    unsigned long ticks = g_ticks;
    int is_running = 0;
    for (int i = 0; i < 100 && !is_running; i++) {
        sleep_ms(10);
        is_running = (g_ticks != ticks);
    }
    g_stop_ticks = 1;
    pthread_join(thread, NULL);
    if (failed_line != 0) {
        THROW_ERROR("the other thread ran in the child, or the child failed the check at line %d",
                    failed_line);
    }
    if (!is_running) {
        THROW_ERROR("the other thread did not run after the child had ended");
    }
    if (get_tid() != tid || !pthread_equal(pthread_self(), self) ||
            g_tls_value != TLS_VALUE) {
        THROW_ERROR("the thread that vforked is not the same (tid %d, was %d)", get_tid(), tid);
    }
    int status = 0;
    if (wait_for_child(child_pid, &status) != child_pid || !EXITED_WITH(status, 253)) {
        THROW_ERROR("wait child status error (status 0x%x)", status);
    }
    return 0;
}

#define VFORK_THREAD_RESULT ((void *)0x1234)
static volatile int g_thread_done;
static volatile int g_thread_failed;

static void *vfork_thread(void *_arg) {
    g_thread_failed = (vfork_sys_exit_round(LEAVE_SYS_EXIT, 9, 9) < 0);
    // A thread that ends too early would not get to this
    sleep_ms(100);
    g_thread_done = 1;
    // The end of the thread is an ordinary exit system call, which must end the thread, not a child
    return VFORK_THREAD_RESULT;
}

// The vfork is not called by the main thread. The thread must not end with its child. The main thread
// waits for the end of the thread, which the LibOS signals with the clear_child_tid of the thread (see
// set_tid_address(2)): the end of the child must not do that.
int test_vfork_sys_exit_in_thread() {
    g_thread_done = 0;
    g_thread_failed = 0;
    pthread_t thread;
    if (pthread_create(&thread, NULL, vfork_thread, NULL) < 0) {
        THROW_ERROR("pthread_create failed");
    }
    void *result = NULL;
    if (pthread_join(thread, &result) != 0) {
        THROW_ERROR("pthread_join failed");
    }
    if (g_thread_failed) {
        THROW_ERROR("the vfork in the thread failed");
    }
    if (!g_thread_done || result != VFORK_THREAD_RESULT) {
        THROW_ERROR("the thread has been ended by the exit of its child (done %d, result %p)",
                    g_thread_done, result);
    }
    return 0;
}

static volatile int g_tid_word;
static volatile int g_thread_round_failed;

// A thread that vforks, and then ends with the exit system call itself, without libc
static void *sys_exit_thread(void *_arg) {
    g_thread_round_failed = (vfork_sys_exit_round(LEAVE_SYS_EXIT, 6, 6) < 0);
    // The LibOS clears this word when the thread ends. This is for the end of the thread that
    // test_vfork_sys_exit_ordinary_thread can see: pthread_join of libc would not return.
    syscall(SYS_set_tid_address, &g_tid_word);
    syscall(SYS_exit, THREAD_EXIT_CODE);
    return NULL;
}

// The program of test_vfork_sys_exit_ordinary_thread, with ARG_SYS_EXIT_THREAD: the exit system call
// of a thread that is not a vforked child ends that thread only, after the vfork rounds of its own
// thread and another one. Returns 0 if it did.
static int run_sys_exit_thread() {
    g_tid_word = 1;
    g_thread_round_failed = 0;
    if (vfork_sys_exit_round(LEAVE_SYS_EXIT, 5, 5) < 0) {
        return 1;
    }
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    pthread_t thread;
    if (pthread_create(&thread, &attr, sys_exit_thread, NULL) != 0) {
        return 2;
    }
    // Wait for ten seconds at most. The main thread is frozen while the thread vforks.
    for (int i = 0; i < 1000 && g_tid_word != 0; i++) {
        sleep_ms(10);
    }
    if (g_tid_word != 0) {
        return 3;
    }
    if (g_thread_round_failed) {
        return 4;
    }
    // The process goes on, and so does the vfork
    if (vfork_sys_exit_round(LEAVE_SYS_EXIT, 7, 7) < 0) {
        return 5;
    }
    return 0;
}

static void *exit_group_thread(void *_arg) {
    sleep_ms(200);
    syscall(SYS_exit_group, 5);
    return NULL;
}

// The program of test_vfork_sys_exit_ordinary_thread, with ARG_SYS_EXIT_MAIN: the exit system call of
// the main thread ends it only, and the process lives until the other thread ends it with the code 5.
static int run_sys_exit_main() {
    pthread_t thread;
    if (pthread_create(&thread, NULL, exit_group_thread, NULL) != 0) {
        return 2;
    }
    syscall(SYS_exit, 0);
    return 3;
}

static int run_sys_exit_program(char *arg, int expected_code) {
    char *argv[] = { SELF_PATH, arg, NULL };
    pid_t pid;
    if (posix_spawn(&pid, SELF_PATH, NULL, NULL, argv, NULL) != 0) {
        THROW_ERROR("failed to spawn the program %s", arg);
    }
    int status = 0;
    if (wait_for_child(pid, &status) != pid || !EXITED_WITH(status, expected_code)) {
        THROW_ERROR("the program %s did not exit with %d (status 0x%x)", arg, expected_code,
                    status);
    }
    return 0;
}

// The exit system call of a thread that is not a vforked child must still end that thread only, also if
// the thread has run vforked children. The programs end their threads without libc, which leaves the state
// of libc inconsistent, so this test case starts them as programs of their own.
int test_vfork_sys_exit_ordinary_thread() {
    if (run_sys_exit_program(ARG_SYS_EXIT_THREAD, 0) < 0) {
        return -1;
    }
    return run_sys_exit_program(ARG_SYS_EXIT_MAIN, 5);
}

static char g_child_thread_stack[64 * 1024] __attribute__((aligned(16)));
// A thread with a thread pointer that clone requires (CLONE_SETTLS), but no thread-local state
static char g_child_thread_tls[256] __attribute__((aligned(64)));
// The address that clone makes the LibOS clear when the thread ends
static volatile int g_child_thread_tid;
static volatile int g_child_thread_ran;

// The thread that the child creates ends at once. It makes the system call only: it has no state
// that libc can use.
__attribute__((noinline, optimize("no-stack-protector")))
static int child_thread_func(void *_arg) {
    g_child_thread_ran = 1;
    syscall(SYS_exit, THREAD_EXIT_CODE);
    return 0;
}

// The vforked child creates a thread and leaves. The thread is a thread of the process, which the vfork has
// not frozen. If the child leaves at once, the thread has not started when the parent goes on, and the LibOS
// must not try to wake it as it wakes the threads that it has frozen (it has no event yet, and that ended
// the enclave). If the child waits for the thread to end first (child_waits), the exit of the thread, which
// is not the exit of the child, must end that thread only: not the child, and not the thread of the parent
// that the child runs on. In both cases the thread runs, and the process lives, with the parent that goes on.
//
// This test case has different behaviors for Linux and Occlum, see do_vfork.rs
static int run_child_thread(int how, int child_waits) {
    g_failed_line = 0;
    g_child_thread_tid = 1;
    g_child_thread_ran = 0;
    pid_t tid = get_tid();

    pid_t child_pid = vfork();
    if (child_pid == 0) {
        int flags = CLONE_VM | CLONE_FS | CLONE_FILES | CLONE_SIGHAND | CLONE_THREAD |
                    CLONE_SYSVSEM | CLONE_SETTLS | CLONE_CHILD_CLEARTID;
        CHILD_CHECK(clone(child_thread_func, g_child_thread_stack + sizeof(g_child_thread_stack),
                          flags, NULL, NULL, g_child_thread_tls, (void *)&g_child_thread_tid) > 0);
        if (child_waits) {
            for (long i = 0; i < 50000000 && g_child_thread_tid != 0; i++) {
                sched_yield();
            }
            CHILD_CHECK(g_child_thread_tid == 0 && g_child_thread_ran == 1);
        }
        vfork_child_leave(how, 7);
    }

    if (child_pid <= 0) {
        THROW_ERROR("vfork failed, returned %d", child_pid);
    }
    if (g_failed_line != 0) {
        THROW_ERROR("the child failed the check at line %d", g_failed_line);
    }
    if (get_tid() != tid) {
        THROW_ERROR("the thread that vforked is not the same (tid %d, was %d)", get_tid(), tid);
    }
    // The parent goes on at once, with the exit status of the child, though a thread of the child may still
    // run (it is not how Linux does it, see the limitation 5 in do_vfork.rs)
    int status = 0;
    if (wait_for_child(child_pid, &status) != child_pid || !EXITED_WITH(status, 7)) {
        THROW_ERROR("wait child status error (status 0x%x)", status);
    }
    // The thread of the child ends, too (wait for ten seconds at most)
    for (int i = 0; i < 1000 && g_child_thread_tid != 0; i++) {
        sleep_ms(10);
    }
    if (g_child_thread_tid != 0 || g_child_thread_ran != 1) {
        THROW_ERROR("the thread of the child did not run and end (tid word %d, ran %d)",
                    g_child_thread_tid, g_child_thread_ran);
    }
    return 0;
}

static int run_child_thread_rounds(int child_waits) {
    for (int i = 0; i < 20; i++) {
        if (run_child_thread(i & 1 ? LEAVE_EXIT : LEAVE_SYS_EXIT, child_waits) < 0) {
            THROW_ERROR("vfork and thread of the child failed in round %d", i);
        }
    }
    return 0;
}

// The child leaves at once, with the thread that it created not started
int test_vfork_child_creates_thread() {
    return run_child_thread_rounds(0);
}

// The thread of the child ends with the exit system call while the child runs
int test_vfork_child_thread_exits() {
    return run_child_thread_rounds(1);
}

static volatile int g_thread_tid;
static volatile int g_thread_vforked;
static volatile int g_signals;
static volatile int g_signal_tid;

static void count_signal(int sig) {
    g_signal_tid = get_tid();
    g_signals++;
}

static void *signal_thread(void *_arg) {
    g_thread_tid = get_tid();
    if (vfork_sys_exit_round(LEAVE_SYS_EXIT, 9, 9) < 0) {
        return (void *)1;
    }
    g_thread_vforked = 1;
    // Wait for the signal (ten seconds at most)
    for (int i = 0; i < 1000 && g_signals == 0; i++) {
        sleep_ms(10);
    }
    return (void *)(long)(g_signals == 1 && g_signal_tid == g_thread_tid ? 0 : 2);
}

// The thread that has vforked a child, which ended with the exit system call, must still be a thread of
// the process: another thread can send it a signal, which it handles. (The LibOS must not remove the
// thread, as it does when a thread ends.)
int test_vfork_sys_exit_thread_signal() {
    g_thread_tid = 0;
    g_thread_vforked = 0;
    g_signals = 0;
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = count_signal;
    if (sigaction(SIGUSR1, &sa, NULL) < 0) {
        THROW_ERROR("failed to set the signal handler");
    }
    pthread_t thread;
    if (pthread_create(&thread, NULL, signal_thread, NULL) != 0) {
        THROW_ERROR("pthread_create failed");
    }
    // Wait for the vfork of the thread (ten seconds at most)
    for (int i = 0; i < 1000 && !g_thread_vforked; i++) {
        sleep_ms(10);
    }
    long ret = g_thread_vforked ? syscall(SYS_tgkill, getpid(), g_thread_tid, SIGUSR1) : -1;
    void *result = NULL;
    int join_ret = pthread_join(thread, &result);
    sa.sa_handler = SIG_DFL;
    sigaction(SIGUSR1, &sa, NULL);
    if (ret != 0) {
        THROW_ERROR("the thread that vforked can not be sent a signal (thread %d, vforked %d, "
                    "tgkill returned %ld)", g_thread_tid, g_thread_vforked, ret);
    }
    if (join_ret != 0 || result != NULL) {
        THROW_ERROR("the thread did not handle the signal (join %d, result %p, signals %d, "
                    "handled by thread %d)", join_ret, result, g_signals, g_signal_tid);
    }
    return 0;
}

#ifdef __GLIBC__
static pthread_mutex_t g_robust_mutex;
static volatile int g_robust_result;

// The lock word of a robust mutex gets this bit when the thread that owns it has ended (see futex(2))
#define FUTEX_OWNER_DIED_BIT    0x40000000

static void *robust_mutex_thread(void *_arg) {
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST);
    pthread_mutex_init(&g_robust_mutex, &attr);
    if (pthread_mutex_lock(&g_robust_mutex) != 0) {
        g_robust_result = -1;
        return NULL;
    }
    if (vfork_sys_exit_round(LEAVE_SYS_EXIT, 9, 9) < 0) {
        g_robust_result = -2;
        return NULL;
    }
    // The thread has not ended, so its mutex is not marked as one whose owner has died
    g_robust_result = (g_robust_mutex.__data.__lock & FUTEX_OWNER_DIED_BIT) ? -3 : 1;
    pthread_mutex_unlock(&g_robust_mutex);
    return NULL;
}
#endif

// The robust list of a thread is the one of the parent, not of its vforked child, which must not walk
// it when it ends: the mutexes that the thread has locked are not those of a thread that has ended. (The
// case reads the lock word of a glibc mutex, so it is only run with glibc.)
int test_vfork_sys_exit_robust_mutex() {
#ifdef __GLIBC__
    g_robust_result = 0;
    pthread_t thread;
    if (pthread_create(&thread, NULL, robust_mutex_thread, NULL) != 0) {
        THROW_ERROR("pthread_create failed");
    }
    pthread_join(thread, NULL);
    if (g_robust_result != 1) {
        THROW_ERROR("the robust mutex of the parent was touched (result %d)", g_robust_result);
    }
#else
    printf("\t\tskipped, the robust mutexes are only tested with glibc\n");
#endif
    return 0;
}

// Like test_vfork_nested_stop_child_thread, with the exit system call: the other thread stays frozen until
// the child, which is the outermost one, has returned, also if the grandchild and the child end with it.
//
// This test case has different behaviors for Linux and Occlum, see do_vfork.rs
int test_vfork_nested_sys_exit_stop_child_thread() {
    return run_nested_stop_child_thread(LEAVE_SYS_EXIT);
}

static test_case_t test_cases[] = {
    TEST_CASE(test_vfork_exit_and_wait),
    TEST_CASE(test_multiple_vfork_execve),
    TEST_CASE(test_vfork_isolate_file_table),
    TEST_CASE(test_vfork_stop_child_thread),
    TEST_CASE(test_vfork_multiple_threads),
    TEST_CASE(test_vfork_killed_child),
    TEST_CASE(test_vfork_nested_exit_and_wait),
    TEST_CASE(test_vfork_nested_grandchild_execve),
    TEST_CASE(test_vfork_nested_child_execve),
    TEST_CASE(test_vfork_nested_both_execve),
    TEST_CASE(test_vfork_nested_without_wait),
    TEST_CASE(test_vfork_nested_execve_waits_for_grandchild),
    TEST_CASE(test_vfork_nested_failing_execve),
    TEST_CASE(test_vfork_nested_depth),
    TEST_CASE(test_vfork_nested_stop_child_thread),
    TEST_CASE(test_vfork_nested_repeated),
    TEST_CASE(test_vfork_nested_killed_child),
    TEST_CASE(test_vfork_killed_child_with_thread),
    TEST_CASE(test_vfork_nested_killed_child_with_thread),
    TEST_CASE(test_vfork_nested_depth_limit),
    TEST_CASE(test_vfork_wait_for_process_group),
    TEST_CASE(test_vfork_sys_exit_codes),
    TEST_CASE(test_vfork_sys_exit_syscall_insn),
    TEST_CASE(test_vfork_sys_exit_nested),
    TEST_CASE(test_vfork_sys_exit_with_thread),
    TEST_CASE(test_vfork_sys_exit_in_thread),
    TEST_CASE(test_vfork_sys_exit_ordinary_thread),
    TEST_CASE(test_vfork_child_creates_thread),
    TEST_CASE(test_vfork_child_thread_exits),
    TEST_CASE(test_vfork_sys_exit_thread_signal),
    TEST_CASE(test_vfork_sys_exit_robust_mutex),
    TEST_CASE(test_vfork_nested_sys_exit_stop_child_thread),
};

// Without an argument, all the test cases are run. With an argument, only the test cases with this
// string in their name are run, e.g., "occlum exec /bin/vfork test_vfork_nested_depth". The
// programs that the test cases execute and spawn get an argument that starts with "--".
int main(int argc, char *argv[]) {
    // Show the results of the test cases as they are done: a case that hangs must not hide the others
    setvbuf(stdout, NULL, _IOLBF, 0);
    if (argc >= 3 && strcmp(argv[1], ARG_EXIT) == 0) {
        return atoi(argv[2]);
    } else if (argc >= 3 && strcmp(argv[1], ARG_EXEC_EXIT) == 0) {
        exec_exit(argv[2]);
        return EXEC_FAILED;
    } else if (argc >= 3 && strcmp(argv[1], ARG_WAIT_CHILD) == 0) {
        // Exit with 0 if a child, which this program did not create, exited with the code
        int status = 0;
        pid_t pid = wait_for_child(-1, &status);
        return (pid > 0 && EXITED_WITH(status, atoi(argv[2]))) ? 0 : 1;
    } else if (argc >= 2 && strcmp(argv[1], ARG_PROCESS_GROUP) == 0) {
        return wait_for_process_group() == 0 ? 0 : 1;
    } else if (argc >= 3 && strcmp(argv[1], ARG_VICTIM) == 0) {
        return run_victim(atoi(argv[2]), argc >= 4 && strcmp(argv[3], ARG_THREAD) == 0);
    } else if (argc >= 2 && strcmp(argv[1], ARG_SYS_EXIT_THREAD) == 0) {
        return run_sys_exit_thread();
    } else if (argc >= 2 && strcmp(argv[1], ARG_SYS_EXIT_MAIN) == 0) {
        return run_sys_exit_main();
    }

    test_case_t selected[ARRAY_SIZE(test_cases)];
    int num_selected = 0;
    for (int i = 0; i < ARRAY_SIZE(test_cases); i++) {
        if (argc < 2 || strstr(test_cases[i].name, argv[1]) != NULL) {
            selected[num_selected++] = test_cases[i];
        }
    }
    if (num_selected == 0) {
        printf("no test case matches %s\n", argv[1]);
        return -1;
    }
    return test_suite_run(selected, num_selected);
}
