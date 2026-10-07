#define _GNU_SOURCE
#include <stdio.h>
#include <fcntl.h>
#include <stdlib.h>
#include <poll.h>
#include <signal.h>
#include <spawn.h>
#include <time.h>
#include <sys/wait.h>
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
#define EXEC_FAILED     99

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

// The parent vforks a child, which vforks a grandchild. The grandchild exits with 42 or executes a
// program that does. The child waits for the grandchild, if it is told to, and exits with 7 or
// executes a program that does. The parent waits for the child.
//
// The files of a vforked child are not those of its parent. When the grandchild returns to the
// child, the child must find the files that it had before the vfork (the fd that the grandchild
// opened is closed, the ones that it opened itself and those of the parent are open), and the same
// holds for the parent when the child returns.
static int run_nested(int grandchild_execs, int child_execs, int child_waits) {
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
            if (grandchild_execs) {
                exec_exit("42");
                _exit(EXEC_FAILED);
            }
            _exit(42);
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
        if (child_execs) {
            exec_exit("7");
            _exit(EXEC_FAILED);
        }
        _exit(7);
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
    if (!grandchild_execs &&
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
// particular not when the grandchild returns to the child.
//
// This test case has different behaviors for Linux and Occlum, see do_vfork.rs
int test_vfork_nested_stop_child_thread() {
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
            _exit(0);
        }
        CHILD_CHECK(pid > 0);
        ticks = g_ticks;
        sleep_ms(200);
        CHILD_CHECK(g_ticks == ticks);
        _exit(0);
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
