#include <sys/epoll.h>
#include <sys/ipc.h>
#include <sys/sem.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <sys/file.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <spawn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <errno.h>
#include "test.h"
#include "kernel_heap.h"

// ============================================================================
// Helper definition
// ============================================================================

// Blocking calls that end without being woken up by the object they wait for
// (interrupted by a signal, or timed out) used to leave a waker in the wait
// queue of the object, which is only emptied by the next event of the object.
// So the kernel heap grew with every such call on an object without events
// (about 45 bytes per call). The tests below make the calls and check that the
// kernel heap stays flat.

#define CALLS 100
#define WARMUP_CALLS 5
// The unfixed growth is about 4 kB for 100 calls. The heap is read in kB, so the
// noise of an allocation that is in flight when it is read is 1 kB at most.
#define LIMIT_KB 2

static const char *g_self_path = "/bin/waiter_leak";
static const char *g_file_path = "/tmp/waiter_leak.file";
static const char *g_sock_path = "/tmp/waiter_leak.sock";

// ============================================================================
// Helper functions
// ============================================================================

// The interrupter thread sends SIGUSR2 to the main thread every 2 ms. The signal
// handler is installed without SA_RESTART, so blocking calls fail with EINTR.
static volatile int g_interrupter_run;
static pthread_t g_interrupter;
static pid_t g_main_tid;

static void on_sigusr2(int sig) {
}

static void *interrupter_main(void *arg) {
    struct timespec ts = { 0, 2000000 };
    while (g_interrupter_run) {
        nanosleep(&ts, NULL);
        if (g_interrupter_run) {
            syscall(SYS_tgkill, getpid(), g_main_tid, SIGUSR2);
        }
    }
    return NULL;
}

static int start_interrupter(void) {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_sigusr2;
    if (sigaction(SIGUSR2, &sa, NULL) < 0) {
        THROW_ERROR("failed to install the signal handler");
    }
    g_main_tid = syscall(SYS_gettid);
    g_interrupter_run = 1;
    if (pthread_create(&g_interrupter, NULL, interrupter_main, NULL) != 0) {
        THROW_ERROR("failed to create the interrupter thread");
    }
    return 0;
}

static void stop_interrupter(void) {
    g_interrupter_run = 0;
    pthread_join(g_interrupter, NULL);
}

// A call makes a blocking call that is expected to end early. It returns 0 if it
// did, and -1 if it did something else (it prints the reason).
typedef int (*call_func_t)(void *);

static int check_calls_leak_nothing(call_func_t call, void *arg, int interrupt) {
    if (interrupt && start_interrupter() < 0) {
        return -1;
    }
    int ret = -1;
    for (int i = 0; i < WARMUP_CALLS; i++) {
        if (call(arg) < 0) {
            goto out;
        }
    }
    long before = kernel_heap_in_use();
    if (before == -2) {
        printf("\t\tSKIPPED: the kernel heap monitor is not enabled\n");
        ret = 0;
        goto out;
    }
    if (before < 0) {
        printf("\t\tERROR: failed to get the kernel heap in use\n");
        goto out;
    }
    for (int i = 0; i < CALLS; i++) {
        if (call(arg) < 0) {
            goto out;
        }
    }
    long after = kernel_heap_in_use();
    if (after - before > LIMIT_KB) {
        printf("\t\tERROR: the kernel heap grew by %ld kB in %d calls (limit %d kB)\n",
               after - before, CALLS, LIMIT_KB);
        goto out;
    }
    ret = 0;
out:
    if (interrupt) {
        stop_interrupter();
    }
    return ret;
}

static int expect_failure(long ret, int errno1, int errno2, const char *what) {
    if (ret != -1 || (errno != errno1 && errno != errno2)) {
        printf("\t\tERROR: %s returned %ld with errno %d (%s), expected the errno %d or %d\n",
               what, ret, errno, strerror(errno), errno1, errno2);
        return -1;
    }
    return 0;
}

// ============================================================================
// Test cases
// ============================================================================

// read() from a pipe without data
static int pipe_read_call(void *arg) {
    int *fds = arg;
    char c;
    return expect_failure(read(fds[0], &c, 1), EINTR, EINTR, "read");
}

int test_pipe_read_interrupted() {
    int fds[2];
    if (pipe(fds) < 0) {
        THROW_ERROR("failed to create the pipe");
    }
    int ret = check_calls_leak_nothing(pipe_read_call, fds, 1);
    close_files(2, fds[0], fds[1]);
    return ret;
}

// write() to a full pipe
static int pipe_write_call(void *arg) {
    int *fds = arg;
    return expect_failure(write(fds[1], "x", 1), EINTR, EINTR, "write");
}

int test_pipe_write_interrupted() {
    int fds[2];
    if (pipe(fds) < 0) {
        THROW_ERROR("failed to create the pipe");
    }
    // Fill the pipe
    char buf[4096] = { 0 };
    int flags = fcntl(fds[1], F_GETFL);
    fcntl(fds[1], F_SETFL, flags | O_NONBLOCK);
    while (write(fds[1], buf, sizeof(buf)) > 0) {
    }
    if (errno != EAGAIN) {
        THROW_ERROR("failed to fill the pipe");
    }
    fcntl(fds[1], F_SETFL, flags);

    int ret = check_calls_leak_nothing(pipe_write_call, fds, 1);
    close_files(2, fds[0], fds[1]);
    return ret;
}

// accept() on a listening unix socket without connections. Occlum returns EAGAIN
// instead of EINTR.
static int accept_call(void *arg) {
    int *fd = arg;
    int conn = accept(*fd, NULL, NULL);
    if (conn >= 0) {
        close(conn);
    }
    return expect_failure(conn, EINTR, EAGAIN, "accept");
}

int test_unix_accept_interrupted() {
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    struct sockaddr_un addr = { .sun_family = AF_UNIX };
    strncpy(addr.sun_path, g_sock_path, sizeof(addr.sun_path) - 1);
    unlink(g_sock_path);
    if (fd < 0 || bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0 || listen(fd, 8) < 0) {
        THROW_ERROR("failed to set up the listening socket");
    }
    int ret = check_calls_leak_nothing(accept_call, &fd, 1);
    close(fd);
    unlink(g_sock_path);
    return ret;
}

// epoll_wait() on an epoll file without events
static int epoll_wait_call(void *arg) {
    int *epfd = arg;
    struct epoll_event event;
    return expect_failure(epoll_wait(*epfd, &event, 1, -1), EINTR, EINTR, "epoll_wait");
}

int test_epoll_wait_interrupted() {
    int fds[2];
    int epfd = epoll_create1(0);
    struct epoll_event event = { .events = EPOLLIN };
    if (epfd < 0 || pipe(fds) < 0) {
        THROW_ERROR("failed to create the epoll file or the pipe");
    }
    event.data.fd = fds[0];
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, fds[0], &event) < 0) {
        THROW_ERROR("failed to add the pipe to the epoll file");
    }
    int ret = check_calls_leak_nothing(epoll_wait_call, &epfd, 1);
    close_files(3, fds[0], fds[1], epfd);
    return ret;
}

// semop() that has to wait: the interrupted one, and the timed out one
static int semop_call(void *arg) {
    int semid = *(int *)arg;
    struct sembuf op = { .sem_num = 0, .sem_op = -1, .sem_flg = 0 };
    return expect_failure(semop(semid, &op, 1), EINTR, EINTR, "semop");
}

static int semtimedop_call(void *arg) {
    int semid = *(int *)arg;
    struct sembuf op = { .sem_num = 0, .sem_op = -1, .sem_flg = 0 };
    struct timespec timeout = { 0, 1000000 };
    // Occlum returns ETIMEDOUT instead of EAGAIN
    return expect_failure(syscall(SYS_semtimedop, semid, &op, 1, &timeout), EAGAIN, ETIMEDOUT,
                          "semtimedop");
}

static int test_sem(call_func_t call, int interrupt) {
    int semid = semget(IPC_PRIVATE, 1, IPC_CREAT | 0600);
    if (semid < 0) {
        THROW_ERROR("failed to create the semaphore set");
    }
    int ret = check_calls_leak_nothing(call, &semid, interrupt);
    semctl(semid, 0, IPC_RMID);
    return ret;
}

int test_semop_interrupted() {
    return test_sem(semop_call, 1);
}

int test_semtimedop_timeout() {
    return test_sem(semtimedop_call, 0);
}

// flock() on a file that is locked through another open file description
static int flock_call(void *arg) {
    int *fd = arg;
    return expect_failure(flock(*fd, LOCK_EX), EINTR, EINTR, "flock");
}

int test_flock_interrupted() {
    int holder = open(g_file_path, O_RDWR | O_CREAT, 0600);
    int waiter = open(g_file_path, O_RDWR);
    if (holder < 0 || waiter < 0 || flock(holder, LOCK_EX) < 0) {
        THROW_ERROR("failed to lock the file");
    }
    int ret = check_calls_leak_nothing(flock_call, &waiter, 1);
    close_files(2, holder, waiter);
    unlink(g_file_path);
    return ret;
}

// fcntl(F_SETLKW) on a file range that is locked by another process
static int setlkw_call(void *arg) {
    int *fd = arg;
    struct flock fl = { .l_type = F_WRLCK, .l_whence = SEEK_SET, .l_start = 0, .l_len = 100 };
    return expect_failure(fcntl(*fd, F_SETLKW, &fl), EINTR, EINTR, "fcntl(F_SETLKW)");
}

int test_fcntl_setlkw_interrupted() {
    int fd = open(g_file_path, O_RDWR | O_CREAT, 0600);
    if (fd < 0) {
        THROW_ERROR("failed to create the file");
    }
    // The child locks the range and waits until it is killed
    pid_t child;
    char *argv[] = { (char *)g_self_path, "--hold-lock", (char *)g_file_path, NULL };
    if (posix_spawn(&child, g_self_path, NULL, NULL, argv, NULL) != 0) {
        THROW_ERROR("failed to spawn the child");
    }
    int ret = -1;
    int locked = 0;
    for (int i = 0; i < 500 && !locked; i++) {
        struct flock fl = { .l_type = F_WRLCK, .l_whence = SEEK_SET, .l_start = 0, .l_len = 100 };
        locked = fcntl(fd, F_GETLK, &fl) == 0 && fl.l_type != F_UNLCK;
        if (!locked) {
            usleep(10000);
        }
    }
    if (!locked) {
        printf("\t\tERROR: the child did not lock the file\n");
    } else {
        ret = check_calls_leak_nothing(setlkw_call, &fd, 1);
    }
    kill(child, SIGKILL);
    waitpid(child, NULL, 0);
    close(fd);
    unlink(g_file_path);
    return ret;
}

// ============================================================================
// Test suite main
// ============================================================================

static test_case_t test_cases[] = {
    TEST_CASE(test_pipe_read_interrupted),
    TEST_CASE(test_pipe_write_interrupted),
    TEST_CASE(test_unix_accept_interrupted),
    TEST_CASE(test_epoll_wait_interrupted),
    TEST_CASE(test_semop_interrupted),
    TEST_CASE(test_semtimedop_timeout),
    TEST_CASE(test_flock_interrupted),
    TEST_CASE(test_fcntl_setlkw_interrupted),
};

int main(int argc, const char *argv[]) {
    if (argc > 2 && strcmp(argv[1], "--hold-lock") == 0) {
        // The child process: lock a range of the file, and wait to be killed
        int fd = open(argv[2], O_RDWR);
        struct flock fl = { .l_type = F_WRLCK, .l_whence = SEEK_SET, .l_start = 0, .l_len = 100 };
        if (fd < 0 || fcntl(fd, F_SETLK, &fl) < 0) {
            return 1;
        }
        sleep(60);
        return 0;
    }
    return test_suite_run(test_cases, ARRAY_SIZE(test_cases));
}
