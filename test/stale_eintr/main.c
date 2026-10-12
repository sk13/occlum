// Blocking calls and the interrupts that the host sends to the threads that are blocked in them
//
// The LibOS delivers a signal to a thread that is blocked in a call of the host by asking the
// host to interrupt it, which fails the call with EINTR. The host interrupts every 25 ms the
// threads that have a signal to deliver, and the threads that have to exit or to stop (their
// process exits, or another thread of their process calls vfork). Linux fails a blocking call
// with EINTR only if a signal handler has run during it, so an interrupt of the host must not
// end a call in two cases:
// - The signal is never delivered, e.g., it is ignored. It stays in the queue of a thread that
//   is blocked, so the thread would be interrupted at every tick for as long as it is blocked.
// - The interrupt arrives some time after the LibOS has found the reason for it, and the reason
//   may be gone by then: the thread may have run the handler of the signal, and may be blocked
//   in its next call, which the interrupt ends. Or another thread has taken the signal.
//
// usage: stale_eintr [-v] [test case name]...
// -v prints the result of each call, and the names select the test cases to run.
//
// The test cases below check that a blocking call
// 1. does not fail with EINTR unless a handler has run during it, in a stream of signals that
//    a thread handles, with a lot of interrupts that are late, and that the stream interrupts
//    calls, so that the test cannot pass because there are no interrupts;
// 2. is not interrupted by a signal that is ignored, and times out when its timeout is up, not
//    earlier and not later, also if the signal is sent to the process, or by the exit of a
//    child process;
// 3. is not interrupted by a signal that the thread blocks;
// 4. still fails with EINTR if a handler runs;
// 5. ends if its process exits, if its process is killed, and if another thread of its process
//    calls vfork, and if it is called by the child of vfork;
// 6. ends if a signal that is sent to the process is for it: the main thread has exited, or blocks
//    the signal; and if pselect lets in a signal that the signal mask of the thread blocks;
// 7. times out at its time if the interrupts that do not end it come at any time of the call: the
//    signal is taken by another thread.
// and that a sleep tells the time that is left only if it fails with EINTR.
//
// The blocking calls are in the table below. A call has a timeout, or a function that wakes it
// up. The calls with a function are woken up by a thread every 100 us in the stream, which makes
// them short: the interrupt of the host is the more likely to be late for a call that has just
// begun.

#define _GNU_SOURCE
#include <sys/file.h>
#include <sys/ipc.h>
#include <sys/sem.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <spawn.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <errno.h>
#include "test.h"

extern char **environ;

// ============================================================================
// Helper functions
// ============================================================================

static const char *g_self_path = "/bin/stale_eintr";
static int g_is_verbose;

static long now_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000000L + ts.tv_nsec / 1000;
}

static struct timespec us_to_timespec(long us) {
    struct timespec ts = { us / 1000000, (us % 1000000) * 1000 };
    return ts;
}

static void sleep_us(long us) {
    struct timespec ts = us_to_timespec(us);
    while (nanosleep(&ts, &ts) < 0 && errno == EINTR)
        ;
}

// The time that is us after now, in the given clock
static struct timespec timespec_after(clockid_t clock, long us) {
    struct timespec ts;
    clock_gettime(clock, &ts);
    long nsec = ts.tv_nsec + (us % 1000000) * 1000;
    ts.tv_sec += us / 1000000 + nsec / 1000000000;
    ts.tv_nsec = nsec % 1000000000;
    return ts;
}

static int set_signal_action(int signum, void (*handler)(int)) {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = handler;
    sigemptyset(&sa.sa_mask);
    return sigaction(signum, &sa, NULL);
}

static pid_t gettid_(void) {
    return syscall(SYS_gettid);
}

// The helper threads do not take signals: a signal that is sent to the process is
// delivered, or discarded if it is ignored, by the first thread that does not block it
static void block_all_signals(void) {
    sigset_t all;
    sigfillset(&all);
    pthread_sigmask(SIG_BLOCK, &all, NULL);
}

static void close_fd(int *fd) {
    if (*fd >= 0) {
        close(*fd);
        *fd = -1;
    }
}

// The threads have small stacks. glibc gives a thread a stack of 8 MB, which takes a long time to
// get in the LibOS when many threads are created at the same time.
#define THREAD_STACK_SIZE   (512 * 1024)

static int create_thread(pthread_t *thread, void *(*func)(void *), void *arg) {
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setstacksize(&attr, THREAD_STACK_SIZE);
    int ret = pthread_create(thread, &attr, func, arg);
    pthread_attr_destroy(&attr);
    return ret;
}

// ============================================================================
// The blocking calls
// ============================================================================

// The code of the LibOS that a call waits in. The calls of a site are different system calls
// that wait in the same code, so a call of each site tells how the code works.
enum {
    SITE_WAITER,        // the waiters of the LibOS: pipes, sockets of the LibOS, semaphores, locks
    SITE_SIGNAL,        // sigtimedwait and sigsuspend, which wait for a signal
    SITE_NANOSLEEP,     // the sleep of the host
    NUM_SITES
};

enum {
    // The cases with child processes run one call of each kind of wait
    F_CHILD = 1,
    // The call returns if any signal arrives, also one that is ignored, or one that the thread
    // blocks (sigsuspend, which replaces the signal mask): it is not in the cases with such signals
    F_ANY_SIGNAL = 2,
    // The host keeps the timeout of the call, and would start it again if the call were made again
    // after an interrupt that is not for the thread. So the LibOS does not make the call again, and
    // the call fails with EINTR, as a call with a timeout fails on Linux when a signal arrives.
    // The calls are not in the stream, and they need not end by their timeout in case 7.
    F_HOST_TIMEOUT = 4,
};

// What a call needs: the files that it waits for and the numbers that the wake function and
// the call share
typedef struct {
    int pipe_fds[2];
    int fd;
    int epoll_fd;
    int socks[2];
    int listener;
    int sem;
    int lock_fds[2];
    int futex_word;
    key_t sem_key;
    pid_t tid;
    struct sockaddr_storage listener_addr;
    socklen_t listener_addr_len;
    char socket_path[64];
    char lock_path[64];
    // The number of the events that the wake function has produced and that the call has
    // consumed. The wake function stops at some events that the call has not consumed yet, as
    // the events of some calls, e.g., a pending connection of a unix socket, take memory.
    volatile int num_produced;
    volatile int num_consumed;
} ctx_t;

#define MAX_PENDING_EVENTS  4
#define SEM_KEY_BASE        0x5e11e000

// The semaphore set of a process is made with a key that the parent knows, so that the parent
// can remove the set if the process is killed
static key_t sem_key_of(pid_t pid) {
    return SEM_KEY_BASE + pid;
}

static void remove_semaphore_set(key_t key) {
    int sem = semget(key, 0, 0);
    if (sem >= 0) {
        semctl(sem, 0, IPC_RMID);
    }
}

static void ctx_init(ctx_t *c) {
    memset(c, 0, sizeof(*c));
    c->pipe_fds[0] = c->pipe_fds[1] = c->fd = c->epoll_fd = -1;
    c->socks[0] = c->socks[1] = c->listener = c->lock_fds[0] = c->lock_fds[1] = -1;
    c->sem = -1;
    c->tid = gettid_();
    c->sem_key = sem_key_of(getpid());
    snprintf(c->socket_path, sizeof(c->socket_path), "/tmp/stale_eintr.%d.sock", c->tid);
    snprintf(c->lock_path, sizeof(c->lock_path), "/tmp/stale_eintr.%d.lock", c->tid);
}

typedef struct {
    const char *name;
    // Creates what the call waits for. The teardown function removes what has been created
    // if the setup function has failed too.
    int (*setup)(ctx_t *);
    void (*teardown)(ctx_t *);
    // Blocks for at most timeout_us if the call has a timeout (wake is NULL); otherwise,
    // until the wake function makes it return. It returns like a system call.
    long (*call)(ctx_t *, long timeout_us);
    void (*wake)(ctx_t *);
    int site;
    int flags;
} blocking_call_t;

static int can_produce(ctx_t *c) {
    return c->num_produced - c->num_consumed < MAX_PENDING_EVENTS;
}

static int setup_nothing(ctx_t *c) {
    return 0;
}

static void teardown_nothing(ctx_t *c) {
}

// ---- sleeps ----

static long call_nanosleep(ctx_t *c, long timeout_us) {
    struct timespec ts = us_to_timespec(timeout_us);
    return syscall(SYS_nanosleep, &ts, NULL);
}

static long call_clock_nanosleep(ctx_t *c, long timeout_us) {
    struct timespec ts = us_to_timespec(timeout_us);
    return syscall(SYS_clock_nanosleep, CLOCK_MONOTONIC, 0, &ts, NULL);
}

static long call_clock_nanosleep_absolute(ctx_t *c, long timeout_us) {
    struct timespec ts = timespec_after(CLOCK_REALTIME, timeout_us);
    return syscall(SYS_clock_nanosleep, CLOCK_REALTIME, TIMER_ABSTIME, &ts, NULL);
}

// ---- poll, ppoll, select, pselect6 and epoll_wait ----

static int setup_pipe(ctx_t *c) {
    return pipe(c->pipe_fds);
}

static void teardown_pipe(ctx_t *c) {
    close_fd(&c->pipe_fds[0]);
    close_fd(&c->pipe_fds[1]);
}

static long call_pipe_read(ctx_t *c, long timeout_us) {
    char ch;
    long ret = read(c->pipe_fds[0], &ch, 1);
    if (ret > 0) {
        c->num_consumed++;
    }
    return ret;
}

static void wake_pipe_write(ctx_t *c) {
    if (can_produce(c) && write(c->pipe_fds[1], "x", 1) == 1) {
        c->num_produced++;
    }
}

// ---- futex ----

// ---- eventfd and timerfd ----

// ---- unix and TCP sockets ----

static void reset_and_close(int fd) {
    struct linger linger = { 1, 0 };
    setsockopt(fd, SOL_SOCKET, SO_LINGER, &linger, sizeof(linger));
    close(fd);
}

static int setup_listener(ctx_t *c, int domain) {
    c->listener = socket(domain, SOCK_STREAM, 0);
    if (c->listener < 0) {
        return -1;
    }
    if (domain == AF_INET) {
        struct sockaddr_in addr = { .sin_family = AF_INET, .sin_port = 0 };
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        if (bind(c->listener, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
            return -1;
        }
        c->listener_addr_len = sizeof(addr);
        if (getsockname(c->listener, (struct sockaddr *)&c->listener_addr,
                        &c->listener_addr_len) < 0) {
            return -1;
        }
    } else {
        struct sockaddr_un addr = { .sun_family = AF_UNIX };
        strcpy(addr.sun_path, c->socket_path);
        unlink(c->socket_path);
        if (bind(c->listener, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
            return -1;
        }
        memcpy(&c->listener_addr, &addr, sizeof(addr));
        c->listener_addr_len = sizeof(addr);
    }
    // A pending unix connection holds a lot of memory of the LibOS, so keep the backlog small
    return listen(c->listener, 4);
}

static void teardown_listener(ctx_t *c) {
    close_fd(&c->listener);
    unlink(c->socket_path);
}

static int setup_unix_listener(ctx_t *c) {
    return setup_listener(c, AF_UNIX);
}

static int setup_tcp_listener(ctx_t *c) {
    return setup_listener(c, AF_INET);
}

static long call_accept(ctx_t *c, long timeout_us) {
    int fd = accept(c->listener, NULL, NULL);
    if (fd < 0) {
        return -1;
    }
    reset_and_close(fd);
    c->num_consumed++;
    return 0;
}

// The socket gives up after the timeout, which the host keeps
static void set_receive_timeout(int fd, long timeout_us) {
    struct timeval tv = { timeout_us / 1000000, timeout_us % 1000000 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
}

static void wake_connect(ctx_t *c) {
    if (!can_produce(c)) {
        return;
    }
    int fd = socket(c->listener_addr.ss_family, SOCK_STREAM, 0);
    if (fd < 0) {
        return;
    }
    if (connect(fd, (struct sockaddr *)&c->listener_addr, c->listener_addr_len) == 0) {
        c->num_produced++;
    }
    reset_and_close(fd);
}

static int setup_unix_socket_pair(ctx_t *c) {
    return socketpair(AF_UNIX, SOCK_STREAM, 0, c->socks);
}

static int setup_tcp_socket_pair(ctx_t *c) {
    if (setup_tcp_listener(c) < 0) {
        return -1;
    }
    c->socks[0] = socket(AF_INET, SOCK_STREAM, 0);
    if (c->socks[0] < 0 ||
            connect(c->socks[0], (struct sockaddr *)&c->listener_addr,
                    c->listener_addr_len) < 0) {
        return -1;
    }
    c->socks[1] = accept(c->listener, NULL, NULL);
    return c->socks[1] < 0 ? -1 : 0;
}

static void teardown_socket_pair(ctx_t *c) {
    close_fd(&c->socks[0]);
    close_fd(&c->socks[1]);
}

static void teardown_tcp_socket_pair(ctx_t *c) {
    teardown_socket_pair(c);
    teardown_listener(c);
}

static long call_recv(ctx_t *c, long timeout_us) {
    char ch;
    long ret = recv(c->socks[0], &ch, 1, 0);
    if (ret > 0) {
        c->num_consumed++;
    }
    return ret;
}

static long call_recv_with_timeout(ctx_t *c, long timeout_us) {
    set_receive_timeout(c->socks[0], timeout_us);
    return call_recv(c, timeout_us);
}

static void wake_send(ctx_t *c) {
    if (can_produce(c) && send(c->socks[1], "x", 1, MSG_DONTWAIT) == 1) {
        c->num_produced++;
    }
}

// ---- a full pipe ----

static int setup_full_pipe(ctx_t *c) {
    char buf[4096];
    memset(buf, 'x', sizeof(buf));
    if (pipe(c->pipe_fds) < 0) {
        return -1;
    }
    fcntl(c->pipe_fds[1], F_SETFL, O_NONBLOCK);
    while (write(c->pipe_fds[1], buf, sizeof(buf)) > 0)
        ;
    while (write(c->pipe_fds[1], buf, 1) > 0)
        ;
    fcntl(c->pipe_fds[1], F_SETFL, 0);
    // The wake function frees space with a read that does not block
    fcntl(c->pipe_fds[0], F_SETFL, O_NONBLOCK);
    return 0;
}

// A write of a page blocks until the wake function has read a page (a pipe of Linux frees its
// space in pages)
static long call_pipe_write(ctx_t *c, long timeout_us) {
    char buf[4096] = { 0 };
    return write(c->pipe_fds[1], buf, sizeof(buf));
}

static void wake_pipe_read(ctx_t *c) {
    char buf[4096];
    (void) !read(c->pipe_fds[0], buf, sizeof(buf));
}

// ---- System V semaphores ----

static int setup_semaphore(ctx_t *c) {
    union {
        int val;
    } arg = { 0 };
    c->sem = semget(c->sem_key, 1, IPC_CREAT | 0600);
    if (c->sem < 0) {
        return -1;
    }
    return semctl(c->sem, 0, SETVAL, arg);
}

static void teardown_semaphore(ctx_t *c) {
    remove_semaphore_set(c->sem_key);
    c->sem = -1;
}

static long call_semop(ctx_t *c, long timeout_us) {
    struct sembuf sop = { 0, -1, 0 };
    long ret = semop(c->sem, &sop, 1);
    if (ret == 0) {
        c->num_consumed++;
    }
    return ret;
}

static long call_semtimedop(ctx_t *c, long timeout_us) {
    struct timespec ts = us_to_timespec(timeout_us);
    struct sembuf sop = { 0, -1, 0 };
    return syscall(SYS_semtimedop, c->sem, &sop, 1, &ts);
}

static void wake_semaphore(ctx_t *c) {
    struct sembuf sop = { 0, 1, 0 };
    if (can_produce(c) && semop(c->sem, &sop, 1) == 0) {
        c->num_produced++;
    }
}

// ---- file locks ----

static int setup_flock(ctx_t *c) {
    c->lock_fds[0] = open(c->lock_path, O_RDWR | O_CREAT, 0600);
    c->lock_fds[1] = open(c->lock_path, O_RDWR);
    if (c->lock_fds[0] < 0 || c->lock_fds[1] < 0) {
        return -1;
    }
    return flock(c->lock_fds[0], LOCK_EX);
}

static void teardown_flock(ctx_t *c) {
    close_fd(&c->lock_fds[0]);
    close_fd(&c->lock_fds[1]);
    unlink(c->lock_path);
}

static long call_flock(ctx_t *c, long timeout_us) {
    long ret = flock(c->lock_fds[1], LOCK_EX);
    if (ret == 0) {
        flock(c->lock_fds[1], LOCK_UN);
        c->num_consumed++;
    }
    return ret;
}

static void wake_flock(ctx_t *c) {
    // The owner of the lock lets it go until the call has got it (at most 1 ms)
    int num_consumed = c->num_consumed;
    if (flock(c->lock_fds[0], LOCK_UN) == 0) {
        for (int i = 0; i < 20 && c->num_consumed == num_consumed; i++) {
            usleep(50);
        }
        flock(c->lock_fds[0], LOCK_EX);
    }
}

// ---- signals ----

// The signal that sigtimedwait waits for: it is blocked, and ignored by default. The calls use
// the system call, as the sigtimedwait of musl waits again after an interrupt.
#define WAITED_SIGNAL   SIGWINCH

static int setup_sigtimedwait(ctx_t *c) {
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, WAITED_SIGNAL);
    return pthread_sigmask(SIG_BLOCK, &set, NULL) == 0 ? 0 : -1;
}

static void teardown_sigtimedwait(ctx_t *c) {
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, WAITED_SIGNAL);
    pthread_sigmask(SIG_UNBLOCK, &set, NULL);
}

static long call_sigtimedwait(ctx_t *c, long timeout_us) {
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, WAITED_SIGNAL);
    struct timespec ts = us_to_timespec(timeout_us);
    return syscall(SYS_rt_sigtimedwait, &set, NULL, &ts, 8);
}

static long call_sigtimedwait_forever(ctx_t *c, long timeout_us) {
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, WAITED_SIGNAL);
    return syscall(SYS_rt_sigtimedwait, &set, NULL, NULL, 8);
}

static void wake_signal(ctx_t *c) {
    syscall(SYS_tgkill, getpid(), c->tid, WAITED_SIGNAL);
}

// It returns with EINTR when a handler has run. The stream of signals of the first test case,
// and the signal of the third, end it.
static long call_sigsuspend(ctx_t *c, long timeout_us) {
    sigset_t empty;
    sigemptyset(&empty);
    return syscall(SYS_rt_sigsuspend, &empty, 8);
}

static const blocking_call_t g_blocking_calls[] = {
    { "pipe_read", setup_pipe, teardown_pipe, call_pipe_read, wake_pipe_write, SITE_WAITER, F_CHILD },
    { "pipe_write", setup_full_pipe, teardown_pipe, call_pipe_write, wake_pipe_read, SITE_WAITER, 0 },
    { "unix_accept", setup_unix_listener, teardown_listener, call_accept, wake_connect, SITE_WAITER, 0 },
    { "unix_recv", setup_unix_socket_pair, teardown_socket_pair, call_recv, wake_send, SITE_WAITER, 0 },
    { "semop", setup_semaphore, teardown_semaphore, call_semop, wake_semaphore, SITE_WAITER, 0 },
    { "semtimedop", setup_semaphore, teardown_semaphore, call_semtimedop, NULL, SITE_WAITER, F_CHILD },
    { "flock", setup_flock, teardown_flock, call_flock, wake_flock, SITE_WAITER, 0 },
    { "sigtimedwait", setup_sigtimedwait, teardown_sigtimedwait, call_sigtimedwait, NULL, SITE_SIGNAL, F_CHILD },
    { "sigtimedwait_woken", setup_sigtimedwait, teardown_sigtimedwait, call_sigtimedwait_forever, wake_signal, SITE_SIGNAL, 0 },
    { "sigsuspend", setup_nothing, teardown_nothing, call_sigsuspend, NULL, SITE_SIGNAL, F_ANY_SIGNAL },
    { "nanosleep", setup_nothing, teardown_nothing, call_nanosleep, NULL, SITE_NANOSLEEP, F_CHILD },
    { "clock_nanosleep", setup_nothing, teardown_nothing, call_clock_nanosleep, NULL, SITE_NANOSLEEP, 0 },
    { "clock_nanosleep_absolute", setup_nothing, teardown_nothing, call_clock_nanosleep_absolute, NULL, SITE_NANOSLEEP, 0 },
};

#define NUM_BLOCKING_CALLS ARRAY_SIZE(g_blocking_calls)

static const blocking_call_t *find_blocking_call(const char *name) {
    for (size_t i = 0; i < NUM_BLOCKING_CALLS; i++) {
        if (strcmp(g_blocking_calls[i].name, name) == 0) {
            return &g_blocking_calls[i];
        }
    }
    return NULL;
}

// ============================================================================
// The threads that run the calls, and the signals that interrupt them
// ============================================================================

// A thread that runs calls, and the state that the handlers of its signals and the watchdog
// use. A call that fails with EINTR is expected to be interrupted for a handler that has run
// during the call, which the handlers count: in_call is set before the count is read for the
// first time, and cleared after the last time.
typedef struct {
    int site;
    pid_t tid;
    int result;
    pthread_t thread;
    volatile int in_call;
    volatile unsigned long num_handled_in_call;
    volatile unsigned long num_alarms;
    volatile long call_start_us;
    volatile int num_sigusr1;
} worker_t;

#define BARRIER()   __asm__ volatile ("" ::: "memory")

static __thread worker_t *t_worker;
static worker_t g_workers[NUM_SITES];
static int g_num_workers;
static worker_t g_main_worker;

static void count_handled_signal(int signum) {
    worker_t *w = t_worker;
    if (w != NULL && w->in_call) {
        w->num_handled_in_call++;
    }
}

static void count_alarm(int signum) {
    if (t_worker != NULL) {
        t_worker->num_alarms++;
    }
    count_handled_signal(signum);
}

static void count_sigusr1(int signum) {
    if (t_worker != NULL) {
        t_worker->num_sigusr1++;
    }
    count_handled_signal(signum);
}

// A thread that sends signals to a thread, or to the process, after a delay
typedef struct {
    pid_t tid;              // 0 for the process
    int signum;
    long delay_us;
    long interval_us;       // 0 for one signal
    volatile long
    start_us; // the time that the delay is counted from, set when the call begins
    volatile int is_stopped;
    pthread_t thread;
} sender_t;

// Sleeps until delay_us after the time that start_us gets, which the caller sets when the call
// begins: it takes a while to create a thread, and longer in a slow LibOS
static void sleep_until_started(volatile long *start_us, volatile int *is_stopped,
                                long delay_us) {
    while (*start_us == 0 && !*is_stopped) {
        usleep(20);
    }
    long wait_us = *start_us + delay_us - now_us();
    if (wait_us > 0 && !*is_stopped) {
        sleep_us(wait_us);
    }
}

static void *run_sender(void *arg) {
    sender_t *s = arg;
    block_all_signals();
    sleep_until_started(&s->start_us, &s->is_stopped, s->delay_us);
    while (!s->is_stopped) {
        if (s->tid != 0) {
            syscall(SYS_tgkill, getpid(), s->tid, s->signum);
        } else {
            kill(getpid(), s->signum);
        }
        if (s->interval_us == 0) {
            break;
        }
        sleep_us(s->interval_us);
    }
    return NULL;
}

static int start_sender(sender_t *s, pid_t tid, int signum, long delay_us,
                        long interval_us) {
    s->tid = tid;
    s->signum = signum;
    s->delay_us = delay_us;
    s->interval_us = interval_us;
    s->start_us = 0;
    s->is_stopped = 0;
    return create_thread(&s->thread, run_sender, s);
}

static void stop_sender(sender_t *s) {
    s->is_stopped = 1;
    pthread_join(s->thread, NULL);
}

// The watchdog sends a thread SIGALRM if a call has not ended after the limit, so that a call
// that would block for ever does not block the test.
#define WATCHDOG_LIMIT_US   2000000L

static volatile int g_is_watchdog_stopped;
static pthread_t g_watchdog;

static void *run_watchdog(void *arg) {
    block_all_signals();
    while (!g_is_watchdog_stopped) {
        sleep_us(20000);
        for (int i = 0; i <= g_num_workers; i++) {
            worker_t *w = i < g_num_workers ? &g_workers[i] : &g_main_worker;
            long start = w->call_start_us;
            if (w->in_call && start != 0 && now_us() - start > WATCHDOG_LIMIT_US) {
                syscall(SYS_tgkill, getpid(), w->tid, SIGALRM);
                w->call_start_us = now_us();
            }
        }
    }
    return NULL;
}

// Wakes the call up after delay_us, or every 100 us if the delay is 0
typedef struct {
    const blocking_call_t *call;
    ctx_t *ctx;
    long delay_us;
    volatile long start_us;
    volatile int is_stopped;
    volatile int is_paused;
    pthread_t thread;
    int is_started;
} waker_t;

#define WAKE_INTERVAL_US    100

static void *run_waker(void *arg) {
    waker_t *waker = arg;
    block_all_signals();
    if (waker->delay_us > 0) {
        sleep_until_started(&waker->start_us, &waker->is_stopped, waker->delay_us);
        waker->call->wake(waker->ctx);
        return NULL;
    }
    while (!waker->is_stopped) {
        sleep_us(WAKE_INTERVAL_US);
        if (!waker->is_paused) {
            waker->call->wake(waker->ctx);
        }
    }
    return NULL;
}

static void start_waker(waker_t *waker, const blocking_call_t *call, ctx_t *ctx,
                        long delay_us) {
    memset(waker, 0, sizeof(*waker));
    waker->call = call;
    waker->ctx = ctx;
    waker->delay_us = delay_us;
    if (call->wake != NULL) {
        waker->is_started = create_thread(&waker->thread, run_waker, waker) == 0;
    }
}

static void stop_waker(waker_t *waker) {
    waker->is_stopped = 1;
    if (waker->is_started) {
        pthread_join(waker->thread, NULL);
    }
}

typedef struct {
    unsigned long num_calls;
    unsigned long num_timeouts;
    unsigned long num_eintr_with_handler;
    unsigned long num_eintr_without_handler;
    unsigned long num_other_errors;
    int other_errno;
} tally_t;

// Runs the call and tells how it ends: the EINTR is spurious if no handler has run since
// the call has started
static void run_call(worker_t *w, const blocking_call_t *call, ctx_t *ctx,
                     long timeout_us,
                     tally_t *tally) {
    errno = 0;
    w->call_start_us = now_us();
    w->in_call = 1;
    BARRIER();
    unsigned long num_handled_before = w->num_handled_in_call;
    BARRIER();
    long ret = call->call(ctx, timeout_us);
    int err = errno;
    BARRIER();
    unsigned long num_handled_after = w->num_handled_in_call;
    w->in_call = 0;
    BARRIER();
    tally->num_calls++;
    if (ret >= 0) {
        return;
    }
    if (err == EINTR) {
        if (num_handled_after != num_handled_before) {
            tally->num_eintr_with_handler++;
        } else {
            tally->num_eintr_without_handler++;
        }
    } else if (err == ETIMEDOUT || err == EAGAIN) {
        tally->num_timeouts++;
    } else {
        tally->num_other_errors++;
        tally->other_errno = err;
    }
}

// Runs a check for each call that is not excluded (the flags), the calls of a site one
// after the other in a thread of the site, and the sites at the same time. The check of a
// call runs in the thread that signals are sent to. Returns 0 if all checks have passed.
typedef int (*check_t)(worker_t *, const blocking_call_t *);

static check_t g_check;
static int g_skipped_flags;
static volatile int g_num_workers_ready;
static volatile int g_is_start_released;

static int is_checked(const blocking_call_t *call) {
    return (call->flags & g_skipped_flags) == 0;
}

static void *run_worker(void *arg) {
    worker_t *w = arg;
    t_worker = w;
    w->tid = gettid_();
    __sync_fetch_and_add(&g_num_workers_ready, 1);
    while (!g_is_start_released) {
        usleep(200);
    }
    for (size_t i = 0; i < NUM_BLOCKING_CALLS; i++) {
        if (g_blocking_calls[i].site == w->site && is_checked(&g_blocking_calls[i])) {
            w->result |= g_check(w, &g_blocking_calls[i]);
        }
    }
    return NULL;
}

static int num_checked_calls_of_site(int site) {
    int n = 0;
    for (size_t i = 0; i < NUM_BLOCKING_CALLS; i++) {
        n += g_blocking_calls[i].site == site && is_checked(&g_blocking_calls[i]);
    }
    return n;
}

// Starts the threads of the sites, which run the checks at the same time
static int run_checks_on_workers(check_t check, int skipped_flags) {
    g_check = check;
    g_skipped_flags = skipped_flags;
    g_num_workers = 0;
    g_num_workers_ready = 0;
    g_is_start_released = 0;
    int ret = 0;
    for (int site = 0; site < NUM_SITES; site++) {
        if (num_checked_calls_of_site(site) == 0) {
            continue;
        }
        worker_t *w = &g_workers[g_num_workers];
        memset(w, 0, sizeof(*w));
        w->site = site;
        if (create_thread(&w->thread, run_worker, w) != 0) {
            printf("\t\tERROR: failed to create the thread of site %d\n", site);
            ret = -1;
            break;
        }
        g_num_workers++;
    }
    while (g_num_workers_ready < g_num_workers) {
        usleep(200);
    }
    g_is_start_released = 1;
    for (int i = 0; i < g_num_workers; i++) {
        pthread_join(g_workers[i].thread, NULL);
        ret |= g_workers[i].result;
    }
    return ret;
}

// The same, in the main thread
static int run_checks_in_main_thread(check_t check, int skipped_flags,
                                     int only_children_calls) {
    g_check = check;
    g_skipped_flags = skipped_flags;
    int ret = 0;
    for (size_t i = 0; i < NUM_BLOCKING_CALLS; i++) {
        const blocking_call_t *call = &g_blocking_calls[i];
        if (is_checked(call) && (!only_children_calls || (call->flags & F_CHILD))) {
            ret |= check(&g_main_worker, call);
        }
    }
    return ret;
}

// ============================================================================
// Test case: the call does not fail with EINTR if no handler has run
// ============================================================================

// A thread sends the main thread a signal every 200 us, and two threads call
// sched_setaffinity on it. A child process calls vfork in a loop: vfork stops the other threads of
// the process, so the host interrupts them at once, and it interrupts the threads that have a
// signal to deliver, wherever they are. That is thousands of interrupts a second instead of 40,
// and each one may be late.
#define SIGNAL_INTERVAL_US      200
#define NUM_AFFINITY_THREADS    2
#define INTERRUPTER_LIMIT_US    60000000L

static volatile int g_is_stream_stopped;
static int g_stream_signum;
static pid_t g_stream_tid;
static int g_is_stream_process_directed;
static long g_stream_interval_us = SIGNAL_INTERVAL_US;
// If the stream has a gate, it sends signals only until gate_us after gate_start_us, which a thread
// sets before a call, and clears after it
static volatile long g_stream_gate_start_us;
static long g_stream_gate_us;
static pthread_t g_stream_threads[1 + NUM_AFFINITY_THREADS];
static int g_num_stream_threads;
static pid_t g_interrupter_pid;

static void *send_stream_of_signals(void *arg) {
    block_all_signals();
    while (!g_is_stream_stopped) {
        sleep_us(g_stream_interval_us);
        if (g_stream_gate_us != 0 &&
                (g_stream_gate_start_us == 0 ||
                 now_us() - g_stream_gate_start_us >= g_stream_gate_us)) {
            continue;
        }
        if (g_is_stream_process_directed) {
            kill(getpid(), g_stream_signum);
        } else {
            syscall(SYS_tgkill, getpid(), g_stream_tid, g_stream_signum);
        }
    }
    return NULL;
}

static void *set_affinity(void *arg) {
    block_all_signals();
    cpu_set_t cpu_set;
    if (sched_getaffinity(g_stream_tid, sizeof(cpu_set), &cpu_set) < 0) {
        return NULL;
    }
    while (!g_is_stream_stopped) {
        sched_setaffinity(g_stream_tid, sizeof(cpu_set), &cpu_set);
    }
    return NULL;
}

static void *sleep_for_ever(void *arg) {
    for (;;) {
        sleep(1000);
    }
    return NULL;
}

// The process that interrupts the threads of the other processes: it has a thread that is stopped
// for each vfork. It ends after some time, or when its parent has ended.
static int child_interrupter(void) {
    pthread_t thread;
    pid_t parent = getppid();
    if (create_thread(&thread, sleep_for_ever, NULL) != 0) {
        return EXIT_FAILURE;
    }
    long start_us = now_us();
    for (unsigned long i = 0; now_us() - start_us < INTERRUPTER_LIMIT_US; i++) {
        pid_t pid = vfork();
        if (pid == 0) {
            _exit(0);
        }
        if (pid < 0 || waitpid(pid, NULL, 0) != pid) {
            return EXIT_FAILURE;
        }
        if (i % 256 == 0 && getppid() != parent) {
            break;
        }
    }
    return 0;
}

static int start_stream(void) {
    char *argv[] = { (char *)g_self_path, "--interrupter", NULL };
    g_is_stream_stopped = 0;
    g_num_stream_threads = 0;
    if (posix_spawn(&g_interrupter_pid, g_self_path, NULL, NULL, argv, environ) != 0) {
        g_interrupter_pid = 0;
        return -1;
    }
    if (create_thread(&g_stream_threads[0], send_stream_of_signals, NULL) != 0) {
        return -1;
    }
    g_num_stream_threads++;
    for (int i = 0; i < NUM_AFFINITY_THREADS; i++) {
        if (create_thread(&g_stream_threads[g_num_stream_threads], set_affinity,
                          NULL) != 0) {
            return -1;
        }
        g_num_stream_threads++;
    }
    return 0;
}

static void stop_stream(void) {
    g_is_stream_stopped = 1;
    for (int i = 0; i < g_num_stream_threads; i++) {
        pthread_join(g_stream_threads[i], NULL);
    }
    g_num_stream_threads = 0;
    if (g_interrupter_pid > 0) {
        kill(g_interrupter_pid, SIGKILL);
        waitpid(g_interrupter_pid, NULL, 0);
        g_interrupter_pid = 0;
    }
}

// Every LONG_CALL_PERIOD_US, a call is long: it is not woken up, or has a long timeout, so that
// the host interrupts it for the signal that is pending, and a handler runs. The number of these
// interrupts tells that the stream does interrupt calls.
#define STREAM_PER_CALL_US          100000L
// A call goes on until it has been called often enough, but not for longer than this
#define STREAM_MAX_PER_CALL_US      1500000L
#define LONG_CALL_PERIOD_US         40000L
#define SHORT_TIMEOUT_US            100
#define LONG_TIMEOUT_US             100000L
#define MIN_NUM_CALLS               40
#define MIN_NUM_INTERRUPTS          3

static int check_stream_of_signals(worker_t *w, const blocking_call_t *call) {
    ctx_t ctx;
    ctx_init(&ctx);
    // The signals of the stream could interrupt the setup of the call (e.g., the connect of a
    // TCP socket), which is not what is tested, so the thread blocks them meanwhile
    sigset_t stream_signal;
    sigemptyset(&stream_signal);
    sigaddset(&stream_signal, g_stream_signum);
    pthread_sigmask(SIG_BLOCK, &stream_signal, NULL);
    int setup_ret = call->setup(&ctx);
    int setup_errno = errno;
    pthread_sigmask(SIG_UNBLOCK, &stream_signal, NULL);
    if (setup_ret < 0) {
        printf("\t\tERROR: failed to set up %s: %s\n", call->name, strerror(setup_errno));
        call->teardown(&ctx);
        return -1;
    }
    waker_t waker;
    start_waker(&waker, call, &ctx, 0);

    tally_t tally;
    memset(&tally, 0, sizeof(tally));
    unsigned long num_alarms = w->num_alarms;
    long start_us = now_us();
    long last_long_call_us = start_us;
    do {
        int is_long_call = now_us() - last_long_call_us >= LONG_CALL_PERIOD_US;
        waker.is_paused = is_long_call;
        run_call(w, call, &ctx, is_long_call ? LONG_TIMEOUT_US : SHORT_TIMEOUT_US, &tally);
        waker.is_paused = 0;
        if (is_long_call) {
            last_long_call_us = now_us();
        }
    } while (now_us() - start_us < STREAM_PER_CALL_US ||
             (tally.num_calls < MIN_NUM_CALLS && now_us() - start_us < STREAM_MAX_PER_CALL_US));

    // The host interrupts a call that nothing wakes up. A LibOS that is slow spends little time
    // in the calls of the host, so the calls above may have missed the interrupts.
    stop_waker(&waker);
    for (int i = 0; i < MAX_PENDING_EVENTS + MIN_NUM_INTERRUPTS &&
            tally.num_eintr_with_handler < MIN_NUM_INTERRUPTS; i++) {
        run_call(w, call, &ctx, LONG_TIMEOUT_US, &tally);
    }
    call->teardown(&ctx);

    int ret = 0;
    if (tally.num_eintr_without_handler != 0) {
        printf("\t\tERROR: %s failed with EINTR without a signal handler that has run %lu times in %lu calls\n",
               call->name, tally.num_eintr_without_handler, tally.num_calls);
        ret = -1;
    }
    if (tally.num_other_errors != 0) {
        printf("\t\tERROR: %s failed %lu times with errno %d (%s) in %lu calls\n", call->name,
               tally.num_other_errors, tally.other_errno, strerror(tally.other_errno), tally.num_calls);
        ret = -1;
    }
    if (w->num_alarms != num_alarms) {
        printf("\t\tERROR: %s has not ended in %ld ms\n", call->name, WATCHDOG_LIMIT_US / 1000);
        ret = -1;
    }
    if (tally.num_calls < MIN_NUM_CALLS ||
            tally.num_eintr_with_handler < MIN_NUM_INTERRUPTS) {
        printf("\t\tERROR: %s was called only %lu times, and %lu of them were interrupted by a signal\n",
               call->name, tally.num_calls, tally.num_eintr_with_handler);
        ret = -1;
    }
    if (g_is_verbose) {
        printf("\t\t%-24s %6lu calls, %4lu timeouts, %4lu EINTR with a handler, %lu EINTR without\n",
               call->name, tally.num_calls, tally.num_timeouts, tally.num_eintr_with_handler,
               tally.num_eintr_without_handler);
    }
    return ret;
}

int test_stream_of_signals() {
    g_stream_signum = SIGUSR2;
    g_stream_tid = g_main_worker.tid;
    g_is_stream_process_directed = 0;
    g_stream_interval_us = SIGNAL_INTERVAL_US;
    g_stream_gate_us = 0;
    if (set_signal_action(g_stream_signum, count_handled_signal) < 0) {
        THROW_ERROR("failed to set the signal action");
    }
    int ret = -1;
    if (start_stream() == 0) {
        ret = run_checks_in_main_thread(check_stream_of_signals, F_HOST_TIMEOUT, 0);
    } else {
        printf("\t\tERROR: failed to start the stream of signals\n");
    }
    stop_stream();
    return ret;
}

// ============================================================================
// Test case: a signal that is ignored does not interrupt the call
// ============================================================================

// The call has a timeout of 60 ms, or is woken up after 60 ms, and the host would interrupt it
// two or three times in the meantime. It must end after its timeout.
#define IGNORED_TIMEOUT_US      60000L
#define IGNORED_SIGNAL_DELAY_US 2000L
#define IGNORED_SIGNAL_INTERVAL_US 10000L
#define EARLY_TOLERANCE_US      2000L
#define LATE_TOLERANCE_US       30000L

// Checks how the call ends when signals are sent while it blocks: from the thread (tid != 0)
// or to the process
static int check_ignored_signal_sent(worker_t *w, const blocking_call_t *call, int signum,
                                     int is_process_directed) {
    ctx_t ctx;
    ctx_init(&ctx);
    if (call->setup(&ctx) < 0) {
        printf("\t\tERROR: failed to set up %s: %s\n", call->name, strerror(errno));
        call->teardown(&ctx);
        return -1;
    }
    waker_t waker;
    sender_t sender;
    unsigned long num_alarms = w->num_alarms;
    start_waker(&waker, call, &ctx, IGNORED_TIMEOUT_US);
    int has_sender = start_sender(&sender, is_process_directed ? 0 : w->tid, signum,
                                  IGNORED_SIGNAL_DELAY_US, IGNORED_SIGNAL_INTERVAL_US) == 0;
    long start_us = now_us();
    waker.start_us = sender.start_us = start_us;

    tally_t tally;
    memset(&tally, 0, sizeof(tally));
    run_call(w, call, &ctx, IGNORED_TIMEOUT_US, &tally);
    long elapsed_us = now_us() - start_us;
    if (has_sender) {
        stop_sender(&sender);
    }
    stop_waker(&waker);
    call->teardown(&ctx);

    int ret = 0;
    if (tally.num_eintr_without_handler != 0 || tally.num_eintr_with_handler != 0) {
        printf("\t\tERROR: %s failed with EINTR after %ld ms, although the signal is ignored\n",
               call->name, elapsed_us / 1000);
        ret = -1;
    } else if (w->num_alarms != num_alarms) {
        printf("\t\tERROR: %s has not ended in %ld ms\n", call->name, WATCHDOG_LIMIT_US / 1000);
        ret = -1;
    } else if (tally.num_other_errors != 0) {
        printf("\t\tERROR: %s failed with errno %d (%s)\n", call->name, tally.other_errno,
               strerror(tally.other_errno));
        ret = -1;
    } else if (elapsed_us < IGNORED_TIMEOUT_US - EARLY_TOLERANCE_US) {
        printf("\t\tERROR: %s has ended after %ld ms, before its timeout of %ld ms\n", call->name,
               elapsed_us / 1000, IGNORED_TIMEOUT_US / 1000);
        ret = -1;
    } else if (elapsed_us > IGNORED_TIMEOUT_US + LATE_TOLERANCE_US) {
        printf("\t\tERROR: %s has ended after %ld ms, long after its timeout of %ld ms\n",
               call->name,
               elapsed_us / 1000, IGNORED_TIMEOUT_US / 1000);
        ret = -1;
    }
    if (g_is_verbose) {
        printf("\t\t%-24s ended after %ld ms\n", call->name, elapsed_us / 1000);
    }
    return ret;
}

static int check_ignored_signal(worker_t *w, const blocking_call_t *call) {
    return check_ignored_signal_sent(w, call, SIGUSR2, 0);
}

static int check_signal_that_is_ignored_by_default(worker_t *w,
        const blocking_call_t *call) {
    return check_ignored_signal_sent(w, call, SIGCHLD, 1);
}

// The signal is sent to the thread
int test_ignored_signal() {
    if (set_signal_action(SIGUSR2, SIG_IGN) < 0) {
        THROW_ERROR("failed to set the signal action");
    }
    return run_checks_on_workers(check_ignored_signal, F_ANY_SIGNAL);
}

// The signal is sent to the process, and is ignored by default, as SIGCHLD is. The process
// discards it when some thread that does not block it returns from a system call, so the
// helper threads block it, and the main thread, which is blocked in the call, is the only one.
int test_signal_that_is_ignored_by_default() {
    if (set_signal_action(SIGCHLD, SIG_DFL) < 0) {
        THROW_ERROR("failed to set the signal action");
    }
    return run_checks_in_main_thread(check_signal_that_is_ignored_by_default, F_ANY_SIGNAL,
                                     1);
}

// ============================================================================
// Test case: a signal that the thread blocks does not interrupt the call
// ============================================================================

#define BLOCKED_SIGNAL_DELAY_US     20000L

static int check_blocked_signal(worker_t *w, const blocking_call_t *call) {
    ctx_t ctx;
    ctx_init(&ctx);
    sigset_t sigusr1_mask, old_mask, pending;
    sigemptyset(&sigusr1_mask);
    sigaddset(&sigusr1_mask, SIGUSR1);
    pthread_sigmask(SIG_BLOCK, &sigusr1_mask, &old_mask);
    if (call->setup(&ctx) < 0) {
        printf("\t\tERROR: failed to set up %s: %s\n", call->name, strerror(errno));
        call->teardown(&ctx);
        pthread_sigmask(SIG_SETMASK, &old_mask, NULL);
        return -1;
    }
    waker_t waker;
    sender_t sender;
    unsigned long num_alarms = w->num_alarms;
    int num_sigusr1 = w->num_sigusr1;
    start_waker(&waker, call, &ctx, IGNORED_TIMEOUT_US);
    int has_sender = start_sender(&sender, w->tid, SIGUSR1, BLOCKED_SIGNAL_DELAY_US, 0) == 0;
    long start_us = now_us();
    waker.start_us = sender.start_us = start_us;

    tally_t tally;
    memset(&tally, 0, sizeof(tally));
    run_call(w, call, &ctx, IGNORED_TIMEOUT_US, &tally);
    long elapsed_us = now_us() - start_us;
    if (has_sender) {
        stop_sender(&sender);
    }
    stop_waker(&waker);
    call->teardown(&ctx);
    sigpending(&pending);
    int is_pending = sigismember(&pending, SIGUSR1);
    int num_sigusr1_in_call = w->num_sigusr1 - num_sigusr1;
    pthread_sigmask(SIG_SETMASK, &old_mask, NULL);
    int num_sigusr1_after = w->num_sigusr1 - num_sigusr1;

    int ret = 0;
    if (tally.num_eintr_without_handler != 0 || tally.num_eintr_with_handler != 0 ||
            num_sigusr1_in_call != 0) {
        printf("\t\tERROR: %s failed with EINTR after %ld ms, although the signal is blocked\n",
               call->name, elapsed_us / 1000);
        ret = -1;
    } else if (w->num_alarms != num_alarms) {
        printf("\t\tERROR: %s has not ended in %ld ms\n", call->name, WATCHDOG_LIMIT_US / 1000);
        ret = -1;
    } else if (tally.num_other_errors != 0) {
        printf("\t\tERROR: %s failed with errno %d (%s)\n", call->name, tally.other_errno,
               strerror(tally.other_errno));
        ret = -1;
    } else if (elapsed_us < IGNORED_TIMEOUT_US - EARLY_TOLERANCE_US ||
               elapsed_us > IGNORED_TIMEOUT_US + LATE_TOLERANCE_US) {
        printf("\t\tERROR: %s has ended after %ld ms, instead of its timeout of %ld ms\n",
               call->name, elapsed_us / 1000, IGNORED_TIMEOUT_US / 1000);
        ret = -1;
    } else if (!is_pending || num_sigusr1_after != 1) {
        printf("\t\tERROR: the blocked signal is not pending after %s (%d), or has not been "
               "handled once when unblocked (%d)\n", call->name, is_pending, num_sigusr1_after);
        ret = -1;
    }
    if (g_is_verbose) {
        printf("\t\t%-24s ended after %ld ms\n", call->name, elapsed_us / 1000);
    }
    return ret;
}

int test_blocked_signal() {
    if (set_signal_action(SIGUSR1, count_sigusr1) < 0) {
        THROW_ERROR("failed to set the signal action");
    }
    return run_checks_on_workers(check_blocked_signal, F_ANY_SIGNAL);
}

// ============================================================================
// Test case: a handled signal interrupts the call
// ============================================================================

#define HANDLED_TIMEOUT_US      5000000L
#define SIGNAL_DELAY_US         30000L
#define MAX_INTERRUPT_DELAY_US  500000L

static int check_handled_signal(worker_t *w, const blocking_call_t *call) {
    ctx_t ctx;
    ctx_init(&ctx);
    if (call->setup(&ctx) < 0) {
        printf("\t\tERROR: failed to set up %s: %s\n", call->name, strerror(errno));
        call->teardown(&ctx);
        return -1;
    }
    w->num_sigusr1 = 0;
    unsigned long num_alarms = w->num_alarms;
    sender_t sender;
    int has_sender = start_sender(&sender, w->tid, SIGUSR1, SIGNAL_DELAY_US, 0) == 0;
    long start_us = now_us();
    sender.start_us = start_us;

    tally_t tally;
    memset(&tally, 0, sizeof(tally));
    run_call(w, call, &ctx, HANDLED_TIMEOUT_US, &tally);
    long elapsed_us = now_us() - start_us;
    if (has_sender) {
        stop_sender(&sender);
    }
    call->teardown(&ctx);

    int ret = 0;
    if (tally.num_eintr_with_handler == 0 || w->num_sigusr1 != 1 ||
            w->num_alarms != num_alarms) {
        printf("\t\tERROR: %s has not failed with EINTR after the handler of the signal ran "
               "(%lu EINTR, %lu without a handler, %lu timeouts, %lu errors, handler ran %d times) in %ld ms\n",
               call->name, tally.num_eintr_with_handler, tally.num_eintr_without_handler,
               tally.num_timeouts, tally.num_other_errors, w->num_sigusr1, elapsed_us / 1000);
        ret = -1;
    } else if (elapsed_us > SIGNAL_DELAY_US + MAX_INTERRUPT_DELAY_US) {
        printf("\t\tERROR: %s has failed with EINTR only after %ld ms\n", call->name,
               elapsed_us / 1000);
        ret = -1;
    }
    if (g_is_verbose) {
        printf("\t\t%-24s failed with EINTR after %ld ms\n", call->name, elapsed_us / 1000);
    }
    return ret;
}

int test_handled_signal() {
    if (set_signal_action(SIGUSR1, count_sigusr1) < 0) {
        THROW_ERROR("failed to set the signal action");
    }
    return run_checks_on_workers(check_handled_signal, 0);
}

// ============================================================================
// Child processes
// ============================================================================

// The child processes run a blocking call in the modes below. The test process observes how they
// end. They hang if the host interrupt does not end the call, so the test process kills them
// after some time.

#define CHILD_TIMEOUT_MS        5000
#define CHILD_EXIT_STATUS       7
#define CHILD_CALL_TIMEOUT_US   60000000
#define READY_FD                10
// The processes of the test run in a space of 600 MB, and each one needs more than 100 MB, so
// there is no room for many, and the free memory may be in pieces that are too small for a process
// when only a few are running. The children are started two at a time.
#define CHILD_BATCH_SIZE        2

static ctx_t g_child_ctx;

// Blocks in the call, for ever or until CHILD_CALL_TIMEOUT_US
static void block_in_call(const blocking_call_t *call) {
    call->call(&g_child_ctx, CHILD_CALL_TIMEOUT_US);
}

static void *block_in_call_in_thread(void *arg) {
    block_in_call(arg);
    return NULL;
}

static int start_blocked_thread(const blocking_call_t *call) {
    pthread_t thread;
    ctx_init(&g_child_ctx);
    if (call->setup(&g_child_ctx) < 0 ||
            create_thread(&thread, block_in_call_in_thread, (void *)call) != 0) {
        return -1;
    }
    return 0;
}

// The other thread of the process blocks in the call, and the main thread exits the process
static int child_exit_while_blocked(const blocking_call_t *call) {
    if (start_blocked_thread(call) < 0) {
        return EXIT_FAILURE;
    }
    usleep(100 * 1000);
    exit(CHILD_EXIT_STATUS);
}

// The main thread blocks in the call after it has told the test process
static int child_block(const blocking_call_t *call) {
    ctx_init(&g_child_ctx);
    if (call->setup(&g_child_ctx) < 0 || write(READY_FD, "r", 1) != 1) {
        return EXIT_FAILURE;
    }
    block_in_call(call);
    return EXIT_FAILURE;
}

// The other thread of the process blocks in the call, and the main thread calls vfork. The
// thread is forced to stop for the vfork, so the call has to end for vfork to return.
static int child_vfork_while_blocked(const blocking_call_t *call) {
    if (start_blocked_thread(call) < 0) {
        return EXIT_FAILURE;
    }
    usleep(100 * 1000);
    pid_t pid = vfork();
    if (pid == 0) {
        _exit(0);
    }
    int status;
    if (pid < 0 || waitpid(pid, &status, 0) != pid) {
        return EXIT_FAILURE;
    }
    exit(CHILD_EXIT_STATUS);
}

// The child of vfork, which runs on the thread of its parent, blocks in the call after it
// has told the test process its pid
static int child_vfork_child_blocks(const blocking_call_t *call) {
    ctx_init(&g_child_ctx);
    if (call->setup(&g_child_ctx) < 0) {
        return EXIT_FAILURE;
    }
    pid_t pid = vfork();
    if (pid == 0) {
        pid_t child_pid = getpid();
        if (write(READY_FD, &child_pid, sizeof(child_pid)) == sizeof(child_pid)) {
            block_in_call(call);
        }
        _exit(EXIT_FAILURE);
    }
    int status;
    if (pid < 0 || waitpid(pid, &status, 0) != pid || !WIFSIGNALED(status)) {
        return EXIT_FAILURE;
    }
    exit(CHILD_EXIT_STATUS);
}

static void handle_sigusr1_in_thread(int signum) {
    t_worker->num_sigusr1++;
}

// A thread blocks in the call and ends the process with the status that tells that it has
// succeeded if the call fails with EINTR after the handler has run
static void *main_exits_thread(void *arg) {
    const blocking_call_t *call = arg;
    worker_t worker;
    memset(&worker, 0, sizeof(worker));
    t_worker = &worker;
    errno = 0;
    long ret = call->call(&g_child_ctx, CHILD_CALL_TIMEOUT_US);
    exit(ret < 0 && errno == EINTR &&
         worker.num_sigusr1 == 1 ? CHILD_EXIT_STATUS : EXIT_FAILURE);
}

static void *send_sigusr1_to_process(void *arg) {
    block_all_signals();
    usleep(200 * 1000);
    kill(getpid(), SIGUSR1);
    return NULL;
}

// The main thread exits, or blocks the signal, so that a signal that is sent to the process is
// delivered to the thread that blocks in the call. The thread that sends the signal blocks it, as
// the signal would be delivered to it otherwise.
static int child_signal_left_to_thread(const blocking_call_t *call, int does_main_exit) {
    pthread_t blocked_thread, sender;
    sigset_t sigusr1_mask;
    sigemptyset(&sigusr1_mask);
    sigaddset(&sigusr1_mask, SIGUSR1);
    ctx_init(&g_child_ctx);
    if (set_signal_action(SIGUSR1, handle_sigusr1_in_thread) < 0 ||
            call->setup(&g_child_ctx) < 0 ||
            create_thread(&blocked_thread, main_exits_thread, (void *)call) != 0 ||
            pthread_sigmask(SIG_BLOCK, &sigusr1_mask, NULL) != 0 ||
            create_thread(&sender, send_sigusr1_to_process, NULL) != 0) {
        return EXIT_FAILURE;
    }
    if (does_main_exit) {
        pthread_exit(NULL);
    }
    for (;;) {
        sleep(1000);
    }
}

typedef struct {
    const blocking_call_t *call;
    pid_t pid;
    pid_t reported_pid;
    int ready_fd;
    int status;
    int has_ended;
} child_t;

// Returns 0, or the error that has made it fail
static int spawn_child(const char *mode, child_t *child) {
    char *argv[] = { (char *)g_self_path, (char *)mode, (char *)child->call->name, NULL };
    posix_spawn_file_actions_t file_actions;
    int ready_pipe[2] = { -1, -1 };
    if (pipe2(ready_pipe, O_CLOEXEC) < 0) {
        return errno;
    }
    posix_spawn_file_actions_init(&file_actions);
    posix_spawn_file_actions_adddup2(&file_actions, ready_pipe[1], READY_FD);
    errno = 0;
    int ret = posix_spawn(&child->pid, g_self_path, &file_actions, NULL, argv, environ);
    // The posix_spawn of glibc returns -1 and sets errno
    int err = ret == -1 ? errno : ret;
    posix_spawn_file_actions_destroy(&file_actions);
    close(ready_pipe[1]);
    if (ret != 0) {
        close(ready_pipe[0]);
        return err != 0 ? err : EINVAL;
    }
    child->ready_fd = ready_pipe[0];
    return 0;
}

// Waits until the children end. They are killed if they do not end in time. Returns whether all
// have ended in time.
static int wait_for_children(child_t *children, int num_children) {
    int num_ended = 0;
    for (int i = 0; i < CHILD_TIMEOUT_MS / 10 && num_ended < num_children; i++) {
        for (int j = 0; j < num_children; j++) {
            if (!children[j].has_ended && waitpid(children[j].pid, &children[j].status, WNOHANG) ==
                    children[j].pid) {
                children[j].has_ended = 1;
                num_ended++;
            }
        }
        if (num_ended < num_children) {
            usleep(10 * 1000);
        }
    }
    int is_all_ended = num_ended == num_children;
    for (int j = 0; j < num_children; j++) {
        if (!children[j].has_ended) {
            // The child may be unable to die, so do not wait for ever
            kill(children[j].pid, SIGKILL);
            for (int i = 0; i < CHILD_TIMEOUT_MS / 10 &&
                    waitpid(children[j].pid, NULL, WNOHANG) == 0; i++) {
                usleep(10 * 1000);
            }
        }
    }
    return is_all_ended;
}

// Waits until the child tells that it is ready: it has sent a byte, or its pid
static int wait_until_ready(child_t *child, int is_pid_reported) {
    struct pollfd pollfd = { child->ready_fd, POLLIN, 0 };
    size_t len = is_pid_reported ? sizeof(child->reported_pid) : 1;
    char buf[sizeof(child->reported_pid)];
    if (poll(&pollfd, 1, CHILD_TIMEOUT_MS) != 1 ||
            read(child->ready_fd, buf, len) != (ssize_t)len) {
        return -1;
    }
    memcpy(&child->reported_pid, buf, is_pid_reported ? len : 0);
    return 0;
}

enum {
    CHILD_EXITS,            // the child exits while a thread is blocked in the call
    CHILD_IS_KILLED,        // the test process kills the child, which is blocked in the call
    CHILD_FORKS,            // the child calls vfork while a thread is blocked in the call
    CHILD_OF_VFORK_IS_KILLED,   // the test process kills the child of vfork, which blocks
    CHILD_SIGNAL_LEFT_TO_THREAD,
};

// Runs a child process for each call of the cases with child processes, a few at a time, and
// checks how it ends
static int check_children(int kind, const char *mode, int is_status_checked, int signum) {
    const blocking_call_t *calls[NUM_BLOCKING_CALLS];
    size_t num_calls = 0;
    for (size_t i = 0; i < NUM_BLOCKING_CALLS; i++) {
        if (g_blocking_calls[i].flags & F_CHILD) {
            calls[num_calls++] = &g_blocking_calls[i];
        }
    }
    int ret = 0;
    size_t next = 0;
    while (next < num_calls) {
        child_t children[CHILD_BATCH_SIZE];
        int num_children = 0;
        while (next < num_calls && num_children < CHILD_BATCH_SIZE) {
            child_t *child = &children[num_children];
            memset(child, 0, sizeof(*child));
            child->call = calls[next];
            child->ready_fd = -1;
            int err = spawn_child(mode, child);
            for (int i = 0; i < 20 && err == ENOMEM && num_children == 0; i++) {
                // The memory of a process that has ended is not free at once
                usleep(50 * 1000);
                err = spawn_child(mode, child);
            }
            if (err == ENOMEM && num_children > 0) {
                // The call goes in the next batch, when the children have ended
                break;
            }
            next++;
            if (err != 0) {
                printf("\t\tERROR: failed to spawn the child that blocks in %s: %s\n",
                       child->call->name, strerror(err));
                ret = -1;
                continue;
            }
            num_children++;
        }
        int is_ready[CHILD_BATCH_SIZE];
        for (int i = 0; i < num_children; i++) {
            is_ready[i] = 1;
            if (kind == CHILD_IS_KILLED || kind == CHILD_OF_VFORK_IS_KILLED) {
                // The child is in the call a little after it has told it is ready
                is_ready[i] = wait_until_ready(&children[i], kind == CHILD_OF_VFORK_IS_KILLED) == 0;
                if (!is_ready[i]) {
                    printf("\t\tERROR: the child that blocks in %s is not ready\n",
                           children[i].call->name);
                    ret = -1;
                }
            }
        }
        if (kind == CHILD_IS_KILLED || kind == CHILD_OF_VFORK_IS_KILLED) {
            usleep(100 * 1000);
            for (int i = 0; i < num_children; i++) {
                if (is_ready[i]) {
                    kill(kind == CHILD_IS_KILLED ? children[i].pid : children[i].reported_pid,
                         kind == CHILD_IS_KILLED ? (i % 2 == 0 ? SIGKILL : signum) : SIGTERM);
                }
            }
        }
        int is_all_ended = wait_for_children(children, num_children);
        for (int i = 0; i < num_children; i++) {
            child_t *child = &children[i];
            const char *name = child->call->name;
            close_fd(&child->ready_fd);
            remove_semaphore_set(sem_key_of(child->pid));
            if (!child->has_ended) {
                printf("\t\tERROR: the child that blocks in %s did not end in %d ms\n", name,
                       CHILD_TIMEOUT_MS);
                ret = -1;
                continue;
            }
            int status = child->status;
            int is_ok;
            switch (kind) {
                case CHILD_IS_KILLED: {
                    int expected = i % 2 == 0 ? SIGKILL : signum;
                    is_ok = !is_ready[i] || (WIFSIGNALED(status) && WTERMSIG(status) == expected);
                    break;
                }
                case CHILD_OF_VFORK_IS_KILLED:
                    // The LibOS runs the child of vfork on the thread of its parent, with the
                    // pid of its parent until it calls execve, so a signal that kills it ends
                    // the process of the parent too
                    is_ok = !is_ready[i] || (WIFSIGNALED(status) && WTERMSIG(status) == SIGTERM) ||
                            (WIFEXITED(status) && WEXITSTATUS(status) == CHILD_EXIT_STATUS);
                    break;
                default:
                    is_ok = WIFEXITED(status) && (is_status_checked ?
                                                  WEXITSTATUS(status) == CHILD_EXIT_STATUS :
                                                  WEXITSTATUS(status) != EXIT_FAILURE);
            }
            if (!is_ok) {
                printf("\t\tERROR: the child that blocks in %s has ended with the status 0x%x\n",
                       name, status);
                ret = -1;
            }
        }
        if (!is_all_ended) {
            ret = -1;
        }
    }
    return ret;
}

// ============================================================================
// Test case: a call ends if its process exits or is killed, or if another thread calls vfork
// ============================================================================

int test_process_exits() {
    return check_children(CHILD_EXITS, "--exit-while-blocked", 1, 0);
}

// The test process kills the child process, with SIGKILL and SIGTERM in turn
int test_process_is_killed() {
    return check_children(CHILD_IS_KILLED, "--block", 1, SIGTERM);
}

// The status of the child that calls vfork is not checked, as it is 0 instead of the expected
// status now and then, also if the thread of the process does not block in a call. That is
// another bug.
int test_vfork_in_other_thread() {
    return check_children(CHILD_FORKS, "--vfork-while-blocked", 0, 0);
}

int test_vfork_child_is_killed() {
    return check_children(CHILD_OF_VFORK_IS_KILLED, "--vfork-child-blocks", 0, SIGTERM);
}

// ============================================================================
// Test case: the signals that the main thread leaves, and the signal mask of pselect
// ============================================================================

// A signal that is sent to a process is delivered to its main thread unless the main thread
// blocks it, and to another thread if the main thread has exited. A call of that thread must
// fail with EINTR then.
int test_signal_after_main_thread_exits() {
    return check_children(CHILD_SIGNAL_LEFT_TO_THREAD, "--main-exits", 1, 0);
}

int test_signal_that_the_main_thread_blocks() {
    return check_children(CHILD_SIGNAL_LEFT_TO_THREAD, "--main-blocks-signal", 1, 0);
}

// pselect replaces the signal mask while it waits. The signal that the original mask blocks and
// the new mask does not ends the call. The call must see the new mask when the host interrupts it.
int test_pselect_with_temporary_mask() {
    if (set_signal_action(SIGUSR1, count_sigusr1) < 0) {
        THROW_ERROR("failed to set the signal action");
    }
    sigset_t sigusr1_mask, empty_mask, old_mask, mask;
    sigemptyset(&sigusr1_mask);
    sigaddset(&sigusr1_mask, SIGUSR1);
    sigemptyset(&empty_mask);
    if (sigprocmask(SIG_BLOCK, &sigusr1_mask, &old_mask) < 0) {
        THROW_ERROR("failed to block SIGUSR1");
    }

    sender_t sender;
    struct timespec timeout = us_to_timespec(HANDLED_TIMEOUT_US);
    if (start_sender(&sender, g_main_worker.tid, SIGUSR1, SIGNAL_DELAY_US, 0) != 0) {
        THROW_ERROR("failed to create the thread");
    }
    long start_us = now_us();
    sender.start_us = start_us;
    int ret = pselect(0, NULL, NULL, NULL, &timeout, &empty_mask);
    int err = errno;
    long elapsed_us = now_us() - start_us;
    stop_sender(&sender);
    sigprocmask(SIG_SETMASK, NULL, &mask);
    int is_sigusr1_blocked = sigismember(&mask, SIGUSR1);
    sigprocmask(SIG_SETMASK, &old_mask, NULL);

    if (ret != -1 || err != EINTR) {
        THROW_ERROR("pselect returned %d with errno %d after %ld ms instead of failing with EINTR",
                    ret, err, elapsed_us / 1000);
    }
    if (elapsed_us > SIGNAL_DELAY_US + MAX_INTERRUPT_DELAY_US) {
        THROW_ERROR("pselect has failed with EINTR only after %ld ms", elapsed_us / 1000);
    }
    if (!is_sigusr1_blocked) {
        THROW_ERROR("the original signal mask is not in effect after pselect");
    }
    return 0;
}

// ============================================================================
// Test case: a sleep tells the time that is left only if it fails with EINTR
// ============================================================================

#define SLEEP_US            100000L
#define REM_TOLERANCE_US    30000L

static int check_sleep_rem(const char *name, int is_clock_nanosleep, long sleep_us,
                           sender_t *sender) {
    struct timespec req = us_to_timespec(sleep_us);
    struct timespec rem = { 123, 456 };
    long start_us = now_us();
    if (sender != NULL) {
        sender->start_us = start_us;
    }
    long ret = is_clock_nanosleep ?
               syscall(SYS_clock_nanosleep, CLOCK_MONOTONIC, 0, &req, &rem) :
               syscall(SYS_nanosleep, &req, &rem);
    int err = errno;
    long elapsed_us = now_us() - start_us;
    if (ret == 0) {
        if (rem.tv_sec != 123 || rem.tv_nsec != 456) {
            THROW_ERROR("%s has written rem, although it has not failed: %ld s %ld ns", name,
                        (long)rem.tv_sec, rem.tv_nsec);
        }
        if (elapsed_us < sleep_us - EARLY_TOLERANCE_US ||
                elapsed_us > sleep_us + LATE_TOLERANCE_US) {
            THROW_ERROR("%s has slept %ld ms instead of %ld ms", name, elapsed_us / 1000,
                        sleep_us / 1000);
        }
        return 0;
    }
    if (err != EINTR) {
        THROW_ERROR("%s has failed with errno %d", name, err);
    }
    long rem_us = rem.tv_sec * 1000000L + rem.tv_nsec / 1000;
    if (rem_us + elapsed_us < sleep_us - REM_TOLERANCE_US ||
            rem_us + elapsed_us > sleep_us + REM_TOLERANCE_US) {
        THROW_ERROR("%s has slept %ld ms and tells that %ld ms are left of %ld ms", name,
                    elapsed_us / 1000, rem_us / 1000, sleep_us / 1000);
    }
    return 1;
}

int test_sleep_rem() {
    if (set_signal_action(SIGUSR1, count_sigusr1) < 0 ||
            set_signal_action(SIGUSR2, SIG_IGN) < 0) {
        THROW_ERROR("failed to set the signal actions");
    }
    for (int is_clock_nanosleep = 0; is_clock_nanosleep <= 1; is_clock_nanosleep++) {
        const char *name = is_clock_nanosleep ? "clock_nanosleep" : "nanosleep";
        // The sleep ends by itself
        if (check_sleep_rem(name, is_clock_nanosleep, SLEEP_US, NULL) != 0) {
            THROW_ERROR("%s has been interrupted", name);
        }
        // The sleep ends by itself, with an ignored signal pending
        sender_t sender;
        if (start_sender(&sender, g_main_worker.tid, SIGUSR2, 5000, 10000) != 0) {
            THROW_ERROR("failed to create the thread");
        }
        int ret = check_sleep_rem(name, is_clock_nanosleep, SLEEP_US, &sender);
        stop_sender(&sender);
        if (ret != 0) {
            THROW_ERROR("%s has been interrupted by an ignored signal", name);
        }
        // The sleep is interrupted by a signal that is handled
        if (start_sender(&sender, g_main_worker.tid, SIGUSR1, SIGNAL_DELAY_US, 0) != 0) {
            THROW_ERROR("failed to create the thread");
        }
        ret = check_sleep_rem(name, is_clock_nanosleep, HANDLED_TIMEOUT_US, &sender);
        stop_sender(&sender);
        if (ret != 1) {
            THROW_ERROR("%s has not been interrupted by a signal that is handled", name);
        }
    }
    return 0;
}

// ============================================================================
// Test case: a TCP socket that is connected cannot connect again
// ============================================================================

// ============================================================================
// Test case: the exit of a child process does not end a call or its timeout
// ============================================================================

// The exit of a child sends SIGCHLD to the process, which ignores it by default. The signal stays
// pending as the process has no other thread than the one that waits.
#define CHILD_LIFE_US           30000L
#define RECV_TIMEOUT_US         200000L

static int child_exit_soon(void) {
    usleep(CHILD_LIFE_US);
    return 0;
}

int test_child_exit_during_receive_timeout() {
    if (set_signal_action(SIGCHLD, SIG_DFL) < 0) {
        THROW_ERROR("failed to set the signal action");
    }
    ctx_t ctx;
    ctx_init(&ctx);
    if (setup_tcp_socket_pair(&ctx) < 0) {
        teardown_tcp_socket_pair(&ctx);
        THROW_ERROR("failed to set up the sockets");
    }
    pid_t pid;
    char *argv[] = { (char *)g_self_path, "--exit-soon", NULL };
    if (posix_spawn(&pid, g_self_path, NULL, NULL, argv, environ) != 0) {
        teardown_tcp_socket_pair(&ctx);
        THROW_ERROR("failed to spawn the child");
    }
    long start_us = now_us();
    long ret = call_recv_with_timeout(&ctx, RECV_TIMEOUT_US);
    int err = errno;
    long elapsed_us = now_us() - start_us;
    waitpid(pid, NULL, 0);
    teardown_tcp_socket_pair(&ctx);

    if (ret != -1 || err != EAGAIN) {
        THROW_ERROR("recv returned %ld with errno %d after %ld ms instead of failing with EAGAIN",
                    ret, err, elapsed_us / 1000);
    }
    if (elapsed_us < RECV_TIMEOUT_US - EARLY_TOLERANCE_US ||
            elapsed_us > RECV_TIMEOUT_US + LATE_TOLERANCE_US) {
        THROW_ERROR("recv has failed after %ld ms instead of %ld ms", elapsed_us / 1000,
                    RECV_TIMEOUT_US / 1000);
    }
    return 0;
}

// ============================================================================
// Test case: a stale interrupt does not end a call or change its time
// ============================================================================

// A signal that is sent to the process is delivered to a thread that does not block it, if the
// main thread blocks it. The host interrupts the threads that can take the signal, and the one that
// does not take it is interrupted for nothing. That is the case of the thread X, which runs the
// calls, when the thread Y takes the signal: all interrupts for it are stale or for a signal that
// is handled in Y, and it has to wait for the time that is left of its timeout. The signal is
// sent in the first part of the call only: the last interrupt is far from the end of the call,
// so a call that waits for its whole timeout again after it ends long after its time.
#define TIMED_CALL_US           40000L
#define TIMED_SIGNALS_US        20000L
#define TIMED_TOLERANCE_US      8000L
#define TIMED_SIGNAL_INTERVAL_US    5000L
#define TIMED_TIME_PER_CALL_US  400000L
#define TIMED_TIME_OF_HOST_TIMEOUT_US   100000L
#define TIMED_NUM_CALLS         2

static volatile unsigned long g_num_process_handled;
static volatile int g_timed_thread_tid;
static int g_timed_result;

static void count_process_signal(int signum) {
    __sync_fetch_and_add(&g_num_process_handled, 1);
}

static void unblock_signal(int signum) {
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, signum);
    pthread_sigmask(SIG_UNBLOCK, &set, NULL);
}

static void *take_signals(void *arg) {
    block_all_signals();
    unblock_signal(SIGUSR1);
    while (!g_is_stream_stopped) {
        sleep_us(20);
    }
    return NULL;
}

static int is_timed_call(const blocking_call_t *call) {
    return call->wake == NULL && !(call->flags & F_ANY_SIGNAL);
}

static int check_timed_call(worker_t *w, const blocking_call_t *call) {
    ctx_t ctx;
    ctx_init(&ctx);
    if (call->setup(&ctx) < 0) {
        printf("\t\tERROR: failed to set up %s: %s\n", call->name, strerror(errno));
        call->teardown(&ctx);
        return -1;
    }
    int num_ended = 0, num_interrupted = 0, ret = 0;
    long max_over_us = -1000000;
    long start_us = now_us();
    long time_us = (call->flags & F_HOST_TIMEOUT) ? TIMED_TIME_OF_HOST_TIMEOUT_US :
                   TIMED_TIME_PER_CALL_US;
    while (num_ended < TIMED_NUM_CALLS && now_us() - start_us < time_us && ret == 0) {
        unsigned long num_handled = g_num_process_handled;
        unsigned long num_alarms = w->num_alarms;
        errno = 0;
        long call_start_us = now_us();
        w->call_start_us = call_start_us;
        w->in_call = 1;
        g_stream_gate_start_us = call_start_us;
        long call_ret = call->call(&ctx, TIMED_CALL_US);
        int err = errno;
        g_stream_gate_start_us = 0;
        w->in_call = 0;
        long elapsed_us = now_us() - call_start_us;
        if (w->num_alarms != num_alarms) {
            printf("\t\tERROR: %s has not ended in %ld ms\n", call->name,
                   WATCHDOG_LIMIT_US / 1000);
            ret = -1;
        } else if (call_ret < 0 && err == EINTR) {
            // A handler has run in the process, in the thread Y, which the thread X may not
            // see yet. A call with a timeout of the host fails with EINTR for any stale
            // interrupt, see F_HOST_TIMEOUT.
            for (int i = 0; i < 200 && g_num_process_handled == num_handled; i++) {
                usleep(100);
            }
            if (g_num_process_handled == num_handled && !(call->flags & F_HOST_TIMEOUT)) {
                printf("\t\tERROR: %s failed with EINTR after %ld us, but no handler has run\n",
                       call->name, elapsed_us);
                ret = -1;
            }
            num_interrupted++;
        } else if (call_ret < 0 && err != ETIMEDOUT && err != EAGAIN) {
            printf("\t\tERROR: %s failed with errno %d (%s)\n", call->name, err, strerror(err));
            ret = -1;
        } else {
            if (elapsed_us - TIMED_CALL_US > max_over_us) {
                max_over_us = elapsed_us - TIMED_CALL_US;
            }
            if (elapsed_us < TIMED_CALL_US - EARLY_TOLERANCE_US ||
                    elapsed_us > TIMED_CALL_US + TIMED_TOLERANCE_US) {
                printf("\t\tERROR: %s has ended after %ld us instead of %ld us\n", call->name,
                       elapsed_us, TIMED_CALL_US);
                ret = -1;
            }
            num_ended++;
        }
    }
    call->teardown(&ctx);
    if (ret == 0 && num_ended == 0 && !(call->flags & F_HOST_TIMEOUT)) {
        printf("\t\tERROR: %s has been interrupted %d times, and has not ended by its timeout\n",
               call->name, num_interrupted);
        ret = -1;
    }
    if (g_is_verbose) {
        printf("\t\t%-24s ended by its timeout %d times (at most %ld us late), and %d calls were interrupted\n",
               call->name, num_ended, max_over_us, num_interrupted);
    }
    return ret;
}

static void *run_timed_calls(void *arg) {
    worker_t *w = arg;
    block_all_signals();
    unblock_signal(SIGUSR1);
    unblock_signal(SIGALRM);
    t_worker = w;
    w->tid = gettid_();
    g_timed_thread_tid = w->tid;
    while (!g_is_start_released) {
        usleep(200);
    }
    g_timed_result = 0;
    for (size_t i = 0; i < NUM_BLOCKING_CALLS; i++) {
        if (is_timed_call(&g_blocking_calls[i])) {
            g_timed_result |= check_timed_call(w, &g_blocking_calls[i]);
        }
    }
    return NULL;
}

int test_stale_interrupts_of_timed_calls() {
    g_stream_signum = SIGUSR1;
    g_is_stream_process_directed = 1;
    g_stream_interval_us = TIMED_SIGNAL_INTERVAL_US;
    g_stream_gate_us = TIMED_SIGNALS_US;
    if (set_signal_action(SIGUSR1, count_process_signal) < 0) {
        THROW_ERROR("failed to set the signal action");
    }
    // The main thread blocks the signal, and the thread that sends it
    sigset_t sigusr1_mask, old_mask;
    sigemptyset(&sigusr1_mask);
    sigaddset(&sigusr1_mask, SIGUSR1);
    pthread_sigmask(SIG_BLOCK, &sigusr1_mask, &old_mask);
    pthread_t taker, runner;
    g_is_start_released = 0;
    g_timed_thread_tid = 0;
    g_is_stream_stopped = 0;
    int ret = -1;
    if (create_thread(&taker, take_signals, NULL) != 0) {
        pthread_sigmask(SIG_SETMASK, &old_mask, NULL);
        THROW_ERROR("failed to create the thread");
    }
    // The watchdog ends a call that does not end
    memset(&g_workers[0], 0, sizeof(g_workers[0]));
    g_num_workers = 1;
    if (create_thread(&runner, run_timed_calls, &g_workers[0]) == 0) {
        while (g_timed_thread_tid == 0) {
            usleep(200);
        }
        g_stream_tid = g_timed_thread_tid;
        if (start_stream() == 0) {
            g_is_start_released = 1;
            pthread_join(runner, NULL);
            ret = g_timed_result;
        } else {
            g_is_start_released = 1;
            pthread_join(runner, NULL);
        }
        stop_stream();
    }
    g_is_stream_stopped = 1;
    pthread_join(taker, NULL);
    pthread_sigmask(SIG_SETMASK, &old_mask, NULL);
    return ret == 0 ? 0 : -1;
}

// ============================================================================
// Test suite main
// ============================================================================

static test_case_t test_cases[] = {
    TEST_CASE(test_stream_of_signals),
    TEST_CASE(test_ignored_signal),
    TEST_CASE(test_signal_that_is_ignored_by_default),
    TEST_CASE(test_blocked_signal),
    TEST_CASE(test_child_exit_during_receive_timeout),
    TEST_CASE(test_handled_signal),
    TEST_CASE(test_process_exits),
    TEST_CASE(test_process_is_killed),
    TEST_CASE(test_vfork_in_other_thread),
    TEST_CASE(test_vfork_child_is_killed),
    TEST_CASE(test_signal_after_main_thread_exits),
    TEST_CASE(test_signal_that_the_main_thread_blocks),
    TEST_CASE(test_pselect_with_temporary_mask),
    TEST_CASE(test_sleep_rem),
    TEST_CASE(test_stale_interrupts_of_timed_calls),
};

int main(int argc, const char *argv[]) {
#ifdef SGX_MODE_SIM
    // The simulation mode crashes when the host interrupts a thread: an ocall that an interrupt
    // has hit returns a status that is not SGX_SUCCESS, and the LibOS panics on it. It does with
    // the unfixed LibOS too. This test makes the host interrupt threads on purpose, so it does not
    // run in the simulation mode.
    printf("\tskipped, the interrupts of the host crash the LibOS in the simulation mode\n");
    return 0;
#endif
    g_main_worker.tid = gettid_();
    t_worker = &g_main_worker;
    if (argc > 1 && strcmp(argv[1], "--interrupter") == 0) {
        return child_interrupter();
    }
    if (argc > 1 && strcmp(argv[1], "--exit-soon") == 0) {
        return child_exit_soon();
    }
    if (argc > 2 && argv[1][0] == '-' && argv[1][1] == '-') {
        const blocking_call_t *call = find_blocking_call(argv[2]);
        if (call == NULL) {
            fprintf(stderr, "ERROR: unknown call: %s\n", argv[2]);
            return EXIT_FAILURE;
        }
        const char *mode = argv[1];
        if (strcmp(mode, "--exit-while-blocked") == 0) {
            return child_exit_while_blocked(call);
        } else if (strcmp(mode, "--block") == 0) {
            return child_block(call);
        } else if (strcmp(mode, "--vfork-while-blocked") == 0) {
            return child_vfork_while_blocked(call);
        } else if (strcmp(mode, "--vfork-child-blocks") == 0) {
            return child_vfork_child_blocks(call);
        } else if (strcmp(mode, "--main-exits") == 0) {
            return child_signal_left_to_thread(call, 1);
        } else if (strcmp(mode, "--main-blocks-signal") == 0) {
            return child_signal_left_to_thread(call, 0);
        }
        fprintf(stderr, "ERROR: unknown mode: %s\n", mode);
        return EXIT_FAILURE;
    }
    // The options: -v prints the result of each call, and the other arguments select test cases
    test_case_t selected_test_cases[ARRAY_SIZE(test_cases)];
    int num_selected_test_cases = 0;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-v") == 0) {
            g_is_verbose = 1;
            continue;
        }
        for (size_t ti = 0; ti < ARRAY_SIZE(test_cases); ti++) {
            if (strstr(test_cases[ti].name, argv[i]) != NULL) {
                selected_test_cases[num_selected_test_cases++] = test_cases[ti];
            }
        }
    }

    if (set_signal_action(SIGALRM, count_alarm) < 0 ||
            create_thread(&g_watchdog, run_watchdog, NULL) != 0) {
        fprintf(stderr, "ERROR: failed to start the watchdog\n");
        return EXIT_FAILURE;
    }
    int ret = num_selected_test_cases > 0 ?
              test_suite_run(selected_test_cases, num_selected_test_cases) :
              test_suite_run(test_cases, ARRAY_SIZE(test_cases));
    g_is_watchdog_stopped = 1;
    pthread_join(g_watchdog, NULL);
    return ret;
}
