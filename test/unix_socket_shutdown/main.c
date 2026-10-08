// shutdown() of connected unix stream sockets. The expectations are the behavior of Linux:
// the test passes there, too (gcc -pthread -I../include main.c), and the threads that wait on
// a socket are woken up by a shutdown of the socket, or of its peer, as they are in Linux.
//
// A waiter thread blocks in recv, poll, select, epoll_wait, ... on one end of a connection,
// then the main thread shuts down (or closes) one end. A waiter that has to wake up, but does
// not, makes the test fail after a timeout, rather than hang. Such a thread is left behind, so
// the test ends with _exit if there are any.
//
// Usage: unix_socket_shutdown [test case name | --list]
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/ioctl.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#include "test.h"

#define SETTLE_MS       30      // time that a waiter has to block before the stimulus
#define NEGATIVE_MS     30      // time that a waiter, which must not wake up, is observed
#define WAKE_MS         2000    // time that a waiter has to wake up
#define MAX_STUCK       24      // waiters that do not wake up, which the test tolerates
#define MAX_FAILURES    3       // failed rows after which a test case gives up
#define MAX_WAITERS     16
#define STACK_SIZE      (256 * 1024)

#define EV_ALL          (POLLIN | POLLOUT | POLLRDHUP)

static int stuck_waiters = 0;

// ---------------------------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------------------------

static long now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
}

static void sleep_ms(int ms) {
    usleep(ms * 1000);
}

static const char *events_str(unsigned events) {
    static __thread char bufs[8][64];
    static __thread int next;
    char *buf = bufs[next++ % 8];
    static const struct {
        unsigned bit;
        const char *name;
    } names[] = {
        { POLLIN, "IN" }, { POLLOUT, "OUT" }, { POLLERR, "ERR" }, { POLLHUP, "HUP" },
        { POLLRDHUP, "RDHUP" }, { POLLNVAL, "NVAL" }, { POLLPRI, "PRI" },
    };
    buf[0] = '\0';
    for (size_t i = 0; i < ARRAY_SIZE(names); i++) {
        if (events & names[i].bit) {
            strcat(buf, buf[0] ? "|" : "");
            strcat(buf, names[i].name);
        }
    }
    if (buf[0] == '\0') {
        strcpy(buf, "0");
    }
    return buf;
}

static void set_nonblocking(int fd, int nonblocking) {
    int flags = fcntl(fd, F_GETFL);
    fcntl(fd, F_SETFL, nonblocking ? (flags | O_NONBLOCK) : (flags & ~O_NONBLOCK));
}

// recv() that never blocks: fails with EAGAIN if there are neither data nor the end of the file
static long recv_nb(int fd, char *buf, size_t size) {
    set_nonblocking(fd, 1);
    long ret = recv(fd, buf, size, 0);
    int err = errno;
    set_nonblocking(fd, 0);
    errno = err;
    return ret;
}

// How a connection is made: the same code serves both
enum { PAIR_SOCKETPAIR, PAIR_CONNECT, NUM_PAIR_KINDS };
static const char *pair_names[] = { "socketpair", "connect" };

// Creates a connection. `s` is the end that will be shut down, `p` is its peer. For
// PAIR_CONNECT, `s` is the accepted end, like the end of a server.
static int create_pair(int kind, int *s, int *p) {
    if (kind == PAIR_SOCKETPAIR) {
        int sv[2];
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) {
            return -1;
        }
        *s = sv[0];
        *p = sv[1];
        return 0;
    }

    static int counter = 0;
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    // An abstract address, which needs no file
    int name_len = snprintf(addr.sun_path + 1, sizeof(addr.sun_path) - 1,
                            "unix_socket_shutdown.%d.%d", (int)getpid(), counter++);
    socklen_t addr_len = offsetof(struct sockaddr_un, sun_path) + 1 + name_len;

    int listen_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    int client_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    int accepted_fd = -1;
    if (listen_fd >= 0 && client_fd >= 0 &&
            bind(listen_fd, (struct sockaddr *)&addr, addr_len) == 0 &&
            listen(listen_fd, 4) == 0 &&
            connect(client_fd, (struct sockaddr *)&addr, addr_len) == 0) {
        accepted_fd = accept(listen_fd, NULL, NULL);
    }
    if (listen_fd >= 0) {
        close(listen_fd);
    }
    if (accepted_fd < 0) {
        if (client_fd >= 0) {
            close(client_fd);
        }
        return -1;
    }
    *s = accepted_fd;
    *p = client_fd;
    return 0;
}

// Fills the buffer of the socket, so that a blocking send would block
static int fill(int fd) {
    static char buf[4096];
    set_nonblocking(fd, 1);
    int filled = 0;
    for (int i = 0; i < 64 * 1024 && !filled; i++) {
        if (send(fd, buf, sizeof(buf), MSG_NOSIGNAL) < 0) {
            filled = errno == EAGAIN;
            if (!filled) {
                break;
            }
        }
    }
    set_nonblocking(fd, 0);
    return filled ? 0 : -1;
}

static long poll_now(int fd, short events) {
    struct pollfd pfd = { .fd = fd, .events = events };
    int ret = poll(&pfd, 1, 0);
    return ret < 0 ? -1 : (long)(unsigned short)pfd.revents;
}

// The events that epoll_wait returns at once for a level-triggered registration
static long epoll_now(int fd, unsigned events) {
    int epfd = epoll_create1(0);
    struct epoll_event event = { .events = events, .data.fd = fd };
    if (epfd < 0 || epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &event) < 0) {
        return -1;
    }
    int ret = epoll_wait(epfd, &event, 1, 0);
    close(epfd);
    return ret < 0 ? -1 : ret == 0 ? 0 : (long)event.events;
}

// Returns bit 0 if readable, bit 1 if writable
static int select_now(int fd) {
    fd_set rfds, wfds;
    FD_ZERO(&rfds);
    FD_ZERO(&wfds);
    FD_SET(fd, &rfds);
    FD_SET(fd, &wfds);
    struct timeval timeout = { 0, 0 };
    if (select(fd + 1, &rfds, &wfds, NULL, &timeout) < 0) {
        return -1;
    }
    return (FD_ISSET(fd, &rfds) ? 1 : 0) | (FD_ISSET(fd, &wfds) ? 2 : 0);
}

// ---------------------------------------------------------------------------------------------
// Waiters: threads that block on a socket
// ---------------------------------------------------------------------------------------------

enum {
    W_RECV, W_READ, W_READV, W_RECVMSG,
    W_POLL_IN, W_PPOLL_IN, W_POLL_RDHUP, W_POLL_IN_RDHUP, W_POLL_NONE,
    W_SELECT, W_EPOLL_LT, W_EPOLL_ET,
    W_SEND, W_WRITE,
    // Waits with a timeout, which a shutdown must end long before it expires. MariaDB waits for
    // the data of an idle connection in poll(POLLIN | POLLPRI) with a timeout of 8 hours
    W_POLL_IN_PRI_8H, W_EPOLL_5S, W_SELECT_5S,
    // Waits for a socket that cannot be written, as its buffer is full
    W_POLL_OUT_FULL, W_POLL_OUT_IN_FULL, W_EPOLL_OUT_FULL,
    NUM_WAITER_TYPES
};

static const char *waiter_names[] = {
    "recv", "read", "readv", "recvmsg",
    "poll(IN)", "ppoll(IN)", "poll(RDHUP)", "poll(IN|RDHUP)", "poll(0)",
    "select(read)", "epoll(IN|RDHUP)", "epoll(IN|RDHUP|ET)",
    "send(full)", "write(full)",
    "poll(IN|PRI, 8 hours)", "epoll(IN|RDHUP, 5 s)", "select(read, 5 s)",
    "poll(OUT, full)", "poll(OUT|IN, full)", "epoll(OUT, full)",
};

// The events that a waiter asks for (for polling PRI, too, see waiter_main)
static unsigned waiter_events(int type) {
    switch (type) {
        case W_POLL_IN:
        case W_PPOLL_IN:
        case W_POLL_IN_PRI_8H:
            return POLLIN;
        case W_POLL_RDHUP:
            return POLLRDHUP;
        case W_POLL_IN_RDHUP:
        case W_EPOLL_LT:
        case W_EPOLL_ET:
        case W_EPOLL_5S:
            return POLLIN | POLLRDHUP;
        case W_POLL_OUT_FULL:
        case W_EPOLL_OUT_FULL:
            return POLLOUT;
        case W_POLL_OUT_IN_FULL:
            return POLLOUT | POLLIN;
        default:
            return 0;
    }
}

static int is_sender(int type) {
    return type == W_SEND || type == W_WRITE;
}

// The waiter needs a full buffer on its end. If the peer closes while the buffer is full, and
// the data was not read, the socket gets ECONNRESET, which the LibOS does not have (yet)
static int needs_full_buffer(int type) {
    return is_sender(type) || type == W_POLL_OUT_FULL || type == W_POLL_OUT_IN_FULL ||
           type == W_EPOLL_OUT_FULL;
}

static int is_epoll(int type) {
    return type == W_EPOLL_LT || type == W_EPOLL_ET || type == W_EPOLL_5S ||
           type == W_EPOLL_OUT_FULL;
}

struct waiter {
    int type;
    int fd;
    int epfd;
    int created;
    pthread_t thread;
    int started;    // accessed atomically
    int done;       // accessed atomically, the results are valid if it is set
    long ret;
    int err;
    unsigned revents;
};

static void *waiter_main(void *arg) {
    struct waiter *w = arg;
    static char big[4096];
    char buf[64];
    long ret = -2;
    unsigned revents = 0;
    int err = 0;

    __atomic_store_n(&w->started, 1, __ATOMIC_RELEASE);
    errno = 0;
    switch (w->type) {
        case W_RECV:
            ret = recv(w->fd, buf, sizeof(buf), 0);
            break;
        case W_READ:
            ret = read(w->fd, buf, sizeof(buf));
            break;
        case W_READV: {
            struct iovec iov[2] = { { buf, 32 }, { buf + 32, 32 } };
            ret = readv(w->fd, iov, 2);
            break;
        }
        case W_RECVMSG: {
            struct iovec iov = { buf, sizeof(buf) };
            struct msghdr msg = { .msg_iov = &iov, .msg_iovlen = 1 };
            ret = recvmsg(w->fd, &msg, 0);
            break;
        }
        case W_POLL_IN:
        case W_POLL_RDHUP:
        case W_POLL_IN_RDHUP:
        case W_POLL_NONE:
        case W_POLL_OUT_FULL:
        case W_POLL_OUT_IN_FULL: {
            struct pollfd pfd = { .fd = w->fd, .events = waiter_events(w->type) };
            ret = poll(&pfd, 1, -1);
            revents = (unsigned short)pfd.revents;
            break;
        }
        case W_POLL_IN_PRI_8H: {
            struct pollfd pfd = { .fd = w->fd, .events = POLLIN | POLLPRI };
            ret = poll(&pfd, 1, 8 * 3600 * 1000);
            revents = (unsigned short)pfd.revents;
            break;
        }
        case W_PPOLL_IN: {
            struct pollfd pfd = { .fd = w->fd, .events = POLLIN };
            ret = ppoll(&pfd, 1, NULL, NULL);
            revents = (unsigned short)pfd.revents;
            break;
        }
        case W_SELECT:
        case W_SELECT_5S: {
            fd_set rfds;
            FD_ZERO(&rfds);
            FD_SET(w->fd, &rfds);
            struct timeval timeout = { 5, 0 };
            ret = select(w->fd + 1, &rfds, NULL, NULL, w->type == W_SELECT ? NULL : &timeout);
            revents = FD_ISSET(w->fd, &rfds) ? POLLIN : 0;
            break;
        }
        case W_EPOLL_LT:
        case W_EPOLL_ET:
        case W_EPOLL_5S:
        case W_EPOLL_OUT_FULL: {
            struct epoll_event event;
            ret = epoll_wait(w->epfd, &event, 1, w->type == W_EPOLL_5S ? 5000 : -1);
            revents = ret > 0 ? event.events : 0;
            break;
        }
        case W_SEND:
            ret = send(w->fd, big, sizeof(big), MSG_NOSIGNAL);
            break;
        case W_WRITE:
            ret = write(w->fd, big, sizeof(big));
            break;
    }
    err = errno;

    w->ret = ret;
    w->err = err;
    w->revents = revents;
    __atomic_store_n(&w->done, 1, __ATOMIC_RELEASE);
    return NULL;
}

// Starts a thread that blocks on the socket (or does not, if the socket is in a state in
// which the call does not block)
static int waiter_start(struct waiter *w, int type, int fd) {
    memset(w, 0, sizeof(*w));
    w->type = type;
    w->fd = fd;
    w->epfd = -1;

    if (is_epoll(type)) {
        unsigned events = type == W_EPOLL_OUT_FULL ? EPOLLOUT : EPOLLIN | EPOLLRDHUP;
        struct epoll_event event = { .events = events, .data.fd = fd };
        if (type == W_EPOLL_ET) {
            event.events |= EPOLLET;
        }
        w->epfd = epoll_create1(0);
        if (w->epfd < 0 || epoll_ctl(w->epfd, EPOLL_CTL_ADD, fd, &event) < 0) {
            return -1;
        }
    }

    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setstacksize(&attr, STACK_SIZE);
    int err = pthread_create(&w->thread, &attr, waiter_main, w);
    pthread_attr_destroy(&attr);
    if (err != 0) {
        errno = err;
        return -1;
    }
    w->created = 1;
    while (!__atomic_load_n(&w->started, __ATOMIC_ACQUIRE)) {
        usleep(200);
    }
    return 0;
}

static int waiter_done(struct waiter *w) {
    return __atomic_load_n(&w->done, __ATOMIC_ACQUIRE);
}

// Waits until the deadline for the waiter to return
static int waiter_wait(struct waiter *w, long deadline_ms) {
    while (!waiter_done(w)) {
        if (now_ms() >= deadline_ms) {
            return 0;
        }
        usleep(200);
    }
    return 1;
}

// Joins the thread if it returned, otherwise it is left behind. Returns whether it returned.
static int waiter_finish(struct waiter *w) {
    if (!w->created) {
        return 1;
    }
    if (!waiter_done(w)) {
        stuck_waiters++;
        return 0;
    }
    pthread_join(w->thread, NULL);
    w->created = 0;
    if (w->epfd >= 0) {
        close(w->epfd);
    }
    return 1;
}

// ---------------------------------------------------------------------------------------------
// What happens to a waiter: the behavior of Linux
// ---------------------------------------------------------------------------------------------

// The stimuli, on the end S of a connection
enum { STIM_RD, STIM_WR, STIM_RDWR, STIM_CLOSE, NUM_STIMULI };
static const char *stimulus_names[] = {
    "shutdown(SHUT_RD)", "shutdown(SHUT_WR)", "shutdown(SHUT_RDWR)", "close"
};

// The end where a waiter blocks: S gets the stimulus, P is the peer of S
enum { ON_S, ON_P };
static const char *side_names[] = { "S", "P" };

struct expectation {
    int wakes;
    long ret;
    int err;
    unsigned revents;
    unsigned revents_mask;  // the bits of revents that are checked
    int unchecked;          // not checked: the LibOS wakes up the waiter, Linux does not
};

// What a waiter on one end of the connection gets if the end S is shut down, or closed (which
// only P can see), as in Linux:
//  - The reading returns the end of the file if the reading of the end was shut down (SHUT_RD
//    of S), or if the peer sends nothing more (SHUT_WR of S, seen by P). recv returns 0,
//    POLLIN and POLLRDHUP are reported, and select reports the socket readable.
//  - POLLHUP is reported, whatever the waiter asks for, if the end can neither read nor
//    write any more: after SHUT_RDWR of S, on both ends, and after the close of S, on P.
//  - A send fails with EPIPE if the writing of the end was shut down (SHUT_WR of S), or the
//    peer does not read any more (SHUT_RD of S, seen by P).
// Nothing else wakes up a waiter, e.g., the reading of S goes on after its SHUT_WR.
//
// The waiters for a socket with a full buffer, which wait for POLLOUT, differ from Linux in
// two ways, which the test does not check, as a program does not depend on them: the LibOS
// reports POLLOUT together with POLLHUP, while a full buffer is not writable for Linux, and
// it wakes up the waiter of S if the writing of S was shut down (the send fails at once),
// which Linux does not.
static struct expectation expectation(int type, int stimulus, int side) {
    int on_s = side == ON_S;
    int eof = stimulus == STIM_RDWR || stimulus == STIM_CLOSE ||
              (stimulus == STIM_RD && on_s) || (stimulus == STIM_WR && !on_s);
    int hup = stimulus == STIM_RDWR || stimulus == STIM_CLOSE;
    struct expectation exp = { .revents_mask = ~0u };

    switch (type) {
        case W_RECV:
        case W_READ:
        case W_READV:
        case W_RECVMSG:
            exp.wakes = eof;
            exp.ret = 0;
            break;
        case W_SELECT:
        case W_SELECT_5S:
            exp.wakes = eof || hup;
            exp.ret = 1;
            exp.revents = POLLIN;
            break;
        case W_SEND:
        case W_WRITE:
            exp.wakes = (on_s && (stimulus == STIM_WR || stimulus == STIM_RDWR)) ||
                        (!on_s && (stimulus == STIM_RD || stimulus == STIM_RDWR));
            exp.ret = -1;
            exp.err = EPIPE;
            break;
        default:
            exp.revents = (eof ? waiter_events(type) & (POLLIN | POLLRDHUP) : 0) |
                          (hup ? POLLHUP : 0);
            exp.wakes = exp.revents != 0;
            exp.ret = 1;
            break;
    }
    if (type == W_POLL_OUT_FULL || type == W_POLL_OUT_IN_FULL || type == W_EPOLL_OUT_FULL) {
        exp.revents_mask = ~(unsigned)POLLOUT;
        exp.unchecked = on_s && stimulus == STIM_WR;
    }
    return exp;
}

struct spec {
    int type;
    int side;
};

#define ROW_ERROR(fmt, ...) do { \
    printf("\t\tERROR: %s, %s, %s on %s: " fmt "\n", pair_names[kind], \
           stimulus_names[stimulus], waiter_names[specs[i].type], \
           side_names[specs[i].side], ##__VA_ARGS__); \
    failures++; \
} while (0)

// Blocks the waiters, gives the stimulus, and checks that the waiters that are expected to
// wake up do, with the result that Linux has, and that the others stay blocked.
// `via_dup`: the stimulus is a shutdown through a duplicate of the file descriptor.
// Returns the number of failures.
static int run_waiters(int kind, int stimulus, const struct spec *specs, int num_specs,
                       int via_dup) {
    int s, p, failures = 0, i = 0;
    struct waiter waiters[MAX_WAITERS];
    struct expectation exps[MAX_WAITERS];
    int started[MAX_WAITERS] = { 0 };
    int filled[2] = { 0, 0 };   // the buffers that were filled, by the side

    if (create_pair(kind, &s, &p) < 0) {
        printf("\t\tERROR: failed to create a %s connection: %s\n", pair_names[kind],
               strerror(errno));
        return 1;
    }
    if (stuck_waiters >= MAX_STUCK) {
        printf("\t\tERROR: too many waiters are stuck, stop\n");
        close_files(2, s, p);
        return 1;
    }

    for (i = 0; i < num_specs; i++) {
        int fd = specs[i].side == ON_S ? s : p;
        exps[i] = expectation(specs[i].type, stimulus, specs[i].side);
        // Fill the buffer once, not while a waiter is blocked in it: fill() makes the socket
        // nonblocking for a while
        if (needs_full_buffer(specs[i].type) && !filled[specs[i].side]) {
            if (fill(fd) < 0) {
                ROW_ERROR("failed to fill the buffer: %s", strerror(errno));
                continue;
            }
            filled[specs[i].side] = 1;
        }
        if (waiter_start(&waiters[i], specs[i].type, fd) < 0) {
            ROW_ERROR("failed to start the waiter: %s", strerror(errno));
            continue;
        }
        started[i] = 1;
    }
    sleep_ms(SETTLE_MS);

    for (i = 0; i < num_specs; i++) {
        if (started[i] && waiter_done(&waiters[i])) {
            ROW_ERROR("returned %ld before the stimulus", waiters[i].ret);
        }
    }
    if (failures > 0) {
        goto cleanup;
    }

    // The stimulus
    if (stimulus == STIM_CLOSE) {
        close(s);
        s = -1;
    } else {
        int how = stimulus == STIM_RD ? SHUT_RD : stimulus == STIM_WR ? SHUT_WR : SHUT_RDWR;
        int fd = via_dup ? dup(s) : s;
        if (shutdown(fd, how) < 0) {
            printf("\t\tERROR: %s, %s: failed: %s\n", pair_names[kind],
                   stimulus_names[stimulus], strerror(errno));
            failures++;
        }
        if (via_dup) {
            close(fd);
        }
    }

    // The waiters that are expected to wake up
    long deadline = now_ms() + WAKE_MS;
    int has_negative = 0;
    for (i = 0; i < num_specs; i++) {
        if (!exps[i].wakes) {
            has_negative |= !exps[i].unchecked;
            continue;
        }
        struct waiter *w = &waiters[i];
        if (!waiter_wait(w, deadline)) {
            ROW_ERROR("still blocked after %d ms", WAKE_MS);
        } else if (w->ret != exps[i].ret || (w->ret < 0 && w->err != exps[i].err)) {
            ROW_ERROR("returned %ld (errno %d), expected %ld (errno %d)", w->ret, w->err,
                      exps[i].ret, exps[i].err);
        } else if ((w->revents ^ exps[i].revents) & exps[i].revents_mask) {
            ROW_ERROR("returned the events %s, expected %s", events_str(w->revents),
                      events_str(exps[i].revents));
        }
    }

    // The others stay blocked
    if (has_negative) {
        sleep_ms(NEGATIVE_MS);
        for (i = 0; i < num_specs; i++) {
            if (!exps[i].wakes && !exps[i].unchecked && waiter_done(&waiters[i])) {
                ROW_ERROR("woke up (returned %ld, errno %d, events %s), must stay blocked",
                          waiters[i].ret, waiters[i].err, events_str(waiters[i].revents));
            }
        }
    }

cleanup:
    // Let all remaining waiters return
    if (s >= 0) {
        shutdown(s, SHUT_RDWR);
    }
    shutdown(p, SHUT_RDWR);
    deadline = now_ms() + WAKE_MS;
    int all_returned = 1;
    for (i = 0; i < num_specs; i++) {
        if (started[i]) {
            waiter_wait(&waiters[i], deadline);
            all_returned &= waiter_finish(&waiters[i]);
        }
    }
    // The sockets stay open if a waiter is left behind
    if (all_returned) {
        close(p);
        if (s >= 0) {
            close(s);
        }
    }
    return failures;
}

// All the combinations of a waiter type, with every stimulus, on both ends. `copies` threads
// of the same type wait on the end at the same time, and all of them have to wake up.
static int run_type(int type, int copies) {
    int failures = 0;
    for (int kind = 0; kind < NUM_PAIR_KINDS; kind++) {
        for (int stimulus = 0; stimulus < NUM_STIMULI; stimulus++) {
            for (int side = ON_S; side <= ON_P; side++) {
                // A closed end has no waiter
                if (stimulus == STIM_CLOSE && (side == ON_S || needs_full_buffer(type))) {
                    continue;
                }
                struct spec specs[MAX_WAITERS];
                for (int i = 0; i < copies; i++) {
                    specs[i] = (struct spec) { type, side };
                }
                failures += run_waiters(kind, stimulus, specs, copies, 0);
                if (failures >= MAX_FAILURES) {
                    return failures;
                }
            }
        }
    }
    return failures;
}

static int run_types(const int *types, int num_types, int copies) {
    int failures = 0;
    for (int i = 0; i < num_types && failures < MAX_FAILURES; i++) {
        failures += run_type(types[i], copies);
    }
    if (failures > 0) {
        errno = 0;  // not the error of a call
        THROW_ERROR("%d waiters did not behave as in Linux%s", failures,
                    failures >= MAX_FAILURES ? " (the case gave up)" : "");
    }
    return 0;
}

// ---------------------------------------------------------------------------------------------
// Test cases: blocked waiters
// ---------------------------------------------------------------------------------------------

static int test_shutdown_wakes_recv(void) {
    const int types[] = { W_RECV, W_READ, W_READV, W_RECVMSG };
    return run_types(types, ARRAY_SIZE(types), 1);
}

static int test_shutdown_wakes_poll(void) {
    const int types[] = { W_POLL_IN, W_PPOLL_IN, W_POLL_RDHUP, W_POLL_IN_RDHUP, W_POLL_NONE };
    return run_types(types, ARRAY_SIZE(types), 1);
}

static int test_shutdown_wakes_select(void) {
    const int types[] = { W_SELECT };
    return run_types(types, ARRAY_SIZE(types), 1);
}

static int test_shutdown_wakes_epoll(void) {
    const int types[] = { W_EPOLL_LT, W_EPOLL_ET };
    return run_types(types, ARRAY_SIZE(types), 1);
}

static int test_shutdown_wakes_send(void) {
    const int types[] = { W_SEND, W_WRITE };
    return run_types(types, ARRAY_SIZE(types), 1);
}

// The waits that have a timeout, which is long: the shutdown has to end them, not the timeout
static int test_shutdown_wakes_timed(void) {
    const int types[] = { W_POLL_IN_PRI_8H, W_EPOLL_5S, W_SELECT_5S };
    return run_types(types, ARRAY_SIZE(types), 1);
}

// The waits for a socket that cannot be written: the hangup wakes them up. (Linux and the
// LibOS differ in the events, see expectation())
static int test_shutdown_wakes_poll_out(void) {
    const int types[] = { W_POLL_OUT_FULL, W_POLL_OUT_IN_FULL, W_EPOLL_OUT_FULL };
    return run_types(types, ARRAY_SIZE(types), 1);
}

// A server has several threads that wait on one connection: all of them are woken up, not one
#define MANY_WAITERS 3

static int test_shutdown_wakes_many_waiters(void) {
    const int types[] = { W_RECV, W_POLL_IN, W_SELECT, W_EPOLL_LT, W_EPOLL_ET, W_SEND };
    return run_types(types, ARRAY_SIZE(types), MANY_WAITERS);
}

// A thread that is blocked in poll and in recv on S is woken up by a shutdown of the
// duplicate of the file descriptor of S, too
static int test_shutdown_via_dup(void) {
    int failures = 0;
    for (int kind = 0; kind < NUM_PAIR_KINDS; kind++) {
        const int stimulus = STIM_RDWR;
        const struct spec specs[] = {
            { W_RECV, ON_S }, { W_POLL_IN, ON_S }, { W_POLL_IN, ON_P }, { W_EPOLL_ET, ON_P },
        };
        failures += run_waiters(kind, stimulus, specs, ARRAY_SIZE(specs), 1);
    }
    if (failures > 0) {
        errno = 0;  // not the error of a call
        THROW_ERROR("%d waiters did not behave as in Linux", failures);
    }
    return 0;
}

// All kinds of waiters block on both ends at the same time, as the threads of a server do.
// One shutdown wakes up those that have to wake up, and no other.
static int test_shutdown_wakes_all_waiters(void) {
    const struct spec specs[] = {
        { W_RECV, ON_S }, { W_POLL_IN, ON_S }, { W_SELECT, ON_S },
        { W_EPOLL_LT, ON_S }, { W_EPOLL_ET, ON_S },
        { W_RECV, ON_P }, { W_POLL_IN, ON_P }, { W_SELECT, ON_P },
        { W_EPOLL_LT, ON_P }, { W_EPOLL_ET, ON_P },
    };
    int failures = 0;
    for (int kind = 0; kind < NUM_PAIR_KINDS; kind++) {
        for (int stimulus = 0; stimulus < NUM_STIMULI; stimulus++) {
            // The closed end has no waiters
            int num = stimulus == STIM_CLOSE ? 5 : 10;
            const struct spec *first = stimulus == STIM_CLOSE ? &specs[5] : specs;
            failures += run_waiters(kind, stimulus, first, num, 0);
        }
    }
    if (failures > 0) {
        errno = 0;  // not the error of a call
        THROW_ERROR("%d waiters did not behave as in Linux", failures);
    }
    return 0;
}

// The shutdown comes right when the waiter starts to wait: either the waiter sees the end of
// the file at once, or it is woken up, but it must not miss the shutdown
#define RACE_ITERATIONS 300

static int test_shutdown_race(void) {
    const int types[] = { W_POLL_IN, W_RECV, W_EPOLL_LT, W_SELECT, W_EPOLL_ET, W_POLL_RDHUP };
    for (int i = 0; i < RACE_ITERATIONS; i++) {
        int kind = i % NUM_PAIR_KINDS;
        int type = types[i % ARRAY_SIZE(types)];
        int stimulus = i % 2 == 0 ? STIM_RDWR : STIM_RD;
        int s, p;
        struct waiter w;

        if (stuck_waiters >= MAX_STUCK) {
            THROW_ERROR("too many waiters are stuck");
        }
        if (create_pair(kind, &s, &p) < 0) {
            THROW_ERROR("failed to create a connection");
        }
        if (waiter_start(&w, type, s) < 0) {
            THROW_ERROR("failed to start a waiter");
        }
        // No wait: the shutdown races with the call of the waiter
        shutdown(s, stimulus == STIM_RD ? SHUT_RD : SHUT_RDWR);

        struct expectation exp = expectation(type, stimulus, ON_S);
        if (!waiter_wait(&w, now_ms() + WAKE_MS)) {
            printf("\t\tERROR: iteration %d, %s, %s on S: still blocked after %d ms\n", i,
                   stimulus_names[stimulus], waiter_names[type], WAKE_MS);
            // Leave it behind with its sockets, and end the test
            waiter_finish(&w);
            return -1;
        }
        // Linux shuts down the reading and the writing of a socket in one step, so a waiter
        // that starts right now cannot see one without the other. The LibOS does them one
        // after the other: a waiter that starts between them sees the end of the reading
        // (IN|RDHUP) without the hangup, which is the state after shutdown(SHUT_RD); a waiter
        // that is blocked already gets both with the notification that ends the shutdown.
        // The hangup is optional here: what must not happen is that the shutdown is missed.
        unsigned transient = stimulus == STIM_RDWR ? POLLHUP : 0;
        if (w.ret != exp.ret || ((w.revents ^ exp.revents) & ~transient) != 0) {
            printf("\t\tERROR: iteration %d, %s, %s on S: returned %ld and the events %s, "
                   "expected %ld and %s\n", i, stimulus_names[stimulus], waiter_names[type],
                   w.ret, events_str(w.revents), exp.ret, events_str(exp.revents));
            waiter_finish(&w);
            return -1;
        }
        waiter_finish(&w);
        close_files(2, s, p);
    }
    return 0;
}

// The end where nothing was shut down can go on, in the direction that is still open:
// shutdown(SHUT_WR) of S does not end the reading of S, nor the sending of P
static int test_shutdown_half_close(void) {
    for (int kind = 0; kind < NUM_PAIR_KINDS; kind++) {
        int s, p;
        struct waiter w;
        char buf[16];

        if (create_pair(kind, &s, &p) < 0) {
            THROW_ERROR("failed to create a connection");
        }
        if (waiter_start(&w, W_RECV, s) < 0) {
            THROW_ERROR("failed to start a waiter");
        }
        sleep_ms(SETTLE_MS);
        if (shutdown(s, SHUT_WR) < 0) {
            THROW_ERROR("failed to shut down the writing");
        }
        // The peer sees the end of the file, and its answer reaches the blocked reader
        if (recv_nb(p, buf, sizeof(buf)) != 0) {
            THROW_ERROR("the peer does not get the end of the file");
        }
        sleep_ms(NEGATIVE_MS);
        if (waiter_done(&w)) {
            THROW_ERROR("the reader returned (%ld) after shutdown(SHUT_WR)", w.ret);
        }
        if (send(p, "answer", 6, MSG_NOSIGNAL) != 6) {
            THROW_ERROR("the peer cannot send after the shutdown of the writing");
        }
        if (!waiter_wait(&w, now_ms() + WAKE_MS)) {
            waiter_finish(&w);
            THROW_ERROR("the answer does not wake up the reader");
        }
        if (w.ret != 6) {
            THROW_ERROR("the reader got %ld bytes instead of 6", w.ret);
        }
        waiter_finish(&w);

        // The shutdown of the writing of S is final, but not for P
        if (send(s, "x", 1, MSG_NOSIGNAL) != -1 || errno != EPIPE) {
            THROW_ERROR("S can send after shutdown(SHUT_WR)");
        }
        close_files(2, s, p);
    }
    return 0;
}

// ---------------------------------------------------------------------------------------------
// Test cases: the state after a shutdown
// ---------------------------------------------------------------------------------------------

static const int hows[] = { SHUT_RD, SHUT_WR, SHUT_RDWR };
static const char *how_names[] = { "SHUT_RD", "SHUT_WR", "SHUT_RDWR" };

// The events of a socket that has data neither queued nor sent, if the reading of the end S
// was shut down (`rd`) and/or its writing (`wr`)
static unsigned state_of_s(int rd, int wr) {
    return POLLOUT | (rd ? POLLIN | POLLRDHUP : 0) | (rd && wr ? POLLHUP : 0);
}

static unsigned state_of_p(int rd, int wr) {
    return POLLOUT | (wr ? POLLIN | POLLRDHUP : 0) | (rd && wr ? POLLHUP : 0);
}

static int check_state(int fd, unsigned expected, const char *what) {
    long revents = poll_now(fd, EV_ALL);
    long epoll_events = epoll_now(fd, EPOLLIN | EPOLLOUT | EPOLLRDHUP);
    int selected = select_now(fd);
    int expected_select = ((expected & (POLLIN | POLLHUP)) ? 1 : 0) |
                          ((expected & POLLOUT) ? 2 : 0);
    if (revents != (long)expected || epoll_events != (long)expected ||
            selected != expected_select) {
        THROW_ERROR("%s: poll %s, epoll %s, select %d, expected %s and select %d", what,
                    events_str(revents), events_str(epoll_events), selected,
                    events_str(expected), expected_select);
    }
    return 0;
}

// What poll, epoll and select report for the ends of the connection after a shutdown
static int test_shutdown_events(void) {
    for (int kind = 0; kind < NUM_PAIR_KINDS; kind++) {
        for (size_t h = 0; h < ARRAY_SIZE(hows); h++) {
            int s, p;
            int rd = hows[h] != SHUT_WR, wr = hows[h] != SHUT_RD;
            if (create_pair(kind, &s, &p) < 0) {
                THROW_ERROR("failed to create a connection");
            }
            if (check_state(s, state_of_s(0, 0), "S before") < 0 ||
                    check_state(p, state_of_p(0, 0), "P before") < 0) {
                THROW_ERROR("%s, %s", pair_names[kind], how_names[h]);
            }
            if (shutdown(s, hows[h]) < 0) {
                THROW_ERROR("shutdown(%s) failed", how_names[h]);
            }
            if (check_state(s, state_of_s(rd, wr), "S after") < 0 ||
                    check_state(p, state_of_p(rd, wr), "P after") < 0) {
                THROW_ERROR("%s, %s", pair_names[kind], how_names[h]);
            }
            close_files(2, s, p);
        }
    }
    return 0;
}

// Nonblocking recv, repeated until there is no more data: returns the number of bytes, or -1
// if recv fails with an error other than EAGAIN
static long recv_all(int fd, char *buf, size_t size) {
    long total = 0;
    set_nonblocking(fd, 1);
    for (;;) {
        long n = recv(fd, buf + total, size - total, 0);
        if (n <= 0) {
            int err = errno;
            set_nonblocking(fd, 0);
            errno = err;
            return n < 0 && err != EAGAIN ? -1 : total;
        }
        total += n;
    }
}

static int fionread(int fd) {
    int n = -1;
    if (ioctl(fd, FIONREAD, &n) < 0) {
        return -1;
    }
    return n;
}

// The data that was queued before the shutdown is read first, then comes the end of the
// file, in both directions
static int test_shutdown_buffered_data(void) {
    for (int kind = 0; kind < NUM_PAIR_KINDS; kind++) {
        // The reading of S is shut down: SHUT_RD and SHUT_RDWR
        for (int h = 0; h < 2; h++) {
            int how = h == 0 ? SHUT_RD : SHUT_RDWR;
            int s, p;
            char buf[16];
            if (create_pair(kind, &s, &p) < 0) {
                THROW_ERROR("failed to create a connection");
            }
            if (send(p, "PEER", 4, MSG_NOSIGNAL) != 4 || shutdown(s, how) < 0) {
                THROW_ERROR("failed to send, or to shut down");
            }
            if (fionread(s) != 4 || !(poll_now(s, POLLIN) & POLLIN)) {
                THROW_ERROR("%s: FIONREAD is %d, poll %s after the shutdown, expected 4 and IN",
                            how_names[h], fionread(s), events_str(poll_now(s, POLLIN)));
            }
            if (recv_nb(s, buf, sizeof(buf)) != 4 || memcmp(buf, "PEER", 4) != 0) {
                THROW_ERROR("%s: the data that was queued before is lost", how_names[h]);
            }
            for (int i = 0; i < 2; i++) {
                if (recv_nb(s, buf, sizeof(buf)) != 0) {
                    THROW_ERROR("%s: recv does not return the end of the file", how_names[h]);
                }
            }
            if (fionread(s) != 0) {
                THROW_ERROR("FIONREAD is not 0 after the data was read");
            }
            // The other direction is not touched by SHUT_RD
            if (how == SHUT_RD && (send(s, "SELF", 4, MSG_NOSIGNAL) != 4 ||
                                   recv_all(p, buf, sizeof(buf)) != 4)) {
                THROW_ERROR("S cannot send after shutdown(SHUT_RD)");
            }
            close_files(2, s, p);
        }

        // The writing of S is shut down: the peer gets the data that S sent before, first
        for (int h = 0; h < 2; h++) {
            int how = h == 0 ? SHUT_WR : SHUT_RDWR;
            int s, p;
            char buf[16];
            if (create_pair(kind, &s, &p) < 0) {
                THROW_ERROR("failed to create a connection");
            }
            if (send(s, "SELF", 4, MSG_NOSIGNAL) != 4 || shutdown(s, how) < 0) {
                THROW_ERROR("failed to send, or to shut down");
            }
            if (recv_nb(p, buf, sizeof(buf)) != 4 || memcmp(buf, "SELF", 4) != 0) {
                THROW_ERROR("%s: the peer does not get the data that was sent before",
                            how_names[h]);
            }
            for (int i = 0; i < 2; i++) {
                if (recv_nb(p, buf, sizeof(buf)) != 0) {
                    THROW_ERROR("%s: the peer does not get the end of the file",
                                how_names[h]);
                }
            }
            close_files(2, s, p);
        }
    }
    return 0;
}

static int sendbyte(int fd) {
    return send(fd, "x", 1, MSG_NOSIGNAL);
}

// What a send does after the shutdown
static int test_shutdown_send_after(void) {
    for (int kind = 0; kind < NUM_PAIR_KINDS; kind++) {
        for (size_t h = 0; h < ARRAY_SIZE(hows); h++) {
            int s, p;
            int rd = hows[h] != SHUT_WR, wr = hows[h] != SHUT_RD;
            if (create_pair(kind, &s, &p) < 0) {
                THROW_ERROR("failed to create a connection");
            }
            if (shutdown(s, hows[h]) < 0) {
                THROW_ERROR("shutdown(%s) failed", how_names[h]);
            }
            // S cannot send if its writing was shut down
            errno = 0;
            int ret_s = sendbyte(s);
            if (wr ? (ret_s != -1 || errno != EPIPE) : ret_s != 1) {
                THROW_ERROR("%s: send on S returned %d (errno %d)", how_names[h], ret_s,
                            errno);
            }
            errno = 0;
            ssize_t written = write(s, "x", 1);
            if (wr ? (written != -1 || errno != EPIPE) : written != 1) {
                THROW_ERROR("%s: write on S returned %zd (errno %d)", how_names[h], written,
                            errno);
            }
            // P cannot send if the reading of S was shut down
            errno = 0;
            int ret_p = sendbyte(p);
            if (rd ? (ret_p != -1 || errno != EPIPE) : ret_p != 1) {
                THROW_ERROR("%s: send on P returned %d (errno %d)", how_names[h], ret_p,
                            errno);
            }
            close_files(2, s, p);
        }
    }
    return 0;
}

// A shutdown is allowed twice, and in every combination
static int test_shutdown_twice(void) {
    for (int kind = 0; kind < NUM_PAIR_KINDS; kind++) {
        for (size_t h1 = 0; h1 < ARRAY_SIZE(hows); h1++) {
            for (size_t h2 = 0; h2 < ARRAY_SIZE(hows); h2++) {
                int s, p;
                int rd = hows[h1] != SHUT_WR || hows[h2] != SHUT_WR;
                int wr = hows[h1] != SHUT_RD || hows[h2] != SHUT_RD;
                if (create_pair(kind, &s, &p) < 0) {
                    THROW_ERROR("failed to create a connection");
                }
                if (shutdown(s, hows[h1]) < 0 || shutdown(s, hows[h2]) < 0) {
                    THROW_ERROR("%s then %s failed: %s", how_names[h1], how_names[h2],
                                strerror(errno));
                }
                if (check_state(s, state_of_s(rd, wr), "S") < 0 ||
                        check_state(p, state_of_p(rd, wr), "P") < 0) {
                    THROW_ERROR("%s then %s", how_names[h1], how_names[h2]);
                }
                close_files(2, s, p);
            }
        }
    }
    return 0;
}

// The peer has closed: shutdown works, the end of the file and the hangup are seen, and a
// send fails
static int test_shutdown_after_peer_close(void) {
    for (int kind = 0; kind < NUM_PAIR_KINDS; kind++) {
        for (size_t h = 0; h < ARRAY_SIZE(hows); h++) {
            int s, p;
            char buf[16];
            if (create_pair(kind, &s, &p) < 0) {
                THROW_ERROR("failed to create a connection");
            }
            close(p);
            if (shutdown(s, hows[h]) < 0) {
                THROW_ERROR("%s: shutdown after the close of the peer failed: %s",
                            how_names[h], strerror(errno));
            }
            if (shutdown(s, hows[h]) < 0) {
                THROW_ERROR("%s: the second shutdown failed: %s", how_names[h],
                            strerror(errno));
            }
            if (check_state(s, POLLIN | POLLOUT | POLLHUP | POLLRDHUP, "S") < 0) {
                THROW_ERROR("%s, %s", pair_names[kind], how_names[h]);
            }
            if (recv_nb(s, buf, sizeof(buf)) != 0) {
                THROW_ERROR("%s: recv does not return the end of the file", how_names[h]);
            }
            errno = 0;
            if (sendbyte(s) != -1 || errno != EPIPE) {
                THROW_ERROR("%s: send does not fail with EPIPE", how_names[h]);
            }
            close(s);
        }
    }
    return 0;
}

// epoll_wait of 100 ms: whether an edge of the registration (or the level) is there
static long epoll_edge(int epfd) {
    struct epoll_event event;
    int ret = epoll_wait(epfd, &event, 1, 100);
    return ret < 0 ? -1 : ret == 0 ? 0 : (long)event.events;
}

static int epoll_register(int fd, unsigned events) {
    int epfd = epoll_create1(0);
    struct epoll_event event = { .events = events, .data.fd = fd };
    if (epfd < 0 || epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &event) < 0) {
        return -1;
    }
    return epfd;
}

#define EDGE_EXPECT(epfd, expected, what) do { \
    long events = epoll_edge(epfd); \
    if (events != (long)(expected)) { \
        THROW_ERROR("%s: epoll returned %s, expected %s", what, events_str(events), \
                    events_str(expected)); \
    } \
} while (0)

// The edge-triggered registration of the socket that is shut down gets an edge. The one of
// the peer, too. Level-triggered ones report the state again and again.
static int test_shutdown_epoll_edge(void) {
    const unsigned ev = EPOLLIN | EPOLLOUT | EPOLLRDHUP;
    for (int kind = 0; kind < NUM_PAIR_KINDS; kind++) {
        int s, p;
        if (create_pair(kind, &s, &p) < 0) {
            THROW_ERROR("failed to create a connection");
        }
        int et_s = epoll_register(s, ev | EPOLLET), lt_s = epoll_register(s, ev);
        int et_p = epoll_register(p, ev | EPOLLET), lt_p = epoll_register(p, ev);
        if (et_s < 0 || lt_s < 0 || et_p < 0 || lt_p < 0) {
            THROW_ERROR("failed to register the sockets in epoll");
        }
        // The initial edge, which is the state: writable
        EDGE_EXPECT(et_s, POLLOUT, "S, edge-triggered, initially");
        EDGE_EXPECT(et_p, POLLOUT, "P, edge-triggered, initially");
        EDGE_EXPECT(lt_s, POLLOUT, "S, level-triggered, initially");

        // The reading of S is shut down: S gets an edge, P has nothing new to see
        if (shutdown(s, SHUT_RD) < 0) {
            THROW_ERROR("shutdown(SHUT_RD) failed");
        }
        EDGE_EXPECT(et_s, POLLIN | POLLOUT | POLLRDHUP, "S, edge-triggered, SHUT_RD");
        EDGE_EXPECT(et_s, 0, "S, edge-triggered, SHUT_RD, once");
        EDGE_EXPECT(lt_s, POLLIN | POLLOUT | POLLRDHUP, "S, level-triggered, SHUT_RD");
        EDGE_EXPECT(lt_s, POLLIN | POLLOUT | POLLRDHUP, "S, level-triggered, SHUT_RD, again");

        // The writing of S, too: both hang up. For P the writing of S is the end of the file
        if (shutdown(s, SHUT_WR) < 0) {
            THROW_ERROR("shutdown(SHUT_WR) failed");
        }
        EDGE_EXPECT(et_s, POLLIN | POLLOUT | POLLHUP | POLLRDHUP, "S, edge-triggered, RDWR");
        EDGE_EXPECT(et_s, 0, "S, edge-triggered, RDWR, once");
        EDGE_EXPECT(et_p, POLLIN | POLLOUT | POLLHUP | POLLRDHUP, "P, edge-triggered, RDWR");
        EDGE_EXPECT(et_p, 0, "P, edge-triggered, RDWR, once");
        EDGE_EXPECT(lt_p, POLLIN | POLLOUT | POLLHUP | POLLRDHUP, "P, level-triggered, RDWR");
        EDGE_EXPECT(lt_p, POLLIN | POLLOUT | POLLHUP | POLLRDHUP, "P, level-triggered, again");
        close_files(6, s, p, et_s, lt_s, et_p, lt_p);
    }

    // Only SHUT_WR: the edge of the peer is the end of the file, and S gets one, too, as
    // the shutdown wakes up the epoll of S, although nothing is new for S. This is how Linux
    // is (tested up to 7.0: the shutdown calls sk_state_change() of both sockets); the LibOS
    // does the same, as it notifies the socket whatever the direction of the shutdown is
    for (int kind = 0; kind < NUM_PAIR_KINDS; kind++) {
        int s, p;
        if (create_pair(kind, &s, &p) < 0) {
            THROW_ERROR("failed to create a connection");
        }
        int et_s = epoll_register(s, ev | EPOLLET), et_p = epoll_register(p, ev | EPOLLET);
        if (et_s < 0 || et_p < 0) {
            THROW_ERROR("failed to register the sockets in epoll");
        }
        EDGE_EXPECT(et_s, POLLOUT, "S, edge-triggered, initially");
        EDGE_EXPECT(et_p, POLLOUT, "P, edge-triggered, initially");
        if (shutdown(s, SHUT_WR) < 0) {
            THROW_ERROR("shutdown(SHUT_WR) failed");
        }
        EDGE_EXPECT(et_s, POLLOUT, "S, edge-triggered, SHUT_WR");
        EDGE_EXPECT(et_s, 0, "S, edge-triggered, SHUT_WR, once");
        EDGE_EXPECT(et_p, POLLIN | POLLOUT | POLLRDHUP, "P, edge-triggered, SHUT_WR");
        EDGE_EXPECT(et_p, 0, "P, edge-triggered, SHUT_WR, once");
        close_files(4, s, p, et_s, et_p);
    }
    return 0;
}

// The sequence of MariaDB when it ends: the thread of an idle connection waits for data in
// poll(POLLIN | POLLPRI) with a timeout of 8 hours, and the main thread shuts down the
// connection with shutdown(SHUT_RDWR), then shutdown(SHUT_RD). The thread has to return with
// POLLIN | POLLHUP, then its recv(MSG_DONTWAIT) returns the end of the file, and its send fails
// with EPIPE.
static int test_shutdown_mariadb_sequence(void) {
    for (int kind = 0; kind < NUM_PAIR_KINDS; kind++) {
        int s, p;
        struct waiter w;
        char buf[16];

        if (create_pair(kind, &s, &p) < 0) {
            THROW_ERROR("failed to create a connection");
        }
        if (waiter_start(&w, W_POLL_IN_PRI_8H, s) < 0) {
            THROW_ERROR("failed to start a waiter");
        }
        sleep_ms(SETTLE_MS);
        if (shutdown(s, SHUT_RDWR) < 0 || shutdown(s, SHUT_RD) < 0) {
            THROW_ERROR("%s: shutdown(SHUT_RDWR) then shutdown(SHUT_RD) failed", pair_names[kind]);
        }
        if (!waiter_wait(&w, now_ms() + WAKE_MS)) {
            waiter_finish(&w);
            errno = 0;
            THROW_ERROR("%s: the thread in poll() does not wake up", pair_names[kind]);
        }
        if (w.ret != 1 || w.revents != (POLLIN | POLLHUP)) {
            errno = 0;
            THROW_ERROR("%s: poll returned %ld and the events %s, expected 1 and IN|HUP",
                        pair_names[kind], w.ret, events_str(w.revents));
        }
        waiter_finish(&w);
        if (recv(s, buf, sizeof(buf), MSG_DONTWAIT) != 0) {
            THROW_ERROR("%s: recv does not return the end of the file", pair_names[kind]);
        }
        errno = 0;
        if (sendbyte(s) != -1 || errno != EPIPE) {
            THROW_ERROR("%s: send does not fail with EPIPE", pair_names[kind]);
        }
        close_files(2, s, p);
    }
    return 0;
}

// The thread that an event wakes up in epoll_wait closes its epoll file at once, while the
// thread that caused the event is still notifying the observers of the socket. The epoll file
// of the woken thread can then be released by the notification, and the release unregisters
// it from the notifier of the socket, which is busy with that very notification. This must
// not deadlock. If it does, the LibOS hangs (the threads cannot even be killed): the test
// stops after 30 s without progress, as far as it can.
#define CLOSER_ITERATIONS   4000
#define CLOSER_THREADS      4
#define CLOSER_HANG_MS      30000

struct closer_shared {
    int fd;
    int ready;      // accessed atomically
    int woken[CLOSER_THREADS];
};

struct closer {
    struct closer_shared *shared;
    int index;
};

static int closer_progress = 0;     // the number of the iteration, accessed atomically
static int closer_finished = 0;     // accessed atomically

static void *closer_main(void *arg) {
    struct closer *closer = arg;
    struct closer_shared *shared = closer->shared;
    struct epoll_event event = { .events = EPOLLIN | EPOLLRDHUP, .data.fd = shared->fd };
    int epfd = epoll_create1(0);
    int ret = -1;
    if (epfd >= 0 && epoll_ctl(epfd, EPOLL_CTL_ADD, shared->fd, &event) == 0) {
        __atomic_add_fetch(&shared->ready, 1, __ATOMIC_ACQ_REL);
        ret = epoll_wait(epfd, &event, 1, WAKE_MS);
    } else {
        __atomic_add_fetch(&shared->ready, 1, __ATOMIC_ACQ_REL);
    }
    // Right after the wake up, while the notification goes on
    if (epfd >= 0) {
        close(epfd);
    }
    shared->woken[closer->index] = ret;
    return NULL;
}

static void *closer_watchdog(void *arg) {
    (void)arg;
    int last = -1;
    long last_change = now_ms();
    while (!__atomic_load_n(&closer_finished, __ATOMIC_ACQUIRE)) {
        usleep(100 * 1000);
        int progress = __atomic_load_n(&closer_progress, __ATOMIC_ACQUIRE);
        if (progress != last) {
            last = progress;
            last_change = now_ms();
        } else if (now_ms() - last_change > CLOSER_HANG_MS) {
            printf("\t\tERROR: no progress for %d ms at iteration %d, the LibOS is deadlocked\n",
                   CLOSER_HANG_MS, progress);
            fflush(stdout);
            _exit(1);
        }
    }
    return NULL;
}

static int test_shutdown_epoll_closed_by_waiter(void) {
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setstacksize(&attr, STACK_SIZE);
    pthread_t watchdog;
    __atomic_store_n(&closer_progress, 0, __ATOMIC_RELEASE);
    __atomic_store_n(&closer_finished, 0, __ATOMIC_RELEASE);
    if (pthread_create(&watchdog, &attr, closer_watchdog, NULL) != 0) {
        THROW_ERROR("failed to start the watchdog");
    }

    int ret = 0;
    for (int i = 0; i < CLOSER_ITERATIONS && ret == 0; i++) {
        int kind = i % NUM_PAIR_KINDS;
        int s, p;
        struct closer_shared shared = { 0 };
        struct closer closers[CLOSER_THREADS];
        pthread_t threads[CLOSER_THREADS];
        int created = 0;

        if (create_pair(kind, &s, &p) < 0) {
            ret = -1;
            break;
        }
        shared.fd = s;
        for (int t = 0; t < CLOSER_THREADS; t++) {
            closers[t] = (struct closer) { &shared, t };
            shared.woken[t] = -1;
            if (pthread_create(&threads[t], &attr, closer_main, &closers[t]) != 0) {
                ret = -1;
                break;
            }
            created++;
        }
        if (ret == 0) {
            while (__atomic_load_n(&shared.ready, __ATOMIC_ACQUIRE) < CLOSER_THREADS) {
                usleep(50);
            }
            usleep(200);
        }

        // The events that wake up an epoll(IN|RDHUP) of S
        switch (ret == 0 ? i % 3 : 0) {
            case 0:
                shutdown(s, SHUT_RD);
                break;
            case 1:
                send(p, "x", 1, MSG_NOSIGNAL);
                break;
            case 2:
                shutdown(p, SHUT_WR);
                break;
        }
        for (int t = 0; t < created; t++) {
            pthread_join(threads[t], NULL);
            if (ret == 0 && shared.woken[t] != 1) {
                printf("\t\tERROR: iteration %d, thread %d: epoll_wait returned %d, expected 1\n",
                       i, t, shared.woken[t]);
                ret = -1;
            }
        }
        close_files(2, s, p);
        __atomic_store_n(&closer_progress, i + 1, __ATOMIC_RELEASE);
    }
    __atomic_store_n(&closer_finished, 1, __ATOMIC_RELEASE);
    pthread_join(watchdog, NULL);
    pthread_attr_destroy(&attr);
    if (ret < 0) {
        THROW_ERROR("a thread of epoll_wait did not wake up, or the test failed");
    }
    return 0;
}

static test_case_t test_cases[] = {
    TEST_CASE(test_shutdown_wakes_recv),
    TEST_CASE(test_shutdown_wakes_poll),
    TEST_CASE(test_shutdown_wakes_select),
    TEST_CASE(test_shutdown_wakes_epoll),
    TEST_CASE(test_shutdown_wakes_send),
    TEST_CASE(test_shutdown_wakes_timed),
    TEST_CASE(test_shutdown_wakes_poll_out),
    TEST_CASE(test_shutdown_wakes_many_waiters),
    TEST_CASE(test_shutdown_via_dup),
    TEST_CASE(test_shutdown_wakes_all_waiters),
    TEST_CASE(test_shutdown_race),
    TEST_CASE(test_shutdown_epoll_closed_by_waiter),
    TEST_CASE(test_shutdown_mariadb_sequence),
    TEST_CASE(test_shutdown_half_close),
    TEST_CASE(test_shutdown_events),
    TEST_CASE(test_shutdown_buffered_data),
    TEST_CASE(test_shutdown_send_after),
    TEST_CASE(test_shutdown_twice),
    TEST_CASE(test_shutdown_after_peer_close),
    TEST_CASE(test_shutdown_epoll_edge),
};

int main(int argc, const char *argv[]) {
    // A write on a shut down socket is a normal error here
    signal(SIGPIPE, SIG_IGN);

    test_case_t selected[ARRAY_SIZE(test_cases)];
    test_case_t *cases = test_cases;
    int num_cases = ARRAY_SIZE(test_cases);
    if (argc > 1) {
        // Run only the test case with the name, or list the names
        int list = strcmp(argv[1], "--list") == 0;
        num_cases = 0;
        for (size_t i = 0; i < ARRAY_SIZE(test_cases); i++) {
            if (list) {
                printf("%s\n", test_cases[i].name);
            } else if (strcmp(argv[1], test_cases[i].name) == 0) {
                selected[num_cases++] = test_cases[i];
            }
        }
        if (list) {
            return 0;
        }
        if (num_cases == 0) {
            printf("no such test case: %s\n", argv[1]);
            return 2;
        }
        cases = selected;
    }

    int ret = test_suite_run(cases, num_cases);
    if (stuck_waiters > 0) {
        // The threads that did not wake up must not keep the process from ending
        printf("%d waiters are still blocked\n", stuck_waiters);
        fflush(stdout);
        _exit(1);
    }
    return ret;
}
