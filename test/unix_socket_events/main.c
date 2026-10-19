// What unix stream sockets do around shutdown() and close(): the events that poll() and epoll tell,
// also while a thread shuts a socket down and with a full buffer; the calls on sockets that are not
// connected; the reset of a connection whose peer is closed with bytes that it did not read; and
// what the threads that wait are woken up with. The expectations are the behavior of Linux (checked
// on Linux 7.0): the test passes there, too (gcc -pthread -I../include main.c).
//
// A call that may block runs in a thread, so that a call that does not return when it should makes
// the test case fail after a timeout instead of hanging it. A watchdog thread ends the test if a
// test case takes too long.
//
// Usage: unix_socket_events [test case name | --list]
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>
#include <linux/sockios.h>

#include "test.h"

#define WATCHDOG_MS     120000  // time that a test case may take

// What the test case is doing, which the errors tell
static const char *context = "";

#define CHECK(cond, fmt, ...) do { \
        if (!(cond)) { \
            THROW_ERROR("%s: " fmt, context, ##__VA_ARGS__); \
        } \
    } while (0)

// ---------------------------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------------------------

static atomic_long watchdog_deadline;

static const char *current_case = "";

static long now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
}

static void sleep_ms(int ms) {
    usleep(ms * 1000);
}

static void *watchdog_main(void *arg) {
    for (;;) {
        sleep_ms(100);
        long deadline = atomic_load(&watchdog_deadline);
        if (deadline != 0 && now_ms() > deadline) {
            printf("\t\tERROR:test case %s did not finish in %d ms, it hangs\n",
                   current_case, WATCHDOG_MS);
            fflush(stdout);
            _exit(1);
        }
    }
    return NULL;
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
        { POLLRDHUP, "RDHUP" }, { POLLNVAL, "NVAL" },
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

#define EV_ALL  (POLLIN | POLLOUT | POLLRDHUP)
#define EV_IN   POLLIN
#define EV_OUT  POLLOUT
#define EV_HUP  (POLLIN | POLLHUP | POLLRDHUP)

// The events of the socket, all that poll() has for it, at once. -1 if poll() fails.
static long poll_now(int fd) {
    struct pollfd pfd = { .fd = fd, .events = EV_ALL };
    int ret = poll(&pfd, 1, 0);
    return ret < 0 ? -1 : (long)(unsigned short)pfd.revents;
}

// Checks the events of a socket
#define CHECK_EVENTS(fd, expected, what) do { \
        long __events = poll_now(fd); \
        unsigned __expected = (expected); \
        CHECK(__events == (long)__expected, "%s: the events are %s, instead of %s", what, \
              __events < 0 ? "an error" : events_str(__events), events_str(__expected)); \
    } while (0)

static void set_nonblocking(int fd, int nonblocking) {
    int flags = fcntl(fd, F_GETFL);
    fcntl(fd, F_SETFL, nonblocking ? (flags | O_NONBLOCK) : (flags & ~O_NONBLOCK));
}

typedef struct {
    struct sockaddr_un sa;
    socklen_t len;
} unix_addr_t;

// A new abstract address, which needs no file
static unix_addr_t new_addr(void) {
    static int counter;
    unix_addr_t a;
    memset(&a, 0, sizeof(a));
    a.sa.sun_family = AF_UNIX;
    int n = snprintf(a.sa.sun_path + 1, sizeof(a.sa.sun_path) - 1,
                     "unix_socket_events.%d.%d", (int)getpid(), counter++);
    a.len = offsetof(struct sockaddr_un, sun_path) + 1 + n;
    return a;
}

// How the two ends of a connection are made
enum { PAIR_SOCKETPAIR, PAIR_CLIENT_FIRST, PAIR_ACCEPTED_FIRST, NUM_PAIR_KINDS };
static const char *pair_names[] = { "socketpair", "client, accepted", "accepted, client" };

// Makes a connection. `a` and `b` are its ends: for PAIR_CLIENT_FIRST, `a` is the client and `b` the
// socket that accept() returns; for PAIR_ACCEPTED_FIRST, it is the other way around. Returns 0, or
// -1 on failure.
static int make_pair(int kind, int *a, int *b) {
    int sv[2];
    if (kind == PAIR_SOCKETPAIR) {
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) {
            return -1;
        }
        *a = sv[0];
        *b = sv[1];
        return 0;
    }

    unix_addr_t addr = new_addr();
    int listen_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    int client_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    int accepted_fd = -1;
    if (listen_fd >= 0 && client_fd >= 0 &&
            bind(listen_fd, (struct sockaddr *)&addr.sa, addr.len) == 0 &&
            listen(listen_fd, 4) == 0 &&
            connect(client_fd, (struct sockaddr *)&addr.sa, addr.len) == 0) {
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
    *a = kind == PAIR_CLIENT_FIRST ? client_fd : accepted_fd;
    *b = kind == PAIR_CLIENT_FIRST ? accepted_fd : client_fd;
    return 0;
}

// Fills the send buffer of the socket, so that a write would block
static int fill(int fd) {
    static char buf[4096];
    int filled = 0;
    for (int i = 0; i < 64 * 1024 && !filled; i++) {
        if (send(fd, buf, sizeof(buf), MSG_NOSIGNAL | MSG_DONTWAIT) < 0) {
            if (errno != EAGAIN) {
                return -1;
            }
            filled = 1;
        }
    }
    return filled ? 0 : -1;
}

// Reads all the bytes that there are
static long drain(int fd) {
    static char buf[8192];
    long total = 0;
    for (;;) {
        ssize_t n = recv(fd, buf, sizeof(buf), MSG_DONTWAIT);
        if (n <= 0) {
            return total;
        }
        total += n;
    }
}

// ---------------------------------------------------------------------------------------------
// The state that poll() sees while a thread shuts a socket down
// ---------------------------------------------------------------------------------------------

typedef struct {
    int fds[2];
    atomic_int stop;
    atomic_int started;
    long polls;
    long bad;               // the polls that saw a state which is neither before nor after
    unsigned bad_events;
    int bad_fd;
} poller_t;

// The states of a connection of two empty sockets: before a shutdown of both directions, and
// after it. Linux changes all the flags of both ends together, so poll() sees nothing else.
static int is_whole_state(unsigned events) {
    return events == EV_OUT || events == (POLLIN | POLLOUT | POLLRDHUP | POLLHUP);
}

static void *poller_main(void *arg) {
    poller_t *p = arg;
    atomic_store(&p->started, 1);
    while (!atomic_load(&p->stop)) {
        for (int i = 0; i < 2; i++) {
            long events = poll_now(p->fds[i]);
            p->polls++;
            if (!is_whole_state(events) && p->bad++ == 0) {
                p->bad_events = events;
                p->bad_fd = i;
            }
        }
    }
    return NULL;
}

// A thread polls both ends of a connection while the main thread shuts down one end for reading
// and writing. A poll sees the state before the shutdown, or after it: never the end of the file
// (IN and RDHUP) without the hangup (HUP), as that is a state that no end has.
static int test_shutdown_changes_the_events_at_once(void) {
    context = "socketpair";
    for (int i = 0; i < 150; i++) {
        poller_t p;
        memset(&p, 0, sizeof(p));
        CHECK(socketpair(AF_UNIX, SOCK_STREAM, 0, p.fds) == 0, "failed to make a socket pair");
        pthread_t thread;
        CHECK(pthread_create(&thread, NULL, poller_main, &p) == 0, "failed to start a thread");
        while (!atomic_load(&p.started)) {
            usleep(50);
        }
        // The shutdown comes at another time of the loop of the poller each time
        usleep(100 + (i % 10) * 40);
        int ret = shutdown(p.fds[i % 2], SHUT_RDWR);
        usleep(200);
        atomic_store(&p.stop, 1);
        pthread_join(thread, NULL);
        CHECK(ret == 0, "failed to shut down the socket");
        CHECK(p.bad == 0, "poll() of the end %d returned %s while the socket was shut down, "
              "%ld times in %ld polls", p.bad_fd, events_str(p.bad_events), p.bad, p.polls);
        // The state after the shutdown stays
        CHECK_EVENTS(p.fds[0], POLLIN | POLLOUT | POLLRDHUP | POLLHUP, "after the shutdown");
        CHECK_EVENTS(p.fds[1], POLLIN | POLLOUT | POLLRDHUP | POLLHUP, "after the shutdown");
        close(p.fds[0]);
        close(p.fds[1]);
    }
    return 0;
}

// ---------------------------------------------------------------------------------------------
// The events of a socket with a full buffer
// ---------------------------------------------------------------------------------------------

// A socket whose send buffer is full is not writable, whether it has been shut down for writing or
// not: the shutdown does not make room. A hangup is reported, but no POLLOUT.
static int test_full_buffer_is_not_writable_after_shutdown(void) {
    static const struct {
        int how;
        const char *name;
        unsigned events;
    } hows[] = {
        { SHUT_WR, "SHUT_WR", 0 },
        { SHUT_RDWR, "SHUT_RDWR", POLLIN | POLLHUP | POLLRDHUP },
    };
    for (int kind = 0; kind < NUM_PAIR_KINDS; kind++) {
        context = pair_names[kind];
        for (size_t i = 0; i < ARRAY_SIZE(hows); i++) {
            int a, b;
            CHECK(make_pair(kind, &a, &b) == 0, "failed to make a connection");
            CHECK(fill(a) == 0, "failed to fill the buffer");
            CHECK_EVENTS(a, 0, "full buffer");
            CHECK(shutdown(a, hows[i].how) == 0, "failed to shut down the socket");
            CHECK_EVENTS(a, hows[i].events, hows[i].name);
            // The other end reads what is in the buffer, and then, there is room
            drain(b);
            CHECK_EVENTS(a, hows[i].events | POLLOUT, hows[i].name);
            close(a);
            close(b);
        }
    }
    return 0;
}

// The same for the end whose peer shuts down: its own buffer is full, and it is writable only when
// there is room
static int test_full_buffer_of_the_peer_of_a_shutdown(void) {
    for (int kind = 0; kind < NUM_PAIR_KINDS; kind++) {
        context = pair_names[kind];
        // The peer shuts down writing: the end reads the end of the file, and still cannot write
        int a, b;
        CHECK(make_pair(kind, &a, &b) == 0, "failed to make a connection");
        CHECK(fill(b) == 0, "failed to fill the buffer");
        CHECK(shutdown(a, SHUT_WR) == 0, "failed to shut down the socket");
        CHECK_EVENTS(b, POLLIN | POLLRDHUP, "peer of SHUT_WR");
        drain(a);
        CHECK_EVENTS(b, POLLIN | POLLOUT | POLLRDHUP, "peer of SHUT_WR, drained");
        close(a);
        close(b);

        // The peer shuts down both ways
        CHECK(make_pair(kind, &a, &b) == 0, "failed to make a connection");
        CHECK(fill(b) == 0, "failed to fill the buffer");
        CHECK(shutdown(a, SHUT_RDWR) == 0, "failed to shut down the socket");
        CHECK_EVENTS(b, POLLIN | POLLHUP | POLLRDHUP, "peer of SHUT_RDWR");
        close(a);
        close(b);

        // The peer shuts down reading: nothing arrives any more, which is not an end of the file
        CHECK(make_pair(kind, &a, &b) == 0, "failed to make a connection");
        CHECK(fill(b) == 0, "failed to fill the buffer");
        CHECK(shutdown(a, SHUT_RD) == 0, "failed to shut down the socket");
        CHECK_EVENTS(b, 0, "peer of SHUT_RD");
        close(a);
        close(b);
    }
    return 0;
}

// ---------------------------------------------------------------------------------------------
// Sockets that are not connected
// ---------------------------------------------------------------------------------------------

// The name of an error that a call returned, or its value
static const char *result_str(long result) {
    static __thread char bufs[4][32];
    static __thread int next;
    char *buf = bufs[next++ % 4];
    static const struct {
        int err;
        const char *name;
    } names[] = {
        { EAGAIN, "EAGAIN" }, { ECONNREFUSED, "ECONNREFUSED" }, { ECONNRESET, "ECONNRESET" },
        { EINVAL, "EINVAL" }, { ENOTCONN, "ENOTCONN" }, { EPIPE, "EPIPE" },
    };
    if (result < 0) {
        for (size_t i = 0; i < ARRAY_SIZE(names); i++) {
            if (names[i].err == -result) {
                snprintf(buf, sizeof(bufs[0]), "-%s", names[i].name);
                return buf;
            }
        }
    }
    snprintf(buf, sizeof(bufs[0]), "%ld", result);
    return buf;
}

// Checks what a call returns: a count, or the negative errno
#define CHECK_CALL(call, expected, what) do { \
        errno = 0; \
        long __ret = (call); \
        long __got = __ret < 0 ? -(long)errno : __ret; \
        long __want = (expected); \
        CHECK(__got == __want, "%s: %s returned %s, instead of %s", what, #call, \
              result_str(__got), result_str(__want)); \
    } while (0)

static int bind_to(int fd, const unix_addr_t *a) {
    return bind(fd, (const struct sockaddr *)&a->sa, a->len);
}

// A socket that is not connected: just created, or bound, or listening
enum { SOCK_IDLE, SOCK_BOUND, SOCK_LISTENING, NUM_SOCK_KINDS };
static const char *sock_names[] = { "idle", "bound", "listening" };

static int make_socket(int kind, unix_addr_t *a) {
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    *a = new_addr();
    if (fd >= 0 && kind != SOCK_IDLE && bind_to(fd, a) < 0) {
        close(fd);
        return -1;
    }
    if (fd >= 0 && kind == SOCK_LISTENING && listen(fd, 4) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

// The calls on a socket that is not connected, or that listens, which cannot move bytes: reading
// fails with EINVAL, but no bytes is no error, and writing fails with ENOTCONN. FIONREAD says that
// an idle socket has no bytes, and fails for a listening one. SIOCOUTQ and SIOCATMARK work.
static int test_calls_on_an_idle_or_listening_socket(void) {
    for (int kind = SOCK_IDLE; kind < NUM_SOCK_KINDS; kind++) {
        unix_addr_t a;
        int fd = make_socket(kind, &a);
        CHECK(fd >= 0, "failed to make a socket");
        context = sock_names[kind];
        char buf[16];
        struct iovec iov = { buf, sizeof(buf) };
        struct iovec empty = { buf, 0 };
        struct msghdr msg = { .msg_iov = &iov, .msg_iovlen = 1 };

        CHECK_CALL(read(fd, buf, sizeof(buf)), -EINVAL, "read");
        CHECK_CALL(readv(fd, &iov, 1), -EINVAL, "readv");
        CHECK_CALL(recv(fd, buf, sizeof(buf), 0), -EINVAL, "recv");
        CHECK_CALL(recv(fd, buf, sizeof(buf), MSG_PEEK | MSG_DONTWAIT), -EINVAL, "recv peek");
        CHECK_CALL(recvfrom(fd, buf, sizeof(buf), 0, NULL, NULL), -EINVAL, "recvfrom");
        CHECK_CALL(recvmsg(fd, &msg, 0), -EINVAL, "recvmsg");
        // Reading no bytes returns at once
        CHECK_CALL(read(fd, buf, 0), 0, "read of no bytes");
        CHECK_CALL(readv(fd, &empty, 1), 0, "readv of no bytes");
        CHECK_CALL(write(fd, "x", 1), -ENOTCONN, "write");
        CHECK_CALL(writev(fd, &iov, 1), -ENOTCONN, "writev");
        CHECK_CALL(send(fd, "x", 1, MSG_NOSIGNAL), -ENOTCONN, "send");
        CHECK_CALL(sendmsg(fd, &msg, MSG_NOSIGNAL), -ENOTCONN, "sendmsg");

        int n = -1;
        CHECK_CALL(ioctl(fd, FIONREAD, &n), kind == SOCK_LISTENING ? -EINVAL : 0, "FIONREAD");
        CHECK(kind == SOCK_LISTENING || n == 0, "FIONREAD returns %d bytes", n);
        n = -1;
        CHECK_CALL(ioctl(fd, SIOCOUTQ, &n), 0, "SIOCOUTQ");
        CHECK(n == 0, "SIOCOUTQ returns %d bytes", n);
        n = -1;
        CHECK_CALL(ioctl(fd, SIOCATMARK, &n), 0, "SIOCATMARK");
        CHECK(n == 0, "SIOCATMARK returns %d", n);
        close(fd);
    }
    return 0;
}

// FIONREAD and SIOCOUTQ of a connected socket: the bytes that are not read by the socket, and
// those of the socket that its peer has not read
static int test_queued_bytes_of_a_connection(void) {
    for (int kind = 0; kind < NUM_PAIR_KINDS; kind++) {
        int a, b;
        context = pair_names[kind];
        CHECK(make_pair(kind, &a, &b) == 0, "failed to make a connection");
        int n = -1;
        CHECK_CALL(ioctl(a, SIOCOUTQ, &n), 0, "SIOCOUTQ");
        CHECK(n == 0, "SIOCOUTQ returns %d bytes of an empty socket", n);
        CHECK_CALL(write(a, "hello", 5), 5, "write");
        CHECK_CALL(ioctl(b, FIONREAD, &n), 0, "FIONREAD");
        CHECK(n == 5, "FIONREAD returns %d bytes, instead of 5", n);
        CHECK_CALL(ioctl(a, SIOCOUTQ, &n), 0, "SIOCOUTQ");
        CHECK(n > 0, "SIOCOUTQ returns %d bytes while the peer has not read 5", n);
        CHECK_CALL(ioctl(a, FIONREAD, &n), 0, "FIONREAD of the writer");
        CHECK(n == 0, "FIONREAD of the writer returns %d bytes", n);
        char buf[16];
        CHECK_CALL(read(b, buf, sizeof(buf)), 5, "read");
        CHECK_CALL(ioctl(a, SIOCOUTQ, &n), 0, "SIOCOUTQ");
        CHECK(n == 0, "SIOCOUTQ returns %d bytes after the peer has read them", n);
        CHECK_CALL(ioctl(a, SIOCATMARK, &n), 0, "SIOCATMARK");
        CHECK(n == 0, "SIOCATMARK returns %d", n);
        close(a);
        close(b);
    }
    return 0;
}

static int connect_to(int fd, const unix_addr_t *a) {
    return connect(fd, (const struct sockaddr *)&a->sa, a->len);
}

static const struct {
    int how;
    const char *name;
} all_hows[] = { { SHUT_RD, "SHUT_RD" }, { SHUT_WR, "SHUT_WR" }, { SHUT_RDWR, "SHUT_RDWR" } };

// shutdown() of a socket that is not connected succeeds, and its flags stay: a read fails with
// EINVAL, a write with ENOTCONN, the socket has hung up and is readable (the end of the file) if
// its reading was shut down
static int test_shutdown_of_an_idle_or_bound_socket(void) {
    for (int kind = SOCK_IDLE; kind <= SOCK_BOUND; kind++) {
        for (size_t i = 0; i < ARRAY_SIZE(all_hows); i++) {
            unix_addr_t a;
            int fd = make_socket(kind, &a);
            CHECK(fd >= 0, "failed to make a socket");
            char buf[16];
            int how = all_hows[i].how;
            int reads = how != SHUT_WR;
            unsigned events = POLLOUT | POLLHUP | (reads ? POLLIN | POLLRDHUP : 0);
            char what[64];
            snprintf(what, sizeof(what), "%s, %s", sock_names[kind], all_hows[i].name);
            context = what;

            CHECK_EVENTS(fd, POLLOUT | POLLHUP, "before the shutdown");
            CHECK_CALL(shutdown(fd, how), 0, "shutdown");
            CHECK_EVENTS(fd, events, "after the shutdown");
            CHECK_CALL(read(fd, buf, sizeof(buf)), -EINVAL, "read");
            CHECK_CALL(recv(fd, buf, sizeof(buf), MSG_DONTWAIT), -EINVAL, "recv");
            CHECK_CALL(write(fd, "x", 1), -ENOTCONN, "write");
            CHECK_CALL(send(fd, "x", 1, MSG_NOSIGNAL), -ENOTCONN, "send");
            // It is not an error to shut down again, or in another way
            CHECK_CALL(shutdown(fd, how), 0, "shutdown again");
            CHECK_EVENTS(fd, events, "after the shutdown again");
            int other = (how + 1) % 3;
            CHECK_CALL(shutdown(fd, other), 0, "shutdown in another way");
            int shut_rd = reads || other != SHUT_WR;
            CHECK_EVENTS(fd, POLLOUT | POLLHUP | (shut_rd ? POLLIN | POLLRDHUP : 0),
                         "after both");
            close(fd);
        }
    }
    return 0;
}

// A socket that was shut down stays so when it is connected: the end of its file is read, or its
// writes fail, and the peer cannot write to a socket whose reading was shut down
static int test_shutdown_of_an_idle_socket_stays_after_connect(void) {
    for (int bound = 0; bound <= 1; bound++) {
        for (size_t i = 0; i < ARRAY_SIZE(all_hows); i++) {
            unix_addr_t a;
            int l = make_socket(SOCK_LISTENING, &a);
            CHECK(l >= 0, "failed to listen");
            int c = socket(AF_UNIX, SOCK_STREAM, 0);
            unix_addr_t own = new_addr();
            CHECK(c >= 0 && (!bound || bind_to(c, &own) == 0), "failed to make a socket");
            int how = all_hows[i].how;
            char what[64];
            snprintf(what, sizeof(what), "%s, %s", bound ? "bound" : "idle", all_hows[i].name);
            context = what;
            char buf[16];

            CHECK_CALL(shutdown(c, how), 0, "shutdown");
            CHECK_CALL(connect_to(c, &a), 0, "connect");
            int s = accept(l, NULL, NULL);
            CHECK(s >= 0, "failed to accept");
            if (how != SHUT_WR) {
                // The end of the file, and the peer is not read by its server
                CHECK_CALL(recv(c, buf, sizeof(buf), MSG_DONTWAIT), 0, "recv");
                CHECK_EVENTS(c, POLLIN | POLLOUT | POLLRDHUP | (how == SHUT_RDWR ? POLLHUP : 0),
                             "events of the client");
                CHECK_CALL(send(s, "x", 1, MSG_NOSIGNAL), -EPIPE, "send by the server");
            } else {
                CHECK_EVENTS(c, POLLOUT, "events of the client");
            }
            if (how != SHUT_RD) {
                CHECK_CALL(send(c, "x", 1, MSG_NOSIGNAL), -EPIPE, "send");
            } else {
                CHECK_CALL(send(c, "x", 1, MSG_NOSIGNAL), 1, "send");
            }
            close(c);
            close(s);
            close(l);
        }
    }
    return 0;
}

// A socket that was shut down listens shut down: if its reading was shut down, connections are
// refused, and accept() fails at once
static int test_shutdown_of_a_bound_socket_stays_after_listen(void) {
    for (size_t i = 0; i < ARRAY_SIZE(all_hows); i++) {
        unix_addr_t a;
        int l = make_socket(SOCK_BOUND, &a);
        CHECK(l >= 0, "failed to make a socket");
        int how = all_hows[i].how;
        context = all_hows[i].name;
        CHECK_CALL(shutdown(l, how), 0, "shutdown");
        CHECK_CALL(listen(l, 4), 0, "listen");
        // accept() does not wait, so that it fails if it is not told that the listener is shut down
        fcntl(l, F_SETFL, fcntl(l, F_GETFL) | O_NONBLOCK);
        int c = socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0);
        CHECK(c >= 0, "failed to make a socket");
        if (how == SHUT_WR) {
            CHECK_EVENTS(l, 0, "events of the listener");
            CHECK_CALL(connect_to(c, &a), 0, "connect");
            CHECK_EVENTS(l, POLLIN, "events of the listener with a connection");
        } else {
            unsigned events = POLLIN | POLLRDHUP | (how == SHUT_RDWR ? POLLHUP : 0);
            CHECK_EVENTS(l, events, "events of the listener");
            CHECK_CALL(connect_to(c, &a), -ECONNREFUSED, "connect");
            CHECK_CALL(accept(l, NULL, NULL), -EAGAIN, "accept");
        }
        close(c);
        close(l);
    }
    return 0;
}

// ---------------------------------------------------------------------------------------------
// A connection that is reset
// ---------------------------------------------------------------------------------------------

// Reads the error of the socket, which is cleared by it
static int so_error(int fd) {
    int err = -1;
    socklen_t len = sizeof(err);
    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0) {
        return -1;
    }
    return err;
}

#define CHECK_SO_ERROR(fd, expected, what) do { \
        int __err = so_error(fd); \
        CHECK(__err == (expected), "%s: SO_ERROR is %d, instead of %d", what, __err, expected); \
    } while (0)

// The events of a connection whose peer was closed (the end of the file, and a hangup), with the
// error of a reset if there is one
#define EV_CLOSED       (POLLIN | POLLOUT | POLLHUP | POLLRDHUP)
#define EV_RESET        (EV_CLOSED | POLLERR)

// What the closing of a socket tells its peer: the ends of connections are made, `a` is closed, and
// the peer `b` finds out. `how` is what is done before the close.
enum {
    CLOSE_PLAIN,            // nothing: the end of the file
    CLOSE_UNREAD_AT_B,      // `a` has written bytes that `b` has not read: the end of the file
    CLOSE_UNREAD_AT_A,      // `b` has written bytes that `a` has not read: a reset
    CLOSE_BOTH_UNREAD,      // both have: a reset, after `b` has read what `a` wrote
    CLOSE_A_READ_ALL,       // `b` has written bytes, which `a` has read: the end of the file
    CLOSE_A_SHUT_RD,        // `a` has shut down reading with bytes of `b` that it has not read: a reset
    NUM_CLOSE_KINDS
};
static const char *close_names[] = {
    "plain close", "unread bytes at the peer", "unread bytes at the closed socket",
    "unread bytes at both", "bytes read before the close", "unread bytes after SHUT_RD",
};

static int prepare_close(int how, int a, int b) {
    char buf[8];
    switch (how) {
        case CLOSE_UNREAD_AT_B:
            return write(a, "u", 1) == 1 ? 0 : -1;
        case CLOSE_UNREAD_AT_A:
        case CLOSE_A_SHUT_RD:
            if (write(b, "u", 1) != 1) {
                return -1;
            }
            return how == CLOSE_A_SHUT_RD ? shutdown(a, SHUT_RD) : 0;
        case CLOSE_BOTH_UNREAD:
            return write(a, "1", 1) == 1 && write(b, "2", 1) == 1 ? 0 : -1;
        case CLOSE_A_READ_ALL:
            return write(b, "r", 1) == 1 && read(a, buf, sizeof(buf)) == 1 ? 0 : -1;
        default:
            return 0;
    }
}

static int is_reset(int how) {
    return how == CLOSE_UNREAD_AT_A || how == CLOSE_BOTH_UNREAD || how == CLOSE_A_SHUT_RD;
}

// If the closed socket has bytes that it has not read, its peer gets a reset: the first read fails
// with ECONNRESET, instead of returning the end of the file, and poll() reports an error. The error
// is gone after that. Bytes that the peer has sent before are read first, and then it is the error
// that SO_ERROR returns. A close that leaves no bytes is the end of the file, for all kinds of
// connections (a socket pair, and the client or the server of a connection).
static int test_close_with_unread_bytes_resets_the_peer(void) {
    for (int kind = 0; kind < NUM_PAIR_KINDS; kind++) {
        for (int how = 0; how < NUM_CLOSE_KINDS; how++) {
            int a, b;
            char what[128];
            snprintf(what, sizeof(what), "%s, %s", pair_names[kind], close_names[how]);
            context = what;
            CHECK(make_pair(kind, &a, &b) == 0, "failed to make a connection");
            CHECK(prepare_close(how, a, b) == 0, "failed to prepare the close");
            close(a);
            char buf[8];
            int reset = is_reset(how);
            // The bytes that the peer has not read come before the error, and before the end
            int data = how == CLOSE_UNREAD_AT_B || how == CLOSE_BOTH_UNREAD;

            CHECK_EVENTS(b, reset ? EV_RESET : EV_CLOSED, "events");
            if (data) {
                CHECK_CALL(recv(b, buf, sizeof(buf), MSG_DONTWAIT), 1, "the first read");
            }
            if (reset && data) {
                // The error is still there, and read once by SO_ERROR
                CHECK_EVENTS(b, EV_RESET, "events after reading the bytes");
                CHECK_SO_ERROR(b, ECONNRESET, "after the bytes");
            } else if (reset) {
                CHECK_CALL(recv(b, buf, sizeof(buf), MSG_DONTWAIT), -ECONNRESET, "the first read");
            } else {
                CHECK_SO_ERROR(b, 0, "no reset");
            }
            // Only once: then it is the end of the file
            CHECK_EVENTS(b, EV_CLOSED, "events after the error");
            CHECK_SO_ERROR(b, 0, "after the error");
            CHECK_CALL(recv(b, buf, sizeof(buf), MSG_DONTWAIT), 0, "the second read");
            CHECK_CALL(recv(b, buf, sizeof(buf), MSG_DONTWAIT), 0, "the third read");
            CHECK_CALL(send(b, "w", 1, MSG_NOSIGNAL), -EPIPE, "send");
            CHECK_CALL(shutdown(b, SHUT_RDWR), 0, "shutdown");
            close(b);
        }
    }
    return 0;
}

// SO_ERROR returns the reset, and a read finds the end of the file after that
static int test_so_error_returns_the_reset_once(void) {
    for (int kind = 0; kind < NUM_PAIR_KINDS; kind++) {
        int a, b;
        context = pair_names[kind];
        CHECK(make_pair(kind, &a, &b) == 0, "failed to make a connection");
        CHECK(write(b, "u", 1) == 1, "failed to write");
        CHECK_SO_ERROR(b, 0, "before the close");
        close(a);
        CHECK_SO_ERROR(b, ECONNRESET, "first");
        CHECK_SO_ERROR(b, 0, "second");
        char buf[8];
        CHECK_CALL(recv(b, buf, sizeof(buf), MSG_DONTWAIT), 0, "read");
        close(b);
    }
    return 0;
}

// The read of a message that peeks at the bytes is told of the error, too, and so are the other
// calls that read
static int test_reset_is_told_to_all_calls_that_read(void) {
    enum { PEEK, READ, READV, RECVMSG, RECVFROM, NUM_CALLS };
    for (int call = 0; call < NUM_CALLS; call++) {
        int a, b;
        CHECK(make_pair(PAIR_SOCKETPAIR, &a, &b) == 0, "failed to make a connection");
        CHECK(write(b, "u", 1) == 1, "failed to write");
        close(a);
        char buf[8];
        struct iovec iov = { buf, sizeof(buf) };
        struct msghdr msg = { .msg_iov = &iov, .msg_iovlen = 1 };
        struct iovec none = { buf, 0 };
        set_nonblocking(b, 1);
        // Reading no bytes is no read, and leaves the error to the next one
        CHECK_CALL(read(b, buf, 0), 0, "read of no bytes");
        CHECK_CALL(readv(b, &none, 1), 0, "readv of no bytes");
        switch (call) {
            case PEEK:
                CHECK_CALL(recv(b, buf, sizeof(buf), MSG_PEEK), -ECONNRESET, "recv peek");
                break;
            case READ:
                CHECK_CALL(read(b, buf, sizeof(buf)), -ECONNRESET, "read");
                break;
            case READV:
                CHECK_CALL(readv(b, &iov, 1), -ECONNRESET, "readv");
                break;
            case RECVMSG:
                CHECK_CALL(recvmsg(b, &msg, 0), -ECONNRESET, "recvmsg");
                break;
            case RECVFROM:
                CHECK_CALL(recvfrom(b, buf, sizeof(buf), 0, NULL, NULL), -ECONNRESET, "recvfrom");
                break;
        }
        CHECK_CALL(recv(b, buf, sizeof(buf), 0), 0, "the second read");
        close(b);
    }
    return 0;
}

// A write to a connection that was reset fails with EPIPE, and raises SIGPIPE, like to any
// connection that is closed. The error stays for the read.
static atomic_int sigpipes;

static void on_sigpipe(int sig) {
    atomic_fetch_add(&sigpipes, 1);
}

static int test_write_to_a_reset_connection(void) {
    for (int kind = 0; kind < NUM_PAIR_KINDS; kind++) {
        int a, b;
        context = pair_names[kind];
        CHECK(make_pair(kind, &a, &b) == 0, "failed to make a connection");
        CHECK(write(b, "u", 1) == 1, "failed to write");
        close(a);
        int signals = atomic_load(&sigpipes);
        CHECK_CALL(write(b, "w", 1), -EPIPE, "write");
        CHECK(atomic_load(&sigpipes) == signals + 1, "the write raises no SIGPIPE");
        CHECK_CALL(send(b, "w", 1, MSG_NOSIGNAL), -EPIPE, "send");
        CHECK(atomic_load(&sigpipes) == signals + 1, "the send raises SIGPIPE");
        CHECK_EVENTS(b, EV_RESET, "events");
        char buf[8];
        CHECK_CALL(recv(b, buf, sizeof(buf), MSG_DONTWAIT), -ECONNRESET, "read");
        close(b);
    }
    return 0;
}

// The socket stays open as long as a descriptor of it is: only its last close resets the peer
static int test_reset_comes_with_the_last_close(void) {
    int a, b;
    CHECK(make_pair(PAIR_SOCKETPAIR, &a, &b) == 0, "failed to make a connection");
    CHECK(write(b, "u", 1) == 1, "failed to write");
    int d = dup(a);
    CHECK(d >= 0, "failed to dup");
    close(a);
    CHECK_EVENTS(b, POLLOUT, "events with the duplicate open");
    close(d);
    CHECK_EVENTS(b, EV_RESET, "events");
    char buf[8];
    CHECK_CALL(recv(b, buf, sizeof(buf), MSG_DONTWAIT), -ECONNRESET, "read");
    close(b);
    return 0;
}

// A listener that is closed resets the connections that it has not accepted, whether the client has
// written or not. The connections that it has accepted do not notice. A client that has closed is
// not reset, whatever the listener has done: its connection is read to the end of the file.
static int test_close_of_a_listener_resets_pending_connections(void) {
    for (int wrote = 0; wrote <= 1; wrote++) {
        unix_addr_t a;
        int l = make_socket(SOCK_LISTENING, &a);
        CHECK(l >= 0, "failed to listen");
        char what[64];
        snprintf(what, sizeof(what), "%s", wrote ? "the client has written" : "idle client");
        context = what;
        int gone = socket(AF_UNIX, SOCK_STREAM, 0);
        int accepted = socket(AF_UNIX, SOCK_STREAM, 0);
        int pending = socket(AF_UNIX, SOCK_STREAM, 0);
        CHECK(gone >= 0 && accepted >= 0 && pending >= 0, "failed to make sockets");
        char buf[8];
        CHECK_CALL(connect_to(gone, &a), 0, "connect");
        CHECK_CALL(write(gone, "early", 5), 5, "write");
        close(gone);
        int s_gone = accept(l, NULL, NULL);
        CHECK(s_gone >= 0, "failed to accept");
        CHECK_CALL(read(s_gone, buf, sizeof(buf)), 5, "read of the client that closed");
        CHECK_CALL(read(s_gone, buf, sizeof(buf)), 0, "read of the end of the file");
        CHECK_SO_ERROR(s_gone, 0, "no reset of the client that closed");
        close(s_gone);
        CHECK_CALL(connect_to(accepted, &a), 0, "connect");
        int s = accept(l, NULL, NULL);
        CHECK(s >= 0, "failed to accept");
        CHECK_CALL(connect_to(pending, &a), 0, "connect");
        if (wrote) {
            CHECK_CALL(write(pending, "d", 1), 1, "write");
        }
        close(l);

        // The connection that was accepted goes on
        CHECK_EVENTS(accepted, POLLOUT, "events of the accepted connection");
        CHECK_CALL(write(s, "ok", 2), 2, "write to the client of the accepted connection");
        CHECK_CALL(recv(accepted, buf, sizeof(buf), 0), 2, "read of the accepted connection");
        // The other is reset
        CHECK_EVENTS(pending, EV_RESET, "events of the pending connection");
        CHECK_CALL(recv(pending, buf, sizeof(buf), MSG_DONTWAIT), -ECONNRESET, "read");
        CHECK_EVENTS(pending, EV_CLOSED, "events after the error");
        CHECK_CALL(recv(pending, buf, sizeof(buf), MSG_DONTWAIT), 0, "the second read");
        CHECK_CALL(send(pending, "x", 1, MSG_NOSIGNAL), -EPIPE, "send");
        close(pending);
        close(accepted);
        close(s);
    }
    return 0;
}

// The same, with SO_ERROR before any read
static int test_close_of_a_listener_sets_so_error(void) {
    unix_addr_t a;
    int l = make_socket(SOCK_LISTENING, &a);
    CHECK(l >= 0, "failed to listen");
    int pending = socket(AF_UNIX, SOCK_STREAM, 0);
    CHECK_CALL(connect_to(pending, &a), 0, "connect");
    close(l);
    CHECK_SO_ERROR(pending, ECONNRESET, "first");
    CHECK_SO_ERROR(pending, 0, "second");
    char buf[8];
    CHECK_CALL(recv(pending, buf, sizeof(buf), MSG_DONTWAIT), 0, "read");
    close(pending);
    return 0;
}

// What blocked calls get when the peer is closed with unread bytes
enum { OP_RECV, OP_SEND, OP_POLL, OP_EPOLL };

typedef struct {
    int kind;
    int fd;
    pthread_t thread;
    atomic_int started;
    atomic_int done;
    long ret;
    int err;
    unsigned revents;
} op_t;

static void *op_main(void *arg) {
    op_t *op = arg;
    char buf[8];
    long ret = -2;
    unsigned revents = 0;
    atomic_store(&op->started, 1);
    errno = 0;
    switch (op->kind) {
        case OP_RECV:
            ret = recv(op->fd, buf, sizeof(buf), 0);
            break;
        case OP_SEND:
            ret = send(op->fd, buf, 1, 0);
            break;
        case OP_POLL: {
            struct pollfd pfd = { .fd = op->fd, .events = POLLIN };
            ret = poll(&pfd, 1, -1);
            revents = (unsigned short)pfd.revents;
            break;
        }
        case OP_EPOLL: {
            // The descriptor is an epoll file
            struct epoll_event event;
            ret = epoll_wait(op->fd, &event, 1, -1);
            revents = ret > 0 ? event.events : 0;
            break;
        }
    }
    op->err = errno;
    op->ret = ret;
    op->revents = revents;
    atomic_store(&op->done, 1);
    return NULL;
}

static int op_start(op_t *op, int kind, int fd) {
    memset(op, 0, sizeof(*op));
    op->kind = kind;
    op->fd = fd;
    if (pthread_create(&op->thread, NULL, op_main, op) != 0) {
        return -1;
    }
    while (!atomic_load(&op->started)) {
        usleep(100);
    }
    // The call has blocked by now
    usleep(100 * 1000);
    return atomic_load(&op->done) ? -1 : 0;
}

// Waits for the thread to return. Returns -1 if it does not.
static int op_finish(op_t *op) {
    for (int i = 0; i < 5000 && !atomic_load(&op->done); i++) {
        usleep(1000);
    }
    if (!atomic_load(&op->done)) {
        return -1;
    }
    pthread_join(op->thread, NULL);
    return 0;
}

// A read that waits is told of the reset when the peer is closed. A poll that waits is told, too.
static int test_close_with_unread_bytes_wakes_a_blocked_read_with_the_error(void) {
    for (int kind = 0; kind < NUM_PAIR_KINDS; kind++) {
        for (int call = OP_RECV; call <= OP_POLL; call += OP_POLL - OP_RECV) {
            int a, b;
            context = pair_names[kind];
            CHECK(make_pair(kind, &a, &b) == 0, "failed to make a connection");
            CHECK(write(b, "u", 1) == 1, "failed to write");
            op_t op;
            CHECK(op_start(&op, call, b) == 0, "the %s does not wait", call == OP_RECV ? "read" :
                  "poll");
            close(a);
            CHECK(op_finish(&op) == 0, "the %s is not woken by the close", call == OP_RECV ? "read" :
                  "poll");
            if (call == OP_RECV) {
                CHECK(op.ret == -1 && op.err == ECONNRESET, "the read returns %ld, errno %d, "
                      "instead of ECONNRESET", op.ret, op.err);
            } else {
                // Only IN was asked for, and an error and a hangup are always reported
                unsigned expected = POLLIN | POLLERR | POLLHUP;
                CHECK(op.ret == 1 && op.revents == expected, "the poll returns %ld, events %s, "
                      "instead of %s", op.ret, events_str(op.revents), events_str(expected));
            }
            close(b);
        }
    }
    return 0;
}

// A write that waits for room gets the error, too, and no SIGPIPE, if the peer is closed. Without
// unread bytes, it is EPIPE, as before: one end only is closed, as the peer has shut down reading.
static int test_close_wakes_a_blocked_write_with_the_error(void) {
    for (int kind = 0; kind < NUM_PAIR_KINDS; kind++) {
        for (int how = 0; how < 2; how++) {
            int a, b;
            char what[64];
            snprintf(what, sizeof(what), "%s, %s", pair_names[kind],
                     how ? "SHUT_RD" : "close");
            context = what;
            CHECK(make_pair(kind, &a, &b) == 0, "failed to make a connection");
            // The send buffer of `a` is full, and `b` has not read it
            CHECK(fill(a) == 0, "failed to fill the buffer");
            op_t op;
            int signals = atomic_load(&sigpipes);
            CHECK(op_start(&op, OP_SEND, a) == 0, "the write does not wait");
            if (how == 0) {
                close(b);
            } else {
                CHECK(shutdown(b, SHUT_RD) == 0, "failed to shut down");
            }
            CHECK(op_finish(&op) == 0, "the write is not woken");
            // A close with bytes that nobody has read is a reset, and a shutdown is not
            int err = how == 0 ? ECONNRESET : EPIPE;
            CHECK(op.ret == -1 && op.err == err, "the write returns %ld, errno %d, instead of %d",
                  op.ret, op.err, err);
            CHECK(atomic_load(&sigpipes) == signals, "the write that waited raises SIGPIPE");
            close(a);
            if (how == 1) {
                close(b);
            }
        }
    }
    return 0;
}

// A peer that reads while the socket is closed finds the bytes that the socket sent, then, once, the
// error of a reset if the closed socket had bytes that it had not read, and the end of the file.
typedef struct {
    int fd;
    atomic_int started;
    atomic_int done;
    long bytes;
    int resets;
    int in_order;       // no byte after an error, and no error after the end of the file
    int bad_errno;
} reader_t;

static void *reader_main(void *arg) {
    reader_t *r = arg;
    char buf[64];
    atomic_store(&r->started, 1);
    r->in_order = 1;
    for (;;) {
        errno = 0;
        ssize_t n = recv(r->fd, buf, sizeof(buf), 0);
        if (n > 0) {
            r->in_order &= r->resets == 0;
            r->bytes += n;
        } else if (n == 0) {
            break;
        } else if (errno == ECONNRESET) {
            r->resets++;
        } else {
            r->bad_errno = errno;
            break;
        }
    }
    atomic_store(&r->done, 1);
    return NULL;
}

static int test_close_while_the_peer_reads(void) {
    for (int i = 0; i < 120; i++) {
        int a, b;
        char what[64];
        int unread_at_a = i % 2;
        snprintf(what, sizeof(what), "%s, round %d, %s", pair_names[(i / 2) % NUM_PAIR_KINDS], i,
                 unread_at_a ? "unread bytes" : "no unread bytes");
        context = what;
        CHECK(make_pair((i / 2) % NUM_PAIR_KINDS, &a, &b) == 0, "failed to make a connection");
        static const char data[100];
        long sent = 0;
        for (int k = 0; k < i % 4; k++) {
            CHECK(write(a, data, sizeof(data)) == sizeof(data), "failed to write");
            sent += sizeof(data);
        }
        if (unread_at_a) {
            CHECK(write(b, "u", 1) == 1, "failed to write");
        }
        reader_t r;
        memset(&r, 0, sizeof(r));
        r.fd = b;
        pthread_t thread;
        CHECK(pthread_create(&thread, NULL, reader_main, &r) == 0, "failed to start a thread");
        while (!atomic_load(&r.started)) {
            usleep(50);
        }
        usleep((i % 5) * 100);
        close(a);
        for (int w = 0; w < 5000 && !atomic_load(&r.done); w++) {
            usleep(1000);
        }
        CHECK(atomic_load(&r.done), "the read is not woken by the close");
        pthread_join(thread, NULL);
        CHECK(r.bad_errno == 0, "read fails with errno %d", r.bad_errno);
        CHECK(r.bytes == sent, "read %ld bytes, instead of %ld", r.bytes, sent);
        CHECK(r.resets == unread_at_a, "read fails with ECONNRESET %d times, instead of %d",
              r.resets, unread_at_a);
        CHECK(r.in_order, "a byte comes after the error");
        close(b);
    }
    return 0;
}

// The bytes that a closed socket has not read are gone: its peer, whose buffer was full, has room
// again, and a write fails with EPIPE, instead of waiting for it
static int test_full_buffer_has_room_after_the_peer_closes(void) {
    for (int kind = 0; kind < NUM_PAIR_KINDS; kind++) {
        context = pair_names[kind];
        int a, b;
        CHECK(make_pair(kind, &a, &b) == 0, "failed to make a connection");
        CHECK(fill(a) == 0, "failed to fill the buffer");
        CHECK_EVENTS(a, 0, "full buffer");
        close(b);
        CHECK_EVENTS(a, EV_RESET, "events after the close of the peer");
        CHECK_CALL(send(a, "x", 1, MSG_NOSIGNAL | MSG_DONTWAIT), -EPIPE, "send");
        char buf[8];
        CHECK_CALL(recv(a, buf, sizeof(buf), MSG_DONTWAIT), -ECONNRESET, "read");
        close(a);
    }
    return 0;
}

// ---------------------------------------------------------------------------------------------
// Edge triggered epoll
// ---------------------------------------------------------------------------------------------

static int epoll_add(int epfd, int fd, unsigned events) {
    struct epoll_event event = { .events = events, .data.fd = fd };
    return epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &event);
}

// The events that epoll_wait returns at once: 0 if there are none, -1 if it fails
static long epoll_now(int epfd) {
    struct epoll_event event;
    int ret = epoll_wait(epfd, &event, 1, 0);
    return ret < 0 ? -1 : ret == 0 ? 0 : (long)event.events;
}

// Checks the events of epoll_wait, which does not wait: there are none if `expected` is 0
#define CHECK_EPOLL(epfd, expected, what) do { \
        long __events = epoll_now(epfd); \
        unsigned __expected = (expected); \
        CHECK(__events == (long)__expected, "%s: epoll_wait returns the events %s, instead of %s", \
              what, __events < 0 ? "an error" : events_str(__events), events_str(__expected)); \
    } while (0)

#define EV_EDGE (EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET)

// epoll tells the reset: an error, and the hangup that comes with it, once
static int test_epoll_is_told_of_the_reset(void) {
    for (int kind = 0; kind < NUM_PAIR_KINDS; kind++) {
        context = pair_names[kind];
        int a, b;
        CHECK(make_pair(kind, &a, &b) == 0, "failed to make a connection");
        CHECK(write(b, "u", 1) == 1, "failed to write");
        int edge = epoll_create1(0);
        int level = epoll_create1(0);
        CHECK(edge >= 0 && epoll_add(edge, b, EV_EDGE) == 0, "failed to make an epoll file");
        CHECK(level >= 0 && epoll_add(level, b, EPOLLIN | EPOLLOUT | EPOLLRDHUP) == 0,
              "failed to make an epoll file");
        CHECK_EPOLL(edge, POLLOUT, "at the registration");
        CHECK_EPOLL(level, POLLOUT, "of the level triggered registration");

        close(a);
        CHECK_EPOLL(edge, EV_RESET, "after the close");
        CHECK_EPOLL(edge, 0, "again");
        CHECK_EPOLL(level, EV_RESET, "of the level triggered registration after the close");
        // The error is gone after it is read, and the registration that is level triggered has the
        // end of the file
        char buf[8];
        CHECK_CALL(recv(b, buf, sizeof(buf), MSG_DONTWAIT), -ECONNRESET, "read");
        CHECK_EPOLL(level, EV_CLOSED, "of the level triggered registration after the read");
        close(edge);
        close(level);
        close(b);
    }
    return 0;
}

// A thread that waits in epoll_wait for the socket is woken up by the close that resets it
static int test_epoll_wait_is_woken_by_the_reset(void) {
    for (int kind = 0; kind < NUM_PAIR_KINDS; kind++) {
        context = pair_names[kind];
        int a, b;
        CHECK(make_pair(kind, &a, &b) == 0, "failed to make a connection");
        CHECK(write(b, "u", 1) == 1, "failed to write");
        int ep = epoll_create1(0);
        CHECK(ep >= 0 && epoll_add(ep, b, EV_EDGE) == 0, "failed to make an epoll file");
        CHECK_EPOLL(ep, POLLOUT, "at the registration");
        op_t op;
        CHECK(op_start(&op, OP_EPOLL, ep) == 0, "epoll_wait does not wait");
        close(a);
        CHECK(op_finish(&op) == 0, "epoll_wait is not woken");
        CHECK(op.ret == 1 && op.revents == EV_RESET, "epoll_wait returns %ld, events %s, "
              "instead of %s", op.ret, events_str(op.revents), events_str(EV_RESET));
        close(ep);
        close(b);
    }
    return 0;
}

static test_case_t test_cases[] = {
    TEST_CASE(test_shutdown_changes_the_events_at_once),
    TEST_CASE(test_full_buffer_is_not_writable_after_shutdown),
    TEST_CASE(test_full_buffer_of_the_peer_of_a_shutdown),
    TEST_CASE(test_calls_on_an_idle_or_listening_socket),
    TEST_CASE(test_queued_bytes_of_a_connection),
    TEST_CASE(test_shutdown_of_an_idle_or_bound_socket),
    TEST_CASE(test_shutdown_of_an_idle_socket_stays_after_connect),
    TEST_CASE(test_shutdown_of_a_bound_socket_stays_after_listen),
    TEST_CASE(test_close_with_unread_bytes_resets_the_peer),
    TEST_CASE(test_so_error_returns_the_reset_once),
    TEST_CASE(test_reset_is_told_to_all_calls_that_read),
    TEST_CASE(test_write_to_a_reset_connection),
    TEST_CASE(test_reset_comes_with_the_last_close),
    TEST_CASE(test_close_of_a_listener_resets_pending_connections),
    TEST_CASE(test_close_of_a_listener_sets_so_error),
    TEST_CASE(test_close_with_unread_bytes_wakes_a_blocked_read_with_the_error),
    TEST_CASE(test_close_wakes_a_blocked_write_with_the_error),
    TEST_CASE(test_close_while_the_peer_reads),
    TEST_CASE(test_full_buffer_has_room_after_the_peer_closes),
    TEST_CASE(test_epoll_is_told_of_the_reset),
    TEST_CASE(test_epoll_wait_is_woken_by_the_reset),
};

int main(int argc, const char *argv[]) {
    // A write to a socket that cannot write is a normal error here, but some tests count the signals
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_sigpipe;
    sigaction(SIGPIPE, &sa, NULL);

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

    pthread_t watchdog;
    pthread_create(&watchdog, NULL, watchdog_main, NULL);
    int ret = 0;
    for (int i = 0; i < num_cases && ret == 0; i++) {
        current_case = cases[i].name;
        atomic_store(&watchdog_deadline, now_ms() + WATCHDOG_MS);
        if (cases[i].func() < 0) {
            printf("  func %s - [ERR]\n", cases[i].name);
            ret = -1;
        } else {
            printf("  func %s - [OK]\n", cases[i].name);
        }
        atomic_store(&watchdog_deadline, 0);
    }
    fflush(stdout);
    return ret;
}
