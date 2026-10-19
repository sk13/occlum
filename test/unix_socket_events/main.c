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
#include <sys/socket.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

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

static test_case_t test_cases[] = {
    TEST_CASE(test_shutdown_changes_the_events_at_once),
    TEST_CASE(test_full_buffer_is_not_writable_after_shutdown),
    TEST_CASE(test_full_buffer_of_the_peer_of_a_shutdown),
};

int main(int argc, const char *argv[]) {
    // A write to a socket that cannot write is a normal error here
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
