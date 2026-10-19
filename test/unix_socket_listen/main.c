// The listening side of unix stream sockets: the backlog, connect() on a full backlog, and listen()
// while another thread waits in accept(). The expectations are the behavior of Linux (checked on
// Linux 7.0): the test passes there, too (gcc -pthread -I../include main.c).
//
// The calls that may block run in threads, so that a call that does not return when it should makes
// the test case fail after a timeout instead of hanging it. A thread that never returns is left
// behind, so the test ends with _exit if there are any. A watchdog thread ends the test if a test
// case takes too long, e.g., when a call of the main thread hangs.
//
// Usage: unix_socket_listen [test case name | --list]
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
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#include "test.h"

#define SETTLE_MS       150     // time that a thread has to block before the stimulus
#define WAKE_MS         5000    // time that a thread has to wake up
#define WATCHDOG_MS     60000   // time that a test case may take
#define STACK_SIZE      (256 * 1024)
#define MAX_CLIENTS     40      // connections cost memory
// The test enclave has 64 threads at the most (max_num_of_threads of test/Occlum.json), and when
// the host is busy, a burst of threads that comes near to it hangs the enclave: pthread_create()
// fails with "Out of TCS", and nothing returns any more. So no test case runs more threads at once
// than these, which are 16 with the main thread and the watchdog.
#define MAX_THREADS     14

#define CHECK(cond, fmt, ...) do { \
        if (!(cond)) { \
            THROW_ERROR(fmt, ##__VA_ARGS__); \
        } \
    } while (0)


// ---------------------------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------------------------

static atomic_int stuck_threads;

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

static void on_signal(int sig) {
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
                     "unix_socket_listen.%d.%d", (int)getpid(), counter++);
    a.len = offsetof(struct sockaddr_un, sun_path) + 1 + n;
    return a;
}

static void set_nonblocking(int fd, int nonblocking) {
    int flags = fcntl(fd, F_GETFL);
    fcntl(fd, F_SETFL, nonblocking ? (flags | O_NONBLOCK) : (flags & ~O_NONBLOCK));
}

static int new_socket(int nonblocking) {
    return socket(AF_UNIX, SOCK_STREAM | (nonblocking ? SOCK_NONBLOCK : 0), 0);
}

// A socket that listens at a new address, which is returned in `a`. Returns -1 on failure.
static int listener(unix_addr_t *a, int backlog) {
    *a = new_addr();
    int fd = new_socket(0);
    if (fd < 0) {
        return -1;
    }
    if (bind(fd, (struct sockaddr *)&a->sa, a->len) < 0 || listen(fd, backlog) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static int connect_to(int fd, const unix_addr_t *a) {
    return connect(fd, (struct sockaddr *)&a->sa, a->len);
}

// Sets SO_RCVTIMEO or SO_SNDTIMEO of a socket
static int set_timeout(int fd, int optname, long ms) {
    struct timeval timeout = { ms / 1000, (ms % 1000) * 1000 };
    return setsockopt(fd, SOL_SOCKET, optname, &timeout, sizeof(timeout));
}

// Returns whether connect() fails with the error
static int connect_fails(int fd, const unix_addr_t *a, int err) {
    errno = 0;
    return connect_to(fd, a) == -1 && errno == err;
}

// Returns whether accept() fails with the error
static int accept_fails(int fd, int err) {
    errno = 0;
    return accept(fd, NULL, NULL) == -1 && errno == err;
}

// Connects nonblocking sockets until the backlog is full, which makes connect() fail with EAGAIN.
// Returns the number of the connections that were made, whose sockets are in `fds`, or -1 if
// connect() fails with another error, or the limit is reached.
static int fill_backlog(const unix_addr_t *a, int *fds, int max) {
    int n = 0;
    while (n < max) {
        int c = new_socket(1);
        if (c < 0) {
            return -1;
        }
        if (connect_to(c, a) == 0) {
            fds[n++] = c;
            continue;
        }
        int err = errno;
        close(c);
        errno = err;
        return err == EAGAIN ? n : -1;
    }
    errno = 0;
    return -1;
}

// Accepts the connections of a listener until there are no more, which it does not wait for.
// Returns their number, or -1 if accept() fails with another error than EAGAIN.
static int accept_all(int l) {
    set_nonblocking(l, 1);
    int n = 0;
    for (;;) {
        int s = accept(l, NULL, NULL);
        if (s < 0) {
            set_nonblocking(l, 0);
            return errno == EAGAIN ? n : -1;
        }
        close(s);
        n++;
    }
}

static void close_all(int *fds, int n) {
    for (int i = 0; i < n; i++) {
        close(fds[i]);
    }
}

static long poll_now(int fd, short events) {
    struct pollfd pfd = { .fd = fd, .events = events };
    int ret = poll(&pfd, 1, 0);
    return ret < 0 ? -1 : (long)(unsigned short)pfd.revents;
}

static const char *events_str(unsigned events) {
    static __thread char bufs[4][64];
    static __thread int next;
    char *buf = bufs[next++ % 4];
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

// ---------------------------------------------------------------------------------------------
// Operations in threads
// ---------------------------------------------------------------------------------------------

enum { OP_ACCEPT, OP_CONNECT, OP_LISTEN };

typedef struct {
    int kind;
    int fd;
    unix_addr_t addr;   // OP_CONNECT
    int arg;            // the backlog of OP_LISTEN
    pthread_t thread;
    int created;
    atomic_int started;
    atomic_int done;    // the results are valid if it is set
    atomic_int tid;
    long ret;
    int err;
    unsigned revents;
    long start_ms;
    long elapsed_ms;
} op_t;

static void *op_main(void *arg) {
    op_t *op = arg;
    long ret = -2;
    unsigned revents = 0;

    atomic_store(&op->tid, (int)syscall(SYS_gettid));
    atomic_store(&op->started, 1);
    op->start_ms = now_ms();
    errno = 0;
    switch (op->kind) {
        case OP_ACCEPT:
            ret = accept(op->fd, NULL, NULL);
            break;
        case OP_CONNECT:
            ret = connect_to(op->fd, &op->addr);
            break;
        case OP_LISTEN:
            ret = listen(op->fd, op->arg);
            break;
    }
    op->err = errno;
    op->ret = ret;
    op->revents = revents;
    op->elapsed_ms = now_ms() - op->start_ms;
    atomic_store(&op->done, 1);
    return NULL;
}

// Starts the thread of an operation, and returns once it has started
static int op_run(op_t *op) {
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setstacksize(&attr, STACK_SIZE);
    int err = pthread_create(&op->thread, &attr, op_main, op);
    pthread_attr_destroy(&attr);
    if (err != 0) {
        errno = err;
        return -1;
    }
    op->created = 1;
    while (!atomic_load(&op->started)) {
        usleep(200);
    }
    return 0;
}

static int op_start(op_t *op, int kind, int fd) {
    memset(op, 0, sizeof(*op));
    op->kind = kind;
    op->fd = fd;
    return op_run(op);
}

static int op_start_connect(op_t *op, int fd, const unix_addr_t *a) {
    memset(op, 0, sizeof(*op));
    op->kind = OP_CONNECT;
    op->fd = fd;
    op->addr = *a;
    return op_run(op);
}

static int op_start_listen(op_t *op, int fd, int backlog) {
    memset(op, 0, sizeof(*op));
    op->kind = OP_LISTEN;
    op->fd = fd;
    op->arg = backlog;
    return op_run(op);
}

static int op_done(op_t *op) {
    return atomic_load(&op->done);
}

// Waits until the thread has returned, for at most `ms`. Returns whether it has.
static int op_wait(op_t *op, int ms) {
    long deadline = now_ms() + ms;
    while (!op_done(op)) {
        if (now_ms() >= deadline) {
            return 0;
        }
        usleep(200);
    }
    return 1;
}

// Lets the thread block, and returns whether it still blocks
static int op_blocks(op_t *op) {
    sleep_ms(SETTLE_MS);
    return !op_done(op);
}

// Joins the thread if it has returned, or leaves it behind
static void op_finish(op_t *op) {
    if (!op->created) {
        return;
    }
    if (op_done(op)) {
        pthread_join(op->thread, NULL);
    } else {
        atomic_fetch_add(&stuck_threads, 1);
    }
    op->created = 0;
}

// Interrupts the thread of the operation with a signal until the call returns. A signal that comes
// before the thread has entered the call has no effect, so it is sent again.
static void interrupt(op_t *op) {
    long deadline = now_ms() + WAKE_MS;
    while (!op_done(op) && now_ms() < deadline) {
        syscall(SYS_tgkill, getpid(), atomic_load(&op->tid), SIGUSR1);
        op_wait(op, 25);
    }
}

// Returns the number of the operations that have returned
static int count_done(op_t *ops, int n) {
    int done = 0;
    for (int i = 0; i < n; i++) {
        done += op_done(&ops[i]);
    }
    return done;
}

#define EXPECT_RETURNS(op, want_ret, want_err, what) do { \
        if (!op_wait(op, WAKE_MS)) { \
            THROW_ERROR(what " does not return"); \
        } \
        if ((op)->ret != (want_ret) || ((want_ret) == -1 && (op)->err != (want_err))) { \
            errno = (op)->err; \
            THROW_ERROR(what " returned %ld, instead of " #want_ret " with errno " #want_err, \
                        (op)->ret); \
        } \
    } while (0)

// ---------------------------------------------------------------------------------------------
// The backlog
// ---------------------------------------------------------------------------------------------

// A listener takes one connection more than the backlog says
static int test_backlog_takes_one_more_connection(void) {
    static const int backlogs[] = { 0, 1, 2, 5, 10 };
    for (size_t i = 0; i < ARRAY_SIZE(backlogs); i++) {
        unix_addr_t a;
        int l = listener(&a, backlogs[i]);
        CHECK(l >= 0, "failed to listen with the backlog %d", backlogs[i]);
        int fds[MAX_CLIENTS];
        int n = fill_backlog(&a, fds, MAX_CLIENTS);
        CHECK(n == backlogs[i] + 1, "the backlog %d takes %d connections, instead of %d",
              backlogs[i], n, backlogs[i] + 1);
        int accepted = accept_all(l);
        CHECK(accepted == n, "%d of the %d connections were accepted", accepted, n);
        // There is room again
        n = fill_backlog(&a, fds, MAX_CLIENTS);
        CHECK(n == backlogs[i] + 1,
              "the backlog %d takes %d connections after the accept", backlogs[i], n);
        close_all(fds, n);
        close(l);
    }
    return 0;
}

// A negative backlog is the greatest one
static int test_negative_backlog_is_the_greatest(void) {
    unix_addr_t a;
    int l = listener(&a, -1);
    CHECK(l >= 0, "failed to listen with the backlog -1");
    // The connections cost memory, so that not many of them are made
    enum { CONNECTIONS = 30 };
    int fds[CONNECTIONS];
    // fill_backlog() fails with errno 0 if it reaches the limit without a full backlog
    int n = fill_backlog(&a, fds, CONNECTIONS);
    CHECK(n == -1 && errno == 0, "the backlog -1 is full after %d connections", n);
    close_all(fds, CONNECTIONS);
    close(l);
    return 0;
}

// Threads that connect at once, round after round. They are started once and wait for each round,
// so that there is no burst of new threads, which MAX_THREADS is about.
typedef struct {
    const unix_addr_t *addr;    // the listener of the round
    atomic_int round;           // the main thread starts a round by counting up
    atomic_int ready;           // the threads that wait for the round
    atomic_int finished;        // the threads that have connected in the round
    atomic_int stop;
} pool_t;

typedef struct {
    pool_t *pool;
    int fd;                     // the socket that connects in the round
    int err;                    // 0, or the error of connect()
} connector_t;

static void *connect_in_rounds(void *arg) {
    connector_t *connector = arg;
    pool_t *pool = connector->pool;
    int round = 0;
    for (;;) {
        atomic_fetch_add(&pool->ready, 1);
        while (atomic_load(&pool->round) == round) {
        }
        if (atomic_load(&pool->stop)) {
            return NULL;
        }
        round = atomic_load(&pool->round);
        connector->err = connect_to(connector->fd, pool->addr) == 0 ? 0 : errno;
        atomic_fetch_add(&pool->finished, 1);
    }
}

// Lets the threads connect to a new listener, round after round, and checks the result
static int connect_rounds(pool_t *pool, connector_t *connectors, int threads) {
    // A backlog of 0 takes one connection only, so that two that get through are one too many
    static const int backlogs[] = { 0, 1, 3 };
    enum { ROUNDS = 150 };
    for (int round = 1; round <= ROUNDS; round++) {
        int backlog = backlogs[round % ARRAY_SIZE(backlogs)];
        unix_addr_t a;
        int l = listener(&a, backlog);
        CHECK(l >= 0, "failed to listen");
        for (int i = 0; i < threads; i++) {
            connectors[i].fd = new_socket(1);
        }
        while (atomic_load(&pool->ready) < threads) {
        }
        atomic_store(&pool->ready, 0);
        atomic_store(&pool->finished, 0);
        pool->addr = &a;
        atomic_store(&pool->round, round);
        while (atomic_load(&pool->finished) < threads) {
        }

        int connected = 0, refused = 0;
        for (int i = 0; i < threads; i++) {
            connected += connectors[i].err == 0;
            refused += connectors[i].err == EAGAIN;
        }
        CHECK(connected == backlog + 1 && refused == threads - connected,
              "%d connections were made and %d refused with the backlog %d, instead of %d and %d",
              connected, refused, backlog, backlog + 1, threads - backlog - 1);
        CHECK(accept_all(l) == backlog + 1, "the queue holds another number of connections");
        for (int i = 0; i < threads; i++) {
            close(connectors[i].fd);
        }
        close(l);
    }
    return 0;
}

// Many threads connect at once: exactly as many as the backlog allows get through, and the others
// fail with EAGAIN
static int test_concurrent_connects_fill_the_backlog_exactly(void) {
    enum { THREADS = MAX_THREADS };
    pool_t pool;
    memset(&pool, 0, sizeof(pool));
    connector_t connectors[THREADS];
    pthread_t threads[THREADS];
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setstacksize(&attr, STACK_SIZE);
    int started = 0, err = 0;
    while (started < THREADS && err == 0) {
        connectors[started] = (connector_t) { &pool, -1, 0 };
        err = pthread_create(&threads[started], &attr, connect_in_rounds, &connectors[started]);
        started += err == 0;
    }
    pthread_attr_destroy(&attr);
    int ret = -1;
    if (err == 0) {
        ret = connect_rounds(&pool, connectors, THREADS);
    } else {
        printf("\t\tERROR:failed to start the thread %d: %s\n", started, strerror(err));
    }

    // The threads that wait for a round end with it
    while (atomic_load(&pool.ready) < started) {
    }
    atomic_store(&pool.stop, 1);
    atomic_fetch_add(&pool.round, 1);
    for (int i = 0; i < started; i++) {
        pthread_join(threads[i], NULL);
    }
    return ret;
}

// A full backlog makes a nonblocking connect() fail with EAGAIN, and the socket can connect later
static int test_nonblocking_connect_on_full_backlog(void) {
    unix_addr_t a;
    int l = listener(&a, 1);
    CHECK(l >= 0, "failed to listen");
    int fds[MAX_CLIENTS];
    int n = fill_backlog(&a, fds, MAX_CLIENTS);
    CHECK(n == 2, "the backlog 1 takes %d connections", n);

    int c = new_socket(1);
    for (int i = 0; i < 3; i++) {
        CHECK(connect_fails(c, &a, EAGAIN),
              "connect() on a full backlog does not fail with EAGAIN");
    }
    // The socket stays unconnected
    errno = 0;
    CHECK(send(c, "x", 1, MSG_NOSIGNAL) == -1 && errno == ENOTCONN,
          "the socket that could not connect is connected");

    int s = accept(l, NULL, NULL);
    CHECK(s >= 0, "failed to accept");
    CHECK(connect_to(c, &a) == 0, "connect() fails after the accept made room");
    errno = 0;
    int c2 = new_socket(1);
    CHECK(connect_fails(c2, &a, EAGAIN), "the backlog takes more than it should");
    close(c2);
    close(s);
    close(c);
    close_all(fds, n);
    close(l);
    return 0;
}

// A blocking connect() on a full backlog waits for an accept
static int test_blocking_connect_waits_for_accept(void) {
    unix_addr_t a;
    int l = listener(&a, 0);
    CHECK(l >= 0, "failed to listen");
    int first = new_socket(0);
    CHECK(connect_to(first, &a) == 0, "failed to connect");

    int c = new_socket(0);
    op_t con;
    CHECK(op_start_connect(&con, c, &a) == 0, "failed to start the thread");
    CHECK(op_blocks(&con), "connect() on a full backlog does not wait");

    int s = accept(l, NULL, NULL);
    CHECK(s >= 0, "failed to accept");
    EXPECT_RETURNS(&con, 0, 0, "connect() after the accept");
    op_finish(&con);

    // The connection works, and it is the next one that accept() returns
    int s2 = accept(l, NULL, NULL);
    CHECK(s2 >= 0, "failed to accept the connection that waited");
    char buf[8];
    int works = write(c, "hi", 2) == 2 && read(s2, buf, sizeof(buf)) == 2;
    CHECK(works && memcmp(buf, "hi", 2) == 0, "the connection that waited does not work");
    close(s2);
    close(s);
    close(c);
    close(first);
    close(l);
    return 0;
}

// Each accept lets one of the waiting connect() calls go on
static int test_blocked_connects_proceed_one_by_one(void) {
    enum { WAITING = 4 };
    unix_addr_t a;
    int l = listener(&a, 1);
    CHECK(l >= 0, "failed to listen");
    int fds[MAX_CLIENTS];
    int n = fill_backlog(&a, fds, MAX_CLIENTS);
    CHECK(n == 2, "the backlog 1 takes %d connections", n);

    int cs[WAITING];
    op_t cons[WAITING];
    for (int i = 0; i < WAITING; i++) {
        cs[i] = new_socket(0);
        CHECK(op_start_connect(&cons[i], cs[i], &a) == 0, "failed to start the thread");
    }
    sleep_ms(SETTLE_MS);
    for (int i = 0; i < WAITING; i++) {
        CHECK(!op_done(&cons[i]), "connect() %d on a full backlog does not wait", i);
    }

    // The connections that are queued are accepted first, one connection of those that wait is
    // queued for each
    for (int round = 1; round <= WAITING; round++) {
        int s = accept(l, NULL, NULL);
        CHECK(s >= 0, "failed to accept");
        close(s);
        // Exactly one connect() goes on for each accept(): wait for it, and see that no other does
        long deadline = now_ms() + WAKE_MS;
        while (count_done(cons, WAITING) < round && now_ms() < deadline) {
            usleep(1000);
        }
        sleep_ms(SETTLE_MS);
        int done = count_done(cons, WAITING);
        CHECK(done == round,
              "%d connect() calls returned after %d accepts, instead of %d", done, round,
              round);
    }
    for (int i = 0; i < WAITING; i++) {
        EXPECT_RETURNS(&cons[i], 0, 0, "connect() that waited");
        op_finish(&cons[i]);
    }
    int rest = accept_all(l);
    CHECK(rest == 2, "%d connections are left after the accepts, instead of 2", rest);
    for (int i = 0; i < WAITING; i++) {
        close(cs[i]);
    }
    close_all(fds, n);
    close(l);
    return 0;
}

// Many connect() calls wait for room, and every one of them gets it in the end, whichever order
// the accepts and the wake-ups come in
static int test_many_blocked_connects_all_proceed(void) {
    enum { WAITING = MAX_THREADS };
    unix_addr_t a;
    int l = listener(&a, 1);
    CHECK(l >= 0, "failed to listen");
    // An accept that waits for a connection too long fails
    CHECK(set_timeout(l, SO_RCVTIMEO, 5000) == 0, "failed to set SO_RCVTIMEO");

    int cs[WAITING];
    op_t cons[WAITING];
    for (int i = 0; i < WAITING; i++) {
        cs[i] = new_socket(0);
        CHECK(op_start_connect(&cons[i], cs[i], &a) == 0, "failed to start the thread");
    }
    for (int i = 0; i < WAITING; i++) {
        int s = accept(l, NULL, NULL);
        CHECK(s >= 0, "failed to accept the connection %d of %d", i, (int)WAITING);
        close(s);
    }
    for (int i = 0; i < WAITING; i++) {
        EXPECT_RETURNS(&cons[i], 0, 0, "connect() that waited for room");
        op_finish(&cons[i]);
        close(cs[i]);
    }
    close(l);
    return 0;
}

// Whether the program runs in Occlum
static int in_occlum(void) {
    return getenv("OCCLUM") != NULL;
}

// A connect() that is interrupted while an accept() makes room leaves the room to the connect() that
// waits behind it. Linux does not: the accept() wakes up the connect() that is interrupted, and the
// other one waits for the next accept() although there is room, so that only Occlum is checked
static int test_interrupted_connect_leaves_the_room_to_the_next(void) {
    enum { ROUNDS = 120, DELAYS = 30 };
    int interrupted = 0;
    for (int round = 0; round < ROUNDS; round++) {
        unix_addr_t a;
        int l = listener(&a, 0);
        CHECK(l >= 0, "failed to listen");
        int first = new_socket(0);
        CHECK(connect_to(first, &a) == 0, "failed to connect");

        int ca = new_socket(0), cb = new_socket(0);
        op_t con_a, con_b;
        CHECK(op_start_connect(&con_a, ca, &a) == 0, "failed to start the thread");
        sleep_ms(20);
        CHECK(op_start_connect(&con_b, cb, &a) == 0, "failed to start the thread");
        sleep_ms(20);
        CHECK(!op_done(&con_a) && !op_done(&con_b),
              "connect() on a full backlog does not wait");

        // The accept, which makes room, comes a little after the signal. The delays go up to the time
        // that Occlum needs to interrupt a thread, so that one of them hits the moment at which the
        // thread is woken up by the accept, and interrupted
        syscall(SYS_tgkill, getpid(), atomic_load(&con_a.tid), SIGUSR1);
        usleep((round % DELAYS) * 1000);
        int s = accept(l, NULL, NULL);
        CHECK(s >= 0, "failed to accept");
        CHECK(op_wait(&con_a, WAKE_MS), "connect() does not return");
        if (con_a.ret == 0) {
            // The connect() has the room, the other one waits for the next accept()
            CHECK(op_blocks(&con_b), "connect() on a full backlog does not wait");
            int s2 = accept(l, NULL, NULL);
            CHECK(s2 >= 0, "failed to accept the connection of the first thread");
            close(s2);
            EXPECT_RETURNS(&con_b, 0, 0, "connect() that waited behind another one");
        } else {
            errno = con_a.err;
            CHECK(con_a.ret == -1 && con_a.err == EINTR, "connect() returned %ld",
                  con_a.ret);
            interrupted++;
            if (in_occlum()) {
                EXPECT_RETURNS(&con_b, 0, 0,
                               "connect() that waited behind an interrupted one");
            }
        }
        // The listener refuses the connect() that still waits
        close(l);
        CHECK(op_wait(&con_b, WAKE_MS),
              "connect() does not return after the listener is closed");
        op_finish(&con_a);
        op_finish(&con_b);
        close(s);
        close(ca);
        close(cb);
        close(first);
    }
    // The signal comes before the accept often enough for the test to mean something
    CHECK(interrupted > 0, "connect() was never interrupted in %d rounds", (int)ROUNDS);
    return 0;
}

// SO_SNDTIMEO ends the wait of connect() on a full backlog with EAGAIN
static int test_connect_timeout_on_full_backlog(void) {
    unix_addr_t a;
    int l = listener(&a, 0);
    CHECK(l >= 0, "failed to listen");
    int first = new_socket(0);
    CHECK(connect_to(first, &a) == 0, "failed to connect");

    int c = new_socket(0);
    CHECK(set_timeout(c, SO_SNDTIMEO, 300) == 0, "failed to set SO_SNDTIMEO");
    op_t con;
    CHECK(op_start_connect(&con, c, &a) == 0, "failed to start the thread");
    EXPECT_RETURNS(&con, -1, EAGAIN, "connect() with a timeout on a full backlog");
    CHECK(con.elapsed_ms >= 250 && con.elapsed_ms < WAKE_MS,
          "connect() with a timeout of 300 ms returned after %ld ms", con.elapsed_ms);
    op_finish(&con);

    // The timeout does not make a nonblocking socket wait
    int nb = new_socket(1);
    CHECK(set_timeout(nb, SO_SNDTIMEO, 300) == 0, "failed to set SO_SNDTIMEO");
    long start = now_ms();
    CHECK(connect_fails(nb, &a, EAGAIN), "nonblocking connect() does not fail");
    CHECK(now_ms() - start < 200, "nonblocking connect() waits");

    // A connection that is accepted ends the wait of the socket early
    CHECK(set_timeout(c, SO_SNDTIMEO, 10000) == 0, "failed to set SO_SNDTIMEO");
    CHECK(op_start_connect(&con, c, &a) == 0, "failed to start the thread");
    CHECK(op_blocks(&con), "connect() does not wait");
    int s = accept(l, NULL, NULL);
    CHECK(s >= 0, "failed to accept");
    EXPECT_RETURNS(&con, 0, 0, "connect() with a timeout after an accept");
    op_finish(&con);
    close(s);
    close(nb);
    close(c);
    close(first);
    close(l);
    return 0;
}

// A signal interrupts the wait of connect() on a full backlog, which does not leave anything behind
static int test_connect_on_full_backlog_is_interrupted(void) {
    unix_addr_t a;
    int l = listener(&a, 0);
    CHECK(l >= 0, "failed to listen");
    int first = new_socket(0);
    CHECK(connect_to(first, &a) == 0, "failed to connect");

    int c = new_socket(0);
    for (int i = 0; i < 3; i++) {
        op_t con;
        CHECK(op_start_connect(&con, c, &a) == 0, "failed to start the thread");
        CHECK(op_blocks(&con), "connect() does not wait");
        interrupt(&con);
        EXPECT_RETURNS(&con, -1, EINTR, "interrupted connect()");
        op_finish(&con);
    }

    // The socket is not connected, and connects as soon as there is room
    int s = accept(l, NULL, NULL);
    CHECK(s >= 0, "failed to accept");
    CHECK(connect_to(c, &a) == 0, "connect() fails after the interrupted ones");
    // ... and the room is not taken by the interrupted calls
    int fds[4];
    int n = fill_backlog(&a, fds, 4);
    CHECK(n == 0, "the backlog takes %d connections more than it should", n);
    close(s);
    close(c);
    close(first);
    close(l);
    return 0;
}

// The wait of connect() ends with ECONNREFUSED if the listener is closed meanwhile. The
// threads that wait do not see EPIPE or any other error
static int test_blocked_connect_is_refused_when_the_listener_closes(void) {
    unix_addr_t a;
    int l = listener(&a, 0);
    CHECK(l >= 0, "failed to listen");
    int first = new_socket(0);
    CHECK(connect_to(first, &a) == 0, "failed to connect");

    int cs[3];
    op_t cons[3];
    for (int i = 0; i < 3; i++) {
        cs[i] = new_socket(0);
        CHECK(op_start_connect(&cons[i], cs[i], &a) == 0, "failed to start the thread");
    }
    sleep_ms(SETTLE_MS);
    close(l);
    for (int i = 0; i < 3; i++) {
        EXPECT_RETURNS(&cons[i], -1, ECONNREFUSED, "connect() whose listener is closed");
        op_finish(&cons[i]);
        close(cs[i]);
    }
    close(first);
    return 0;
}

// ---------------------------------------------------------------------------------------------
// listen() again
// ---------------------------------------------------------------------------------------------

// listen() on a listening socket changes the backlog at once, also if another thread waits in
// accept(), and the connections reach the accept() that waits
static int test_listen_again_while_accept_waits(void) {
    static const int backlogs[] = { 5, 0, 1, -1 };
    unix_addr_t a;
    int l = listener(&a, 2);
    CHECK(l >= 0, "failed to listen");
    op_t acc;
    CHECK(op_start(&acc, OP_ACCEPT, l) == 0, "failed to start the thread");
    CHECK(op_blocks(&acc), "accept() does not wait");

    for (size_t i = 0; i < ARRAY_SIZE(backlogs); i++) {
        op_t lis;
        CHECK(op_start_listen(&lis, l, backlogs[i]) == 0, "failed to start the thread");
        EXPECT_RETURNS(&lis, 0, 0, "listen() while another thread waits in accept()");
        op_finish(&lis);
        CHECK(!op_done(&acc), "accept() returned without a connection");
    }

    // A connection reaches the accept() that waits, and a second one waits in the queue
    int c = new_socket(0);
    op_t con;
    CHECK(op_start_connect(&con, c, &a) == 0, "failed to start the thread");
    EXPECT_RETURNS(&con, 0, 0, "connect() after listen() again");
    op_finish(&con);
    CHECK(op_wait(&acc, WAKE_MS) && acc.ret >= 0, "accept() does not get the connection");
    close((int)acc.ret);
    op_finish(&acc);
    int c2 = new_socket(0);
    CHECK(connect_to(c2, &a) == 0, "failed to connect for the second time");
    int s = accept(l, NULL, NULL);
    CHECK(s >= 0, "failed to accept the second connection");
    close(s);
    close(c2);
    close(c);
    close(l);
    return 0;
}

// The connections that are queued stay if the backlog is made smaller, and there is no room for
// more of them until they are accepted
static int test_smaller_backlog_keeps_the_connections(void) {
    enum { QUEUED = 5 };
    unix_addr_t a;
    int l = listener(&a, 8);
    CHECK(l >= 0, "failed to listen");
    int cs[QUEUED];
    for (int i = 0; i < QUEUED; i++) {
        cs[i] = new_socket(0);
        CHECK(connect_to(cs[i], &a) == 0, "failed to connect");
        char msg = 'a' + i;
        CHECK(write(cs[i], &msg, 1) == 1, "failed to write");
    }
    CHECK(listen(l, 1) == 0, "failed to listen with a smaller backlog");

    int extra = new_socket(1);
    CHECK(connect_fails(extra, &a, EAGAIN),
          "the connections that are queued leave room for a new one");

    // The connections are accepted in the order in which they came, with their data
    set_nonblocking(l, 1);
    for (int i = 0; i < QUEUED; i++) {
        int s = accept(l, NULL, NULL);
        CHECK(s >= 0, "only %d of %d queued connections were accepted", i, (int)QUEUED);
        char msg = 0;
        CHECK(read(s, &msg, 1) == 1 && msg == 'a' + i,
              "wrong data of the accepted connection %d", i);
        close(s);
        if (i == 1) {
            // There are more connections than the backlog allows
            errno = 0;
            CHECK(connect_to(extra, &a) == -1 && errno == EAGAIN,
                  "there is room after %d accepts, with %d connections left", i + 1,
                  QUEUED - i - 1);
        }
    }
    CHECK(accept_fails(l, EAGAIN), "more connections than were queued");
    // Now there is room for the connections that the backlog allows
    CHECK(connect_to(extra, &a) == 0, "connect() fails with room");
    int c2 = new_socket(1);
    CHECK(connect_to(c2, &a) == 0, "the backlog 1 does not take two connections");
    int c3 = new_socket(1);
    CHECK(connect_fails(c3, &a, EAGAIN), "the backlog 1 takes three connections");
    close(c3);
    close(c2);
    close(extra);
    for (int i = 0; i < QUEUED; i++) {
        close(cs[i]);
    }
    close(l);
    return 0;
}

// A greater backlog lets the waiting connect() calls go on at once
static int test_greater_backlog_wakes_blocked_connects(void) {
    unix_addr_t a;
    int l = listener(&a, 0);
    CHECK(l >= 0, "failed to listen");
    int first = new_socket(0);
    CHECK(connect_to(first, &a) == 0, "failed to connect");

    int cs[3];
    op_t cons[3];
    for (int i = 0; i < 3; i++) {
        cs[i] = new_socket(0);
        CHECK(op_start_connect(&cons[i], cs[i], &a) == 0, "failed to start the thread");
    }
    sleep_ms(SETTLE_MS);
    for (int i = 0; i < 3; i++) {
        CHECK(!op_done(&cons[i]), "connect() on a full backlog does not wait");
    }
    CHECK(listen(l, 5) == 0, "failed to listen with a greater backlog");
    for (int i = 0; i < 3; i++) {
        EXPECT_RETURNS(&cons[i], 0, 0, "connect() after the backlog grew");
        op_finish(&cons[i]);
        close(cs[i]);
    }
    int accepted = accept_all(l);
    CHECK(accepted == 4, "%d connections were queued, instead of 4", accepted);
    close(first);
    close(l);
    return 0;
}

// ---------------------------------------------------------------------------------------------
// accept()
// ---------------------------------------------------------------------------------------------

// A signal interrupts accept()
static int test_accept_is_interrupted(void) {
    unix_addr_t a;
    int l = listener(&a, 2);
    CHECK(l >= 0, "failed to listen");
    for (int i = 0; i < 3; i++) {
        op_t acc;
        CHECK(op_start(&acc, OP_ACCEPT, l) == 0, "failed to start the thread");
        CHECK(op_blocks(&acc), "accept() does not wait");
        interrupt(&acc);
        EXPECT_RETURNS(&acc, -1, EINTR, "interrupted accept()");
        op_finish(&acc);
    }
    // The next connection is accepted, once
    int c = new_socket(0);
    CHECK(connect_to(c, &a) == 0, "failed to connect");
    int s = accept(l, NULL, NULL);
    CHECK(s >= 0, "failed to accept after the interrupted calls");
    close(s);
    close(c);
    close(l);
    return 0;
}

// The events of a listener: readable if a connection is waiting
static int test_events_of_a_listener(void) {
    unix_addr_t a;
    int l = listener(&a, 4);
    CHECK(l >= 0, "failed to listen");
    CHECK(poll_now(l, EV_ALL) == 0, "events without connections: %s",
          events_str(poll_now(l, EV_ALL)));
    int c1 = new_socket(0);
    int c2 = new_socket(0);
    CHECK(connect_to(c1, &a) == 0 && connect_to(c2, &a) == 0, "failed to connect");
    CHECK(poll_now(l, EV_ALL) == POLLIN, "events with connections: %s",
          events_str(poll_now(l, EV_ALL)));
    int s = accept(l, NULL, NULL);
    CHECK(s >= 0, "failed to accept");
    close(s);
    CHECK(poll_now(l, EV_ALL) == POLLIN, "events with a connection: %s",
          events_str(poll_now(l, EV_ALL)));
    s = accept(l, NULL, NULL);
    CHECK(s >= 0, "failed to accept");
    close(s);
    CHECK(poll_now(l, EV_ALL) == 0, "events without connections after the accepts: %s",
          events_str(poll_now(l, EV_ALL)));
    close(c2);
    close(c1);
    close(l);
    return 0;
}

static test_case_t test_cases[] = {
    TEST_CASE(test_backlog_takes_one_more_connection),
    TEST_CASE(test_negative_backlog_is_the_greatest),
    TEST_CASE(test_concurrent_connects_fill_the_backlog_exactly),
    TEST_CASE(test_nonblocking_connect_on_full_backlog),
    TEST_CASE(test_blocking_connect_waits_for_accept),
    TEST_CASE(test_blocked_connects_proceed_one_by_one),
    TEST_CASE(test_many_blocked_connects_all_proceed),
    TEST_CASE(test_interrupted_connect_leaves_the_room_to_the_next),
    TEST_CASE(test_connect_timeout_on_full_backlog),
    TEST_CASE(test_connect_on_full_backlog_is_interrupted),
    TEST_CASE(test_blocked_connect_is_refused_when_the_listener_closes),
    TEST_CASE(test_listen_again_while_accept_waits),
    TEST_CASE(test_smaller_backlog_keeps_the_connections),
    TEST_CASE(test_greater_backlog_wakes_blocked_connects),
    TEST_CASE(test_accept_is_interrupted),
    TEST_CASE(test_events_of_a_listener),
};

int main(int argc, const char *argv[]) {
    signal(SIGPIPE, SIG_IGN);
    // The signal of the tests that interrupt a call: without SA_RESTART
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_signal;
    sigaction(SIGUSR1, &sa, NULL);

    test_case_t selected[sizeof(test_cases) / sizeof(test_cases[0])];
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
    if (atomic_load(&stuck_threads) > 0) {
        // Threads are left behind in calls that do not return
        _exit(1);
    }
    return ret;
}
