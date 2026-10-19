// The send buffer of unix stream sockets (SO_SNDBUF): how large a program may make it, and what
// happens when threads change it while the socket connects, writes, is polled and is closed. The
// test passes on Linux, too (gcc -pthread -I../include main.c), and the LibOS does what Linux does:
//
//  - A size above net.core.wmem_max (212992 unless the system is tuned) is cut to it: the socket has
//    twice the size that is set (SO_SNDBUF reads it), and the bytes in flight to the peer are
//    limited by that. In the LibOS, the connection of a socket may queue these bytes from the
//    kernel heap, so that a socket that is set to a huge size and has a slow reader cannot take
//    the heap. (SO_SNDBUFFORCE, which Linux allows root without limit, is not limited.)
//  - Threads that set the size at the same time, or that set it while the socket connects, leave
//    the connection with the size that the socket has, which getsockopt() reads.
//  - A thread that waits for room, a thread that polls the socket, a thread that writes, and a
//    thread that closes the peer all go on while the size is changed again and again.
//
// Usage: unix_socket_sndbuf [test case name | --list]
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
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#include "test.h"

#define DEFAULT_WMEM_MAX    212992      // net.core.wmem_max of Linux, which the LibOS follows
#define ROUNDS              600
#define WATCHDOG_MS         120000      // the time that the test may take

#define CHECK(cond, fmt, ...) do { \
        if (!(cond)) { \
            THROW_ERROR(fmt, ##__VA_ARGS__); \
        } \
    } while (0)

static atomic_long watchdog_deadline;

static long now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
}

static void *watchdog_main(void *arg) {
    for (;;) {
        usleep(100000);
        long deadline = atomic_load(&watchdog_deadline);
        if (deadline != 0 && now_ms() > deadline) {
            printf("\t\tERROR:the test case did not finish in %d ms, it hangs\n", WATCHDOG_MS);
            fflush(stdout);
            _exit(1);
        }
    }
    return NULL;
}

static int in_occlum(void) {
    return getenv("OCCLUM") != NULL;
}

// The limit of SO_SNDBUF of the system that the test runs on
static long wmem_max(void) {
    long value = DEFAULT_WMEM_MAX;
    FILE *file = fopen("/proc/sys/net/core/wmem_max", "r");
    if (file != NULL) {
        if (fscanf(file, "%ld", &value) != 1) {
            value = DEFAULT_WMEM_MAX;
        }
        fclose(file);
    }
    return value;
}

static int set_sndbuf(int fd, int size) {
    return setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &size, sizeof(size));
}

static int get_sndbuf(int fd) {
    int size = -1;
    socklen_t len = sizeof(size);
    if (getsockopt(fd, SOL_SOCKET, SO_SNDBUF, &size, &len) < 0) {
        return -1;
    }
    return size;
}

static int make_pair(int sv[2]) {
    return socketpair(AF_UNIX, SOCK_STREAM, 0, sv);
}

static void set_nonblocking(int fd, int nonblocking) {
    int flags = fcntl(fd, F_GETFL);
    fcntl(fd, F_SETFL, nonblocking ? (flags | O_NONBLOCK) : (flags & ~O_NONBLOCK));
}

// Writes to the socket until it is full, without blocking. Returns the number of the bytes, or -1.
static long fill(int fd) {
    static __thread char chunk[4096];
    int was_nonblocking = (fcntl(fd, F_GETFL) & O_NONBLOCK) != 0;
    long total = 0;
    size_t size = sizeof(chunk);

    set_nonblocking(fd, 1);
    while (total < (64L << 20)) {
        ssize_t n = write(fd, chunk, size);
        if (n > 0) {
            total += n;
        } else if (n < 0 && errno == EAGAIN && size > 1) {
            // The space that is left may be less than a chunk
            size = 1;
        } else {
            break;
        }
    }
    int err = errno;
    set_nonblocking(fd, was_nonblocking);
    errno = err;
    return total < (64L << 20) && errno == EAGAIN ? total : -1;
}

// Reads from the socket until there is nothing more to read, without blocking. Returns the number
// of the bytes, or -1.
static long drain(int fd) {
    static __thread char chunk[65536];
    int was_nonblocking = (fcntl(fd, F_GETFL) & O_NONBLOCK) != 0;
    long total = 0;
    ssize_t n;

    set_nonblocking(fd, 1);
    while ((n = read(fd, chunk, sizeof(chunk))) > 0) {
        total += n;
    }
    int err = errno;
    set_nonblocking(fd, was_nonblocking);
    errno = err;
    return n < 0 && errno == EAGAIN ? total : -1;
}

// ---------------------------------------------------------------------------------------------
// The limit
// ---------------------------------------------------------------------------------------------

// A size that is above the limit is cut to it, and so is the number of the bytes that a socket
// takes. A socket that is set to a size below it has twice that size.
static int test_large_sizes_are_limited(void) {
    long limit = wmem_max();
    int sv[2];
    CHECK(make_pair(sv) == 0, "failed to create a socket pair");

    static const int sizes[] = { 100000, DEFAULT_WMEM_MAX, 1 << 20, 4 << 20, 1 << 30, INT32_MAX };
    for (size_t i = 0; i < ARRAY_SIZE(sizes); i++) {
        CHECK(set_sndbuf(sv[0], sizes[i]) == 0, "failed to set SO_SNDBUF to %d", sizes[i]);
        long want = 2 * (sizes[i] < limit ? sizes[i] : limit);
        CHECK(get_sndbuf(sv[0]) == want, "SO_SNDBUF of %d reads %d, instead of %ld", sizes[i],
              get_sndbuf(sv[0]), want);
    }

    // The bytes that the socket takes are cut to the limit, too, whatever the size that was set
    CHECK(set_sndbuf(sv[0], 1 << 30) == 0, "failed to set SO_SNDBUF");
    long taken = fill(sv[0]);
    CHECK(taken > 100000 &&
          taken <= 2 * limit, "a socket with SO_SNDBUF of 1 GiB takes %ld bytes",
          taken);
    if (in_occlum() && limit == DEFAULT_WMEM_MAX) {
        // The queue of the LibOS counts bytes, which Linux does not, so that it is exact
        CHECK(taken == 2 * DEFAULT_WMEM_MAX, "a socket with SO_SNDBUF of 1 GiB takes %ld bytes, "
              "instead of %d", taken, 2 * DEFAULT_WMEM_MAX);
    }
    CHECK(drain(sv[1]) == taken, "not all the bytes arrive");
    close(sv[0]);
    close(sv[1]);
    return 0;
}

// ---------------------------------------------------------------------------------------------
// Threads that change the size
// ---------------------------------------------------------------------------------------------

// A thread that sets the size of a socket as soon as the others have started, which the test
// lets them do together, to hit the moments at which the calls overlap
typedef struct {
    int fd;
    int size;
    atomic_int *ready;
    atomic_int *go;
    pthread_t thread;
} setter_t;

static void *setter_main(void *arg) {
    setter_t *s = arg;
    atomic_fetch_add(s->ready, 1);
    while (!atomic_load(s->go)) {
    }
    set_sndbuf(s->fd, s->size);
    return NULL;
}

// The socket takes about the number of bytes that its size says
static int check_size_matches_the_bytes(int fd, int peer, const char *what) {
    int size = get_sndbuf(fd);
    long taken = fill(fd);
    CHECK(taken >= 0, "failed to fill the socket (%s)", what);
    if (size == 8192) {
        CHECK(taken < 50000, "%s: the socket has the size %d, but takes %ld bytes", what, size,
              taken);
    } else if (size == 200000) {
        CHECK(taken > 100000, "%s: the socket has the size %d, but takes %ld bytes", what, size,
              taken);
    } else {
        THROW_ERROR("%s: the socket has the size %d", what, size);
    }
    CHECK(drain(peer) == taken, "%s: not all the bytes arrive", what);
    return 0;
}

// Two threads set two sizes at the same time: the socket ends up with the connection of the size
// that SO_SNDBUF reads, whichever thread was last
static int test_concurrent_setsockopt_agree(void) {
    for (int round = 0; round < ROUNDS; round++) {
        int sv[2];
        CHECK(make_pair(sv) == 0, "failed to create a socket pair");
        atomic_int ready = 0, go = 0;
        setter_t setters[2] = {
            { sv[0], 4096, &ready, &go }, { sv[0], 100000, &ready, &go },
        };
        for (int i = 0; i < 2; i++) {
            CHECK(pthread_create(&setters[i].thread, NULL, setter_main, &setters[i]) == 0,
                  "failed to create a thread");
        }
        while (atomic_load(&ready) < 2) {
        }
        atomic_store(&go, 1);
        for (int i = 0; i < 2; i++) {
            pthread_join(setters[i].thread, NULL);
        }
        char what[64];
        snprintf(what, sizeof(what), "round %d", round);
        CHECK(check_size_matches_the_bytes(sv[0], sv[1], what) == 0, "the size is wrong");
        close(sv[0]);
        close(sv[1]);
    }
    return 0;
}

typedef struct {
    int fd;
    struct sockaddr_un addr;
    socklen_t len;
    atomic_int *ready;
    atomic_int *go;
    int err;
    pthread_t thread;
} connector_t;

static void *connector_main(void *arg) {
    connector_t *c = arg;
    atomic_fetch_add(c->ready, 1);
    while (!atomic_load(c->go)) {
    }
    c->err = connect(c->fd, (struct sockaddr *)&c->addr, c->len) < 0 ? errno : 0;
    return NULL;
}

// A thread sets the size of a socket while another thread connects it: the connection has the size
// at the end, whether the size was set before, during or after the connection was made
static int test_setsockopt_races_with_connect(void) {
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    int n = snprintf(addr.sun_path + 1, sizeof(addr.sun_path) - 1, "unix_socket_sndbuf.%d",
                     (int)getpid());
    socklen_t len = offsetof(struct sockaddr_un, sun_path) + 1 + n;
    int l = socket(AF_UNIX, SOCK_STREAM, 0);
    CHECK(bind(l, (struct sockaddr *)&addr, len) == 0 &&
          listen(l, 8) == 0, "failed to listen");

    for (int round = 0; round < ROUNDS; round++) {
        int c = socket(AF_UNIX, SOCK_STREAM, 0);
        CHECK(c >= 0, "failed to create a socket");
        atomic_int ready = 0, go = 0;
        connector_t connector = { .fd = c, .addr = addr, .len = len, .ready = &ready, .go = &go };
        setter_t setter = { c, 4096, &ready, &go };
        CHECK(pthread_create(&connector.thread, NULL, connector_main, &connector) == 0,
              "failed to create a thread");
        CHECK(pthread_create(&setter.thread, NULL, setter_main, &setter) == 0,
              "failed to create a thread");
        while (atomic_load(&ready) < 2) {
        }
        atomic_store(&go, 1);
        pthread_join(connector.thread, NULL);
        pthread_join(setter.thread, NULL);
        CHECK(connector.err == 0, "connect failed: %s", strerror(connector.err));
        int s = accept(l, NULL, NULL);
        CHECK(s >= 0, "failed to accept");

        char what[64];
        snprintf(what, sizeof(what), "round %d", round);
        CHECK(check_size_matches_the_bytes(c, s, what) == 0, "the size is wrong");
        close(c);
        close(s);
    }
    close(l);
    return 0;
}

// ---------------------------------------------------------------------------------------------
// Waiting writers, pollers and a close, while the size changes
// ---------------------------------------------------------------------------------------------

typedef struct {
    int fd;
    atomic_int stop;
    atomic_long done;       // the bytes that a thread has moved
    pthread_t thread;
} actor_t;

// Changes the size again and again, up and down
static void *resizer_main(void *arg) {
    actor_t *a = arg;
    int sizes[] = { 4096, 100000, 8192, 50000, 4096, 200000 };
    for (int i = 0; !atomic_load(&a->stop); i++) {
        set_sndbuf(a->fd, sizes[i % ARRAY_SIZE(sizes)]);
        atomic_fetch_add(&a->done, 1);
        if (i % 16 == 0) {
            usleep(100);
        }
    }
    return NULL;
}

// Writes blocking, until the peer is gone
static void *writer_main(void *arg) {
    actor_t *a = arg;
    static __thread char buf[16384];
    while (!atomic_load(&a->stop)) {
        ssize_t n = write(a->fd, buf, sizeof(buf));
        if (n <= 0) {
            break;
        }
        atomic_fetch_add(&a->done, n);
    }
    return NULL;
}

// Reads a little at a time, which keeps the buffer full, until it is told to stop
static void *reader_main(void *arg) {
    actor_t *a = arg;
    static __thread char buf[3000];
    while (!atomic_load(&a->stop)) {
        struct pollfd pfd = { a->fd, POLLIN, 0 };
        if (poll(&pfd, 1, 10) == 1) {
            ssize_t n = read(a->fd, buf, sizeof(buf));
            if (n <= 0) {
                break;
            }
            atomic_fetch_add(&a->done, n);
        }
    }
    return NULL;
}

// Waits for the socket to be writable, with epoll and with poll, which the resizing of the buffer
// has to notify
static void *poller_main(void *arg) {
    actor_t *a = arg;
    int ep = epoll_create1(0);
    struct epoll_event ev = { .events = EPOLLOUT | EPOLLIN | EPOLLET, .data.fd = a->fd };
    epoll_ctl(ep, EPOLL_CTL_ADD, a->fd, &ev);
    while (!atomic_load(&a->stop)) {
        struct epoll_event out[4];
        int n = epoll_wait(ep, out, 4, 5);
        atomic_fetch_add(&a->done, n > 0 ? n : 0);
        struct pollfd pfd = { a->fd, POLLOUT, 0 };
        poll(&pfd, 1, 1);
    }
    close(ep);
    return NULL;
}

static int test_resizing_while_others_wait_poll_and_close(void) {
    for (int round = 0; round < 20; round++) {
        int sv[2];
        CHECK(make_pair(sv) == 0, "failed to create a socket pair");
        // The threads have their own descriptors, so that closing the one of the peer does not
        // make them use another file
        int writer_fd = dup(sv[0]), resizer_fd = dup(sv[0]), poller_fd = dup(sv[0]);
        int reader_fd = dup(sv[1]);
        CHECK(writer_fd >= 0 && resizer_fd >= 0 && poller_fd >= 0 && reader_fd >= 0,
              "failed to dup");
        actor_t writer = { .fd = writer_fd }, resizer = { .fd = resizer_fd },
                poller = { .fd = poller_fd }, reader = { .fd = reader_fd };
        CHECK(pthread_create(&reader.thread, NULL, reader_main, &reader) == 0 &&
              pthread_create(&writer.thread, NULL, writer_main, &writer) == 0 &&
              pthread_create(&resizer.thread, NULL, resizer_main, &resizer) == 0 &&
              pthread_create(&poller.thread, NULL, poller_main, &poller) == 0,
              "failed to create the threads");

        // Let them work, and then close the peer, which ends the writer with EPIPE
        usleep(30000);
        long moved = atomic_load(&writer.done);
        CHECK(moved > 0, "the writer has moved no bytes");
        atomic_store(&reader.stop, 1);
        pthread_join(reader.thread, NULL);
        close(reader_fd);
        close(sv[1]);
        // The writer is blocked, as nobody reads, until the size is raised, or the peer is gone
        usleep(5000);
        atomic_store(&resizer.stop, 1);
        atomic_store(&poller.stop, 1);
        pthread_join(resizer.thread, NULL);
        pthread_join(poller.thread, NULL);
        // The writer fails when nobody reads: the buffer is full, and the peer is closed already
        signal(SIGPIPE, SIG_IGN);
        pthread_join(writer.thread, NULL);
        close(writer_fd);
        close(resizer_fd);
        close(poller_fd);
        close(sv[0]);
    }
    return 0;
}

static test_case_t test_cases[] = {
    TEST_CASE(test_large_sizes_are_limited),
    TEST_CASE(test_concurrent_setsockopt_agree),
    TEST_CASE(test_setsockopt_races_with_connect),
    TEST_CASE(test_resizing_while_others_wait_poll_and_close),
};

int main(int argc, const char *argv[]) {
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
        atomic_store(&watchdog_deadline, now_ms() + WATCHDOG_MS);
        if (cases[i].func() < 0) {
            printf("  func %s - [ERR]\n", cases[i].name);
            ret = -1;
        } else {
            printf("  func %s - [OK]\n", cases[i].name);
        }
        atomic_store(&watchdog_deadline, 0);
        fflush(stdout);
    }
    fflush(stdout);
    return ret;
}
