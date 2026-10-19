// Reading and writing on unix stream sockets: what the calls return when they block, how long they
// wait, and what the flags of the send and receive calls do. The expectations are
// the behavior of Linux (checked on Linux 7.0): the test passes there, too (gcc -pthread
// -I../include main.c).
//
//  - A blocking write, send, writev or sendmsg waits until everything is written, unless a signal
//    interrupts it or the timeout of SO_SNDTIMEO is over, which return the bytes written so far (or
//    EINTR and EAGAIN if there are none). A nonblocking socket, or MSG_DONTWAIT, writes what fits.
//  - SO_RCVTIMEO and SO_SNDTIMEO end a wait with EAGAIN. A receive with MSG_WAITALL returns the
//    bytes that came by then. The timeouts belong to the socket, not to the descriptor.
//  - MSG_DONTWAIT, MSG_PEEK and MSG_WAITALL, and the flags that have no effect.
//
// MSG_OOB is not supported by the LibOS: it takes the byte as plain data (see
// test_oob_flag_does_not_hang).
//
// The calls that may block run in threads, so that a call that does not return when it should
// makes the test case fail after a timeout instead of hanging it. A thread that never returns is
// left behind, so the test ends with _exit if there are any. A watchdog thread ends the test if a
// test case takes too long.
//
// Usage: unix_socket_io [test case name | --list]
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#include "test.h"

#define MIB             (1024 * 1024)
#define SETTLE_MS       150     // time that a call has to block before the stimulus
#define WAKE_MS         5000    // time that a call has to return
#define WATCHDOG_MS     120000  // time that a test case may take
#define TIMEOUT_MS      200     // the timeout that the tests set
#define SLACK_MS        2500    // time that a timeout may take more than it says (a loaded host)
#define EARLY_MS        20      // time that a timeout may take less than it says
#define STACK_SIZE      (256 * 1024)
#define MAX_PIECES      4
#define STIM_CLOSE      (-1)

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

static int get_tid(void) {
    return (int)syscall(SYS_gettid);
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

// SIGUSR1 interrupts the calls (the handler is installed without SA_RESTART)
static void on_signal(int sig) {
}

static void set_nonblocking(int fd, int nonblocking) {
    int flags = fcntl(fd, F_GETFL);
    fcntl(fd, F_SETFL, nonblocking ? (flags | O_NONBLOCK) : (flags & ~O_NONBLOCK));
}

static int is_nonblocking(int fd) {
    return (fcntl(fd, F_GETFL) & O_NONBLOCK) != 0;
}

static int make_pair(int sv[2]) {
    return socketpair(AF_UNIX, SOCK_STREAM, 0, sv);
}

static int set_timeout(int fd, int optname, long ms) {
    struct timeval timeout = { ms / 1000, (ms % 1000) * 1000 };
    return setsockopt(fd, SOL_SOCKET, optname, &timeout, sizeof(timeout));
}

// Writes to the socket until it is full, without blocking. Returns the number of the bytes, or -1.
static long fill(int fd) {
    static char chunk[4096];
    int was_nonblocking = is_nonblocking(fd);
    long total = 0;
    size_t size = sizeof(chunk);

    set_nonblocking(fd, 1);
    while (total < 64 * MIB) {
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
    return total < 64 * MIB && errno == EAGAIN ? total : -1;
}

// Reads from the socket until there is nothing more to read, without blocking. Returns the number
// of the bytes, or -1.
static long drain(int fd) {
    static char chunk[65536];
    int was_nonblocking = is_nonblocking(fd);
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

// A pattern that tells where a byte belongs: its value follows from its position
static char pattern_byte(size_t pos) {
    return (char)(pos * 7 + pos / 251);
}

static void fill_pattern(char *buf, size_t len, size_t pos) {
    for (size_t i = 0; i < len; i++) {
        buf[i] = pattern_byte(pos + i);
    }
}

// Returns the position of the first byte that does not follow the pattern, or `len`
static size_t check_pattern(const char *buf, size_t len, size_t pos) {
    for (size_t i = 0; i < len; i++) {
        if (buf[i] != pattern_byte(pos + i)) {
            return i;
        }
    }
    return len;
}

// ---------------------------------------------------------------------------------------------
// Calls in threads
// ---------------------------------------------------------------------------------------------

typedef struct call call_t;
typedef long (*call_fn)(call_t *);

struct call {
    call_fn fn;
    int fd;
    char *buf;
    size_t len;
    int flags;
    int pieces;                 // the number of the iovecs of the vectored calls
    struct sockaddr_un name;    // the address of the calls that have one
    socklen_t namelen;          // its length: the length that a receive returns
    int msg_flags;              // the flags that recvmsg returns
    int mask_usr2;              // block SIGUSR2 in the thread, before the call
    pthread_t thread;
    int created;
    atomic_int started;
    atomic_int done;            // the results are valid if it is set
    atomic_int tid;
    long ret;
    int err;
    long elapsed_ms;
};

static int fill_iov(struct iovec *iov, char *buf, size_t len, int pieces) {
    if (pieces < 1) {
        pieces = 1;
    }
    if (pieces > MAX_PIECES) {
        pieces = MAX_PIECES;
    }
    size_t each = len / pieces;
    for (int i = 0; i < pieces; i++) {
        iov[i].iov_base = buf + i * each;
        iov[i].iov_len = i == pieces - 1 ? len - i * each : each;
    }
    return pieces;
}

static long call_read(call_t *c) {
    return read(c->fd, c->buf, c->len);
}

static long call_recv(call_t *c) {
    return recv(c->fd, c->buf, c->len, c->flags);
}

static long call_recvfrom(call_t *c) {
    c->namelen = sizeof(c->name);
    return recvfrom(c->fd, c->buf, c->len, c->flags, (struct sockaddr *)&c->name,
                    &c->namelen);
}

static long call_readv(call_t *c) {
    struct iovec iov[MAX_PIECES];
    int n = fill_iov(iov, c->buf, c->len, c->pieces);
    return readv(c->fd, iov, n);
}

static long call_recvmsg(call_t *c) {
    struct iovec iov[MAX_PIECES];
    struct msghdr msg;
    memset(&msg, 0, sizeof(msg));
    msg.msg_iov = iov;
    msg.msg_iovlen = fill_iov(iov, c->buf, c->len, c->pieces);
    msg.msg_name = &c->name;
    msg.msg_namelen = sizeof(c->name);
    long ret = recvmsg(c->fd, &msg, c->flags);
    c->namelen = msg.msg_namelen;
    c->msg_flags = msg.msg_flags;
    return ret;
}

static long call_write(call_t *c) {
    return write(c->fd, c->buf, c->len);
}

static long call_send(call_t *c) {
    return send(c->fd, c->buf, c->len, c->flags);
}

static long call_sendto(call_t *c) {
    if (c->namelen == 0) {
        return sendto(c->fd, c->buf, c->len, c->flags, NULL, 0);
    }
    return sendto(c->fd, c->buf, c->len, c->flags, (struct sockaddr *)&c->name, c->namelen);
}

static long call_writev(call_t *c) {
    struct iovec iov[MAX_PIECES];
    int n = fill_iov(iov, c->buf, c->len, c->pieces);
    return writev(c->fd, iov, n);
}

static long call_sendmsg(call_t *c) {
    struct iovec iov[MAX_PIECES];
    struct msghdr msg;
    memset(&msg, 0, sizeof(msg));
    msg.msg_iov = iov;
    msg.msg_iovlen = fill_iov(iov, c->buf, c->len, c->pieces);
    if (c->namelen != 0) {
        msg.msg_name = &c->name;
        msg.msg_namelen = c->namelen;
    }
    return sendmsg(c->fd, &msg, c->flags);
}

typedef struct {
    const char *name;
    call_fn fn;
} named_call_t;

// The calls that read, and the calls that write, from a socket. The vectored ones get two pieces.
static const named_call_t recv_calls[] = {
    { "read", call_read }, { "recv", call_recv }, { "recvfrom", call_recvfrom },
    { "readv", call_readv }, { "recvmsg", call_recvmsg },
};

static const named_call_t send_calls[] = {
    { "write", call_write }, { "send", call_send }, { "sendto", call_sendto },
    { "writev", call_writev }, { "sendmsg", call_sendmsg },
};

// The calls that take flags
static const named_call_t recv_flag_calls[] = {
    { "recv", call_recv }, { "recvfrom", call_recvfrom }, { "recvmsg", call_recvmsg },
};

static const named_call_t send_flag_calls[] = {
    { "send", call_send }, { "sendto", call_sendto }, { "sendmsg", call_sendmsg },
};

static void *call_main(void *arg) {
    call_t *c = arg;

    atomic_store(&c->tid, get_tid());
    if (c->mask_usr2) {
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGUSR2);
        pthread_sigmask(SIG_BLOCK, &set, NULL);
    }
    atomic_store(&c->started, 1);
    long start = now_ms();
    errno = 0;
    long ret = c->fn(c);
    int err = errno;
    c->ret = ret;
    c->err = err;
    c->elapsed_ms = now_ms() - start;
    atomic_store(&c->done, 1);
    return NULL;
}

static void call_init(call_t *c, call_fn fn, int fd, char *buf, size_t len, int flags) {
    memset(c, 0, sizeof(*c));
    c->fn = fn;
    c->fd = fd;
    c->buf = buf;
    c->len = len;
    c->flags = flags;
    c->pieces = 2;
}

// Starts the thread of the call, and returns once it has started
static int call_run(call_t *c) {
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setstacksize(&attr, STACK_SIZE);
    int err = pthread_create(&c->thread, &attr, call_main, c);
    pthread_attr_destroy(&attr);
    if (err != 0) {
        errno = err;
        return -1;
    }
    c->created = 1;
    while (!atomic_load(&c->started)) {
        usleep(200);
    }
    return 0;
}

static int call_start(call_t *c, call_fn fn, int fd, char *buf, size_t len, int flags) {
    call_init(c, fn, fd, buf, len, flags);
    return call_run(c);
}

// The same for a vectored call with `pieces` iovecs, or for sendmmsg and recvmmsg with `pieces`
// messages
static int call_start_n(call_t *c, call_fn fn, int fd, char *buf, size_t len, int flags,
                        int pieces) {
    call_init(c, fn, fd, buf, len, flags);
    c->pieces = pieces;
    return call_run(c);
}

static int call_done(call_t *c) {
    return atomic_load(&c->done);
}

// Waits until the thread has returned, for at most `ms`. Returns whether it has.
static int call_wait(call_t *c, int ms) {
    long deadline = now_ms() + ms;
    while (!call_done(c)) {
        if (now_ms() >= deadline) {
            return 0;
        }
        usleep(200);
    }
    return 1;
}

// Lets the call block, and returns whether it still blocks
static int call_blocks(call_t *c) {
    sleep_ms(SETTLE_MS);
    return !call_done(c);
}

// Joins the thread if it has returned, or leaves it behind
static void call_finish(call_t *c) {
    if (!c->created) {
        return;
    }
    if (call_done(c)) {
        pthread_join(c->thread, NULL);
    } else {
        atomic_fetch_add(&stuck_threads, 1);
    }
    c->created = 0;
}

// Sends the signal to the thread of the call until the call returns. A signal that comes before
// the thread has entered the call has no effect on it, so it is sent again.
static void send_signals(call_t *c, int sig, int period_ms, int *count) {
    long deadline = now_ms() + WAKE_MS + SLACK_MS;
    while (!call_done(c) && now_ms() < deadline) {
        syscall(SYS_tgkill, getpid(), atomic_load(&c->tid), sig);
        if (count != NULL) {
            (*count)++;
        }
        call_wait(c, period_ms);
    }
}

static void interrupt(call_t *c) {
    send_signals(c, SIGUSR1, 25, NULL);
}

// The call returns what it should within `wait_ms`
#define EXPECT_CALL(c, wait_ms, want_ret, want_err, what) do { \
        if (!call_wait(c, wait_ms)) { \
            THROW_ERROR("%s does not return", what); \
        } \
        if ((c)->ret != (long)(want_ret) || \
                ((long)(want_ret) < 0 && (c)->err != (want_err))) { \
            errno = (c)->err; \
            THROW_ERROR("%s returned %ld, instead of %ld (with errno " #want_err ")", \
                        what, (c)->ret, (long)(want_ret)); \
        } \
    } while (0)

// The call has ended after about `ms` milliseconds
#define EXPECT_TIME(c, ms, what) do { \
        if ((c)->elapsed_ms < (ms) - EARLY_MS || (c)->elapsed_ms > (ms) + SLACK_MS) { \
            THROW_ERROR("%s took %ld ms, instead of %d ms", what, (c)->elapsed_ms, (int)(ms)); \
        } \
    } while (0)

// The call returns what it should, and does not wait for anything, which it checks by the time
// that the call takes: it is less than the timeouts that the tests set for the calls that must
// not wait (a second and more)
#define NOW_MS      1000
#define EXPECT_NOW(c, want_ret, want_err, what) do { \
        EXPECT_CALL(c, WAKE_MS, want_ret, want_err, what); \
        if ((c)->elapsed_ms >= NOW_MS) { \
            THROW_ERROR("%s waited for %ld ms", what, (c)->elapsed_ms); \
        } \
    } while (0)

// ---------------------------------------------------------------------------------------------
// Threads that move data, and threads that do something to a socket after a time
// ---------------------------------------------------------------------------------------------

typedef struct {
    int fd;
    size_t total;           // the number of bytes to move
    size_t chunk;           // the size of a read or write call
    int delay_ms;           // the time to wait before the first call
    int period_ms;          // the time to wait between the calls
    int value;              // the byte to write, or -1 for the pattern
    atomic_long done;       // the number of the bytes that were moved
    size_t bad_pos;         // the position of the first wrong byte that a reader got
    atomic_int err;
    pthread_t thread;
} flow_t;

static void *reader_main(void *arg) {
    flow_t *f = arg;
    char *buf = malloc(f->chunk);

    sleep_ms(f->delay_ms);
    while ((size_t)atomic_load(&f->done) < f->total) {
        ssize_t n = read(f->fd, buf, f->chunk);
        if (n <= 0) {
            atomic_store(&f->err, n < 0 ? errno : -1);
            break;
        }
        size_t pos = atomic_load(&f->done);
        size_t ok = check_pattern(buf, n, pos);
        if (ok < (size_t)n && f->bad_pos == f->total) {
            f->bad_pos = pos + ok;
        }
        atomic_fetch_add(&f->done, n);
        if (f->period_ms > 0) {
            sleep_ms(f->period_ms);
        }
    }
    free(buf);
    return NULL;
}

static void *writer_main(void *arg) {
    flow_t *f = arg;
    char *buf = malloc(f->chunk);

    sleep_ms(f->delay_ms);
    while ((size_t)atomic_load(&f->done) < f->total) {
        size_t pos = atomic_load(&f->done);
        size_t n = f->total - pos < f->chunk ? f->total - pos : f->chunk;
        if (f->value >= 0) {
            memset(buf, f->value, n);
        } else {
            fill_pattern(buf, n, pos);
        }
        ssize_t w = write(f->fd, buf, n);
        if (w <= 0) {
            atomic_store(&f->err, w < 0 ? errno : -1);
            break;
        }
        atomic_fetch_add(&f->done, w);
        if (f->period_ms > 0) {
            sleep_ms(f->period_ms);
        }
    }
    free(buf);
    return NULL;
}

static int flow_start(flow_t *f, void *(*main)(void *), int fd, size_t total,
                      size_t chunk,
                      int delay_ms, int period_ms) {
    memset(f, 0, sizeof(*f));
    f->fd = fd;
    f->total = total;
    f->chunk = chunk;
    f->delay_ms = delay_ms;
    f->period_ms = period_ms;
    f->value = -1;
    f->bad_pos = total;
    return pthread_create(&f->thread, NULL, main, f);
}

// Waits for the thread to move all its bytes, or to fail, for at most `ms`. Returns whether it has.
static int flow_join(flow_t *f, int ms) {
    long deadline = now_ms() + ms;
    while ((size_t)atomic_load(&f->done) < f->total && atomic_load(&f->err) == 0 &&
            now_ms() < deadline) {
        usleep(1000);
    }
    if ((size_t)atomic_load(&f->done) < f->total && atomic_load(&f->err) == 0) {
        atomic_fetch_add(&stuck_threads, 1);
        return 0;
    }
    pthread_join(f->thread, NULL);
    return (size_t)atomic_load(&f->done) >= f->total;
}

typedef struct {
    int fd;
    int delay_ms;
    int how;                // shutdown(): SHUT_RD, SHUT_WR, SHUT_RDWR, or STIM_CLOSE for close()
    const char *data;       // data to write instead
    pthread_t thread;
} stimulus_t;

static void *stimulus_main(void *arg) {
    stimulus_t *s = arg;
    sleep_ms(s->delay_ms);
    if (s->data != NULL) {
        if (write(s->fd, s->data, strlen(s->data)) < 0) {
            return NULL;
        }
    } else if (s->how != STIM_CLOSE) {
        shutdown(s->fd, s->how);
    } else {
        close(s->fd);
    }
    return NULL;
}

// Does something to the socket after `delay_ms`: writes `data` if there is some, else shuts it down
// with `how`, or closes it
static int stimulus_start(stimulus_t *s, int fd, int delay_ms, const char *data,
                          int how) {
    s->fd = fd;
    s->delay_ms = delay_ms;
    s->how = how;
    s->data = data;
    return pthread_create(&s->thread, NULL, stimulus_main, s);
}

static void stimulus_join(stimulus_t *s) {
    pthread_join(s->thread, NULL);
}

// ---------------------------------------------------------------------------------------------
// Blocking writes
// ---------------------------------------------------------------------------------------------

// A blocking write waits for the reader until everything is written
static int test_blocking_writes_write_everything(void) {
    char *data = malloc(MIB);
    fill_pattern(data, MIB, 0);

    for (size_t i = 0; i < ARRAY_SIZE(send_calls); i++) {
        int sv[2];
        CHECK(make_pair(sv) == 0, "failed to create a socket pair");
        flow_t reader;
        CHECK(flow_start(&reader, reader_main, sv[1], MIB, 65536, 100, 0) == 0,
              "failed to start the reader");

        call_t c;
        CHECK(call_start_n(&c, send_calls[i].fn, sv[0], data, MIB, 0, 3) == 0,
              "failed to start %s",
              send_calls[i].name);
        EXPECT_CALL(&c, 30000, MIB, 0, send_calls[i].name);
        call_finish(&c);
        CHECK(flow_join(&reader, WAKE_MS), "the reader of %s did not get everything",
              send_calls[i].name);
        CHECK(reader.bad_pos == MIB, "%s sent a wrong byte at %zu", send_calls[i].name,
              reader.bad_pos);
        close(sv[0]);
        close(sv[1]);
    }
    free(data);
    return 0;
}

// Two writers that write at the same time deliver all their bytes
static int test_concurrent_writes_deliver_everything(void) {
    int sv[2];
    CHECK(make_pair(sv) == 0, "failed to create a socket pair");
    flow_t writers[2];
    for (int i = 0; i < 2; i++) {
        CHECK(flow_start(&writers[i], writer_main, sv[0], 512 * 1024, 512 * 1024, 0, 0) == 0,
              "failed to start a writer");
        writers[i].value = 'A' + i;
    }
    size_t counts[2] = { 0, 0 };
    size_t got = 0;
    char buf[65536];
    while (got < MIB) {
        ssize_t n = read(sv[1], buf, sizeof(buf));
        CHECK(n > 0, "failed to read after %zu bytes", got);
        for (ssize_t j = 0; j < n; j++) {
            CHECK(buf[j] == 'A' || buf[j] == 'B', "a wrong byte is read");
            counts[buf[j] - 'A']++;
        }
        got += n;
    }
    CHECK(counts[0] == 512 * 1024 && counts[1] == 512 * 1024,
          "%zu A and %zu B arrive", counts[0], counts[1]);
    for (int i = 0; i < 2; i++) {
        CHECK(flow_join(&writers[i], WAKE_MS), "a writer does not end");
    }
    close(sv[0]);
    close(sv[1]);
    return 0;
}

// A nonblocking socket, or MSG_DONTWAIT, writes what fits, and fails with EAGAIN if nothing does
static int test_nonblocking_writes_are_partial(void) {
    char *data = malloc(MIB);
    fill_pattern(data, MIB, 0);

    for (int mode = 0; mode < 2; mode++) {
        // A nonblocking socket (all the calls), or MSG_DONTWAIT on a blocking socket (the calls
        // that take flags)
        size_t count = mode == 0 ? ARRAY_SIZE(send_calls) : ARRAY_SIZE(send_flag_calls);
        const named_call_t *calls = mode == 0 ? send_calls : send_flag_calls;
        for (size_t i = 0; i < count; i++) {
            const char *what = calls[i].name;
            int sv[2];
            CHECK(make_pair(sv) == 0, "failed to create a socket pair");
            set_nonblocking(sv[0], mode == 0);

            call_t c;
            CHECK(call_start(&c, calls[i].fn, sv[0], data, MIB, mode == 1 ? MSG_DONTWAIT : 0) == 0,
                  "failed to start %s", what);
            CHECK(call_wait(&c, WAKE_MS), "%s does not return (%s)", what, mode ? "flag" : "mode");
            CHECK(c.ret > 0 && c.ret < MIB, "%s wrote %ld bytes (%s)", what, c.ret,
                  mode ? "flag" : "mode");
            call_finish(&c);
            long written = c.ret;

            CHECK(call_start(&c, calls[i].fn, sv[0], data, 100, mode == 1 ? MSG_DONTWAIT : 0) == 0,
                  "failed to start %s", what);
            EXPECT_NOW(&c, -1, EAGAIN, what);
            call_finish(&c);

            CHECK(drain(sv[1]) == written, "not all the bytes of %s arrive", what);
            CHECK(is_nonblocking(sv[0]) == (mode == 0), "%s changed the mode of the socket", what);
            close(sv[0]);
            close(sv[1]);
        }
    }
    free(data);
    return 0;
}

// A signal ends a blocked write: with the bytes that were written so far, or EINTR if none
static int test_interrupted_write_returns_the_progress(void) {
    char *data = malloc(4 * MIB);
    char *buf = malloc(4 * MIB);
    fill_pattern(data, 4 * MIB, 0);

    for (size_t i = 0; i < ARRAY_SIZE(send_calls); i++) {
        const char *what = send_calls[i].name;
        int sv[2];
        CHECK(make_pair(sv) == 0, "failed to create a socket pair");

        call_t c;
        CHECK(call_start(&c, send_calls[i].fn, sv[0], data, 4 * MIB, 0) == 0,
              "failed to start %s",
              what);
        CHECK(call_blocks(&c), "%s of 4 MiB returns without a reader", what);
        interrupt(&c);
        CHECK(call_wait(&c, WAKE_MS), "%s is not interrupted", what);
        CHECK(c.ret > 0 && c.ret < 4 * MIB, "the interrupted %s returns %ld (errno %d)", what,
              c.ret, c.err);
        call_finish(&c);
        long written = c.ret;

        // The buffer is full now: nothing is written
        CHECK(call_start(&c, send_calls[i].fn, sv[0], data, 100, 0) == 0, "failed to start %s",
              what);
        CHECK(call_blocks(&c), "%s returns on a full buffer", what);
        interrupt(&c);
        EXPECT_CALL(&c, WAKE_MS, -1, EINTR, what);
        call_finish(&c);

        // What was written arrives, and nothing else
        long got = 0;
        while (got < written) {
            ssize_t n = read(sv[1], buf + got, 4 * MIB - got);
            CHECK(n > 0, "failed to read");
            got += n;
        }
        CHECK(check_pattern(buf, got, 0) == (size_t)got, "the bytes of %s are wrong", what);
        CHECK(drain(sv[1]) == 0, "more bytes than %ld arrive", written);
        close(sv[0]);
        close(sv[1]);
    }
    free(buf);
    free(data);
    return 0;
}

// SO_SNDTIMEO: the bytes that fit, or EAGAIN if none, after the timeout
static int test_write_timeout(void) {
    char *data = malloc(MIB);
    fill_pattern(data, MIB, 0);

    for (size_t i = 0; i < ARRAY_SIZE(send_calls); i++) {
        const char *what = send_calls[i].name;
        int sv[2];
        CHECK(make_pair(sv) == 0, "failed to create a socket pair");
        CHECK(set_timeout(sv[0], SO_SNDTIMEO, TIMEOUT_MS) == 0, "failed to set SO_SNDTIMEO");

        // Some bytes fit
        call_t c;
        CHECK(call_start(&c, send_calls[i].fn, sv[0], data, MIB, 0) == 0, "failed to start %s",
              what);
        CHECK(call_wait(&c, WAKE_MS), "%s does not time out", what);
        CHECK(c.ret > 0 && c.ret < MIB, "%s returns %ld (errno %d) at the timeout", what, c.ret,
              c.err);
        EXPECT_TIME(&c, TIMEOUT_MS, what);
        call_finish(&c);
        long written = c.ret;

        // None fit
        CHECK(call_start(&c, send_calls[i].fn, sv[0], data, 100, 0) == 0, "failed to start %s",
              what);
        EXPECT_CALL(&c, WAKE_MS, -1, EAGAIN, what);
        EXPECT_TIME(&c, TIMEOUT_MS, what);
        call_finish(&c);

        CHECK(drain(sv[1]) == written, "not all the bytes of %s arrive", what);
        close(sv[0]);
        close(sv[1]);
    }
    free(data);
    return 0;
}

// The time of SO_SNDTIMEO starts again when a reader makes room: it is how long a write waits for
// room, not how long it takes
static int test_write_timeout_restarts_with_progress(void) {
    const size_t total = 4 * MIB;
    char *data = malloc(total);
    fill_pattern(data, total, 0);

    int sv[2];
    CHECK(make_pair(sv) == 0, "failed to create a socket pair");
    CHECK(set_timeout(sv[0], SO_SNDTIMEO, 400) == 0, "failed to set SO_SNDTIMEO");
    flow_t reader;
    CHECK(flow_start(&reader, reader_main, sv[1], total, MIB, 0, 60) == 0,
          "failed to start the reader");

    call_t c;
    CHECK(call_start(&c, call_write, sv[0], data, total, 0) == 0, "failed to start write");
    EXPECT_CALL(&c, 60000, total, 0, "write with a timeout and a slow reader");
    CHECK(c.elapsed_ms > 400, "the write is over after %ld ms", c.elapsed_ms);
    call_finish(&c);
    CHECK(flow_join(&reader, WAKE_MS), "the reader does not get everything");
    CHECK(reader.bad_pos == total, "a wrong byte at %zu", reader.bad_pos);
    close(sv[0]);
    close(sv[1]);
    free(data);
    return 0;
}

// ---------------------------------------------------------------------------------------------
// Timeouts of reading, and of both
// ---------------------------------------------------------------------------------------------

// SO_RCVTIMEO: EAGAIN after the timeout, from every call that reads, and the data if it comes
// before
static int test_read_timeout(void) {
    char buf[64];

    for (size_t i = 0; i < ARRAY_SIZE(recv_calls); i++) {
        const char *what = recv_calls[i].name;
        int sv[2];
        CHECK(make_pair(sv) == 0, "failed to create a socket pair");
        CHECK(set_timeout(sv[0], SO_RCVTIMEO, TIMEOUT_MS) == 0, "failed to set SO_RCVTIMEO");

        call_t c;
        CHECK(call_start(&c, recv_calls[i].fn, sv[0], buf, sizeof(buf), 0) == 0,
              "failed to start %s", what);
        EXPECT_CALL(&c, WAKE_MS, -1, EAGAIN, what);
        EXPECT_TIME(&c, TIMEOUT_MS, what);
        call_finish(&c);

        // The data arrives before the timeout
        CHECK(set_timeout(sv[0], SO_RCVTIMEO, 3000) == 0, "failed to set SO_RCVTIMEO");
        stimulus_t s;
        CHECK(stimulus_start(&s, sv[1], 100, "ab", 0) == 0, "failed to start");
        CHECK(call_start(&c, recv_calls[i].fn, sv[0], buf, sizeof(buf), 0) == 0,
              "failed to start %s", what);
        EXPECT_CALL(&c, WAKE_MS, 2, 0, what);
        CHECK(c.elapsed_ms < 2500, "%s took %ld ms", what, c.elapsed_ms);
        call_finish(&c);
        stimulus_join(&s);
        close(sv[0]);
        close(sv[1]);
    }
    return 0;
}

// The timeouts belong to the socket: the descriptors that share it have them, they are read when
// a call starts, and SO_RCVTIMEO is not the timeout of writing, nor SO_SNDTIMEO that of reading
static int test_timeouts_belong_to_the_socket(void) {
    char buf[64];
    int sv[2];
    call_t c;
    CHECK(make_pair(sv) == 0, "failed to create a socket pair");

    // Set through one descriptor, used through another
    int dup_fd = dup(sv[0]);
    CHECK(dup_fd >= 0, "failed to dup");
    CHECK(set_timeout(sv[0], SO_RCVTIMEO, TIMEOUT_MS) == 0, "failed to set SO_RCVTIMEO");
    CHECK(call_start(&c, call_read, dup_fd, buf, sizeof(buf), 0) == 0,
          "failed to start read");
    EXPECT_CALL(&c, WAKE_MS, -1, EAGAIN, "read of the dup'ed descriptor");
    EXPECT_TIME(&c, TIMEOUT_MS, "read of the dup'ed descriptor");
    call_finish(&c);

    // Read at the call, so the new value counts
    CHECK(set_timeout(dup_fd, SO_RCVTIMEO, 2 * TIMEOUT_MS) == 0, "failed to set SO_RCVTIMEO");
    CHECK(call_start(&c, call_read, sv[0], buf, sizeof(buf), 0) == 0, "failed to start read");
    EXPECT_CALL(&c, WAKE_MS, -1, EAGAIN, "read after the timeout changed");
    EXPECT_TIME(&c, 2 * TIMEOUT_MS, "read after the timeout changed");
    call_finish(&c);

    // Not the timeout of writing: the buffer is full, and the write waits longer
    CHECK(fill(sv[0]) > 0, "failed to fill the buffer");
    CHECK(call_start(&c, call_write, sv[0], buf, 1, 0) == 0, "failed to start write");
    sleep_ms(3 * TIMEOUT_MS);
    CHECK(!call_done(&c), "SO_RCVTIMEO ended a write");
    stimulus_t s;
    CHECK(stimulus_start(&s, sv[1], 0, NULL, SHUT_RD) == 0, "failed to start");
    stimulus_join(&s);
    EXPECT_CALL(&c, WAKE_MS, -1, EPIPE, "write");
    call_finish(&c);
    close(sv[0]);
    close(dup_fd);

    // Not the timeout of reading
    CHECK(make_pair(sv) == 0, "failed to create a socket pair");
    CHECK(set_timeout(sv[0], SO_SNDTIMEO, TIMEOUT_MS) == 0, "failed to set SO_SNDTIMEO");
    CHECK(call_start(&c, call_read, sv[0], buf, sizeof(buf), 0) == 0, "failed to start read");
    sleep_ms(3 * TIMEOUT_MS);
    CHECK(!call_done(&c), "SO_SNDTIMEO ended a read");
    CHECK(stimulus_start(&s, sv[1], 0, "x", 0) == 0, "failed to start");
    stimulus_join(&s);
    EXPECT_CALL(&c, WAKE_MS, 1, 0, "read");
    call_finish(&c);

    // Another socket has no timeout
    int other[2];
    CHECK(make_pair(other) == 0, "failed to create a socket pair");
    CHECK(call_start(&c, call_read, other[0], buf, sizeof(buf), 0) == 0,
          "failed to start read");
    sleep_ms(3 * TIMEOUT_MS);
    CHECK(!call_done(&c), "a read of a socket without timeout ends");
    CHECK(stimulus_start(&s, other[1], 0, "y", 0) == 0, "failed to start");
    stimulus_join(&s);
    EXPECT_CALL(&c, WAKE_MS, 1, 0, "read");
    call_finish(&c);

    // Zero is no timeout
    CHECK(set_timeout(sv[0], SO_RCVTIMEO, TIMEOUT_MS) == 0, "failed to set SO_RCVTIMEO");
    CHECK(set_timeout(sv[0], SO_RCVTIMEO, 0) == 0, "failed to set SO_RCVTIMEO");
    CHECK(call_start(&c, call_read, sv[0], buf, sizeof(buf), 0) == 0, "failed to start read");
    sleep_ms(3 * TIMEOUT_MS);
    CHECK(!call_done(&c), "a read with a timeout of zero ends");
    CHECK(stimulus_start(&s, sv[1], 0, "z", 0) == 0, "failed to start");
    stimulus_join(&s);
    EXPECT_CALL(&c, WAKE_MS, 1, 0, "read");
    call_finish(&c);

    close(sv[0]);
    close(sv[1]);
    close(other[0]);
    close(other[1]);
    return 0;
}

// A nonblocking socket does not wait, whatever the timeouts are
static int test_nonblocking_socket_ignores_the_timeouts(void) {
    char buf[64];
    int sv[2];
    call_t c;
    CHECK(make_pair(sv) == 0, "failed to create a socket pair");
    set_nonblocking(sv[0], 1);
    CHECK(set_timeout(sv[0], SO_RCVTIMEO, 2000) == 0, "failed to set SO_RCVTIMEO");
    CHECK(set_timeout(sv[0], SO_SNDTIMEO, 2000) == 0, "failed to set SO_SNDTIMEO");

    CHECK(call_start(&c, call_read, sv[0], buf, sizeof(buf), 0) == 0, "failed to start read");
    CHECK(call_wait(&c, WAKE_MS), "read does not return");
    CHECK(c.ret == -1 && c.err == EAGAIN &&
          c.elapsed_ms < 1000, "read returns %ld after %ld ms",
          c.ret, c.elapsed_ms);
    call_finish(&c);

    CHECK(fill(sv[0]) > 0, "failed to fill the buffer");
    CHECK(call_start(&c, call_write, sv[0], buf, 1, 0) == 0, "failed to start write");
    CHECK(call_wait(&c, WAKE_MS), "write does not return");
    CHECK(c.ret == -1 && c.err == EAGAIN &&
          c.elapsed_ms < 1000, "write returns %ld after %ld ms",
          c.ret, c.elapsed_ms);
    call_finish(&c);
    close(sv[0]);
    close(sv[1]);
    return 0;
}

// A signal that does not interrupt a call, because the thread ignores it or blocks it, neither
// ends the wait of the timeout early, nor makes it start again
static int test_timeout_is_not_changed_by_other_signals(void) {
    const int timeout = 800;
    char buf[64];

    struct sigaction ignore, handle, old;
    memset(&ignore, 0, sizeof(ignore));
    ignore.sa_handler = SIG_IGN;
    memset(&handle, 0, sizeof(handle));
    handle.sa_handler = on_signal;

    for (int mode = 0; mode < 2; mode++) {
        // The signal is ignored, or it is blocked in the thread
        sigaction(SIGUSR2, mode == 0 ? &ignore : &handle, &old);
        for (int write_side = 0; write_side < 2; write_side++) {
            int sv[2];
            CHECK(make_pair(sv) == 0, "failed to create a socket pair");
            CHECK(set_timeout(sv[0], write_side ? SO_SNDTIMEO : SO_RCVTIMEO, timeout) == 0,
                  "failed to set the timeout");
            if (write_side) {
                CHECK(fill(sv[0]) > 0, "failed to fill the buffer");
            }
            call_t c;
            call_init(&c, write_side ? call_write : call_read, sv[0], buf, 1, 0);
            c.mask_usr2 = mode == 1;
            CHECK(call_run(&c) == 0, "failed to start the call");
            int sent = 0;
            send_signals(&c, SIGUSR2, 100, &sent);
            CHECK(call_wait(&c, WAKE_MS), "the call does not return");
            CHECK(c.ret == -1 && c.err == EAGAIN, "%s returns %ld (errno %d)",
                  write_side ? "write" : "read", c.ret, c.err);
            CHECK(c.elapsed_ms >= timeout - EARLY_MS && c.elapsed_ms < timeout + SLACK_MS,
                  "%s took %ld ms with %d signals, instead of %d ms",
                  write_side ? "write" : "read", c.elapsed_ms, sent, timeout);
            call_finish(&c);
            close(sv[0]);
            close(sv[1]);
        }
        sigaction(SIGUSR2, &old, NULL);
    }
    return 0;
}

// ---------------------------------------------------------------------------------------------
// Interrupted reads
// ---------------------------------------------------------------------------------------------

// A signal ends a blocked read with EINTR, also with the timeout set and with MSG_PEEK
static int test_interrupted_reads(void) {
    char buf[64];

    for (size_t i = 0; i < ARRAY_SIZE(recv_calls); i++) {
        const char *what = recv_calls[i].name;
        int sv[2];
        CHECK(make_pair(sv) == 0, "failed to create a socket pair");
        call_t c;
        CHECK(call_start(&c, recv_calls[i].fn, sv[0], buf, sizeof(buf), 0) == 0,
              "failed to start %s", what);
        CHECK(call_blocks(&c), "%s returns on an empty socket", what);
        interrupt(&c);
        EXPECT_CALL(&c, WAKE_MS, -1, EINTR, what);
        call_finish(&c);

        // With a timeout, which is far away
        CHECK(set_timeout(sv[0], SO_RCVTIMEO, 30000) == 0, "failed to set SO_RCVTIMEO");
        CHECK(call_start(&c, recv_calls[i].fn, sv[0], buf, sizeof(buf), 0) == 0,
              "failed to start %s", what);
        CHECK(call_blocks(&c), "%s returns on an empty socket", what);
        interrupt(&c);
        EXPECT_CALL(&c, WAKE_MS, -1, EINTR, what);
        CHECK(c.elapsed_ms < 10000, "%s took %ld ms", what, c.elapsed_ms);
        call_finish(&c);
        close(sv[0]);
        close(sv[1]);
    }

    for (size_t i = 0; i < ARRAY_SIZE(recv_flag_calls); i++) {
        const char *what = recv_flag_calls[i].name;
        int sv[2];
        CHECK(make_pair(sv) == 0, "failed to create a socket pair");
        call_t c;
        CHECK(call_start(&c, recv_flag_calls[i].fn, sv[0], buf, sizeof(buf), MSG_PEEK) == 0,
              "failed to start %s", what);
        CHECK(call_blocks(&c), "%s of MSG_PEEK returns on an empty socket", what);
        interrupt(&c);
        EXPECT_CALL(&c, WAKE_MS, -1, EINTR, what);
        call_finish(&c);

        // The bytes that came so far, if some did
        CHECK(write(sv[1], "abc", 3) == 3, "failed to write");
        CHECK(call_start(&c, recv_flag_calls[i].fn, sv[0], buf, 8, MSG_WAITALL) == 0,
              "failed to start %s", what);
        CHECK(call_blocks(&c), "%s of MSG_WAITALL returns with 3 of 8 bytes", what);
        interrupt(&c);
        EXPECT_CALL(&c, WAKE_MS, 3, 0, what);
        CHECK(memcmp(buf, "abc", 3) == 0, "%s returns wrong bytes", what);
        call_finish(&c);
        close(sv[0]);
        close(sv[1]);
    }
    return 0;
}

// ---------------------------------------------------------------------------------------------
// MSG_WAITALL
// ---------------------------------------------------------------------------------------------

// MSG_WAITALL waits for all the bytes, or the end of the file, or the timeout
static int test_msg_waitall(void) {
    char buf[64];

    for (size_t i = 0; i < ARRAY_SIZE(recv_flag_calls); i++) {
        const char *what = recv_flag_calls[i].name;
        int sv[2];
        call_t c;
        stimulus_t s;

        // The rest of the bytes comes later
        CHECK(make_pair(sv) == 0, "failed to create a socket pair");
        CHECK(write(sv[1], "12345", 5) == 5, "failed to write");
        CHECK(stimulus_start(&s, sv[1], 200, "67890", 0) == 0, "failed to start");
        CHECK(call_start(&c, recv_flag_calls[i].fn, sv[0], buf, 10, MSG_WAITALL) == 0,
              "failed to start %s", what);
        EXPECT_CALL(&c, WAKE_MS, 10, 0, what);
        CHECK(c.elapsed_ms >= 150, "%s returned after %ld ms, without the rest", what,
              c.elapsed_ms);
        CHECK(memcmp(buf, "1234567890", 10) == 0, "%s returns wrong bytes", what);
        call_finish(&c);
        stimulus_join(&s);
        close(sv[0]);
        close(sv[1]);

        // The end of the file comes first
        CHECK(make_pair(sv) == 0, "failed to create a socket pair");
        CHECK(write(sv[1], "xy", 2) == 2, "failed to write");
        CHECK(stimulus_start(&s, sv[1], 100, NULL, SHUT_WR) == 0, "failed to start");
        CHECK(call_start(&c, recv_flag_calls[i].fn, sv[0], buf, 10, MSG_WAITALL) == 0,
              "failed to start %s", what);
        EXPECT_CALL(&c, WAKE_MS, 2, 0, what);
        call_finish(&c);
        stimulus_join(&s);
        close(sv[0]);
        close(sv[1]);

        // The timeout is over
        CHECK(make_pair(sv) == 0, "failed to create a socket pair");
        CHECK(set_timeout(sv[0], SO_RCVTIMEO, TIMEOUT_MS) == 0, "failed to set SO_RCVTIMEO");
        CHECK(write(sv[1], "abc", 3) == 3, "failed to write");
        CHECK(call_start(&c, recv_flag_calls[i].fn, sv[0], buf, 8, MSG_WAITALL) == 0,
              "failed to start %s", what);
        EXPECT_CALL(&c, WAKE_MS, 3, 0, what);
        EXPECT_TIME(&c, TIMEOUT_MS, what);
        call_finish(&c);
        CHECK(call_start(&c, recv_flag_calls[i].fn, sv[0], buf, 8, MSG_WAITALL) == 0,
              "failed to start %s", what);
        EXPECT_CALL(&c, WAKE_MS, -1, EAGAIN, what);
        EXPECT_TIME(&c, TIMEOUT_MS, what);
        call_finish(&c);
        close(sv[0]);
        close(sv[1]);

        // MSG_DONTWAIT: the bytes that are there, or EAGAIN
        CHECK(make_pair(sv) == 0, "failed to create a socket pair");
        CHECK(write(sv[1], "abc", 3) == 3, "failed to write");
        CHECK(call_start(&c, recv_flag_calls[i].fn, sv[0], buf, 8,
                         MSG_WAITALL | MSG_DONTWAIT) == 0,
              "failed to start %s", what);
        EXPECT_NOW(&c, 3, 0, what);
        call_finish(&c);
        CHECK(call_start(&c, recv_flag_calls[i].fn, sv[0], buf, 8,
                         MSG_WAITALL | MSG_DONTWAIT) == 0,
              "failed to start %s", what);
        EXPECT_NOW(&c, -1, EAGAIN, what);
        call_finish(&c);
        close(sv[0]);
        close(sv[1]);
    }

    // More bytes than the buffer of the socket holds, which come in small pieces
    {
        int sv[2];
        call_t c;
        flow_t writer;
        char *data = malloc(MIB);
        CHECK(make_pair(sv) == 0, "failed to create a socket pair");
        CHECK(flow_start(&writer, writer_main, sv[1], MIB, 4096, 100, 0) == 0,
              "failed to start the writer");
        CHECK(call_start(&c, call_recv, sv[0], data, MIB, MSG_WAITALL) == 0,
              "failed to start recv");
        EXPECT_CALL(&c, 30000, MIB, 0, "recv of 1 MiB");
        call_finish(&c);
        CHECK(check_pattern(data, MIB, 0) == MIB, "recv returns wrong bytes");
        CHECK(flow_join(&writer, WAKE_MS), "the writer does not end");
        close(sv[0]);
        close(sv[1]);
        free(data);
    }
    return 0;
}

// ---------------------------------------------------------------------------------------------
// MSG_DONTWAIT and MSG_PEEK
// ---------------------------------------------------------------------------------------------

// MSG_DONTWAIT makes a call of a blocking socket fail with EAGAIN instead of waiting, and leaves
// the socket blocking
static int test_msg_dontwait(void) {
    char buf[64];

    for (size_t i = 0; i < ARRAY_SIZE(recv_flag_calls); i++) {
        const char *what = recv_flag_calls[i].name;
        int sv[2];
        CHECK(make_pair(sv) == 0, "failed to create a socket pair");
        call_t c;
        CHECK(call_start(&c, recv_flag_calls[i].fn, sv[0], buf, sizeof(buf), MSG_DONTWAIT) == 0,
              "failed to start %s", what);
        EXPECT_NOW(&c, -1, EAGAIN, what);
        call_finish(&c);

        // With data it reads
        CHECK(write(sv[1], "abc", 3) == 3, "failed to write");
        CHECK(call_start(&c, recv_flag_calls[i].fn, sv[0], buf, sizeof(buf), MSG_DONTWAIT) == 0,
              "failed to start %s", what);
        EXPECT_NOW(&c, 3, 0, what);
        call_finish(&c);

        // Without a timeout of its own
        CHECK(set_timeout(sv[0], SO_RCVTIMEO, 3000) == 0, "failed to set SO_RCVTIMEO");
        CHECK(call_start(&c, recv_flag_calls[i].fn, sv[0], buf, sizeof(buf), MSG_DONTWAIT) == 0,
              "failed to start %s", what);
        EXPECT_NOW(&c, -1, EAGAIN, what);
        call_finish(&c);
        CHECK(!is_nonblocking(sv[0]), "%s changed the mode of the socket", what);
        close(sv[0]);
        close(sv[1]);
    }

    for (size_t i = 0; i < ARRAY_SIZE(send_flag_calls); i++) {
        const char *what = send_flag_calls[i].name;
        int sv[2];
        CHECK(make_pair(sv) == 0, "failed to create a socket pair");
        CHECK(fill(sv[0]) > 0, "failed to fill the buffer");
        call_t c;
        CHECK(call_start(&c, send_flag_calls[i].fn, sv[0], buf, 2, MSG_DONTWAIT) == 0,
              "failed to start %s", what);
        EXPECT_NOW(&c, -1, EAGAIN, what);
        call_finish(&c);
        CHECK(set_timeout(sv[0], SO_SNDTIMEO, 3000) == 0, "failed to set SO_SNDTIMEO");
        CHECK(call_start(&c, send_flag_calls[i].fn, sv[0], buf, 2, MSG_DONTWAIT) == 0,
              "failed to start %s", what);
        EXPECT_NOW(&c, -1, EAGAIN, what);
        call_finish(&c);
        CHECK(!is_nonblocking(sv[0]), "%s changed the mode of the socket", what);
        close(sv[0]);
        close(sv[1]);
    }
    return 0;
}

// MSG_PEEK returns the bytes without taking them
static int test_msg_peek(void) {
    char buf[64];

    for (size_t i = 0; i < ARRAY_SIZE(recv_flag_calls); i++) {
        const char *what = recv_flag_calls[i].name;
        int sv[2];
        call_t c;
        CHECK(make_pair(sv) == 0, "failed to create a socket pair");
        CHECK(write(sv[1], "abcdef", 6) == 6, "failed to write");

        // Some of the bytes, and all of them, as often as asked
        for (int round = 0; round < 2; round++) {
            memset(buf, 0, sizeof(buf));
            CHECK(call_start(&c, recv_flag_calls[i].fn, sv[0], buf, 3, MSG_PEEK) == 0,
                  "failed to start %s", what);
            EXPECT_NOW(&c, 3, 0, what);
            CHECK(memcmp(buf, "abc", 3) == 0, "%s peeks wrong bytes", what);
            call_finish(&c);
        }
        int pending = -1;
        CHECK(ioctl(sv[0], FIONREAD, &pending) == 0 && pending == 6,
              "%s took bytes: %d are left", what, pending);
        memset(buf, 0, sizeof(buf));
        CHECK(call_start(&c, recv_flag_calls[i].fn, sv[0], buf, sizeof(buf), MSG_PEEK) == 0,
              "failed to start %s", what);
        EXPECT_NOW(&c, 6, 0, what);
        CHECK(memcmp(buf, "abcdef", 6) == 0, "%s peeks wrong bytes", what);
        call_finish(&c);

        // The next read gets them all, once
        memset(buf, 0, sizeof(buf));
        CHECK(call_start(&c, recv_flag_calls[i].fn, sv[0], buf, sizeof(buf), 0) == 0,
              "failed to start %s", what);
        EXPECT_NOW(&c, 6, 0, what);
        CHECK(memcmp(buf, "abcdef", 6) == 0, "%s reads wrong bytes after a peek", what);
        call_finish(&c);

        // Nothing is left to peek
        CHECK(call_start(&c, recv_flag_calls[i].fn, sv[0], buf, sizeof(buf),
                         MSG_PEEK | MSG_DONTWAIT) == 0, "failed to start %s", what);
        EXPECT_NOW(&c, -1, EAGAIN, what);
        call_finish(&c);

        // MSG_WAITALL does not make a peek wait
        CHECK(write(sv[1], "xyz", 3) == 3, "failed to write");
        CHECK(call_start(&c, recv_flag_calls[i].fn, sv[0], buf, 8, MSG_PEEK | MSG_WAITALL) == 0,
              "failed to start %s", what);
        EXPECT_NOW(&c, 3, 0, what);
        call_finish(&c);

        // The end of the file
        CHECK(drain(sv[0]) == 3, "failed to read");
        CHECK(shutdown(sv[1], SHUT_WR) == 0, "failed to shut down");
        CHECK(call_start(&c, recv_flag_calls[i].fn, sv[0], buf, sizeof(buf), MSG_PEEK) == 0,
              "failed to start %s", what);
        EXPECT_NOW(&c, 0, 0, what);
        call_finish(&c);
        close(sv[0]);
        close(sv[1]);
    }

    // A peek does not make room for the sender
    {
        int sv[2];
        call_t c;
        CHECK(make_pair(sv) == 0, "failed to create a socket pair");
        long filled = fill(sv[0]);
        CHECK(filled > 4096, "failed to fill the buffer");
        char *big = malloc(4096);
        CHECK(call_start(&c, call_recv, sv[1], big, 4096, MSG_PEEK) == 0, "failed to start recv");
        EXPECT_NOW(&c, 4096, 0, "recv");
        call_finish(&c);
        free(big);
        CHECK(call_start(&c, call_send, sv[0], buf, 1, MSG_DONTWAIT) == 0, "failed to start");
        EXPECT_NOW(&c, -1, EAGAIN, "send after a peek");
        call_finish(&c);
        CHECK(drain(sv[1]) == filled, "not all the bytes arrive");
        close(sv[0]);
        close(sv[1]);
    }
    return 0;
}

// A peek waits for data like a read, and ends with the end of the file or the timeout
static int test_msg_peek_waits_for_data(void) {
    char buf[64];
    int sv[2];
    call_t c;
    stimulus_t s;
    CHECK(make_pair(sv) == 0, "failed to create a socket pair");

    CHECK(call_start(&c, call_recv, sv[0], buf, sizeof(buf), MSG_PEEK) == 0,
          "failed to start recv");
    CHECK(call_blocks(&c), "recv(MSG_PEEK) returns on an empty socket");
    CHECK(stimulus_start(&s, sv[1], 0, "xyz", 0) == 0, "failed to start");
    stimulus_join(&s);
    EXPECT_CALL(&c, WAKE_MS, 3, 0, "recv(MSG_PEEK)");
    CHECK(memcmp(buf, "xyz", 3) == 0, "recv(MSG_PEEK) peeks wrong bytes");
    call_finish(&c);

    // The bytes are still there
    memset(buf, 0, sizeof(buf));
    CHECK(call_start(&c, call_read, sv[0], buf, sizeof(buf), 0) == 0, "failed to start read");
    EXPECT_NOW(&c, 3, 0, "read");
    CHECK(memcmp(buf, "xyz", 3) == 0, "read gets wrong bytes");
    call_finish(&c);

    // The timeout
    CHECK(set_timeout(sv[0], SO_RCVTIMEO, TIMEOUT_MS) == 0, "failed to set SO_RCVTIMEO");
    CHECK(call_start(&c, call_recv, sv[0], buf, sizeof(buf), MSG_PEEK) == 0,
          "failed to start recv");
    EXPECT_CALL(&c, WAKE_MS, -1, EAGAIN, "recv(MSG_PEEK)");
    EXPECT_TIME(&c, TIMEOUT_MS, "recv(MSG_PEEK)");
    call_finish(&c);
    CHECK(set_timeout(sv[0], SO_RCVTIMEO, 0) == 0, "failed to set SO_RCVTIMEO");

    // The end of the file
    CHECK(call_start(&c, call_recv, sv[0], buf, sizeof(buf), MSG_PEEK) == 0,
          "failed to start recv");
    CHECK(call_blocks(&c), "recv(MSG_PEEK) returns on an empty socket");
    CHECK(stimulus_start(&s, sv[1], 0, NULL, SHUT_WR) == 0, "failed to start");
    stimulus_join(&s);
    EXPECT_CALL(&c, WAKE_MS, 0, 0, "recv(MSG_PEEK)");
    call_finish(&c);
    close(sv[0]);
    close(sv[1]);
    return 0;
}

// A peek of a long queue, which the LibOS keeps in many pieces, returns the bytes in order,
// whatever the pieces of the buffers are, and also after a part of them has been read
static int test_msg_peek_of_a_long_queue(void) {
    static const size_t sizes[] = { 1, 100, 1000, 7000, 12345, 3, 20000, 511 };
    const size_t total = 100000;
    char *data = malloc(total);
    char *buf = malloc(total);
    int sv[2];
    call_t c;
    fill_pattern(data, total, 0);
    CHECK(make_pair(sv) == 0, "failed to create a socket pair");

    size_t pos = 0;
    for (size_t i = 0; pos < total; i++) {
        size_t n = sizes[i % ARRAY_SIZE(sizes)];
        n = n < total - pos ? n : total - pos;
        CHECK(write(sv[1], data + pos, n) == (ssize_t)n, "failed to write");
        pos += n;
    }

    static const named_call_t peeks[] = { { "recv", call_recv }, { "recvmsg", call_recvmsg } };
    for (size_t i = 0; i < ARRAY_SIZE(peeks); i++) {
        memset(buf, 0, total);
        CHECK(call_start_n(&c, peeks[i].fn, sv[0], buf, total, MSG_PEEK, 3) == 0,
              "failed to start %s",
              peeks[i].name);
        EXPECT_NOW(&c, total, 0, peeks[i].name);
        CHECK(check_pattern(buf, total, 0) == total, "%s peeks a wrong byte at %zu",
              peeks[i].name,
              check_pattern(buf, total, 0));
        call_finish(&c);
    }

    // Take a part, which is no multiple of a piece of the queue, then peek again
    CHECK(recv(sv[0], buf, 40001, MSG_WAITALL) == 40001, "failed to read");
    CHECK(check_pattern(buf, 40001, 0) == 40001, "read a wrong byte");
    memset(buf, 0, total);
    CHECK(call_start_n(&c, call_recvmsg, sv[0], buf, total, MSG_PEEK, 4) == 0,
          "failed to start recvmsg");
    EXPECT_NOW(&c, total - 40001, 0, "recvmsg");
    CHECK(check_pattern(buf, total - 40001, 40001) == total - 40001,
          "peeks a wrong byte at %zu",
          check_pattern(buf, total - 40001, 40001));
    call_finish(&c);

    // A peek of a few bytes at the boundary
    CHECK(recv(sv[0], buf, 1, MSG_PEEK) == 1 &&
          buf[0] == pattern_byte(40001), "peeked a wrong byte");

    CHECK(recv(sv[0], buf, total - 40001, MSG_WAITALL) == (ssize_t)(total - 40001),
          "failed to read the rest");
    CHECK(check_pattern(buf, total - 40001, 40001) == total - 40001, "read a wrong byte");
    CHECK(drain(sv[0]) == 0, "bytes are left");
    close(sv[0]);
    close(sv[1]);
    free(buf);
    free(data);
    return 0;
}

// The flags that have no effect on a stream socket have none
static int test_msg_flags_without_effect(void) {
    char buf[64];
    int sv[2];
    call_t c;
    CHECK(make_pair(sv) == 0, "failed to create a socket pair");
    CHECK(write(sv[1], "qq", 2) == 2, "failed to write");

    // MSG_TRUNC does not return the length that there is
    CHECK(call_start(&c, call_recv, sv[0], buf, 1, MSG_TRUNC | MSG_DONTWAIT) == 0,
          "failed to start recv");
    EXPECT_NOW(&c, 1, 0, "recv(MSG_TRUNC)");
    call_finish(&c);
    CHECK(call_start(&c, call_recv, sv[0], buf, 1, MSG_CMSG_CLOEXEC | MSG_DONTWAIT) == 0,
          "failed to start recv");
    EXPECT_NOW(&c, 1, 0, "recv(MSG_CMSG_CLOEXEC)");
    call_finish(&c);

    // There is no queue of errors
    CHECK(call_start(&c, call_recv, sv[0], buf, 1, MSG_ERRQUEUE | MSG_DONTWAIT) == 0,
          "failed to start recv");
    EXPECT_NOW(&c, -1, EAGAIN, "recv(MSG_ERRQUEUE)");
    call_finish(&c);

    // Sending
    static const int send_flags[] = { MSG_MORE, MSG_CONFIRM, MSG_EOR, 0x10000000 };
    for (size_t i = 0; i < ARRAY_SIZE(send_flags); i++) {
        CHECK(call_start(&c, call_send, sv[1], "m", 1, send_flags[i]) == 0,
              "failed to start send");
        EXPECT_NOW(&c, 1, 0, "send");
        call_finish(&c);
    }
    CHECK(drain(sv[0]) == (long)ARRAY_SIZE(send_flags), "not all the bytes arrive");
    close(sv[0]);
    close(sv[1]);
    return 0;
}

// The LibOS has no out-of-band data: a byte that is sent with MSG_OOB is plain data. The calls
// of this sequence return what they return in Linux, and none of them waits.
static int test_oob_flag_does_not_hang(void) {
    char buf[8];
    int sv[2];
    call_t c;
    CHECK(make_pair(sv) == 0, "failed to create a socket pair");
    if (send(sv[0], "o", 1, MSG_OOB) < 0 && errno == EOPNOTSUPP) {
        // A kernel without out-of-band data for unix sockets
        close(sv[0]);
        close(sv[1]);
        return 0;
    }
    CHECK(call_start(&c, call_recv, sv[1], buf, 1, MSG_OOB | MSG_DONTWAIT) == 0,
          "failed to start recv");
    EXPECT_NOW(&c, 1, 0, "recv(MSG_OOB)");
    CHECK(buf[0] == 'o', "recv(MSG_OOB) returns a wrong byte");
    call_finish(&c);
    CHECK(call_start(&c, call_recv, sv[1], buf, 1, MSG_DONTWAIT) == 0,
          "failed to start recv");
    EXPECT_NOW(&c, -1, EAGAIN, "recv");
    call_finish(&c);
    close(sv[0]);
    close(sv[1]);
    return 0;
}

static test_case_t test_cases[] = {
    TEST_CASE(test_blocking_writes_write_everything),
    TEST_CASE(test_concurrent_writes_deliver_everything),
    TEST_CASE(test_nonblocking_writes_are_partial),
    TEST_CASE(test_interrupted_write_returns_the_progress),
    TEST_CASE(test_write_timeout),
    TEST_CASE(test_write_timeout_restarts_with_progress),
    TEST_CASE(test_read_timeout),
    TEST_CASE(test_timeouts_belong_to_the_socket),
    TEST_CASE(test_nonblocking_socket_ignores_the_timeouts),
    TEST_CASE(test_timeout_is_not_changed_by_other_signals),
    TEST_CASE(test_interrupted_reads),
    TEST_CASE(test_msg_waitall),
    TEST_CASE(test_msg_dontwait),
    TEST_CASE(test_msg_peek),
    TEST_CASE(test_msg_peek_waits_for_data),
    TEST_CASE(test_msg_peek_of_a_long_queue),
    TEST_CASE(test_msg_flags_without_effect),
    TEST_CASE(test_oob_flag_does_not_hang),
};

int main(int argc, const char *argv[]) {
    // SIGUSR1 interrupts the calls, without SA_RESTART
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_signal;
    sigaction(SIGUSR1, &sa, NULL);

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
        fflush(stdout);
    }
    fflush(stdout);
    if (atomic_load(&stuck_threads) > 0) {
        // Threads are left behind in calls that do not return
        _exit(1);
    }
    return ret;
}
