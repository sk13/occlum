// Reading and writing on unix stream sockets: what the calls return when they block, how long they
// wait, and what the flags and the addresses of the send and receive calls do. The expectations are
// the behavior of Linux (checked on Linux 7.0): the test passes there, too (gcc -pthread
// -I../include main.c).
//
//  - A blocking write, send, writev or sendmsg waits until everything is written, unless a signal
//    interrupts it or the timeout of SO_SNDTIMEO is over, which return the bytes written so far (or
//    EINTR and EAGAIN if there are none). A nonblocking socket, or MSG_DONTWAIT, writes what fits.
//  - SO_RCVTIMEO and SO_SNDTIMEO end a wait with EAGAIN. A receive with MSG_WAITALL returns the
//    bytes that came by then. The timeouts belong to the socket, not to the descriptor.
//  - MSG_DONTWAIT, MSG_PEEK and MSG_WAITALL, the flags that have no effect, and the address of a
//    send on a connected (EISCONN) or an unconnected (EOPNOTSUPP) socket.
//  - recvmsg and recvfrom tell the name of the peer; sendmmsg and recvmmsg work.
//  - Writing to a socket that cannot send raises SIGPIPE in the calling thread, unless the call has
//    MSG_NOSIGNAL, or has waited for room before it failed.
//  - SO_SNDBUF limits the bytes that a socket has in flight to its peer, whenever it is set.
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
#define MAX_MESSAGES    4
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

static atomic_int sigpipes;
static atomic_int sigpipe_tid;

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

static void on_sigpipe(int sig) {
    atomic_store(&sigpipe_tid, get_tid());
    atomic_fetch_add(&sigpipes, 1);
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

static int set_sndbuf(int fd, int size) {
    return setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &size, sizeof(size));
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
                     "unix_socket_io.%d.%d", (int)getpid(), counter++);
    a.len = offsetof(struct sockaddr_un, sun_path) + 1 + n;
    return a;
}

// Makes a listening socket at a new address, which is returned in `listener_addr`, and a client
// that is connected to it, and the socket that the listener accepts. The client is bound to an
// address of its own, which is returned in `client_addr`, if `bind_client`. The listener is closed.
static int make_connection(int *client, int *server, unix_addr_t *listener_addr,
                           unix_addr_t *client_addr, int bind_client) {
    *listener_addr = new_addr();
    int l = socket(AF_UNIX, SOCK_STREAM, 0);
    int c = socket(AF_UNIX, SOCK_STREAM, 0);
    if (l < 0 || c < 0) {
        return -1;
    }
    if (bind(l, (struct sockaddr *)&listener_addr->sa, listener_addr->len) < 0 ||
            listen(l, 4) < 0) {
        return -1;
    }
    if (bind_client) {
        *client_addr = new_addr();
        if (bind(c, (struct sockaddr *)&client_addr->sa, client_addr->len) < 0) {
            return -1;
        }
    }
    if (connect(c, (struct sockaddr *)&listener_addr->sa, listener_addr->len) < 0) {
        return -1;
    }
    int s = accept(l, NULL, NULL);
    if (s < 0) {
        return -1;
    }
    close(l);
    *client = c;
    *server = s;
    return 0;
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
    int pieces;                 // the number of the iovecs of the vectored calls, or messages
    struct sockaddr_un name;    // the address of the calls that have one
    socklen_t namelen;          // its length: the length that a receive returns
    int msg_flags;              // the flags that recvmsg returns
    int mask_usr2;              // block SIGUSR2 in the thread, before the call
    unsigned lens[MAX_MESSAGES];// the lengths that recvmmsg returns
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

// The message i of sendmmsg and recvmmsg is the bytes [i * len, (i + 1) * len) of the buffer, and
// `pieces` is the number of the messages. The calls are made without the C library, as musl
// does not have a sendmmsg system call.
static long call_sendmmsg(call_t *c) {
    struct mmsghdr msgs[MAX_MESSAGES];
    struct iovec iov[MAX_MESSAGES];
    memset(msgs, 0, sizeof(msgs));
    for (int i = 0; i < c->pieces; i++) {
        iov[i].iov_base = c->buf + i * c->len;
        iov[i].iov_len = c->len;
        msgs[i].msg_hdr.msg_iov = &iov[i];
        msgs[i].msg_hdr.msg_iovlen = 1;
    }
    long ret = syscall(SYS_sendmmsg, c->fd, msgs, c->pieces, c->flags);
    for (int i = 0; i < c->pieces; i++) {
        c->lens[i] = msgs[i].msg_len;
    }
    return ret;
}

static long call_recvmmsg(call_t *c) {
    struct mmsghdr msgs[MAX_MESSAGES];
    struct iovec iov[MAX_MESSAGES];
    memset(msgs, 0, sizeof(msgs));
    for (int i = 0; i < c->pieces; i++) {
        iov[i].iov_base = c->buf + i * c->len;
        iov[i].iov_len = c->len;
        msgs[i].msg_hdr.msg_iov = &iov[i];
        msgs[i].msg_hdr.msg_iovlen = 1;
    }
    long ret = syscall(SYS_recvmmsg, c->fd, msgs, c->pieces, c->flags, NULL);
    for (int i = 0; i < c->pieces; i++) {
        c->lens[i] = msgs[i].msg_len;
    }
    return ret;
}

// Writes with SIGPIPE blocked in the thread, and tells whether it has been left pending
static long call_write_sigpipe_blocked(call_t *c) {
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGPIPE);
    pthread_sigmask(SIG_BLOCK, &set, NULL);
    long ret = write(c->fd, c->buf, c->len);
    int err = errno;
    // Takes the signal away, so that it is not raised when the thread unblocks it
    struct timespec zero = { 0, 0 };
    c->msg_flags = sigtimedwait(&set, NULL, &zero) == SIGPIPE;
    pthread_sigmask(SIG_UNBLOCK, &set, NULL);
    errno = err;
    return ret;
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

// A write whose wait for room has timed out makes one more attempt, and if that one writes some
// bytes (a reader has made room just then), the write goes on and waits for room again, for the
// whole time. It does not give up at once, as if the timeout were over. The time between the end
// of a wait and the attempt after it is a few microseconds wide, so no reader can be made to read
// in it. The test makes many writes whose reader drains the socket at times around the end of the
// first wait, and checks the one thing that holds however the times fall: a write that has written
// some of its bytes, not all of them, does not return before a whole timeout has passed since the
// reader took the bytes. It is one-sided: a call that gives up too early is caught if a reader hits the window, which takes
// about one in three of the writes on the LibOS (a LibOS that does not restart the timeout after
// the attempt fails the test, in all runs).
#define PROGRESS_TIMEOUT_MS     10
#define PROGRESS_ROUNDS         150

static long now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000000000L + ts.tv_nsec;
}

typedef struct {
    int fd;
    char *buf;
    size_t len;
    atomic_long start_ns;   // before the call
    long end_ns;            // after the call
    long ret;
    int err;
} timed_write_t;

static void *timed_write_main(void *arg) {
    timed_write_t *w = arg;
    atomic_store(&w->start_ns, now_ns());
    errno = 0;
    w->ret = write(w->fd, w->buf, w->len);
    w->err = errno;
    w->end_ns = now_ns();
    return NULL;
}

static int test_write_goes_on_when_room_comes_after_its_timeout(void) {
    const long timeout_ns = PROGRESS_TIMEOUT_MS * 1000000L;
    int sv[2];
    char *data = calloc(1, MIB);
    CHECK(make_pair(sv) == 0, "failed to create a socket pair");
    CHECK(set_timeout(sv[0], SO_SNDTIMEO, PROGRESS_TIMEOUT_MS) == 0,
          "failed to set SO_SNDTIMEO");
    CHECK(set_sndbuf(sv[0], 4096) == 0, "failed to set SO_SNDBUF");

    int progressed = 0;
    for (int round = 0; round < PROGRESS_ROUNDS; round++) {
        CHECK(fill(sv[0]) > 0, "failed to fill the buffer");

        timed_write_t w = { .fd = sv[0], .buf = data, .len = MIB };
        pthread_t thread;
        atomic_store(&w.start_ns, 0);
        CHECK(pthread_create(&thread, NULL, timed_write_main, &w) == 0,
              "failed to create a thread");
        while (atomic_load(&w.start_ns) == 0) {
            usleep(100);
        }

        // The reader takes the bytes at a time from just before the end of the wait of the writer
        // to long enough after it, which each round moves by 2 us
        long off_ns = -20000 + (round * 37 % 100) * 2000;
        long target = atomic_load(&w.start_ns) + timeout_ns + off_ns;
        long until_target = target - now_ns();
        if (until_target > 3000000) {
            usleep((until_target - 3000000) / 1000);
        }
        while (now_ns() < target) {
        }
        long taken_ns = now_ns();
        CHECK(drain(sv[1]) >= 0, "failed to drain the socket");

        pthread_join(thread, NULL);
        if (w.ret > 0) {
            progressed++;
            // The write has written bytes after the reader took the bytes, and it waits for room
            // again for a whole timeout, unless the reader has made room for all its bytes, which
            // is over when they are written. Half of it is the margin of the clocks.
            CHECK((size_t)w.ret == w.len || w.end_ns >= taken_ns + timeout_ns / 2,
                  "round %d: a write that wrote %ld bytes after its timeout returned %ld us after "
                  "the reader took the bytes, which is less than its timeout of %d ms",
                  round, w.ret, (w.end_ns - taken_ns) / 1000, PROGRESS_TIMEOUT_MS);
        } else {
            CHECK(w.ret == -1 && w.err == EAGAIN, "round %d: write returned %ld (errno %d)", round,
                  w.ret, w.err);
        }
        CHECK(drain(sv[1]) >= 0, "failed to drain the socket");
    }
    // Some of the rounds must have been in time for the writer, or the test tests nothing
    CHECK(progressed > PROGRESS_ROUNDS / 10,
          "the reader was too late for the writer in all but %d "
          "of the %d rounds", progressed, PROGRESS_ROUNDS);
    close(sv[0]);
    close(sv[1]);
    free(data);
    return 0;
}

// The peer goes while a write waits for room: the bytes that were written, or an error if none,
// and no SIGPIPE, as the call has waited
static int test_peer_goes_while_a_write_waits(void) {
    static const int hows[] = { STIM_CLOSE, SHUT_RD };
    char *data = malloc(4 * MIB);
    fill_pattern(data, 4 * MIB, 0);

    for (size_t i = 0; i < ARRAY_SIZE(hows); i++) {
        int how = hows[i];
        int sv[2];
        stimulus_t s;
        call_t c;

        // Some bytes are written before the call waits
        CHECK(make_pair(sv) == 0, "failed to create a socket pair");
        atomic_store(&sigpipes, 0);
        CHECK(stimulus_start(&s, sv[1], SETTLE_MS * 2, NULL, how) == 0, "failed to start");
        CHECK(call_start(&c, call_write, sv[0], data, 4 * MIB, 0) == 0, "failed to start write");
        CHECK(call_wait(&c, WAKE_MS), "the write does not return");
        CHECK(c.ret > 0 && c.ret < 4 * MIB, "the write returns %ld (errno %d)", c.ret, c.err);
        stimulus_join(&s);
        call_finish(&c);
        CHECK(atomic_load(&sigpipes) == 0, "the write raised SIGPIPE");
        close(sv[0]);
        if (how != STIM_CLOSE) {
            close(sv[1]);
        }

        // The buffer is full already: the shutdown wakes the write, which fails with EPIPE (the
        // close with ECONNRESET, in Linux, as there are bytes that nobody reads)
        CHECK(make_pair(sv) == 0, "failed to create a socket pair");
        CHECK(fill(sv[0]) > 0, "failed to fill the buffer");
        CHECK(stimulus_start(&s, sv[1], SETTLE_MS * 2, NULL, how) == 0, "failed to start");
        CHECK(call_start(&c, call_write, sv[0], data, 100, 0) == 0, "failed to start write");
        CHECK(call_wait(&c, WAKE_MS), "the write does not return");
        CHECK(c.ret == -1 && (c.err == EPIPE || (how == STIM_CLOSE && c.err == ECONNRESET)),
              "the write returns %ld (errno %d)", c.ret, c.err);
        stimulus_join(&s);
        call_finish(&c);
        CHECK(atomic_load(&sigpipes) == 0, "the write raised SIGPIPE");
        close(sv[0]);
        if (how != STIM_CLOSE) {
            close(sv[1]);
        }
    }
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

// The timeout of a receive is for the whole call, not for each wait. A receive with MSG_WAITALL that
// bytes wake up again and again, none of which fills its buffer, gives up when its time is over, with
// the bytes that it has: a byte every 20 ms would take the 1.3 s that 64 bytes need, if the time
// started again with each wake-up
static int test_timeout_counts_over_all_waits(void) {
    char buf[64];
    int sv[2];
    call_t c;
    flow_t writer;

    CHECK(make_pair(sv) == 0, "failed to create a socket pair");
    CHECK(set_timeout(sv[0], SO_RCVTIMEO, TIMEOUT_MS) == 0, "failed to set SO_RCVTIMEO");
    CHECK(flow_start(&writer, writer_main, sv[1], 200, 1, 0, 20) == 0,
          "failed to start the writer");
    CHECK(call_start(&c, call_recv, sv[0], buf, sizeof(buf), MSG_WAITALL) == 0,
          "failed to start recv");
    CHECK(call_wait(&c, WAKE_MS), "recv with a timeout does not return, though bytes come");
    // The bytes of the time of the timeout, or none if the host is so slow that none came
    CHECK((c.ret > 0 && c.ret < (long)sizeof(buf)) || (c.ret < 0 && c.err == EAGAIN),
          "recv returned %ld bytes, after the bytes of %ld ms instead of those of %d ms", c.ret,
          c.elapsed_ms, TIMEOUT_MS);
    EXPECT_TIME(&c, TIMEOUT_MS, "recv with MSG_WAITALL");
    call_finish(&c);
    // The writer fails when the socket is closed
    close(sv[0]);
    CHECK(flow_join(&writer, WAKE_MS) ||
          atomic_load(&writer.err) != 0, "the writer does not end");
    close(sv[1]);
    return 0;
}

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

// ---------------------------------------------------------------------------------------------
// SIGPIPE
// ---------------------------------------------------------------------------------------------

// The call fails with EPIPE, and raises SIGPIPE in the thread of the call if `signals` is 1
static int expect_epipe(call_fn fn, int fd, size_t len, int flags, int signals,
                        const char *what) {
    static char data[16] = "0123456789abcde";
    call_t c;

    atomic_store(&sigpipes, 0);
    CHECK(call_start(&c, fn, fd, data, len, flags) == 0, "failed to start %s", what);
    EXPECT_NOW(&c, -1, EPIPE, what);
    call_finish(&c);
    CHECK(atomic_load(&sigpipes) == signals, "%s raised %d SIGPIPE, instead of %d", what,
          atomic_load(&sigpipes), signals);
    CHECK(signals == 0 || atomic_load(&sigpipe_tid) == atomic_load(&c.tid),
          "SIGPIPE of %s is not raised in the thread of the call", what);
    return 0;
}

// Writing to a socket that cannot send fails with EPIPE, and raises SIGPIPE in the thread that
// writes, unless the call has MSG_NOSIGNAL
static int test_write_to_a_closed_peer_raises_sigpipe(void) {
    int sv[2];

    for (size_t i = 0; i < ARRAY_SIZE(send_calls); i++) {
        CHECK(make_pair(sv) == 0, "failed to create a socket pair");
        close(sv[1]);
        // Every call raises it
        for (int round = 0; round < 2; round++) {
            if (expect_epipe(send_calls[i].fn, sv[0], 2, 0, 1, send_calls[i].name) < 0) {
                return -1;
            }
        }
        close(sv[0]);
    }

    // MSG_NOSIGNAL
    for (size_t i = 0; i < ARRAY_SIZE(send_flag_calls); i++) {
        CHECK(make_pair(sv) == 0, "failed to create a socket pair");
        close(sv[1]);
        if (expect_epipe(send_flag_calls[i].fn, sv[0], 2, MSG_NOSIGNAL, 0,
                         send_flag_calls[i].name) < 0) {
            return -1;
        }
        close(sv[0]);
    }

    // No bytes to write do not matter
    CHECK(make_pair(sv) == 0, "failed to create a socket pair");
    close(sv[1]);
    if (expect_epipe(call_write, sv[0], 0, 0, 1, "write of 0 bytes") < 0 ||
            expect_epipe(call_send, sv[0], 0, 0, 1, "send of 0 bytes") < 0 ||
            expect_epipe(call_send, sv[0], 0, MSG_NOSIGNAL, 0, "send of 0 bytes, MSG_NOSIGNAL") < 0) {
        return -1;
    }
    close(sv[0]);

    // The socket that has been shut down for writing, and the one whose peer is shut down for
    // reading
    CHECK(make_pair(sv) == 0, "failed to create a socket pair");
    CHECK(shutdown(sv[0], SHUT_WR) == 0, "failed to shut down");
    if (expect_epipe(call_write, sv[0], 2, 0, 1, "write after shutdown(SHUT_WR)") < 0 ||
            expect_epipe(call_send, sv[0], 2, MSG_NOSIGNAL, 0, "send after shutdown(SHUT_WR)") < 0) {
        return -1;
    }
    close(sv[0]);
    close(sv[1]);
    CHECK(make_pair(sv) == 0, "failed to create a socket pair");
    CHECK(shutdown(sv[1], SHUT_RD) == 0, "failed to shut down");
    if (expect_epipe(call_write, sv[0], 2, 0, 1,
                     "write after shutdown(SHUT_RD) of the peer") < 0) {
        return -1;
    }
    close(sv[0]);
    close(sv[1]);

    // A socket that has been shut down for reading, and a socket whose peer is shut down for
    // writing, can send
    for (int self = 0; self < 2; self++) {
        call_t c;
        CHECK(make_pair(sv) == 0, "failed to create a socket pair");
        CHECK(shutdown(self ? sv[0] : sv[1], self ? SHUT_RD : SHUT_WR) == 0,
              "failed to shut down");
        atomic_store(&sigpipes, 0);
        CHECK(call_start(&c, call_write, sv[0], "ab", 2, 0) == 0, "failed to start write");
        EXPECT_NOW(&c, 2, 0, "write");
        call_finish(&c);
        CHECK(atomic_load(&sigpipes) == 0, "write raised SIGPIPE");
        close(sv[0]);
        close(sv[1]);
    }

    // No SIGPIPE for the errors that are not EPIPE: an unconnected socket, and no bytes on a full
    // buffer
    {
        call_t c;
        int fd = socket(AF_UNIX, SOCK_STREAM, 0);
        atomic_store(&sigpipes, 0);
        CHECK(call_start(&c, call_write, fd, "ab", 2, 0) == 0, "failed to start write");
        EXPECT_NOW(&c, -1, ENOTCONN, "write of an unconnected socket");
        call_finish(&c);
        close(fd);
        CHECK(make_pair(sv) == 0, "failed to create a socket pair");
        CHECK(fill(sv[0]) > 0, "failed to fill the buffer");
        CHECK(call_start(&c, call_write, sv[0], "ab", 0, 0) == 0, "failed to start write");
        EXPECT_NOW(&c, 0, 0, "write of 0 bytes to a full buffer");
        call_finish(&c);
        CHECK(atomic_load(&sigpipes) == 0, "write raised SIGPIPE");
        close(sv[0]);
        close(sv[1]);
    }
    return 0;
}

// A thread that blocks SIGPIPE keeps it pending
static int test_blocked_sigpipe_stays_pending(void) {
    int sv[2];
    call_t c;
    CHECK(make_pair(sv) == 0, "failed to create a socket pair");
    close(sv[1]);
    atomic_store(&sigpipes, 0);
    CHECK(call_start(&c, call_write_sigpipe_blocked, sv[0], "ab", 2, 0) == 0,
          "failed to start write");
    EXPECT_NOW(&c, -1, EPIPE, "write");
    CHECK(c.msg_flags == 1, "SIGPIPE is not pending");
    call_finish(&c);
    CHECK(atomic_load(&sigpipes) == 0, "the handler of SIGPIPE ran");
    close(sv[0]);
    return 0;
}

// sendmmsg to a socket that cannot send fails with EPIPE, and raises SIGPIPE like the other calls
static int test_sendmmsg_to_a_closed_peer_raises_sigpipe(void) {
    int sv[2];
    call_t c;

    for (int nosignal = 0; nosignal < 2; nosignal++) {
        CHECK(make_pair(sv) == 0, "failed to create a socket pair");
        close(sv[1]);
        atomic_store(&sigpipes, 0);
        CHECK(call_start_n(&c, call_sendmmsg, sv[0], "aabb", 2, nosignal ? MSG_NOSIGNAL : 0,
                           2) == 0, "failed to start sendmmsg");
        EXPECT_NOW(&c, -1, EPIPE, "sendmmsg");
        call_finish(&c);
        CHECK(atomic_load(&sigpipes) == (nosignal ? 0 : 1), "sendmmsg raised %d SIGPIPE",
              atomic_load(&sigpipes));
        close(sv[0]);
    }
    return 0;
}

// ---------------------------------------------------------------------------------------------
// Addresses
// ---------------------------------------------------------------------------------------------

// An address in a send is an error: EISCONN if the socket is connected, else EOPNOTSUPP. An
// address of length 0 is no address.
static int test_sendto_and_sendmsg_with_an_address(void) {
    int client, server;
    unix_addr_t laddr, caddr;
    char buf[8];
    call_t c;
    CHECK(make_connection(&client, &server, &laddr, &caddr, 1) == 0, "failed to connect");

    // Connected: the data is not sent
    for (int which = 0; which < 2; which++) {
        for (size_t i = 0; i < 2; i++) {
            // sendto and sendmsg
            call_fn fn = i == 0 ? call_sendto : call_sendmsg;
            const char *what = i == 0 ? "sendto" : "sendmsg";
            call_init(&c, fn, which ? server : client, "x", 1, MSG_NOSIGNAL);
            c.name = which ? caddr.sa : laddr.sa;
            c.namelen = which ? caddr.len : laddr.len;
            CHECK(call_run(&c) == 0, "failed to start %s", what);
            EXPECT_NOW(&c, -1, EISCONN, what);
            call_finish(&c);
        }
    }
    CHECK(drain(server) == 0 && drain(client) == 0, "bytes arrive");

    // An address of length 0 is no address: the data is sent
    CHECK(sendto(client, "ab", 2, MSG_NOSIGNAL, (struct sockaddr *)&laddr.sa, 0) == 2,
          "sendto with an address of length 0 fails");
    CHECK(recv(server, buf, sizeof(buf), 0) == 2, "the bytes do not arrive");
    close(client);
    close(server);

    // Not connected: an idle socket, and a listening socket
    for (int listening = 0; listening < 2; listening++) {
        unix_addr_t a = new_addr();
        int fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (listening) {
            CHECK(bind(fd, (struct sockaddr *)&a.sa, a.len) == 0 && listen(fd, 1) == 0,
                  "failed to listen");
        }
        for (size_t i = 0; i < 2; i++) {
            call_fn fn = i == 0 ? call_sendto : call_sendmsg;
            const char *what = i == 0 ? "sendto" : "sendmsg";
            call_init(&c, fn, fd, "x", 1, MSG_NOSIGNAL);
            c.name = laddr.sa;
            c.namelen = laddr.len;
            CHECK(call_run(&c) == 0, "failed to start %s", what);
            EXPECT_NOW(&c, -1, EOPNOTSUPP, what);
            call_finish(&c);

            // No address, or one of length 0
            CHECK(call_start(&c, fn, fd, "x", 1, MSG_NOSIGNAL) == 0, "failed to start %s", what);
            EXPECT_NOW(&c, -1, ENOTCONN, what);
            call_finish(&c);
        }
        errno = 0;
        CHECK(sendto(fd, "x", 1, MSG_NOSIGNAL, (struct sockaddr *)&laddr.sa, 0) == -1 &&
              errno == ENOTCONN, "sendto with an address of length 0 does not fail with ENOTCONN");
        close(fd);
    }
    return 0;
}

// recvfrom and recvmsg tell the name that the peer is bound to, which is that of the listener for
// the socket that accept returns, and nothing for a peer that has no name
static int test_receive_tells_the_name_of_the_peer(void) {
    static const named_call_t calls[] = { { "recvfrom", call_recvfrom }, { "recvmsg", call_recvmsg } };
    int client, server;
    unix_addr_t laddr, caddr;
    char buf[8];
    call_t c;

    for (int bound = 0; bound < 2; bound++) {
        CHECK(make_connection(&client, &server, &laddr, &caddr, bound) == 0, "failed to connect");
        for (size_t i = 0; i < ARRAY_SIZE(calls); i++) {
            const char *what = calls[i].name;

            // The server reads what the client wrote: the name of the client, if it has one
            CHECK(write(client, "x", 1) == 1, "failed to write");
            CHECK(call_start(&c, calls[i].fn, server, buf, sizeof(buf), 0) == 0,
                  "failed to start %s", what);
            EXPECT_NOW(&c, 1, 0, what);
            if (bound) {
                CHECK(c.namelen == caddr.len && memcmp(&c.name, &caddr.sa, caddr.len) == 0,
                      "%s returns a name of length %u, instead of the name of the client (%u)",
                      what, (unsigned)c.namelen, (unsigned)caddr.len);
            } else {
                CHECK(c.namelen == 0, "%s returns a name of length %u for an unnamed peer", what,
                      (unsigned)c.namelen);
            }
            call_finish(&c);

            // The client reads what the server wrote: the name of the listener
            CHECK(write(server, "y", 1) == 1, "failed to write");
            CHECK(call_start(&c, calls[i].fn, client, buf, sizeof(buf), 0) == 0,
                  "failed to start %s", what);
            EXPECT_NOW(&c, 1, 0, what);
            CHECK(c.namelen == laddr.len && memcmp(&c.name, &laddr.sa, laddr.len) == 0,
                  "%s returns a name of length %u, instead of the name of the listener (%u)",
                  what, (unsigned)c.namelen, (unsigned)laddr.len);
            call_finish(&c);
        }

        // A name that does not fit is cut, and its whole length is returned
        if (bound) {
            struct sockaddr_un small;
            socklen_t len = 5;
            memset(&small, 0, sizeof(small));
            CHECK(write(client, "x", 1) == 1, "failed to write");
            CHECK(recvfrom(server, buf, sizeof(buf), 0, (struct sockaddr *)&small, &len) == 1,
                  "failed to receive");
            CHECK(len == caddr.len && memcmp(&small, &caddr.sa, 5) == 0 &&
                  ((char *)&small)[5] == 0, "a name that is cut is wrong (length %u)",
                  (unsigned)len);
        }
        close(client);
        close(server);
    }

    // A socket pair has no names
    int sv[2];
    CHECK(make_pair(sv) == 0, "failed to create a socket pair");
    CHECK(write(sv[1], "x", 1) == 1, "failed to write");
    CHECK(call_start(&c, call_recvmsg, sv[0], buf, sizeof(buf), 0) == 0,
          "failed to start recvmsg");
    EXPECT_NOW(&c, 1, 0, "recvmsg");
    CHECK(c.namelen == 0, "recvmsg returns a name for a socket pair");
    call_finish(&c);
    close(sv[0]);
    close(sv[1]);
    return 0;
}

// ---------------------------------------------------------------------------------------------
// sendmmsg and recvmmsg
// ---------------------------------------------------------------------------------------------

static int test_sendmmsg(void) {
    int sv[2];
    char buf[16];
    call_t c;

    // Messages are bytes of the stream
    CHECK(make_pair(sv) == 0, "failed to create a socket pair");
    CHECK(call_start_n(&c, call_sendmmsg, sv[0], "aabbcc", 2, 0, 3) == 0,
          "failed to start sendmmsg");
    EXPECT_NOW(&c, 3, 0, "sendmmsg");
    CHECK(c.lens[0] == 2 && c.lens[1] == 2 &&
          c.lens[2] == 2, "sendmmsg sends %u, %u, %u bytes",
          c.lens[0], c.lens[1], c.lens[2]);
    call_finish(&c);
    CHECK(recv(sv[1], buf, sizeof(buf), 0) == 6 && memcmp(buf, "aabbcc", 6) == 0,
          "the bytes of sendmmsg do not arrive");

    // No message
    CHECK(call_start_n(&c, call_sendmmsg, sv[0], "aabbcc", 2, 0, 0) == 0,
          "failed to start sendmmsg");
    EXPECT_NOW(&c, 0, 0, "sendmmsg of no message");
    call_finish(&c);
    close(sv[0]);
    close(sv[1]);

    // A message that waits for the reader, which gets all the bytes
    {
        const size_t total = 2 * MIB;
        char *data = malloc(total);
        fill_pattern(data, total, 0);
        flow_t reader;
        CHECK(make_pair(sv) == 0, "failed to create a socket pair");
        CHECK(flow_start(&reader, reader_main, sv[1], total, 65536, 100, 0) == 0,
              "failed to start the reader");
        CHECK(call_start_n(&c, call_sendmmsg, sv[0], data, MIB, 0, 2) == 0,
              "failed to start sendmmsg");
        EXPECT_CALL(&c, 30000, 2, 0, "sendmmsg");
        CHECK(c.lens[0] == MIB && c.lens[1] == MIB, "sendmmsg sends %u and %u bytes", c.lens[0],
              c.lens[1]);
        call_finish(&c);
        CHECK(flow_join(&reader, WAKE_MS), "the reader does not get everything");
        CHECK(reader.bad_pos == total, "a wrong byte at %zu", reader.bad_pos);
        close(sv[0]);
        close(sv[1]);
        free(data);
    }

    // A peer that is gone: the first message fails
    CHECK(make_pair(sv) == 0, "failed to create a socket pair");
    close(sv[1]);
    CHECK(call_start_n(&c, call_sendmmsg, sv[0], "aabb", 2, MSG_NOSIGNAL, 2) == 0,
          "failed to start sendmmsg");
    EXPECT_NOW(&c, -1, EPIPE, "sendmmsg");
    call_finish(&c);
    close(sv[0]);
    return 0;
}

static int test_recvmmsg(void) {
    int sv[2];
    char buf[4 * 4];
    call_t c;
    stimulus_t s;
    CHECK(make_pair(sv) == 0, "failed to create a socket pair");

    // The messages take the bytes in turn
    CHECK(write(sv[1], "123456789", 9) == 9, "failed to write");
    memset(buf, 0, sizeof(buf));
    CHECK(call_start_n(&c, call_recvmmsg, sv[0], buf, 4, MSG_DONTWAIT, 4) == 0,
          "failed to start recvmmsg");
    EXPECT_NOW(&c, 3, 0, "recvmmsg");
    CHECK(c.lens[0] == 4 && c.lens[1] == 4 &&
          c.lens[2] == 1, "recvmmsg gets %u, %u, %u bytes",
          c.lens[0], c.lens[1], c.lens[2]);
    CHECK(memcmp(buf, "1234", 4) == 0 && memcmp(buf + 4, "5678", 4) == 0 && buf[8] == '9',
          "recvmmsg gets wrong bytes");
    call_finish(&c);

    // Nothing to get
    CHECK(call_start_n(&c, call_recvmmsg, sv[0], buf, 4, MSG_DONTWAIT, 2) == 0,
          "failed to start recvmmsg");
    EXPECT_NOW(&c, -1, EAGAIN, "recvmmsg");
    call_finish(&c);

    // No message
    CHECK(call_start_n(&c, call_recvmmsg, sv[0], buf, 4, MSG_DONTWAIT, 0) == 0,
          "failed to start recvmmsg");
    EXPECT_NOW(&c, 0, 0, "recvmmsg of no message");
    call_finish(&c);

    // MSG_WAITFORONE: the first message waits, the others do not
    CHECK(write(sv[1], "ab", 2) == 2, "failed to write");
    CHECK(call_start_n(&c, call_recvmmsg, sv[0], buf, 4, MSG_WAITFORONE, 2) == 0,
          "failed to start recvmmsg");
    EXPECT_NOW(&c, 1, 0, "recvmmsg");
    CHECK(c.lens[0] == 2 && memcmp(buf, "ab", 2) == 0, "recvmmsg gets wrong bytes");
    call_finish(&c);
    CHECK(call_start_n(&c, call_recvmmsg, sv[0], buf, 4, MSG_WAITFORONE, 2) == 0,
          "failed to start recvmmsg");
    CHECK(call_blocks(&c), "recvmmsg returns on an empty socket");
    CHECK(stimulus_start(&s, sv[1], 0, "q", 0) == 0, "failed to start");
    stimulus_join(&s);
    EXPECT_CALL(&c, WAKE_MS, 1, 0, "recvmmsg");
    CHECK(c.lens[0] == 1 && buf[0] == 'q', "recvmmsg gets wrong bytes");
    call_finish(&c);

    // Without MSG_WAITFORONE all the messages wait
    CHECK(write(sv[1], "cd", 2) == 2, "failed to write");
    CHECK(call_start_n(&c, call_recvmmsg, sv[0], buf, 4, 0, 2) == 0,
          "failed to start recvmmsg");
    CHECK(call_blocks(&c), "recvmmsg returns with one message of two");
    CHECK(stimulus_start(&s, sv[1], 0, "efgh", 0) == 0, "failed to start");
    stimulus_join(&s);
    EXPECT_CALL(&c, WAKE_MS, 2, 0, "recvmmsg");
    call_finish(&c);
    close(sv[0]);
    close(sv[1]);
    return 0;
}

// sendmmsg returns the number of the messages that it has sent if a later message fails: the
// error is for the first message only
static int test_sendmmsg_stops_at_a_failing_message(void) {
    int sv[2];
    char buf[16];
    struct mmsghdr msgs[3];
    struct iovec iov[3];
    CHECK(make_pair(sv) == 0, "failed to create a socket pair");

    // The second message is no memory
    memset(msgs, 0, sizeof(msgs));
    iov[0] = (struct iovec) { "aa", 2 };
    iov[1] = (struct iovec) { (void *)1, 2 };
    iov[2] = (struct iovec) { "cc", 2 };
    for (int i = 0; i < 3; i++) {
        msgs[i].msg_hdr.msg_iov = &iov[i];
        msgs[i].msg_hdr.msg_iovlen = 1;
    }
    errno = 0;
    long ret = syscall(SYS_sendmmsg, sv[0], msgs, 3, 0);
    CHECK(ret == 1, "sendmmsg of a message and one in no memory returned %ld (errno %d)", ret,
          errno);
    CHECK(msgs[0].msg_len == 2, "sendmmsg sent %u bytes of the first message",
          msgs[0].msg_len);
    CHECK(recv(sv[1], buf, sizeof(buf), MSG_DONTWAIT) == 2 && memcmp(buf, "aa", 2) == 0,
          "the bytes of the messages that were sent do not arrive");

    // The first message is no memory: the call fails
    errno = 0;
    ret = syscall(SYS_sendmmsg, sv[0], &msgs[1], 2, 0);
    CHECK(ret == -1 &&
          errno == EFAULT, "sendmmsg of a message in no memory returned %ld (errno %d)",
          ret, errno);
    CHECK(drain(sv[1]) == 0, "bytes arrive from a message that failed");

    // The last message finds no room: the one before has written the bytes that fit
    size_t big_len = 4 * MIB;
    char *big = calloc(1, big_len);
    iov[0] = (struct iovec) { "a", 1 };
    iov[1] = (struct iovec) { big, big_len };
    iov[2] = (struct iovec) { "c", 1 };
    memset(msgs, 0, sizeof(msgs));
    for (int i = 0; i < 3; i++) {
        msgs[i].msg_hdr.msg_iov = &iov[i];
        msgs[i].msg_hdr.msg_iovlen = 1;
    }
    errno = 0;
    ret = syscall(SYS_sendmmsg, sv[0], msgs, 3, MSG_DONTWAIT);
    CHECK(ret == 2,
          "sendmmsg that finds no room for its last message returned %ld (errno %d)", ret,
          errno);
    CHECK(msgs[0].msg_len == 1 && msgs[1].msg_len > 0 && msgs[1].msg_len < big_len,
          "sendmmsg sent %u and %u bytes of its first messages", msgs[0].msg_len, msgs[1].msg_len);
    CHECK(drain(sv[1]) == (long)(1 + msgs[1].msg_len),
          "not all the bytes of the messages arrive");
    close(sv[0]);
    close(sv[1]);
    free(big);
    return 0;
}

// recvmmsg checks its timeout after each message, not while it waits for one: it stops when the
// time is over, and tells the time that is left, which is zero then. Linux does the same.
static int test_recvmmsg_timeout(void) {
    enum { VLEN = 16 };
    int sv[2];
    char bytes[VLEN];
    struct mmsghdr msgs[VLEN];
    struct iovec iov[VLEN];
    CHECK(make_pair(sv) == 0, "failed to create a socket pair");

    // Three bytes are there for the messages of a byte, and the fourth message finds none. The call
    // returns, and the timeout is what is left of the five seconds.
    CHECK(write(sv[1], "abc", 3) == 3, "failed to write");
    memset(msgs, 0, sizeof(msgs));
    for (int i = 0; i < 4; i++) {
        iov[i] = (struct iovec) { bytes + i, 1 };
        msgs[i].msg_hdr.msg_iov = &iov[i];
        msgs[i].msg_hdr.msg_iovlen = 1;
    }
    struct timespec timeout = { 5, 0 };
    long ret = syscall(SYS_recvmmsg, sv[0], msgs, 4, MSG_WAITFORONE, &timeout);
    CHECK(ret == 3, "recvmmsg returned %ld (errno %d)", ret, errno);
    CHECK(memcmp(bytes, "abc", 3) == 0, "recvmmsg gets wrong bytes");
    long left_ns = timeout.tv_sec * 1000000000L + timeout.tv_nsec;
    CHECK(left_ns < 5000000000L && left_ns > 3000000000L,
          "the timeout of recvmmsg is %ld ns after the call, instead of the time that is left",
          left_ns);

    // A byte arrives every 30 ms, and a timeout of 100 ms is over after the fourth. Without the
    // check after each message, the call would wait for all 16 messages (480 ms).
    flow_t sender;
    CHECK(flow_start(&sender, writer_main, sv[1], 20, 1, 0, 30) == 0,
          "failed to start the writer");
    memset(msgs, 0, sizeof(msgs));
    for (int i = 0; i < VLEN; i++) {
        iov[i] = (struct iovec) { bytes + i, 1 };
        msgs[i].msg_hdr.msg_iov = &iov[i];
        msgs[i].msg_hdr.msg_iovlen = 1;
    }
    timeout = (struct timespec) { 0, 100 * 1000000L };
    long start = now_ms();
    ret = syscall(SYS_recvmmsg, sv[0], msgs, VLEN, 0, &timeout);
    long elapsed = now_ms() - start;
    CHECK(ret >= 1 &&
          ret < VLEN, "recvmmsg with a timeout of 100 ms returned %ld messages", ret);
    CHECK(elapsed >= 100 - EARLY_MS && elapsed < 100 + SLACK_MS,
          "recvmmsg with a timeout of 100 ms took %ld ms", elapsed);
    CHECK(timeout.tv_sec == 0 && timeout.tv_nsec == 0,
          "the timeout of recvmmsg is %ld s and %ld ns after the time is over",
          (long)timeout.tv_sec,
          (long)timeout.tv_nsec);
    CHECK(flow_join(&sender, WAKE_MS), "the writer does not write its bytes");
    close(sv[0]);
    close(sv[1]);
    return 0;
}

// ---------------------------------------------------------------------------------------------
// The size of the send buffer
// ---------------------------------------------------------------------------------------------

// SO_SNDBUF limits what a socket has in flight to its peer, whenever it is set. If it is raised, the
// writers that wait for room go on.
static int test_sndbuf_limits_the_bytes_in_flight(void) {
    char buf[64];
    int sv[2];
    call_t c;
    CHECK(make_pair(sv) == 0, "failed to create a socket pair");

    long normal = fill(sv[1]);
    CHECK(normal > 100000, "the buffer of a socket takes %ld bytes", normal);
    CHECK(set_sndbuf(sv[0], 4096) == 0, "failed to set SO_SNDBUF");
    long small = fill(sv[0]);
    CHECK(small > 0 && small < 50000, "the send buffer of 4096 bytes takes %ld bytes", small);

    // A write that waits for room, and the buffer is raised
    CHECK(call_start(&c, call_write, sv[0], buf, 1, 0) == 0, "failed to start write");
    CHECK(call_blocks(&c), "write returns on a full buffer");
    CHECK(set_sndbuf(sv[0], 262144) == 0, "failed to set SO_SNDBUF");
    EXPECT_CALL(&c, WAKE_MS, 1, 0, "write after SO_SNDBUF was raised");
    call_finish(&c);
    CHECK(drain(sv[1]) == small + 1, "not all the bytes arrive");

    // The room that is left after the buffer was made smaller than what it holds comes when the
    // reader has taken enough
    CHECK(set_sndbuf(sv[0], 262144) == 0, "failed to set SO_SNDBUF");
    CHECK(fill(sv[0]) > 100000, "failed to fill the buffer");
    CHECK(set_sndbuf(sv[0], 4096) == 0, "failed to set SO_SNDBUF");
    CHECK(call_start(&c, call_send, sv[0], buf, 1, MSG_DONTWAIT) == 0,
          "failed to start send");
    EXPECT_NOW(&c, -1, EAGAIN, "send to a buffer that is smaller than its bytes");
    call_finish(&c);
    CHECK(drain(sv[1]) > 100000, "not all the bytes arrive");
    CHECK(call_start(&c, call_send, sv[0], buf, 1, MSG_DONTWAIT) == 0,
          "failed to start send");
    EXPECT_NOW(&c, 1, 0, "send after the reader took the bytes");
    call_finish(&c);
    close(sv[0]);
    close(sv[1]);
    return 0;
}

// The size of the send buffer that a socket sets before it connects is the limit of its connection,
// and the socket that accept returns has the default size. The size of the receive buffer limits
// nothing for the sender.
static int test_sndbuf_before_connect(void) {
    unix_addr_t laddr = new_addr();
    int l = socket(AF_UNIX, SOCK_STREAM, 0);
    int c = socket(AF_UNIX, SOCK_STREAM, 0);
    CHECK(bind(l, (struct sockaddr *)&laddr.sa, laddr.len) == 0 && listen(l, 4) == 0,
          "failed to listen");
    CHECK(set_sndbuf(c, 4096) == 0, "failed to set SO_SNDBUF");
    CHECK(connect(c, (struct sockaddr *)&laddr.sa, laddr.len) == 0, "failed to connect");
    int s = accept(l, NULL, NULL);
    CHECK(s >= 0, "failed to accept");

    long from_client = fill(c);
    CHECK(from_client > 0 && from_client < 50000, "the buffer of the client takes %ld bytes",
          from_client);
    long from_server = fill(s);
    CHECK(from_server > 100000, "the buffer of the server takes %ld bytes", from_server);
    close(c);
    close(s);
    close(l);

    int sv[2];
    int size = 4096;
    CHECK(make_pair(sv) == 0, "failed to create a socket pair");
    CHECK(setsockopt(sv[0], SOL_SOCKET, SO_RCVBUF, &size, sizeof(size)) == 0,
          "failed to set SO_RCVBUF");
    long sent = fill(sv[1]);
    CHECK(sent > 100000, "the sender takes %ld bytes for a receive buffer of 4096", sent);
    close(sv[0]);
    close(sv[1]);
    return 0;
}

// sendmmsg takes at most 1024 messages (UIO_MAXIOV), whatever the length of the vector says, and
// recvmmsg has no such limit. Both take no message from a vector of length 0, whatever its address
static int test_mmsg_vector_length(void) {
    enum { VLEN = 1100, LIMIT = 1024 };
    int sv[2];
    char bytes[VLEN];
    char out[VLEN];
    flow_t reader;
    struct mmsghdr *msgs = calloc(VLEN, sizeof(*msgs));
    struct iovec *iov = calloc(VLEN, sizeof(*iov));
    CHECK(msgs != NULL && iov != NULL, "failed to allocate the vector");
    fill_pattern(bytes, sizeof(bytes), 0);
    CHECK(make_pair(sv) == 0, "failed to create a socket pair");

    // A byte for each message, which a reader takes while they are sent. Only 1024 messages are
    // sent.
    for (int i = 0; i < VLEN; i++) {
        iov[i].iov_base = bytes + i;
        iov[i].iov_len = 1;
        msgs[i].msg_hdr.msg_iov = &iov[i];
        msgs[i].msg_hdr.msg_iovlen = 1;
    }
    CHECK(flow_start(&reader, reader_main, sv[1], LIMIT, 4096, 0, 0) == 0,
          "failed to start the reader");
    long ret = syscall(SYS_sendmmsg, sv[0], msgs, VLEN, 0);
    CHECK(ret == LIMIT, "sendmmsg of %d messages sent %ld", VLEN, ret);
    CHECK(msgs[LIMIT - 1].msg_len == 1 && msgs[LIMIT].msg_len == 0,
          "sendmmsg wrote the lengths of the wrong messages");
    CHECK(flow_join(&reader, WAKE_MS), "the reader does not get all the bytes");
    CHECK(reader.bad_pos == LIMIT, "a wrong byte at %zu", reader.bad_pos);
    errno = 0;
    CHECK(recv(sv[1], out, sizeof(out), MSG_DONTWAIT) == -1 && errno == EAGAIN,
          "more bytes than those of the first %d messages arrive", LIMIT);

    // Of the bytes that are there, all the messages of the vector are received
    CHECK(write(sv[0], bytes, VLEN) == VLEN, "failed to write");
    memset(msgs, 0, VLEN * sizeof(*msgs));
    for (int i = 0; i < VLEN; i++) {
        iov[i].iov_base = out + i;
        msgs[i].msg_hdr.msg_iov = &iov[i];
        msgs[i].msg_hdr.msg_iovlen = 1;
    }
    ret = syscall(SYS_recvmmsg, sv[1], msgs, VLEN, MSG_DONTWAIT, NULL);
    CHECK(ret == VLEN, "recvmmsg of %d messages received %ld", VLEN, ret);
    CHECK(memcmp(out, bytes, VLEN) == 0, "recvmmsg gets wrong bytes");

    // No message, and a vector that is no memory
    CHECK(syscall(SYS_sendmmsg, sv[0], NULL, 0, MSG_DONTWAIT) == 0, "sendmmsg of 0 messages");
    CHECK(syscall(SYS_recvmmsg, sv[1], NULL, 0, MSG_DONTWAIT, NULL) == 0,
          "recvmmsg of 0 messages");
    errno = 0;
    CHECK(syscall(SYS_sendmmsg, sv[0], (void *)1, 1, MSG_DONTWAIT) == -1 && errno == EFAULT,
          "sendmmsg of a vector that is no memory: errno %d", errno);
    close(sv[0]);
    close(sv[1]);
    free(msgs);
    free(iov);
    return 0;
}

static test_case_t test_cases[] = {
    TEST_CASE(test_blocking_writes_write_everything),
    TEST_CASE(test_concurrent_writes_deliver_everything),
    TEST_CASE(test_nonblocking_writes_are_partial),
    TEST_CASE(test_interrupted_write_returns_the_progress),
    TEST_CASE(test_write_timeout),
    TEST_CASE(test_write_timeout_restarts_with_progress),
    TEST_CASE(test_write_goes_on_when_room_comes_after_its_timeout),
    TEST_CASE(test_peer_goes_while_a_write_waits),
    TEST_CASE(test_read_timeout),
    TEST_CASE(test_timeouts_belong_to_the_socket),
    TEST_CASE(test_nonblocking_socket_ignores_the_timeouts),
    TEST_CASE(test_timeout_is_not_changed_by_other_signals),
    TEST_CASE(test_interrupted_reads),
    TEST_CASE(test_msg_waitall),
    TEST_CASE(test_timeout_counts_over_all_waits),
    TEST_CASE(test_msg_dontwait),
    TEST_CASE(test_msg_peek),
    TEST_CASE(test_msg_peek_waits_for_data),
    TEST_CASE(test_msg_peek_of_a_long_queue),
    TEST_CASE(test_msg_flags_without_effect),
    TEST_CASE(test_oob_flag_does_not_hang),
    TEST_CASE(test_write_to_a_closed_peer_raises_sigpipe),
    TEST_CASE(test_blocked_sigpipe_stays_pending),
    TEST_CASE(test_sendmmsg_to_a_closed_peer_raises_sigpipe),
    TEST_CASE(test_sendto_and_sendmsg_with_an_address),
    TEST_CASE(test_receive_tells_the_name_of_the_peer),
    TEST_CASE(test_sendmmsg),
    TEST_CASE(test_recvmmsg),
    TEST_CASE(test_sendmmsg_stops_at_a_failing_message),
    TEST_CASE(test_recvmmsg_timeout),
    TEST_CASE(test_mmsg_vector_length),
    TEST_CASE(test_sndbuf_limits_the_bytes_in_flight),
    TEST_CASE(test_sndbuf_before_connect),
};

int main(int argc, const char *argv[]) {
    // SIGUSR1 interrupts the calls, and SIGPIPE is counted: both without SA_RESTART
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_signal;
    sigaction(SIGUSR1, &sa, NULL);
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
        fflush(stdout);
    }
    fflush(stdout);
    if (atomic_load(&stuck_threads) > 0) {
        // Threads are left behind in calls that do not return
        _exit(1);
    }
    return ret;
}
