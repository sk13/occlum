// The control messages of unix stream sockets: the file descriptors of SCM_RIGHTS, and the
// credentials of SCM_CREDENTIALS and SO_PASSCRED. The expectations are the behavior of Linux: the
// test passes there, too (gcc -pthread -I../include main.c), and what differs for Occlum is marked
// in the code. What it checks:
//
//  - a descriptor arrives as a new descriptor of the receiver for the same file, over a socket
//    pair and over a connection, also if the sender has closed its descriptor, or its thread has
//    exited, before the receiver receives it;
//  - the control data does not travel in the bytes: a read gets the bytes only, a receive with a
//    buffer for control messages but without control data from the sender gets the bytes only, and
//    the descriptors come with the first byte of their message, which a receive does not go past;
//  - a descriptor that is not delivered (no buffer, a buffer that is too small, a plain read) is
//    closed, MSG_CTRUNC tells the program, and a peek delivers the descriptors again and again;
//  - the control messages of a send are validated, before anything is sent;
//  - SO_PASSCRED makes the receiver get the credentials of the sender, SCM_CREDENTIALS names them;
//  - nothing is left behind: not a descriptor, not memory, also if the receiver is closed with
//    control messages that nobody has received.
//
// A call that is to return at once has a timeout, so that a failure ends the test case, rather
// than hangs it. A watchdog ends the test if a case blocks anyway.
//
// Usage: unix_socket_ancillary [test case name | --list]
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <spawn.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "kernel_heap.h"
#include "test.h"

#define CHECK(cond, ...) do { \
    if (!(cond)) { \
        THROW_ERROR(__VA_ARGS__); \
    } \
} while (0)

#define WATCHDOG_MS     120000
#define RECV_TIMEOUT_MS 3000    // for the calls that return at once
#define FD_LIMIT        512     // the descriptors that count_fds() looks at
#define NO_BUFFER       ((size_t)-1)

// ---------------------------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------------------------

static atomic_long watchdog_deadline;
static const char *current_case = "";
static const char *self_path;

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

// Whether the program runs in Occlum
static int in_occlum(void) {
    return getenv("OCCLUM") != NULL;
}

static void set_timeout(int fd, int ms) {
    struct timeval tv = { ms / 1000, (ms % 1000) * 1000 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
}

static void set_nonblocking(int fd, int nonblocking) {
    int flags = fcntl(fd, F_GETFL);
    fcntl(fd, F_SETFL, nonblocking ? (flags | O_NONBLOCK) : (flags & ~O_NONBLOCK));
}

static int set_int_opt(int fd, int opt, int val) {
    return setsockopt(fd, SOL_SOCKET, opt, &val, sizeof(val)) < 0 ? -errno : 0;
}

static int get_int_opt(int fd, int opt) {
    int val = -1;
    socklen_t len = sizeof(val);
    if (getsockopt(fd, SOL_SOCKET, opt, &val, &len) < 0) {
        return -errno;
    }
    return val;
}

// The number of the open descriptors
static int count_fds(void) {
    int count = 0;
    for (int fd = 0; fd < FD_LIMIT; fd++) {
        if (fcntl(fd, F_GETFD) != -1) {
            count++;
        }
    }
    return count;
}

enum { PAIR_SOCKETPAIR, PAIR_CONNECT, NUM_PAIR_KINDS };
static const char *pair_names[] = { "socketpair", "connect" };

// A listening socket on a new abstract address
static int listener(struct sockaddr_un *addr, socklen_t *addr_len) {
    static int counter = 0;
    memset(addr, 0, sizeof(*addr));
    addr->sun_family = AF_UNIX;
    int name_len = snprintf(addr->sun_path + 1, sizeof(addr->sun_path) - 1,
                            "unix_socket_ancillary.%d.%d", (int)getpid(), counter++);
    *addr_len = offsetof(struct sockaddr_un, sun_path) + 1 + name_len;
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0 || bind(fd, (struct sockaddr *)addr, *addr_len) < 0 || listen(fd, 8) < 0) {
        return -1;
    }
    return fd;
}

// Creates a connection: `a` is the end that connected, `b` the end that was accepted (if the kind
// is PAIR_CONNECT). Both have a timeout, so that no call hangs.
static int create_pair(int kind, int *a, int *b) {
    if (kind == PAIR_SOCKETPAIR) {
        int sv[2];
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) {
            return -1;
        }
        *a = sv[0];
        *b = sv[1];
    } else {
        struct sockaddr_un addr;
        socklen_t addr_len;
        int l = listener(&addr, &addr_len);
        if (l < 0) {
            return -1;
        }
        *a = socket(AF_UNIX, SOCK_STREAM, 0);
        if (*a < 0 || connect(*a, (struct sockaddr *)&addr, addr_len) < 0) {
            return -1;
        }
        *b = accept(l, NULL, NULL);
        close(l);
        if (*b < 0) {
            return -1;
        }
    }
    set_timeout(*a, RECV_TIMEOUT_MS);
    set_timeout(*b, RECV_TIMEOUT_MS);
    return 0;
}

// Sends `len` bytes with the control messages in `ctl`. Returns the result, or the negative errno.
static long send_ctl(int fd, const void *data, size_t len, void *ctl, size_t ctl_len,
                     int flags) {
    struct iovec iov = { (void *)data, len };
    struct msghdr msg;
    memset(&msg, 0, sizeof(msg));
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_control = ctl_len ? ctl : NULL;
    msg.msg_controllen = ctl_len;
    // The system call: the sendmsg() of musl refuses more than 1,024 bytes of control messages
    long ret = syscall(SYS_sendmsg, fd, &msg, flags | MSG_NOSIGNAL);
    return ret < 0 ? -errno : ret;
}

// Sends bytes with the descriptors in one SCM_RIGHTS message
static long send_fds(int fd, const void *data, size_t len, const int *fds, int num_fds) {
    char ctl[CMSG_SPACE(sizeof(int) * 256)];
    memset(ctl, 0, sizeof(ctl));
    struct cmsghdr *c = (struct cmsghdr *)ctl;
    c->cmsg_level = SOL_SOCKET;
    c->cmsg_type = SCM_RIGHTS;
    c->cmsg_len = CMSG_LEN(sizeof(int) * num_fds);
    memcpy(CMSG_DATA(c), fds, sizeof(int) * num_fds);
    return send_ctl(fd, data, len, ctl, CMSG_SPACE(sizeof(int) * num_fds), 0);
}

static long send_fd(int fd, const void *data, size_t len, int send_fd) {
    return send_fds(fd, data, len, &send_fd, 1);
}

// Sends bytes with SCM_CREDENTIALS
static long send_cred(int fd, const void *data, size_t len, pid_t pid, uid_t uid,
                      gid_t gid) {
    struct ucred cred = { pid, uid, gid };
    char ctl[CMSG_SPACE(sizeof(cred))];
    memset(ctl, 0, sizeof(ctl));
    struct cmsghdr *c = (struct cmsghdr *)ctl;
    c->cmsg_level = SOL_SOCKET;
    c->cmsg_type = SCM_CREDENTIALS;
    c->cmsg_len = CMSG_LEN(sizeof(cred));
    memcpy(CMSG_DATA(c), &cred, sizeof(cred));
    return send_ctl(fd, data, len, ctl, sizeof(ctl), 0);
}

static long send_own_cred(int fd, const void *data, size_t len) {
    return send_cred(fd, data, len, getpid(), getuid(), getgid());
}

// What a receive got
typedef struct {
    long ret;               // the number of bytes, or the negative errno
    char data[4096];
    int flags;              // msg_flags
    size_t controllen;
    int num_cmsgs;
    int num_fds;            // the descriptors of all SCM_RIGHTS messages
    int fds[256];
    int rights_cmsgs;
    int has_cred;
    struct ucred cred;
    int cred_first;         // the credentials come before the descriptors
    size_t cred_cmsg_len;
} got_t;

// Receives up to `len` bytes with a buffer of `ctl_len` bytes for control messages (NO_BUFFER:
// none)
static void recv_ctl(int fd, size_t len, size_t ctl_len, int flags, got_t *got) {
    char ctl[2048];
    memset(got, 0, sizeof(*got));
    memset(ctl, 0, sizeof(ctl));
    struct iovec iov = { got->data, len };
    struct msghdr msg;
    memset(&msg, 0, sizeof(msg));
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    if (ctl_len != NO_BUFFER) {
        msg.msg_control = ctl;
        msg.msg_controllen = ctl_len;
    }
    got->ret = recvmsg(fd, &msg, flags);
    if (got->ret < 0) {
        got->ret = -errno;
        return;
    }
    got->flags = msg.msg_flags;
    got->controllen = ctl_len == NO_BUFFER ? 0 : msg.msg_controllen;
    if (ctl_len == NO_BUFFER) {
        return;
    }
    for (struct cmsghdr *c = CMSG_FIRSTHDR(&msg); c; c = CMSG_NXTHDR(&msg, c)) {
        got->num_cmsgs++;
        if (c->cmsg_level != SOL_SOCKET) {
            continue;
        }
        if (c->cmsg_type == SCM_RIGHTS) {
            int n = (c->cmsg_len - CMSG_LEN(0)) / sizeof(int);
            got->rights_cmsgs++;
            for (int i = 0; i < n && got->num_fds < 256; i++) {
                memcpy(&got->fds[got->num_fds++], CMSG_DATA(c) + i * sizeof(int), sizeof(int));
            }
        } else if (c->cmsg_type == SCM_CREDENTIALS) {
            got->has_cred = 1;
            got->cred_cmsg_len = c->cmsg_len;
            got->cred_first = got->num_fds == 0;
            if (c->cmsg_len >= CMSG_LEN(sizeof(got->cred))) {
                memcpy(&got->cred, CMSG_DATA(c), sizeof(got->cred));
            }
        }
    }
}

static void close_got_fds(got_t *got) {
    for (int i = 0; i < got->num_fds; i++) {
        close(got->fds[i]);
    }
}

// Receives what is there, without waiting
static void recv_now(int fd, size_t len, size_t ctl_len, int flags, got_t *got) {
    recv_ctl(fd, len, ctl_len, flags | MSG_DONTWAIT, got);
}

#define CHECK_GOT(got, expected_ret, expected_data) do { \
        if ((got)->ret != (long)(expected_ret) || \
            ((got)->ret > 0 && memcmp((got)->data, expected_data, (got)->ret) != 0)) { \
            THROW_ERROR("received %ld bytes \"%.*s\", expected %ld \"%s\"", (got)->ret, \
                        (int)((got)->ret > 0 ? (got)->ret : 0), (got)->data, \
                        (long)(expected_ret), expected_data); \
        } \
    } while (0)

// A pipe: the file that the tests send, as the receiver can write to it and read from it
typedef struct {
    int rd;
    int wr;
} pipe_t;

static int make_pipe(pipe_t *p) {
    int fds[2];
    if (pipe(fds) < 0) {
        return -1;
    }
    p->rd = fds[0];
    p->wr = fds[1];
    return 0;
}

// Whether `fd` is the read end of the pipe `p`, which has no data yet: writes a message to the pipe
// and reads it from `fd`
static int is_read_end_of(int fd, pipe_t *p) {
    char msg[16];
    snprintf(msg, sizeof(msg), "pipe%d", p->wr);
    char buf[16] = {0};
    if (write(p->wr, msg, strlen(msg)) != (ssize_t)strlen(msg)) {
        return 0;
    }
    set_nonblocking(fd, 1);
    ssize_t n = read(fd, buf, sizeof(buf));
    return n == (ssize_t)strlen(msg) && memcmp(buf, msg, n) == 0;
}

// Whether no end that writes to the pipe is open any more: the read end `rd` is at the end of the
// file
static int pipe_has_no_writer(int rd) {
    set_nonblocking(rd, 1);
    char c;
    return read(rd, &c, 1) == 0;
}

// ---------------------------------------------------------------------------------------------
// Descriptors (SCM_RIGHTS)
// ---------------------------------------------------------------------------------------------

// The descriptor that arrives is a new one for the file of the sender
static int test_rights_arrive(void) {
    for (int kind = 0; kind < NUM_PAIR_KINDS; kind++) {
        for (int dir = 0; dir < 2; dir++) {
            int a, b;
            CHECK(create_pair(kind, &a, &b) == 0, "failed to create a %s pair", pair_names[kind]);
            int from = dir ? b : a;
            int to = dir ? a : b;
            pipe_t p;
            CHECK(make_pipe(&p) == 0, "failed to create a pipe");
            CHECK(send_fd(from, "x", 1, p.rd) == 1, "failed to send");
            got_t got;
            recv_ctl(to, 8, CMSG_SPACE(sizeof(int)), 0, &got);
            CHECK_GOT(&got, 1, "x");
            CHECK(got.flags == 0, "%s: flags %#x", pair_names[kind], got.flags);
            CHECK(got.controllen == CMSG_SPACE(sizeof(int)), "%s: controllen %zu",
                  pair_names[kind], got.controllen);
            CHECK(got.num_cmsgs == 1 && got.num_fds == 1, "%s: %d messages with %d descriptors",
                  pair_names[kind], got.num_cmsgs, got.num_fds);
            CHECK(got.fds[0] != p.rd, "the received descriptor is the one that was sent");
            CHECK(is_read_end_of(got.fds[0], &p), "the descriptor is not the read end of the pipe");
            close(got.fds[0]);
            close_files(4, a, b, p.rd, p.wr);
        }
    }
    return 0;
}

// The receiver gets the descriptor also if the sender has closed it after the send, which is how
// programs send descriptors
static int test_rights_after_the_sender_closed_the_descriptor(void) {
    for (int kind = 0; kind < NUM_PAIR_KINDS; kind++) {
        int a, b;
        CHECK(create_pair(kind, &a, &b) == 0, "failed to create a %s pair", pair_names[kind]);
        pipe_t p;
        CHECK(make_pipe(&p) == 0, "failed to create a pipe");
        CHECK(send_fd(a, "x", 1, p.rd) == 1, "failed to send");
        close(p.rd);
        got_t got;
        recv_ctl(b, 8, CMSG_SPACE(sizeof(int)), 0, &got);
        CHECK_GOT(&got, 1, "x");
        CHECK(got.num_fds == 1, "%s: no descriptor", pair_names[kind]);
        CHECK(is_read_end_of(got.fds[0], &p), "the descriptor is not the read end of the pipe");
        // the pipe is open as long as a descriptor is: it has no reader if that is closed
        close(got.fds[0]);
        CHECK(write(p.wr, "x", 1) < 0 && errno == EPIPE, "the file is still open");
        close_files(3, a, b, p.wr);
    }
    return 0;
}

// The descriptor of a file that nobody holds any more is delivered, too: the file stays open as
// long as the message is queued
static int test_rights_of_a_file_that_is_open_only_in_the_message(void) {
    int a, b;
    CHECK(create_pair(PAIR_SOCKETPAIR, &a, &b) == 0, "failed to create a pair");
    pipe_t p;
    CHECK(make_pipe(&p) == 0, "failed to create a pipe");
    CHECK(send_fd(a, "x", 1, p.wr) == 1, "failed to send");
    close(p.wr);
    // the pipe has a writer: the descriptor in the message
    CHECK(!pipe_has_no_writer(p.rd),
          "the writer is gone, though its descriptor is in a message");
    got_t got;
    recv_ctl(b, 8, CMSG_SPACE(sizeof(int)), 0, &got);
    CHECK_GOT(&got, 1, "x");
    CHECK(got.num_fds == 1, "no descriptor");
    CHECK(write(got.fds[0], "hi", 2) == 2, "failed to write to the received descriptor");
    char buf[4] = {0};
    CHECK(read(p.rd, buf, sizeof(buf)) == 2 && memcmp(buf, "hi", 2) == 0, "wrong data");
    close(got.fds[0]);
    CHECK(pipe_has_no_writer(p.rd), "the pipe has a writer");
    close_files(3, a, b, p.rd);
    return 0;
}

typedef struct {
    struct sockaddr_un addr;
    socklen_t addr_len;
    int kind;
    int sock;           // the end that sends
    int sent;
    pipe_t pipe;
} sender_t;

// The thread connects, sends the descriptors of a pipe that it has opened itself, closes them and
// exits
static void *sender_main(void *arg) {
    sender_t *s = arg;
    if (s->kind == PAIR_CONNECT) {
        s->sock = socket(AF_UNIX, SOCK_STREAM, 0);
        if (s->sock < 0 || connect(s->sock, (struct sockaddr *)&s->addr, s->addr_len) < 0) {
            return NULL;
        }
        set_timeout(s->sock, RECV_TIMEOUT_MS);
    }
    if (make_pipe(&s->pipe) < 0) {
        return NULL;
    }
    s->sent = send_fd(s->sock, "x", 1, s->pipe.rd) == 1;
    close(s->pipe.rd);
    return NULL;
}

// The sender may be gone when the descriptor is received: the thread that has sent it has exited
static int test_rights_after_the_sender_thread_exited(void) {
    for (int kind = 0; kind < NUM_PAIR_KINDS; kind++) {
        sender_t s;
        memset(&s, 0, sizeof(s));
        s.kind = kind;
        int l = -1;
        int b = -1;
        if (kind == PAIR_CONNECT) {
            l = listener(&s.addr, &s.addr_len);
            CHECK(l >= 0, "failed to listen");
        } else {
            int sv[2];
            CHECK(socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0, "failed to create a pair");
            s.sock = sv[0];
            b = sv[1];
            set_timeout(s.sock, RECV_TIMEOUT_MS);
        }
        pthread_t thread;
        CHECK(pthread_create(&thread, NULL, sender_main, &s) == 0, "failed to create a thread");
        CHECK(pthread_join(thread, NULL) == 0, "failed to join the thread");
        CHECK(s.sent, "%s: the thread failed to send", pair_names[kind]);
        if (kind == PAIR_CONNECT) {
            b = accept(l, NULL, NULL);
            CHECK(b >= 0, "failed to accept");
            close(l);
        }
        set_timeout(b, RECV_TIMEOUT_MS);
        got_t got;
        recv_ctl(b, 8, CMSG_SPACE(sizeof(int)), 0, &got);
        CHECK_GOT(&got, 1, "x");
        CHECK(got.num_fds == 1, "%s: no descriptor", pair_names[kind]);
        CHECK(is_read_end_of(got.fds[0], &s.pipe),
              "the descriptor is not the read end of the pipe");
        close_files(4, got.fds[0], s.pipe.wr, s.sock, b);
    }
    return 0;
}

// A descriptor of a socket works at the receiver: the files are the same, not copies
static int test_rights_of_a_socket(void) {
    int a, b, c, d;
    CHECK(create_pair(PAIR_SOCKETPAIR, &a, &b) == 0, "failed to create a pair");
    CHECK(create_pair(PAIR_CONNECT, &c, &d) == 0, "failed to create a connection");
    CHECK(send_fd(a, "s", 1, c) == 1, "failed to send");
    close(c);
    got_t got;
    recv_ctl(b, 8, CMSG_SPACE(sizeof(int)), 0, &got);
    CHECK_GOT(&got, 1, "s");
    CHECK(got.num_fds == 1, "no descriptor");
    CHECK(write(d, "hello", 5) == 5, "failed to write");
    char buf[8] = {0};
    CHECK(read(got.fds[0], buf, sizeof(buf)) == 5 && memcmp(buf, "hello", 5) == 0,
          "the socket does not work");
    CHECK(write(got.fds[0], "back", 4) == 4, "failed to write to the received socket");
    CHECK(read(d, buf, sizeof(buf)) == 4 && memcmp(buf, "back", 4) == 0, "wrong data");
    close_files(4, a, b, d, got.fds[0]);
    return 0;
}

// The descriptors of other kinds of files can be sent, and the receiver gets the same open file: a
// regular file with its offset, an eventfd, a directory, a device and an epoll file
static int test_rights_of_files_of_other_kinds(void) {
    int a, b;
    CHECK(create_pair(PAIR_SOCKETPAIR, &a, &b) == 0, "failed to create a pair");
    char path[64];
    snprintf(path, sizeof(path), "/tmp/unix_socket_ancillary.%d", (int)getpid());
    int file = open(path, O_RDWR | O_CREAT | O_TRUNC, 0600);
    CHECK(file >= 0, "failed to create a file");
    unlink(path);
    int efd = eventfd(0, EFD_NONBLOCK);
    int dir = open("/", O_RDONLY | O_DIRECTORY);
    int null = open("/dev/null", O_WRONLY);
    int epfd = epoll_create1(0);
    CHECK(efd >= 0 && dir >= 0 && null >= 0 && epfd >= 0, "failed to open the files");
    int fds[5] = { file, efd, dir, null, epfd };
    CHECK(send_fds(a, "x", 1, fds, 5) == 1, "failed to send");
    got_t got;
    recv_ctl(b, 8, 128, 0, &got);
    CHECK_GOT(&got, 1, "x");
    CHECK(got.num_fds == 5, "%d descriptors", got.num_fds);

    // the offset belongs to the open file, which the descriptors share
    CHECK(write(file, "ab", 2) == 2 && write(got.fds[0], "cd", 2) == 2, "failed to write");
    char buf[8] = {0};
    CHECK(pread(file, buf, sizeof(buf), 0) == 4 && memcmp(buf, "abcd", 4) == 0,
          "the file has the wrong content");
    CHECK(lseek(file, 0, SEEK_CUR) == 4, "the offset is not shared");
    uint64_t value = 5;
    CHECK(write(efd, &value, sizeof(value)) == sizeof(value),
          "failed to write to the eventfd");
    value = 0;
    CHECK(read(got.fds[1], &value, sizeof(value)) == sizeof(value) && value == 5,
          "the eventfd does not work");
    struct stat st;
    CHECK(fstat(got.fds[2], &st) == 0 && S_ISDIR(st.st_mode), "the directory is not one");
    CHECK(write(got.fds[3], "x", 1) == 1, "failed to write to the device");
    struct epoll_event ev = { .events = EPOLLIN, .data.u32 = 7 };
    CHECK(epoll_ctl(got.fds[4], EPOLL_CTL_ADD, efd, &ev) == 0,
          "failed to use the epoll file");
    value = 1;
    CHECK(write(efd, &value, sizeof(value)) == sizeof(value),
          "failed to write to the eventfd");
    struct epoll_event out;
    CHECK(epoll_wait(epfd, &out, 1, 1000) == 1 &&
          out.data.u32 == 7, "the epoll file has no event");
    close_got_fds(&got);
    close_files(7, a, b, file, efd, dir, null, epfd);
    return 0;
}

// Several descriptors in one message, in one control message or in several
static int test_rights_several_descriptors(void) {
    int a, b;
    CHECK(create_pair(PAIR_SOCKETPAIR, &a, &b) == 0, "failed to create a pair");
    pipe_t p[3];
    int rds[3];
    for (int i = 0; i < 3; i++) {
        CHECK(make_pipe(&p[i]) == 0, "failed to create a pipe");
        rds[i] = p[i].rd;
    }
    CHECK(send_fds(a, "x", 1, rds, 3) == 1, "failed to send");
    got_t got;
    recv_ctl(b, 8, 256, 0, &got);
    CHECK_GOT(&got, 1, "x");
    CHECK(got.num_fds == 3 && got.rights_cmsgs == 1, "%d descriptors in %d messages",
          got.num_fds, got.rights_cmsgs);
    CHECK(got.controllen == CMSG_SPACE(3 * sizeof(int)), "controllen %zu", got.controllen);
    for (int i = 0; i < 3; i++) {
        CHECK(is_read_end_of(got.fds[i], &p[i]), "the descriptor %d is another file", i);
    }
    close_got_fds(&got);

    // two control messages of one descriptor each arrive as one control message
    char ctl[CMSG_SPACE(sizeof(int)) * 2];
    memset(ctl, 0, sizeof(ctl));
    struct cmsghdr *c = (struct cmsghdr *)ctl;
    for (int i = 0; i < 2; i++) {
        c->cmsg_level = SOL_SOCKET;
        c->cmsg_type = SCM_RIGHTS;
        c->cmsg_len = CMSG_LEN(sizeof(int));
        memcpy(CMSG_DATA(c), &rds[i], sizeof(int));
        c = (struct cmsghdr *)((char *)c + CMSG_SPACE(sizeof(int)));
    }
    CHECK(send_ctl(a, "y", 1, ctl, sizeof(ctl), 0) == 1, "failed to send");
    recv_ctl(b, 8, 256, 0, &got);
    CHECK_GOT(&got, 1, "y");
    CHECK(got.num_fds == 2 && got.rights_cmsgs == 1, "%d descriptors in %d messages",
          got.num_fds, got.rights_cmsgs);
    close_got_fds(&got);

    // the most that a message holds
    int many[254];
    for (int i = 0; i < 254; i++) {
        many[i] = p[0].rd;
    }
    CHECK(send_fds(a, "z", 1, many, 253) == 1, "failed to send 253 descriptors");
    recv_ctl(b, 8, CMSG_SPACE(253 * sizeof(int)), 0, &got);
    CHECK_GOT(&got, 1, "z");
    CHECK(got.num_fds == 253 && got.flags == 0, "%d descriptors, flags %#x", got.num_fds,
          got.flags);
    close_got_fds(&got);
    CHECK(send_fds(a, "z", 1, many, 254) == -EINVAL, "254 descriptors are too many");
    recv_now(b, 8, 128, 0, &got);
    CHECK(got.ret == -EAGAIN, "the failed send has sent something");
    for (int i = 0; i < 3; i++) {
        close(p[i].rd);
        close(p[i].wr);
    }
    close_files(2, a, b);
    return 0;
}

// What is not delivered is closed: no buffer for control messages, a buffer that is too small, a
// plain read. MSG_CTRUNC tells it where there is a buffer
static int test_rights_that_are_not_delivered(void) {
    int a, b;
    CHECK(create_pair(PAIR_SOCKETPAIR, &a, &b) == 0, "failed to create a pair");
    pipe_t p[3];
    int rds[3];
    for (int i = 0; i < 3; i++) {
        CHECK(make_pipe(&p[i]) == 0, "failed to create a pipe");
        rds[i] = p[i].rd;
    }
    int base = count_fds();
    got_t got;

    // 3 descriptors, room for 2: two are delivered
    CHECK(send_fds(a, "x", 1, rds, 3) == 1, "failed to send");
    recv_ctl(b, 8, CMSG_SPACE(2 * sizeof(int)), 0, &got);
    CHECK_GOT(&got, 1, "x");
    CHECK(got.num_fds == 2 &&
          (got.flags & MSG_CTRUNC), "%d descriptors, flags %#x", got.num_fds,
          got.flags);
    CHECK(count_fds() == base + 2, "%d descriptors are open, expected %d", count_fds(),
          base + 2);
    CHECK(is_read_end_of(got.fds[0], &p[0]) && is_read_end_of(got.fds[1], &p[1]),
          "the descriptors are not the first two");
    close_got_fds(&got);

    // no room for one: none is delivered
    size_t sizes[] = { 0, 12, CMSG_LEN(0), CMSG_LEN(0) + 3 };
    for (size_t i = 0; i < ARRAY_SIZE(sizes); i++) {
        CHECK(send_fds(a, "x", 1, rds, 1) == 1, "failed to send");
        recv_ctl(b, 8, sizes[i], 0, &got);
        CHECK_GOT(&got, 1, "x");
        CHECK((got.flags & MSG_CTRUNC) && got.num_fds == 0 && got.controllen == 0,
              "a buffer of %zu bytes: flags %#x, %d descriptors, controllen %zu", sizes[i],
              got.flags, got.num_fds, got.controllen);
        CHECK(count_fds() == base, "a descriptor is open that was not delivered");
    }

    // no buffer
    CHECK(send_fds(a, "x", 1, rds, 1) == 1, "failed to send");
    recv_ctl(b, 8, NO_BUFFER, 0, &got);
    CHECK_GOT(&got, 1, "x");
    CHECK(got.flags & MSG_CTRUNC, "flags %#x", got.flags);
    CHECK(count_fds() == base, "a descriptor is open that was not delivered");

    // a plain read does not deliver it, nor does it see anything else
    CHECK(send_fds(a, "xyz", 3, rds, 1) == 3, "failed to send");
    char buf[8];
    CHECK(read(b, buf, 1) == 1 && buf[0] == 'x', "the read failed");
    CHECK(count_fds() == base, "a descriptor is open that was not delivered");
    // the rest is plain data
    recv_ctl(b, 8, 128, 0, &got);
    CHECK_GOT(&got, 2, "yz");
    CHECK(got.num_cmsgs == 0 && got.flags == 0, "%d control messages, flags %#x",
          got.num_cmsgs, got.flags);
    for (int i = 0; i < 3; i++) {
        close(p[i].rd);
        close(p[i].wr);
    }
    close_files(2, a, b);
    return 0;
}

// The files of the descriptors that are not delivered are released: a pipe has no writer once the
// descriptor of its write end is gone from every place that held it
static int test_rights_that_are_not_delivered_are_released(void) {
    int a, b;
    CHECK(create_pair(PAIR_SOCKETPAIR, &a, &b) == 0, "failed to create a pair");
    // 0: a plain read, 1: no buffer, 2: no room in the buffer, 3: a peek and then a read
    for (int how = 0; how < 4; how++) {
        pipe_t p;
        CHECK(make_pipe(&p) == 0, "failed to create a pipe");
        CHECK(send_fd(a, "x", 1, p.wr) == 1, "failed to send");
        close(p.wr);
        CHECK(!pipe_has_no_writer(p.rd), "case %d: the writer is gone", how);
        got_t got;
        char buf[4];
        switch (how) {
            case 0:
                CHECK(read(b, buf, sizeof(buf)) == 1, "the read failed");
                break;
            case 1:
                recv_ctl(b, 4, NO_BUFFER, 0, &got);
                break;
            case 2:
                recv_ctl(b, 4, CMSG_LEN(0), 0, &got);
                break;
            default:
                recv_ctl(b, 4, 128, MSG_PEEK, &got);
                CHECK(got.num_fds == 1, "the peek has no descriptor");
                CHECK(!pipe_has_no_writer(p.rd), "case %d: the peek took the descriptor", how);
                close(got.fds[0]);
                CHECK(!pipe_has_no_writer(p.rd), "case %d: the descriptor is gone", how);
                CHECK(read(b, buf, sizeof(buf)) == 1, "the read failed");
                break;
        }
        CHECK(pipe_has_no_writer(p.rd), "case %d: the write end is still open", how);
        close(p.rd);
    }
    close_files(2, a, b);
    return 0;
}

// The receiver that is closed while a message is queued releases its files, and so does a peer
// that is shut down
static int test_rights_in_the_queue_of_a_closed_socket_are_released(void) {
    for (int kind = 0; kind < NUM_PAIR_KINDS; kind++) {
        for (int how = 0; how < 3; how++) {
            int a, b;
            CHECK(create_pair(kind, &a, &b) == 0, "failed to create a %s pair", pair_names[kind]);
            pipe_t p;
            CHECK(make_pipe(&p) == 0, "failed to create a pipe");
            CHECK(send_fd(a, "x", 1, p.wr) == 1, "failed to send");
            CHECK(send_fd(a, "y", 1, p.wr) == 1, "failed to send");
            close(p.wr);
            CHECK(!pipe_has_no_writer(p.rd), "the writer is gone");
            if (how == 0) {
                // the receiver is closed, the sender stays
                close(b);
                CHECK(pipe_has_no_writer(p.rd), "%s: the receiver keeps the descriptors",
                      pair_names[kind]);
                close(a);
            } else if (how == 1) {
                // the sender is closed, the receiver keeps what it has been sent
                close(a);
                CHECK(!pipe_has_no_writer(p.rd), "%s: the closed sender has taken the files back",
                      pair_names[kind]);
                close(b);
                CHECK(pipe_has_no_writer(p.rd), "%s: the receiver keeps the descriptors",
                      pair_names[kind]);
            } else {
                // both are closed
                close(a);
                close(b);
                CHECK(pipe_has_no_writer(p.rd), "%s: the descriptors are lost",
                      pair_names[kind]);
            }
            close(p.rd);
        }
    }
    return 0;
}

// The receiver gets what the sender sent before it closed or shut down its socket
static int test_rights_after_close_and_shutdown_of_the_sender(void) {
    for (int how = 0; how < 3; how++) {
        int a, b;
        CHECK(create_pair(PAIR_CONNECT, &a, &b) == 0, "failed to create a connection");
        pipe_t p;
        CHECK(make_pipe(&p) == 0, "failed to create a pipe");
        CHECK(send_fd(a, "xy", 2, p.rd) == 2, "failed to send");
        if (how == 0) {
            close(a);
        } else if (how == 1) {
            shutdown(a, SHUT_WR);
        } else {
            // the receiver shuts down its side for reading, which leaves what is queued
            shutdown(b, SHUT_RD);
        }
        got_t got;
        recv_ctl(b, 8, 128, 0, &got);
        CHECK_GOT(&got, 2, "xy");
        CHECK(got.num_fds == 1, "case %d: %d descriptors", how, got.num_fds);
        CHECK(is_read_end_of(got.fds[0], &p), "the descriptor is not the read end of the pipe");
        close_files(3, got.fds[0], p.rd, p.wr);
        if (how != 0) {
            close(a);
        }
        close(b);
    }
    return 0;
}

// A send to a socket whose peer is closed fails, and the descriptors stay with the sender
static int test_rights_to_a_closed_peer_are_not_sent(void) {
    int a, b;
    CHECK(create_pair(PAIR_SOCKETPAIR, &a, &b) == 0, "failed to create a pair");
    pipe_t p;
    CHECK(make_pipe(&p) == 0, "failed to create a pipe");
    close(b);
    CHECK(send_fd(a, "x", 1, p.wr) == -EPIPE, "the send did not fail with EPIPE");
    close(a);
    CHECK(write(p.wr, "x", 1) == 1, "the descriptor of the sender was closed");
    close_files(2, p.rd, p.wr);
    return 0;
}

// A message without bytes sends nothing, and drops its control messages, as in Linux
static int test_rights_without_bytes(void) {
    int a, b;
    CHECK(create_pair(PAIR_SOCKETPAIR, &a, &b) == 0, "failed to create a pair");
    pipe_t p;
    CHECK(make_pipe(&p) == 0, "failed to create a pipe");
    CHECK(send_fd(a, "", 0, p.wr) == 0, "the send of nothing failed");
    got_t got;
    recv_now(b, 8, 128, 0, &got);
    CHECK(got.ret == -EAGAIN, "the receiver has got something: %ld", got.ret);
    // and the next message has its descriptor, none from the first
    CHECK(send_fd(a, "x", 1, p.rd) == 1, "failed to send");
    recv_now(b, 8, 128, 0, &got);
    CHECK_GOT(&got, 1, "x");
    CHECK(got.num_fds == 1 &&
          is_read_end_of(got.fds[0], &p), "the descriptor is not that of the read end");
    close_files(5, got.fds[0], a, b, p.rd, p.wr);
    return 0;
}

// A socket that is sent to its own receive queue is only referenced from there. Linux finds such
// sockets, and Occlum keeps them until the process ends, but nothing may hang or crash
static int test_socket_that_is_sent_to_itself(void) {
    int a, b;
    CHECK(create_pair(PAIR_SOCKETPAIR, &a, &b) == 0, "failed to create a pair");
    CHECK(send_fd(a, "s", 1, b) == 1, "failed to send");
    close(a);
    close(b);
    CHECK(create_pair(PAIR_SOCKETPAIR, &a, &b) == 0, "failed to create a pair");
    CHECK(send_fd(a, "s", 1, b) == 1, "failed to send");
    got_t got;
    recv_ctl(b, 8, 128, 0, &got);
    CHECK_GOT(&got, 1, "s");
    CHECK(got.num_fds == 1, "no descriptor");
    // the received descriptor is the receiver itself: a message through it arrives
    CHECK(write(a, "t", 1) == 1, "failed to write");
    char c;
    CHECK(read(got.fds[0], &c, 1) == 1 && c == 't', "the descriptor is not the receiver");
    close_files(3, a, b, got.fds[0]);
    return 0;
}

// ---------------------------------------------------------------------------------------------
// The bytes and the control messages are separate
// ---------------------------------------------------------------------------------------------

// A read of a message with a descriptor gets the bytes, and not the control message as bytes
static int test_plain_read_gets_only_the_bytes(void) {
    for (int kind = 0; kind < NUM_PAIR_KINDS; kind++) {
        int a, b;
        CHECK(create_pair(kind, &a, &b) == 0, "failed to create a %s pair", pair_names[kind]);
        pipe_t p;
        CHECK(make_pipe(&p) == 0, "failed to create a pipe");
        CHECK(send_fd(a, "x", 1, p.rd) == 1, "failed to send");
        char buf[64];
        CHECK(read(b, buf, sizeof(buf)) == 1 && buf[0] == 'x', "the read did not get 1 byte");
        set_nonblocking(b, 1);
        CHECK(read(b, buf, sizeof(buf)) < 0 && errno == EAGAIN, "there are more bytes");
        close_files(4, a, b, p.rd, p.wr);
    }
    return 0;
}

// A receive with a buffer for control messages, from a sender that has sent none, gets the bytes
// that were sent, and no others, and it does not wait for control messages
static int test_receive_with_a_buffer_but_without_control_messages(void) {
    int a, b;
    CHECK(create_pair(PAIR_SOCKETPAIR, &a, &b) == 0, "failed to create a pair");
    got_t got;
    // blocking socket: the bytes are there, the call must not wait
    CHECK(write(a, "hello", 5) == 5, "failed to write");
    long start = now_ms();
    recv_ctl(b, 5, 64, 0, &got);
    CHECK_GOT(&got, 5, "hello");
    CHECK(got.controllen == 0 && got.num_cmsgs == 0 && got.flags == 0,
          "controllen %zu, flags %#x", got.controllen, got.flags);
    CHECK(now_ms() - start < RECV_TIMEOUT_MS / 2, "the call has waited");
    // nonblocking socket: the bytes are not lost
    set_nonblocking(b, 1);
    CHECK(write(a, "hello", 5) == 5, "failed to write");
    recv_ctl(b, 5, 64, 0, &got);
    CHECK_GOT(&got, 5, "hello");
    recv_ctl(b, 5, 64, 0, &got);
    CHECK(got.ret == -EAGAIN, "the call returned %ld", got.ret);
    // the bytes that follow are not taken for control messages
    CHECK(write(a, "helloworld!", 11) == 11, "failed to write");
    recv_ctl(b, 5, 64, 0, &got);
    CHECK_GOT(&got, 5, "hello");
    CHECK(got.controllen == 0, "controllen %zu", got.controllen);
    recv_ctl(b, 64, 64, 0, &got);
    CHECK_GOT(&got, 6, "world!");
    close_files(2, a, b);
    return 0;
}

// The descriptors come with the first byte of their message, and a receive stops at its end
static int test_boundaries_of_messages_with_descriptors(void) {
    int a, b;
    CHECK(create_pair(PAIR_SOCKETPAIR, &a, &b) == 0, "failed to create a pair");
    pipe_t p[2];
    CHECK(make_pipe(&p[0]) == 0 && make_pipe(&p[1]) == 0, "failed to create pipes");
    got_t got;

    // bytes before the message are received together with it, bytes after it are not
    CHECK(write(a, "ab", 2) == 2, "failed to write");
    CHECK(send_fd(a, "c", 1, p[0].rd) == 1, "failed to send");
    CHECK(write(a, "de", 2) == 2, "failed to write");
    recv_ctl(b, 16, 128, 0, &got);
    CHECK_GOT(&got, 3, "abc");
    CHECK(got.num_fds == 1 && is_read_end_of(got.fds[0], &p[0]), "the descriptor is wrong");
    close(got.fds[0]);
    recv_ctl(b, 16, 128, 0, &got);
    CHECK_GOT(&got, 2, "de");
    CHECK(got.num_cmsgs == 0, "the bytes after the message have control messages");

    // two messages with descriptors are received one by one, also by a read that is long
    CHECK(send_fd(a, "1", 1, p[0].rd) == 1, "failed to send");
    CHECK(send_fd(a, "2", 1, p[1].rd) == 1, "failed to send");
    recv_ctl(b, 16, 128, 0, &got);
    CHECK_GOT(&got, 1, "1");
    CHECK(got.num_fds == 1 && is_read_end_of(got.fds[0], &p[0]), "the descriptor is wrong");
    close(got.fds[0]);
    recv_ctl(b, 16, 128, 0, &got);
    CHECK_GOT(&got, 1, "2");
    CHECK(got.num_fds == 1 && is_read_end_of(got.fds[0], &p[1]), "the descriptor is wrong");
    close(got.fds[0]);

    // MSG_WAITALL does not go past the message either, and does not wait for more bytes after it,
    // although the bytes that it has are fewer than it wants
    CHECK(send_fd(a, "ab", 2, p[0].rd) == 2, "failed to send");
    CHECK(write(a, "cd", 2) == 2, "failed to write");
    long start = now_ms();
    recv_ctl(b, 4, 128, MSG_WAITALL, &got);
    CHECK_GOT(&got, 2, "ab");
    CHECK(got.num_fds == 1, "no descriptor");
    CHECK(now_ms() - start < RECV_TIMEOUT_MS / 2, "the receive has waited for more bytes");
    close(got.fds[0]);
    recv_ctl(b, 2, 128, MSG_WAITALL, &got);
    CHECK_GOT(&got, 2, "cd");

    // a message that is received in parts: the descriptor comes with the first part
    CHECK(send_fd(a, "xyz", 3, p[0].rd) == 3, "failed to send");
    for (int i = 0; i < 3; i++) {
        recv_ctl(b, 1, 128, 0, &got);
        CHECK(got.ret == 1 && got.data[0] == "xyz"[i], "the byte %d is wrong", i);
        CHECK(got.num_fds == (i == 0), "the byte %d comes with %d descriptors", i, got.num_fds);
        close_got_fds(&got);
    }

    // a read, then bytes with a descriptor: the descriptors of the read are gone
    CHECK(send_fd(a, "uvw", 3, p[0].rd) == 3, "failed to send");
    CHECK(write(a, "!", 1) == 1, "failed to write");
    char buf[8];
    CHECK(read(b, buf, 2) == 2, "the read failed");
    recv_ctl(b, 16, 128, 0, &got);
    CHECK_GOT(&got, 2, "w!");
    CHECK(got.num_cmsgs == 0, "the rest of the message has control messages");
    close_files(6, a, b, p[0].rd, p[0].wr, p[1].rd, p[1].wr);
    return 0;
}

// A receive into several buffers goes up to the end of the message with the descriptors, and the
// bytes are split over the buffers
static int test_receive_into_several_buffers(void) {
    int a, b;
    CHECK(create_pair(PAIR_SOCKETPAIR, &a, &b) == 0, "failed to create a pair");
    pipe_t p;
    CHECK(make_pipe(&p) == 0, "failed to create a pipe");
    for (int variant = 0; variant < 2; variant++) {
        // the message is "abcde" with a descriptor, and 2 bytes follow, or the message is "abc",
        // which the receive does not go past into the bytes that follow
        CHECK(send_fd(a, variant ? "abc" : "abcde", variant ? 3 : 5, p.rd) == (variant ? 3 : 5),
              "failed to send");
        CHECK(write(a, "fg", 2) == 2, "failed to write");
        char b1[2] = {0}, b2[2] = {0}, b3[8] = {0};
        struct iovec iov[3] = { { b1, sizeof(b1) }, { b2, sizeof(b2) }, { b3, sizeof(b3) } };
        char ctl[64];
        struct msghdr msg;
        memset(&msg, 0, sizeof(msg));
        msg.msg_iov = iov;
        msg.msg_iovlen = 3;
        msg.msg_control = ctl;
        msg.msg_controllen = sizeof(ctl);
        long ret = recvmsg(b, &msg, MSG_DONTWAIT);
        CHECK(ret == (variant ? 3 : 5), "variant %d: %ld bytes", variant, ret);
        CHECK(memcmp(b1, "ab", 2) == 0 && memcmp(b2, variant ? "c\0" : "cd", 2) == 0 &&
              (variant || b3[0] == 'e'), "variant %d: the bytes are not in the buffers", variant);
        struct cmsghdr *c = CMSG_FIRSTHDR(&msg);
        CHECK(c != NULL && c->cmsg_type == SCM_RIGHTS, "variant %d: no descriptor", variant);
        int fd;
        memcpy(&fd, CMSG_DATA(c), sizeof(fd));
        close(fd);
        got_t got;
        recv_now(b, 8, 128, 0, &got);
        CHECK_GOT(&got, 2, "fg");
        CHECK(got.num_cmsgs == 0, "variant %d: the bytes after the message have a descriptor",
              variant);
        // what the send of the variant has left: nothing
        recv_now(b, 8, 128, 0, &got);
        CHECK(got.ret == -EAGAIN, "variant %d: %ld bytes are left", variant, got.ret);
    }
    close_files(4, a, b, p.rd, p.wr);
    return 0;
}

// A peek shows the descriptors every time, and takes nothing
static int test_peek_shows_the_descriptors(void) {
    int a, b;
    CHECK(create_pair(PAIR_SOCKETPAIR, &a, &b) == 0, "failed to create a pair");
    pipe_t p;
    CHECK(make_pipe(&p) == 0, "failed to create a pipe");
    CHECK(send_fd(a, "xy", 2, p.rd) == 2, "failed to send");
    CHECK(write(a, "z", 1) == 1, "failed to write");
    got_t got;
    int base = count_fds();
    for (int i = 0; i < 2; i++) {
        recv_ctl(b, 16, 128, MSG_PEEK, &got);
        CHECK_GOT(&got, 2, "xy");
        CHECK(got.num_fds == 1 && is_read_end_of(got.fds[0], &p), "peek %d: no descriptor", i);
        CHECK(count_fds() == base + 1, "the peek has opened %d descriptors", count_fds() - base);
        close(got.fds[0]);
    }
    // a peek without a buffer does not take the descriptors either
    recv_ctl(b, 16, NO_BUFFER, MSG_PEEK, &got);
    CHECK_GOT(&got, 2, "xy");
    CHECK(got.flags & MSG_CTRUNC, "flags %#x", got.flags);
    CHECK(count_fds() == base, "a descriptor is open");
    // a peek that is smaller than the message, and then the read
    recv_ctl(b, 1, 128, MSG_PEEK, &got);
    CHECK_GOT(&got, 1, "x");
    CHECK(got.num_fds == 1, "the short peek has no descriptor");
    close_got_fds(&got);
    recv_ctl(b, 16, 128, 0, &got);
    CHECK_GOT(&got, 2, "xy");
    CHECK(got.num_fds == 1 && is_read_end_of(got.fds[0], &p), "the read has no descriptor");
    close(got.fds[0]);
    // the peek of the next bytes
    recv_ctl(b, 16, 128, MSG_PEEK, &got);
    CHECK_GOT(&got, 1, "z");
    CHECK(got.num_cmsgs == 0, "control messages after the descriptors are gone");
    recv_ctl(b, 16, 128, 0, &got);
    CHECK_GOT(&got, 1, "z");
    close_files(4, a, b, p.rd, p.wr);
    return 0;
}

// A peek looks at the messages that follow the bytes that it takes, like Linux, until it finds the
// descriptors of a message: it shows them although it takes fewer bytes than there are before
// that message
static int test_peek_looks_ahead_for_descriptors(void) {
    int a, b;
    CHECK(create_pair(PAIR_SOCKETPAIR, &a, &b) == 0, "failed to create a pair");
    pipe_t p[2];
    CHECK(make_pipe(&p[0]) == 0 && make_pipe(&p[1]) == 0, "failed to create pipes");
    got_t got;
    CHECK(write(a, "ab", 2) == 2, "failed to write");
    CHECK(send_fd(a, "c", 1, p[0].rd) == 1, "failed to send");
    CHECK(write(a, "d", 1) == 1, "failed to write");
    CHECK(send_fd(a, "e", 1, p[1].rd) == 1, "failed to send");
    // one byte, two bytes: the first message with descriptors is not reached
    for (int n = 1; n <= 2; n++) {
        recv_ctl(b, n, 128, MSG_PEEK, &got);
        CHECK(got.ret == n && memcmp(got.data, "ab", n) == 0, "peek of %d bytes", n);
        CHECK(got.num_fds == 1 && is_read_end_of(got.fds[0], &p[0]),
              "the peek of %d bytes has not shown the descriptor of the first message", n);
        close_got_fds(&got);
    }
    // three bytes and more: it is, and the peek ends there
    for (int n = 3; n <= 5; n++) {
        recv_ctl(b, n, 128, MSG_PEEK, &got);
        CHECK_GOT(&got, 3, "abc");
        CHECK(got.num_fds == 1 && is_read_end_of(got.fds[0], &p[0]), "the peek of %d bytes", n);
        close_got_fds(&got);
    }
    // a peek of one byte that has no room for the descriptor says so
    recv_ctl(b, 1, CMSG_LEN(0), MSG_PEEK, &got);
    CHECK(got.ret == 1 && (got.flags & MSG_CTRUNC) &&
          got.num_fds == 0, "flags %#x", got.flags);
    // a read of one byte does not take the descriptors, a read that reaches them does, and the
    // peek of the next bytes shows those of the next message
    CHECK(read(b, got.data, 1) == 1, "the read failed");
    recv_ctl(b, 2, 128, 0, &got);
    CHECK_GOT(&got, 2, "bc");
    CHECK(got.num_fds == 1 &&
          is_read_end_of(got.fds[0], &p[0]), "the read has no descriptor");
    close_got_fds(&got);
    recv_ctl(b, 1, 128, MSG_PEEK, &got);
    CHECK_GOT(&got, 1, "d");
    CHECK(got.num_fds == 1 &&
          is_read_end_of(got.fds[0], &p[1]), "the next message has no descriptor");
    close_got_fds(&got);
    close_files(6, a, b, p[0].rd, p[0].wr, p[1].rd, p[1].wr);
    return 0;
}

// recvmsg changes the length of the control messages, but not the pointer to their buffer, so that
// the program may receive again with the same msghdr
static int test_receive_again_with_the_same_message(void) {
    int a, b;
    CHECK(create_pair(PAIR_SOCKETPAIR, &a, &b) == 0, "failed to create a pair");
    char data[8];
    char ctl[CMSG_SPACE(sizeof(int))];
    struct iovec iov = { data, sizeof(data) };
    struct msghdr msg;
    memset(&msg, 0, sizeof(msg));
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_control = ctl;
    for (int i = 0; i < 3; i++) {
        CHECK(write(a, "x", 1) == 1, "failed to write");
        msg.msg_controllen = sizeof(ctl);
        msg.msg_flags = 0;
        long ret = recvmsg(b, &msg, 0);
        CHECK(ret == 1, "receive %d returned %ld, errno %d", i, ret, errno);
        CHECK(msg.msg_controllen == 0 && msg.msg_control == ctl,
              "controllen %zu, control %p, expected %p", (size_t)msg.msg_controllen,
              msg.msg_control, (void *)ctl);
    }
    close_files(2, a, b);
    return 0;
}

static int test_cloexec_of_received_descriptors(void) {
    int a, b;
    CHECK(create_pair(PAIR_SOCKETPAIR, &a, &b) == 0, "failed to create a pair");
    pipe_t p;
    CHECK(make_pipe(&p) == 0, "failed to create a pipe");
    for (int cloexec = 0; cloexec < 2; cloexec++) {
        got_t got;
        CHECK(send_fd(a, "x", 1, p.rd) == 1, "failed to send");
        recv_ctl(b, 8, 128, cloexec ? MSG_CMSG_CLOEXEC : 0, &got);
        CHECK_GOT(&got, 1, "x");
        CHECK(got.num_fds == 1, "no descriptor");
        int flags = fcntl(got.fds[0], F_GETFD);
        CHECK(((flags & FD_CLOEXEC) != 0) == cloexec, "the close-on-exec flag is %d, expected %d",
              (flags & FD_CLOEXEC) != 0, cloexec);
        close(got.fds[0]);
    }
    close_files(4, a, b, p.rd, p.wr);
    return 0;
}

// ---------------------------------------------------------------------------------------------
// Invalid control messages
// ---------------------------------------------------------------------------------------------

// The send fails, and sends nothing
static int test_invalid_control_messages(void) {
    int a, b;
    CHECK(create_pair(PAIR_SOCKETPAIR, &a, &b) == 0, "failed to create a pair");
    pipe_t p;
    CHECK(make_pipe(&p) == 0, "failed to create a pipe");
    got_t got;
    union {
        char buf[CMSG_SPACE(sizeof(int)) * 2];
        struct cmsghdr align;
    } ctl;
    struct cmsghdr *c = (struct cmsghdr *)ctl.buf;

#define SEND_RAW(level, type, len, ctl_len) ({ \
        memset(&ctl, 0, sizeof(ctl)); \
        c->cmsg_level = (level); \
        c->cmsg_type = (type); \
        c->cmsg_len = (len); \
        memcpy(CMSG_DATA(c), &p.rd, sizeof(int)); \
        send_ctl(a, "b", 1, ctl.buf, (ctl_len), 0); \
    })
    CHECK(SEND_RAW(SOL_SOCKET, 9999, CMSG_LEN(4), CMSG_SPACE(4)) == -EINVAL,
          "an unknown type");
    CHECK(SEND_RAW(SOL_SOCKET, SCM_RIGHTS, 8, CMSG_SPACE(4)) == -EINVAL,
          "a length below the header");
    CHECK(SEND_RAW(SOL_SOCKET, SCM_RIGHTS, CMSG_LEN(4) + 100, CMSG_SPACE(4)) == -EINVAL,
          "a length beyond the control messages");
    CHECK(SEND_RAW(SOL_SOCKET, SCM_RIGHTS, CMSG_LEN(4), CMSG_LEN(4) - 1) == -EINVAL,
          "a length beyond the buffer");
    CHECK(SEND_RAW(SOL_SOCKET, SCM_CREDENTIALS, CMSG_LEN(8), CMSG_SPACE(8)) == -EINVAL,
          "credentials that are short");
    CHECK(SEND_RAW(SOL_SOCKET, SCM_CREDENTIALS, CMSG_LEN(20), CMSG_SPACE(20)) == -EINVAL,
          "credentials that are long");
    recv_now(b, 8, 128, 0, &got);
    CHECK(got.ret == -EAGAIN, "a failed send has sent %ld bytes", got.ret);

    // a descriptor that is not open, the second of two, and a second message that is invalid
    int bad = 999;
    CHECK(send_fd(a, "b", 1, bad) == -EBADF, "a descriptor that is not open");
    int fds[2] = { p.rd, bad };
    CHECK(send_fds(a, "b", 1, fds, 2) == -EBADF, "a descriptor that is not open, the second");
    int closed = dup(p.rd);
    close(closed);
    CHECK(send_fd(a, "b", 1, closed) == -EBADF, "a descriptor that was closed");
    CHECK(send_fd(a, "b", 1, -1) == -EBADF, "a descriptor that is negative");
    memset(&ctl, 0, sizeof(ctl));
    c->cmsg_level = SOL_SOCKET;
    c->cmsg_type = SCM_RIGHTS;
    c->cmsg_len = CMSG_LEN(sizeof(int));
    memcpy(CMSG_DATA(c), &p.rd, sizeof(int));
    c = (struct cmsghdr *)(ctl.buf + CMSG_SPACE(sizeof(int)));
    c->cmsg_level = SOL_SOCKET;
    c->cmsg_type = 9999;
    c->cmsg_len = CMSG_LEN(0);
    CHECK(send_ctl(a, "b", 1, ctl.buf, sizeof(ctl), 0) == -EINVAL,
          "a valid message and an invalid one");
    recv_now(b, 8, 128, 0, &got);
    CHECK(got.ret == -EAGAIN, "a failed send has sent %ld bytes", got.ret);

    // what a send ignores: another level, a buffer shorter than a header
    c = (struct cmsghdr *)ctl.buf;
    CHECK(SEND_RAW(12345, 1, CMSG_LEN(4), CMSG_SPACE(4)) == 1, "a message of another level");
    CHECK(SEND_RAW(SOL_SOCKET, 9999, CMSG_LEN(4), 3) == 1, "a buffer shorter than a header");
    CHECK(SEND_RAW(SOL_SOCKET, SCM_RIGHTS, CMSG_LEN(0), CMSG_SPACE(0)) == 1, "no descriptor");
    // a pointer to control messages and no bytes for them: a buffer of no bytes is no buffer
    struct iovec iov = { "b", 1 };
    struct msghdr no_ctl;
    memset(&no_ctl, 0, sizeof(no_ctl));
    no_ctl.msg_iov = &iov;
    no_ctl.msg_iovlen = 1;
    no_ctl.msg_control = ctl.buf;
    CHECK(syscall(SYS_sendmsg, a, &no_ctl, MSG_NOSIGNAL) == 1, "a buffer of no bytes");
    // the four bytes come together, as no message has a descriptor
    recv_ctl(b, 8, 128, 0, &got);
    CHECK_GOT(&got, 4, "bbbb");
    CHECK(got.num_cmsgs == 0, "the bytes have control messages");
    close_files(4, a, b, p.rd, p.wr);
    return 0;
}

// ---------------------------------------------------------------------------------------------
// Credentials
// ---------------------------------------------------------------------------------------------

static int check_own_cred(const got_t *got, const char *what) {
    CHECK(got->has_cred, "%s: no credentials", what);
    CHECK(got->cred.pid == getpid() && got->cred.uid == getuid() && got->cred.gid == getgid(),
          "%s: the credentials are pid %d, uid %u, gid %u", what, got->cred.pid, got->cred.uid,
          got->cred.gid);
    CHECK(got->cred_cmsg_len == CMSG_LEN(sizeof(struct ucred)),
          "%s: the message is %zu bytes",
          what, got->cred_cmsg_len);
    return 0;
}

// The credentials of a sender that has not named them, which the receiver asks for
static int test_passcred(void) {
    for (int kind = 0; kind < NUM_PAIR_KINDS; kind++) {
        int a, b;
        CHECK(create_pair(kind, &a, &b) == 0, "failed to create a %s pair", pair_names[kind]);
        CHECK(get_int_opt(b, SO_PASSCRED) == 0, "SO_PASSCRED is set by default");
        CHECK(set_int_opt(b, SO_PASSCRED, 1) == 0, "failed to set SO_PASSCRED");
        CHECK(get_int_opt(b, SO_PASSCRED) == 1, "SO_PASSCRED is not set");
        got_t got;
        // without credentials of the sender, in two sends that the receiver gets together
        CHECK(write(a, "p", 1) == 1 && write(a, "q", 1) == 1, "failed to write");
        recv_ctl(b, 8, 128, 0, &got);
        CHECK_GOT(&got, 2, "pq");
        CHECK(got.controllen == CMSG_SPACE(sizeof(struct ucred)) && got.num_cmsgs == 1 &&
              got.flags == 0, "%s: controllen %zu, %d messages, flags %#x", pair_names[kind],
              got.controllen, got.num_cmsgs, got.flags);
        CHECK(check_own_cred(&got, pair_names[kind]) == 0, "wrong credentials");
        // the other direction, which has not asked for them, gets none
        CHECK(write(b, "r", 1) == 1, "failed to write");
        recv_ctl(a, 8, 128, 0, &got);
        CHECK_GOT(&got, 1, "r");
        CHECK(got.num_cmsgs == 0 && got.controllen == 0, "%s: the peer got control messages",
              pair_names[kind]);
        // turned off again
        CHECK(set_int_opt(b, SO_PASSCRED, 0) == 0, "failed to reset SO_PASSCRED");
        CHECK(write(a, "s", 1) == 1, "failed to write");
        recv_ctl(b, 8, 128, 0, &got);
        CHECK_GOT(&got, 1, "s");
        CHECK(got.num_cmsgs == 0, "%s: credentials without SO_PASSCRED", pair_names[kind]);
        close_files(2, a, b);
    }
    return 0;
}

// The credentials come once for each receive, and with each peek
static int test_passcred_with_peek_and_plain_reads(void) {
    int a, b;
    CHECK(create_pair(PAIR_SOCKETPAIR, &a, &b) == 0, "failed to create a pair");
    CHECK(set_int_opt(b, SO_PASSCRED, 1) == 0, "failed to set SO_PASSCRED");
    CHECK(write(a, "abc", 3) == 3, "failed to write");
    got_t got;
    for (int i = 0; i < 2; i++) {
        recv_ctl(b, 8, 128, MSG_PEEK, &got);
        CHECK_GOT(&got, 3, "abc");
        CHECK(check_own_cred(&got, "peek") == 0, "wrong credentials");
    }
    recv_ctl(b, 1, 128, 0, &got);
    CHECK_GOT(&got, 1, "a");
    CHECK(check_own_cred(&got, "the first byte") == 0, "wrong credentials");
    recv_ctl(b, 8, 128, 0, &got);
    CHECK_GOT(&got, 2, "bc");
    CHECK(check_own_cred(&got, "the rest") == 0, "wrong credentials");
    // a plain read gets nothing but the bytes
    CHECK(write(a, "d", 1) == 1, "failed to write");
    char c;
    CHECK(read(b, &c, 1) == 1 && c == 'd', "the read failed");
    close_files(2, a, b);
    return 0;
}

// The receiver that has not asked for the credentials when the bytes were sent gets the credentials
// of nobody: pid 0, and the user and group that Linux uses for the unknown
static int test_passcred_after_the_send_shows_nobody(void) {
    int a, b;
    CHECK(create_pair(PAIR_SOCKETPAIR, &a, &b) == 0, "failed to create a pair");
    CHECK(write(a, "p", 1) == 1, "failed to write");
    CHECK(set_int_opt(b, SO_PASSCRED, 1) == 0, "failed to set SO_PASSCRED");
    got_t got;
    recv_ctl(b, 8, 128, 0, &got);
    CHECK_GOT(&got, 1, "p");
    CHECK(got.has_cred, "no credentials");
    CHECK(got.cred.pid == 0 && got.cred.uid == 65534 && got.cred.gid == 65534,
          "the credentials are pid %d, uid %u, gid %u", got.cred.pid, got.cred.uid, got.cred.gid);
    close_files(2, a, b);
    return 0;
}

// The credentials that the sender asked to send, SO_PASSCRED of the sender, are queued with the
// bytes, and the receiver gets them when it asks later
static int test_passcred_of_the_sender(void) {
    int a, b;
    CHECK(create_pair(PAIR_SOCKETPAIR, &a, &b) == 0, "failed to create a pair");
    CHECK(set_int_opt(a, SO_PASSCRED, 1) == 0, "failed to set SO_PASSCRED");
    CHECK(write(a, "p", 1) == 1, "failed to write");
    got_t got;
    recv_ctl(b, 8, 128, MSG_PEEK, &got);
    CHECK_GOT(&got, 1, "p");
    CHECK(got.num_cmsgs == 0, "the receiver has not asked for credentials");
    CHECK(set_int_opt(b, SO_PASSCRED, 1) == 0, "failed to set SO_PASSCRED");
    recv_ctl(b, 8, 128, 0, &got);
    CHECK_GOT(&got, 1, "p");
    CHECK(check_own_cred(&got, "the credentials of the sender") == 0, "wrong credentials");
    close_files(2, a, b);
    return 0;
}

// Credentials that the sender names are those of the sender, and are queued, whatever the receiver
// has asked for
static int test_scm_credentials(void) {
    for (int kind = 0; kind < NUM_PAIR_KINDS; kind++) {
        int a, b;
        CHECK(create_pair(kind, &a, &b) == 0, "failed to create a %s pair", pair_names[kind]);
        got_t got;
        CHECK(send_own_cred(a, "q", 1) == 1, "failed to send");
        recv_ctl(b, 8, 128, 0, &got);
        CHECK_GOT(&got, 1, "q");
        CHECK(got.num_cmsgs == 0, "%s: credentials without SO_PASSCRED", pair_names[kind]);
        CHECK(send_own_cred(a, "q", 1) == 1, "failed to send");
        CHECK(set_int_opt(b, SO_PASSCRED, 1) == 0, "failed to set SO_PASSCRED");
        recv_ctl(b, 8, 128, 0, &got);
        CHECK_GOT(&got, 1, "q");
        CHECK(check_own_cred(&got, pair_names[kind]) == 0, "wrong credentials");
        close_files(2, a, b);
    }
    return 0;
}

// A process names its own credentials only. Linux lets a privileged process name other ones, and
// Occlum does not: all its processes run as root, but none may be somebody else
static int test_scm_credentials_of_others(void) {
    int a, b;
    CHECK(create_pair(PAIR_SOCKETPAIR, &a, &b) == 0, "failed to create a pair");
    // the user and the group must be valid, also for a privileged process
    CHECK(send_cred(a, "q", 1, getpid(), -1, getgid()) == -EINVAL, "an invalid user");
    CHECK(send_cred(a, "q", 1, getpid(), getuid(), -1) == -EINVAL, "an invalid group");
    if (in_occlum() || geteuid() != 0) {
        CHECK(send_cred(a, "q", 1, getpid() + 1, getuid(), getgid()) == -EPERM, "another pid");
        CHECK(send_cred(a, "q", 1, 0, getuid(), getgid()) == -EPERM, "pid 0");
        CHECK(send_cred(a, "q", 1, getppid(), getuid(), getgid()) == -EPERM, "the parent");
        CHECK(send_cred(a, "q", 1, getpid(), getuid() + 1, getgid()) == -EPERM, "another user");
        CHECK(send_cred(a, "q", 1, getpid(), getuid(), getgid() + 1) == -EPERM, "another group");
        got_t got;
        recv_now(b, 8, 128, 0, &got);
        CHECK(got.ret == -EAGAIN, "a failed send has sent %ld bytes", got.ret);
    } else {
        printf("\t\tnote: privileged, which may name other credentials: skipped\n");
    }
    close_files(2, a, b);
    return 0;
}

// What a receive does if the buffer is too small for the credentials, like Linux
static int test_passcred_with_a_small_buffer(void) {
    int a, b;
    CHECK(create_pair(PAIR_SOCKETPAIR, &a, &b) == 0, "failed to create a pair");
    CHECK(set_int_opt(b, SO_PASSCRED, 1) == 0, "failed to set SO_PASSCRED");
    got_t got;
    struct {
        size_t size;
        size_t controllen;
        int cmsgs;
        int ctrunc;
    } cases[] = {
        { NO_BUFFER, 0, 0, 1 }, { 0, 0, 0, 1 }, { 12, 0, 0, 1 },
        { 16, 16, 1, 1 }, { 20, 20, 1, 1 }, { 27, 27, 1, 1 },
        { 28, 28, 1, 0 }, { 32, 32, 1, 0 }, { 100, 32, 1, 0 },
    };
    for (size_t i = 0; i < ARRAY_SIZE(cases); i++) {
        CHECK(write(a, "x", 1) == 1, "failed to write");
        recv_ctl(b, 8, cases[i].size, 0, &got);
        CHECK_GOT(&got, 1, "x");
        CHECK(got.controllen == cases[i].controllen && got.num_cmsgs == cases[i].cmsgs &&
              ((got.flags & MSG_CTRUNC) != 0) == cases[i].ctrunc,
              "a buffer of %zd bytes: controllen %zu, %d messages, flags %#x",
              cases[i].size, got.controllen, got.num_cmsgs, got.flags);
    }
    close_files(2, a, b);
    return 0;
}

// The credentials come before the descriptors, and what does not fit is dropped
static int test_passcred_with_descriptors(void) {
    int a, b;
    CHECK(create_pair(PAIR_SOCKETPAIR, &a, &b) == 0, "failed to create a pair");
    pipe_t p;
    CHECK(make_pipe(&p) == 0, "failed to create a pipe");
    CHECK(set_int_opt(b, SO_PASSCRED, 1) == 0, "failed to set SO_PASSCRED");
    got_t got;
    CHECK(send_fd(a, "x", 1, p.rd) == 1, "failed to send");
    recv_ctl(b, 8, 128, 0, &got);
    CHECK_GOT(&got, 1, "x");
    CHECK(got.controllen == CMSG_SPACE(sizeof(struct ucred)) + CMSG_SPACE(sizeof(int)),
          "controllen %zu", got.controllen);
    CHECK(got.num_cmsgs == 2 && got.num_fds == 1 && got.cred_first, "wrong control messages");
    CHECK(check_own_cred(&got, "with a descriptor") == 0, "wrong credentials");
    CHECK(is_read_end_of(got.fds[0], &p), "the descriptor is wrong");
    close(got.fds[0]);

    // no room for the descriptor after the credentials
    int base = count_fds();
    CHECK(send_fd(a, "x", 1, p.rd) == 1, "failed to send");
    recv_ctl(b, 8, CMSG_SPACE(sizeof(struct ucred)) + CMSG_LEN(0), 0, &got);
    CHECK_GOT(&got, 1, "x");
    CHECK(got.has_cred && got.num_fds == 0 &&
          (got.flags & MSG_CTRUNC), "flags %#x", got.flags);
    CHECK(count_fds() == base, "a descriptor is open");

    // a sender that names its credentials and sends a descriptor in one message
    char ctl[CMSG_SPACE(sizeof(struct ucred)) + CMSG_SPACE(sizeof(int))];
    memset(ctl, 0, sizeof(ctl));
    struct cmsghdr *c = (struct cmsghdr *)ctl;
    c->cmsg_level = SOL_SOCKET;
    c->cmsg_type = SCM_RIGHTS;
    c->cmsg_len = CMSG_LEN(sizeof(int));
    memcpy(CMSG_DATA(c), &p.rd, sizeof(int));
    c = (struct cmsghdr *)(ctl + CMSG_SPACE(sizeof(int)));
    struct ucred cred = { getpid(), getuid(), getgid() };
    c->cmsg_level = SOL_SOCKET;
    c->cmsg_type = SCM_CREDENTIALS;
    c->cmsg_len = CMSG_LEN(sizeof(cred));
    memcpy(CMSG_DATA(c), &cred, sizeof(cred));
    CHECK(send_ctl(a, "y", 1, ctl, sizeof(ctl), 0) == 1, "failed to send");
    recv_ctl(b, 8, 128, 0, &got);
    CHECK_GOT(&got, 1, "y");
    CHECK(got.num_fds == 1 && got.has_cred && got.cred_first, "wrong control messages");
    close(got.fds[0]);
    close_files(4, a, b, p.rd, p.wr);
    return 0;
}

// At the end of the file, the credentials are zero
static int test_passcred_at_the_end_of_the_file(void) {
    int a, b;
    CHECK(create_pair(PAIR_SOCKETPAIR, &a, &b) == 0, "failed to create a pair");
    CHECK(set_int_opt(b, SO_PASSCRED, 1) == 0, "failed to set SO_PASSCRED");
    close(a);
    got_t got;
    recv_ctl(b, 8, 128, 0, &got);
    CHECK(got.ret == 0, "the receive returned %ld", got.ret);
    CHECK(got.has_cred && got.cred.pid == 0 && got.cred.uid == 0 && got.cred.gid == 0,
          "the credentials at the end of the file: %d, pid %d, uid %u, gid %u", got.has_cred,
          got.cred.pid, got.cred.uid, got.cred.gid);
    close(b);
    return 0;
}

// The socket that accept() returns has SO_PASSCRED of the listening one, and no other option
static int test_passcred_of_an_accepted_socket(void) {
    struct sockaddr_un addr;
    socklen_t addr_len;
    int l = listener(&addr, &addr_len);
    CHECK(l >= 0, "failed to listen");
    CHECK(set_int_opt(l, SO_PASSCRED, 1) == 0, "failed to set SO_PASSCRED");
    int a = socket(AF_UNIX, SOCK_STREAM, 0);
    CHECK(connect(a, (struct sockaddr *)&addr, addr_len) == 0, "failed to connect");
    int b = accept(l, NULL, NULL);
    CHECK(b >= 0, "failed to accept");
    set_timeout(b, RECV_TIMEOUT_MS);
    CHECK(get_int_opt(b, SO_PASSCRED) == 1, "the accepted socket does not have SO_PASSCRED");
    CHECK(get_int_opt(a, SO_PASSCRED) == 0, "the client has SO_PASSCRED");
    CHECK(write(a, "x", 1) == 1, "failed to write");
    got_t got;
    recv_ctl(b, 8, 128, 0, &got);
    CHECK_GOT(&got, 1, "x");
    // Linux: the sender has no credentials to tell about the bytes that it sent before the
    // connection was accepted: those of nobody
    CHECK(got.has_cred, "no credentials");
    CHECK(write(a, "y", 1) == 1, "failed to write");
    recv_ctl(b, 8, 128, 0, &got);
    CHECK_GOT(&got, 1, "y");
    CHECK(check_own_cred(&got, "the accepted socket") == 0, "wrong credentials");
    close_files(3, a, b, l);
    return 0;
}

// SO_PASSCRED that is set before the socket connects holds for the connection
static int test_passcred_before_connect(void) {
    struct sockaddr_un addr;
    socklen_t addr_len;
    int l = listener(&addr, &addr_len);
    CHECK(l >= 0, "failed to listen");
    int a = socket(AF_UNIX, SOCK_STREAM, 0);
    CHECK(set_int_opt(a, SO_PASSCRED, 1) == 0, "failed to set SO_PASSCRED");
    CHECK(connect(a, (struct sockaddr *)&addr, addr_len) == 0, "failed to connect");
    int b = accept(l, NULL, NULL);
    CHECK(b >= 0, "failed to accept");
    set_timeout(a, RECV_TIMEOUT_MS);
    set_timeout(b, RECV_TIMEOUT_MS);
    CHECK(get_int_opt(a, SO_PASSCRED) == 1, "SO_PASSCRED is gone");
    CHECK(write(b, "x", 1) == 1, "failed to write");
    got_t got;
    recv_ctl(a, 8, 128, 0, &got);
    CHECK_GOT(&got, 1, "x");
    CHECK(check_own_cred(&got, "the connected socket") == 0, "wrong credentials");
    // the socket that has been accepted did not ask for them
    CHECK(write(a, "y", 1) == 1, "failed to write");
    recv_ctl(b, 8, 128, 0, &got);
    CHECK_GOT(&got, 1, "y");
    CHECK(got.num_cmsgs == 0, "the accepted socket got control messages");
    close_files(3, a, b, l);
    return 0;
}

// The credentials of a process that is another one than the receiver: a child process writes to
// an end of a socket pair that it has inherited, and so does the parent. The receiver does not
// glue the bytes of the two together
static int test_passcred_of_two_senders(void) {
    int a, b;
    CHECK(create_pair(PAIR_SOCKETPAIR, &a, &b) == 0, "failed to create a pair");
    CHECK(set_int_opt(b, SO_PASSCRED, 1) == 0, "failed to set SO_PASSCRED");
    char fd_arg[16];
    snprintf(fd_arg, sizeof(fd_arg), "%d", a);
    char *argv[] = { (char *)self_path, "--child-write", fd_arg, NULL };
    pid_t child;
    CHECK(posix_spawn(&child, self_path, NULL, NULL, argv, environ) == 0,
          "failed to spawn the child process");
    int status;
    CHECK(waitpid(child, &status, 0) == child && WIFEXITED(status) &&
          WEXITSTATUS(status) == 0,
          "the child process failed");
    CHECK(write(a, "PQ", 2) == 2, "failed to write");
    got_t got;
    recv_ctl(b, 8, 128, 0, &got);
    CHECK_GOT(&got, 1, "C");
    CHECK(got.has_cred && got.cred.pid == child && got.cred.uid == getuid(),
          "the credentials are those of pid %d, not of the child %d", got.cred.pid, child);
    recv_ctl(b, 8, 128, 0, &got);
    CHECK_GOT(&got, 2, "PQ");
    CHECK(check_own_cred(&got, "the parent") == 0, "wrong credentials");
    close_files(2, a, b);
    return 0;
}

// A peek that delivers credentials does not look ahead past the bytes of another sender: a child
// process writes a byte, the parent sends a descriptor, and the peek of the byte shows the
// credentials of the child, and no descriptor
static int test_passcred_peek_does_not_look_past_another_sender(void) {
    int a, b;
    CHECK(create_pair(PAIR_SOCKETPAIR, &a, &b) == 0, "failed to create a pair");
    pipe_t p;
    CHECK(make_pipe(&p) == 0, "failed to create a pipe");
    CHECK(set_int_opt(b, SO_PASSCRED, 1) == 0, "failed to set SO_PASSCRED");
    char fd_arg[16];
    snprintf(fd_arg, sizeof(fd_arg), "%d", a);
    char *argv[] = { (char *)self_path, "--child-write", fd_arg, NULL };
    pid_t child;
    CHECK(posix_spawn(&child, self_path, NULL, NULL, argv, environ) == 0,
          "failed to spawn the child process");
    int status;
    CHECK(waitpid(child, &status, 0) == child && WIFEXITED(status) &&
          WEXITSTATUS(status) == 0,
          "the child process failed");
    CHECK(send_fd(a, "d", 1, p.rd) == 1, "failed to send");
    got_t got;
    recv_ctl(b, 1, 128, MSG_PEEK, &got);
    CHECK_GOT(&got, 1, "C");
    CHECK(got.has_cred &&
          got.cred.pid == child, "the credentials are not those of the child");
    CHECK(got.num_fds == 0 &&
          !(got.flags & MSG_CTRUNC), "the peek shows %d descriptors, flags %#x",
          got.num_fds, got.flags);
    // without credentials, the same peek shows the descriptor of the parent
    CHECK(set_int_opt(b, SO_PASSCRED, 0) == 0, "failed to reset SO_PASSCRED");
    recv_ctl(b, 1, 128, MSG_PEEK, &got);
    CHECK_GOT(&got, 1, "C");
    CHECK(got.num_fds == 1 && is_read_end_of(got.fds[0], &p), "the peek has no descriptor");
    close_got_fds(&got);
    close_files(4, a, b, p.rd, p.wr);
    return 0;
}

// ---------------------------------------------------------------------------------------------
// Nothing is left behind, and the queues are limited
// ---------------------------------------------------------------------------------------------

#define LEAK_CYCLES     200
#define LEAK_LIMIT_KB   256

// One cycle: a pair, descriptors sent and received (or not), everything closed
static int leak_cycle(int kind, int how) {
    int a, b;
    if (create_pair(kind, &a, &b) != 0) {
        return -1;
    }
    pipe_t p;
    if (make_pipe(&p) != 0) {
        return -1;
    }
    int rds[2] = { p.rd, p.wr };
    for (int i = 0; i < 3; i++) {
        if (send_fds(a, "xy", 2, rds, 2) != 2) {
            return -1;
        }
    }
    got_t got;
    // how: 0 received, 1 half received (the rest is queued when the sockets are closed),
    // 2 not received, 3 plain reads
    for (int i = 0; i < (how == 0 ? 3 : how == 1 ? 1 : 0); i++) {
        recv_ctl(b, 8, 128, 0, &got);
        if (got.ret != 2 || got.num_fds != 2) {
            return -1;
        }
        close_got_fds(&got);
    }
    char buf[8];
    for (int i = 0; how == 3 && i < 3; i++) {
        if (read(b, buf, 2) != 2) {
            return -1;
        }
    }
    close_files(4, a, b, p.rd, p.wr);
    return 0;
}

static int leak_loop(int cycles) {
    for (int i = 0; i < cycles; i++) {
        for (int kind = 0; kind < NUM_PAIR_KINDS; kind++) {
            if (leak_cycle(kind, i % 4) != 0) {
                return -1;
            }
        }
    }
    return 0;
}

// Sending and closing leaves no descriptor and no memory: the files are released, and so is what
// the queues hold, whether the messages were received or not
static int test_nothing_is_left_behind(void) {
    CHECK(leak_loop(8) == 0, "a cycle failed");
    int fds_before = count_fds();
    long before = kernel_heap_in_use();
    CHECK(leak_loop(LEAK_CYCLES) == 0, "a cycle failed");
    CHECK(count_fds() == fds_before, "%d descriptors are open, there were %d", count_fds(),
          fds_before);
    long after = kernel_heap_in_use();
    if (before < 0) {
        printf("\t\tnote: no kernel heap monitor, memory is not checked\n");
        return 0;
    }
    CHECK(after - before <= LEAK_LIMIT_KB, "the kernel heap has grown by %ld kB in %d cycles "
          "(limit %d kB)", after - before, LEAK_CYCLES, LEAK_LIMIT_KB);
    return 0;
}

#define CHAIN_LEVELS    4000

// Sockets that are sent through sockets, level after level, are released when the last of them is
// closed, whatever the number of levels. Linux releases them one after the other. Occlum must not
// release each level from within the release of the level above it, which would use up the stack
// of the thread with a chain of some thousands of levels
static int test_a_chain_of_sockets_is_released(void) {
    int fds_before = count_fds();
    long before = kernel_heap_in_use();
    pipe_t p;
    CHECK(make_pipe(&p) == 0, "failed to create a pipe");
    int level[2];
    CHECK(socketpair(AF_UNIX, SOCK_STREAM, 0, level) == 0, "failed to create a pair");
    // The write end of the pipe is in the queue of the lowest level: the release has to reach it
    CHECK(send_fd(level[0], "x", 1, p.wr) == 1, "failed to send the pipe");
    close(p.wr);
    for (int i = 1; i < CHAIN_LEVELS; i++) {
        int next[2];
        CHECK(socketpair(AF_UNIX, SOCK_STREAM, 0, next) == 0, "failed to create level %d", i);
        long ret = send_fds(next[0], "x", 1, level, 2);
        if (ret == -ETOOMANYREFS && !in_occlum()) {
            // A process without CAP_SYS_RESOURCE may not have more descriptors in flight than
            // RLIMIT_NOFILE allows it to open: the chain ends here, and the release is checked
            close_files(2, next[0], next[1]);
            break;
        }
        CHECK(ret == 1, "failed to send level %d", i - 1);
        close_files(2, level[0], level[1]);
        level[0] = next[0];
        level[1] = next[1];
    }
    CHECK(!pipe_has_no_writer(p.rd), "the chain has lost the pipe");
    // The chain is held by the queue of the end that receives, which is the second
    close(level[0]);
    CHECK(!pipe_has_no_writer(p.rd), "the chain has lost the pipe");
    close(level[1]);
    CHECK(pipe_has_no_writer(p.rd), "the files of the chain are not released");
    close(p.rd);
    CHECK(count_fds() == fds_before, "%d descriptors are open, there were %d", count_fds(),
          fds_before);
    long after = kernel_heap_in_use();
    if (before < 0) {
        printf("\t\tnote: no kernel heap monitor, memory is not checked\n");
        return 0;
    }
    CHECK(after - before <= LEAK_LIMIT_KB, "the chain has left %ld kB", after - before);
    return 0;
}

// A message that is sent, and received after its sender is closed, in the thread of another
// process: the queue does not depend on any of them
static int test_queued_messages_are_not_bound_to_the_sender(void) {
    int a, b;
    CHECK(create_pair(PAIR_CONNECT, &a, &b) == 0, "failed to create a connection");
    pipe_t p;
    CHECK(make_pipe(&p) == 0, "failed to create a pipe");
    for (int i = 0; i < 20; i++) {
        CHECK(send_fd(a, "x", 1, p.rd) == 1, "failed to send");
    }
    close(a);
    for (int i = 0; i < 20; i++) {
        got_t got;
        recv_ctl(b, 8, 128, 0, &got);
        CHECK_GOT(&got, 1, "x");
        CHECK(got.num_fds == 1, "message %d has no descriptor", i);
        close(got.fds[0]);
    }
    close_files(3, b, p.rd, p.wr);
    return 0;
}

// A sender that fills the queue of control messages is stopped, like one that fills the buffer, and
// the queue works again after the receiver has taken what is in it. Linux limits the messages with
// descriptors by the buffer, and the descriptors that are in flight, so the sender fails with
// EAGAIN, or ETOOMANYREFS
static int test_the_queue_of_control_messages_is_limited(void) {
    int a, b;
    CHECK(create_pair(PAIR_SOCKETPAIR, &a, &b) == 0, "failed to create a pair");
    pipe_t p;
    CHECK(make_pipe(&p) == 0, "failed to create a pipe");
    set_nonblocking(a, 1);
    int sent = 0;
    long ret;
    while ((ret = send_fd(a, "x", 1, p.rd)) == 1) {
        sent++;
        CHECK(sent < 100000, "the queue is not limited");
    }
    CHECK(ret == -EAGAIN || ret == -ETOOMANYREFS, "the send failed with %ld", ret);
    CHECK(sent >= 100, "only %d messages fit", sent);
    // every message has its descriptor, and the queue takes more when it has been emptied
    for (int i = 0; i < sent; i++) {
        got_t got;
        recv_now(b, 8, 128, 0, &got);
        CHECK_GOT(&got, 1, "x");
        CHECK(got.num_fds == 1, "message %d of %d has %d descriptors", i, sent, got.num_fds);
        close(got.fds[0]);
    }
    CHECK(send_fd(a, "y", 1, p.rd) == 1, "the queue does not take messages again");
    close_files(4, a, b, p.rd, p.wr);
    return 0;
}

typedef struct {
    int sock;
    pipe_t pipe;
    int result;
} blocked_t;

static void *blocked_sender_main(void *arg) {
    blocked_t *s = arg;
    // blocks until the receiver has made room
    s->result = send_fd(s->sock, "X", 1, s->pipe.rd);
    return NULL;
}

// A blocking send of a message with a descriptor waits for room, and delivers it after the bytes
// that are before it
static int test_blocked_send_with_a_descriptor(void) {
    int a, b;
    CHECK(create_pair(PAIR_SOCKETPAIR, &a, &b) == 0, "failed to create a pair");
    set_timeout(a, 10000);
    pipe_t p;
    CHECK(make_pipe(&p) == 0, "failed to create a pipe");
    // fill the socket
    set_nonblocking(a, 1);
    static char fill[65536];
    memset(fill, 'f', sizeof(fill));
    long total = 0;
    ssize_t n;
    while ((n = write(a, fill, sizeof(fill))) > 0) {
        total += n;
    }
    CHECK(n < 0 && errno == EAGAIN, "the socket is not full");
    set_nonblocking(a, 0);
    blocked_t s = { a, p, 0 };
    pthread_t thread;
    CHECK(pthread_create(&thread, NULL, blocked_sender_main, &s) == 0,
          "failed to create a thread");
    sleep_ms(200);
    // read what is queued, which makes room. The bytes that were queued before the message are
    // received with it, if the receive reaches it: the descriptor comes with the first read that
    // goes into the message
    long got_bytes = 0;
    int fd_seen = 0;
    got_t got;
    while (!fd_seen) {
        recv_ctl(b, 4096, 128, 0, &got);
        CHECK(got.ret > 0, "the receive failed: %ld", got.ret);
        got_bytes += got.ret;
        if (got.num_fds > 0) {
            CHECK(got.num_fds == 1 && got.data[got.ret - 1] == 'X', "the descriptor is not with X");
            CHECK(is_read_end_of(got.fds[0], &p), "the descriptor is wrong");
            close(got.fds[0]);
            fd_seen = 1;
        } else {
            CHECK(got.num_cmsgs == 0 && got.data[got.ret - 1] == 'f',
                  "the bytes before the message are wrong");
        }
    }
    CHECK(pthread_join(thread, NULL) == 0, "failed to join the thread");
    CHECK(s.result == 1, "the send returned %d", s.result);
    CHECK(got_bytes == total + 1, "%ld bytes were received, %ld sent", got_bytes, total + 1);
    close_files(4, a, b, p.rd, p.wr);
    return 0;
}

// A blocked send of a message with a descriptor fails when the peer is closed, and the descriptor
// stays with the sender
static int test_blocked_send_with_a_descriptor_to_a_peer_that_closes(void) {
    int a, b;
    CHECK(create_pair(PAIR_SOCKETPAIR, &a, &b) == 0, "failed to create a pair");
    set_timeout(a, 10000);
    pipe_t p;
    CHECK(make_pipe(&p) == 0, "failed to create a pipe");
    set_nonblocking(a, 1);
    static char fill[65536];
    memset(fill, 'f', sizeof(fill));
    ssize_t n;
    while ((n = write(a, fill, sizeof(fill))) > 0) {
    }
    CHECK(n < 0 && errno == EAGAIN, "the socket is not full");
    set_nonblocking(a, 0);
    blocked_t s = { a, p, 0 };
    pthread_t thread;
    CHECK(pthread_create(&thread, NULL, blocked_sender_main, &s) == 0,
          "failed to create a thread");
    sleep_ms(200);
    close(b);
    CHECK(pthread_join(thread, NULL) == 0, "failed to join the thread");
    // Linux fails with ECONNRESET, as the peer is closed with bytes that it has not received
    CHECK(s.result == -EPIPE || s.result == -ECONNRESET, "the send returned %d", s.result);
    CHECK(is_read_end_of(p.rd, &p), "the descriptor of the sender is gone");
    close_files(3, a, p.rd, p.wr);
    return 0;
}

typedef struct {
    int sock;
    got_t got;
} waiting_receiver_t;

static void *waiting_receiver_main(void *arg) {
    waiting_receiver_t *r = arg;
    // blocks until the message arrives, and takes the control message that belongs to it
    recv_ctl(r->sock, 8, 128, 0, &r->got);
    return NULL;
}

// A receive that waits for a message gets the descriptor that arrives with it, as the message
// does not exist when the receive begins
static int test_receive_that_waits_for_a_message_with_a_descriptor(void) {
    for (int kind = 0; kind < NUM_PAIR_KINDS; kind++) {
        int a, b;
        CHECK(create_pair(kind, &a, &b) == 0, "failed to create a %s pair", pair_names[kind]);
        pipe_t p;
        CHECK(make_pipe(&p) == 0, "failed to create a pipe");
        waiting_receiver_t r;
        memset(&r, 0, sizeof(r));
        r.sock = b;
        pthread_t thread;
        CHECK(pthread_create(&thread, NULL, waiting_receiver_main, &r) == 0,
              "failed to create a thread");
        sleep_ms(200);
        CHECK(send_fd(a, "x", 1, p.rd) == 1, "failed to send");
        CHECK(pthread_join(thread, NULL) == 0, "failed to join the thread");
        CHECK_GOT(&r.got, 1, "x");
        CHECK(r.got.num_fds == 1 && is_read_end_of(r.got.fds[0], &p), "%s: no descriptor",
              pair_names[kind]);
        close_files(4, a, b, p.rd, p.wr);
        close(r.got.fds[0]);
    }
    return 0;
}

// ---------------------------------------------------------------------------------------------
// Senders and a receiver that run at the same time
// ---------------------------------------------------------------------------------------------

#define STREAM_MESSAGES 400

typedef struct {
    int sock;
    int failed;
} stream_sender_t;

// Sends a number of messages, each of a byte with the descriptor of a pipe that holds the same
// number
static void *stream_sender_main(void *arg) {
    stream_sender_t *s = arg;
    for (int i = 0; i < STREAM_MESSAGES; i++) {
        pipe_t p;
        if (make_pipe(&p) != 0) {
            s->failed = 1;
            return NULL;
        }
        unsigned char byte = 0x80 | (i & 0x7f);
        char text[16];
        int len = snprintf(text, sizeof(text), "%d", i);
        if (write(p.wr, text, len) != len || send_fd(s->sock, &byte, 1, p.rd) != 1) {
            s->failed = 1;
            return NULL;
        }
        close_files(2, p.rd, p.wr);
        if (i % 7 == 0) {
            // plain bytes between the messages
            if (write(s->sock, "-", 1) != 1) {
                s->failed = 1;
                return NULL;
            }
        }
    }
    return NULL;
}

// The receiver gets the descriptor of a message with its byte, whenever the call takes place
// relative to the send: a receive that is blocked before the sender has sent, and one that
// finds many messages queued. Every receive that gets a message gets its descriptor, once.
static int test_descriptors_arrive_with_their_bytes_under_load(void) {
    for (int round = 0; round < 3; round++) {
        int a, b;
        CHECK(create_pair(round == 1 ? PAIR_CONNECT : PAIR_SOCKETPAIR, &a, &b) == 0,
              "failed to create a pair");
        set_timeout(b, 10000);
        stream_sender_t s = { a, 0 };
        pthread_t thread;
        // round 0: the receiver is ahead of the sender, round 2: the sender is ahead
        if (round == 2) {
            // let the sender fill the queue first
            set_nonblocking(a, 0);
        }
        CHECK(pthread_create(&thread, NULL, stream_sender_main, &s) == 0,
              "failed to create a thread");
        if (round == 2) {
            sleep_ms(300);
        }
        int expected = 0;
        int messages = 0;
        while (messages < STREAM_MESSAGES && !s.failed) {
            got_t got;
            recv_ctl(b, 64, 2048, 0, &got);
            CHECK(got.ret > 0, "round %d: the receive returned %ld after %d messages", round,
                  got.ret, messages);
            CHECK(got.flags == 0, "round %d: flags %#x", round, got.flags);
            // the bytes: dashes, and at the end one byte of a message with a descriptor
            for (long i = 0; i < got.ret; i++) {
                if (got.data[i] != '-') {
                    CHECK(i == got.ret - 1, "round %d: a byte of a message is followed by %ld "
                          "more bytes", round, got.ret - 1 - i);
                    CHECK((unsigned char)got.data[i] == (0x80 | (expected & 0x7f)), "round %d: "
                          "message %d has the byte %d", round, expected,
                          (unsigned char)got.data[i]);
                }
            }
            if (got.data[got.ret - 1] != '-') {
                CHECK(got.num_fds == 1, "round %d: message %d has %d descriptors", round,
                      expected, got.num_fds);
                char buf[16] = {0};
                char text[16];
                int len = snprintf(text, sizeof(text), "%d", expected);
                set_nonblocking(got.fds[0], 1);
                CHECK(read(got.fds[0], buf, sizeof(buf)) == len && memcmp(buf, text, len) == 0,
                      "round %d: message %d has the descriptor of another file", round, expected);
                close(got.fds[0]);
                expected++;
                messages++;
            } else {
                CHECK(got.num_fds == 0, "round %d: bytes without a message have a descriptor",
                      round);
            }
        }
        CHECK(pthread_join(thread, NULL) == 0, "failed to join the thread");
        CHECK(!s.failed, "round %d: the sender failed", round);
        close_files(2, a, b);
    }
    return 0;
}

typedef struct {
    int sock;
    atomic_int *received;       // the messages that all receivers have got
    atomic_int *seen;           // for each message: how many receivers got it
    int total;
    int failed;
} receiver_t;

// Receives messages with a descriptor until all have arrived, and checks each
static void *receiver_main(void *arg) {
    receiver_t *r = arg;
    while (atomic_load(r->received) < r->total && !r->failed) {
        got_t got;
        recv_ctl(r->sock, 64, 2048, 0, &got);
        if (got.ret == -EAGAIN || got.ret == -EWOULDBLOCK) {
            continue;
        }
        if (got.ret != 1 || got.num_fds != 1 || got.flags != 0) {
            r->failed = 1;
            close_got_fds(&got);
            return NULL;
        }
        char buf[16] = {0};
        set_nonblocking(got.fds[0], 1);
        int n = read(got.fds[0], buf, sizeof(buf) - 1);
        close(got.fds[0]);
        int i = n > 0 ? atoi(buf) : -1;
        if (i < 0 || i >= r->total || (unsigned char)got.data[0] != (0x80 | (i & 0x7f))) {
            r->failed = 1;
            return NULL;
        }
        atomic_fetch_add(&r->seen[i], 1);
        atomic_fetch_add(r->received, 1);
    }
    return NULL;
}

// Receivers on one socket that run at the same time do not share the descriptors of a message,
// and none is lost or taken twice
static int test_receivers_that_run_at_the_same_time(void) {
    enum { RECEIVERS = 4, MESSAGES = 400 };
    int a, b;
    CHECK(create_pair(PAIR_SOCKETPAIR, &a, &b) == 0, "failed to create a pair");
    set_timeout(b, 500);
    static atomic_int seen[MESSAGES];
    atomic_int received = 0;
    receiver_t receivers[RECEIVERS];
    pthread_t threads[RECEIVERS];
    for (int i = 0; i < RECEIVERS; i++) {
        receivers[i] = (receiver_t) { b, &received, seen, MESSAGES, 0 };
        CHECK(pthread_create(&threads[i], NULL, receiver_main, &receivers[i]) == 0,
              "failed to create a thread");
    }
    for (int i = 0; i < MESSAGES; i++) {
        pipe_t p;
        CHECK(make_pipe(&p) == 0, "failed to create a pipe");
        char text[16];
        int len = snprintf(text, sizeof(text), "%d", i);
        unsigned char byte = 0x80 | (i & 0x7f);
        CHECK(write(p.wr, text, len) == len && send_fd(a, &byte, 1, p.rd) == 1, "failed to send");
        close_files(2, p.rd, p.wr);
    }
    int failed = 0;
    for (int i = 0; i < RECEIVERS; i++) {
        CHECK(pthread_join(threads[i], NULL) == 0, "failed to join a thread");
        failed |= receivers[i].failed;
    }
    CHECK(!failed, "a receiver got a message wrong");
    for (int i = 0; i < MESSAGES; i++) {
        CHECK(atomic_load(&seen[i]) == 1, "message %d was received %d times", i,
              atomic_load(&seen[i]));
    }
    close_files(2, a, b);
    return 0;
}

// sendmmsg() and recvmmsg() carry the control messages of each message
static int test_sendmmsg_and_recvmmsg_with_descriptors(void) {
    int a, b;
    CHECK(create_pair(PAIR_SOCKETPAIR, &a, &b) == 0, "failed to create a pair");
    pipe_t p[2];
    CHECK(make_pipe(&p[0]) == 0 && make_pipe(&p[1]) == 0, "failed to create pipes");
    char ctl[2][CMSG_SPACE(sizeof(int))];
    struct iovec iov[2] = { { "A", 1 }, { "B", 1 } };
    struct mmsghdr msgs[2];
    memset(msgs, 0, sizeof(msgs));
    memset(ctl, 0, sizeof(ctl));
    for (int i = 0; i < 2; i++) {
        msgs[i].msg_hdr.msg_iov = &iov[i];
        msgs[i].msg_hdr.msg_iovlen = 1;
        msgs[i].msg_hdr.msg_control = ctl[i];
        msgs[i].msg_hdr.msg_controllen = sizeof(ctl[i]);
        struct cmsghdr *c = (struct cmsghdr *)ctl[i];
        c->cmsg_level = SOL_SOCKET;
        c->cmsg_type = SCM_RIGHTS;
        c->cmsg_len = CMSG_LEN(sizeof(int));
        memcpy(CMSG_DATA(c), &p[i].rd, sizeof(int));
    }
    long sent = syscall(SYS_sendmmsg, a, msgs, 2, MSG_NOSIGNAL);
    CHECK(sent == 2, "sendmmsg sent %ld messages", sent);

    char data[2][8];
    char rctl[2][64];
    struct iovec riov[2] = { { data[0], sizeof(data[0]) }, { data[1], sizeof(data[1]) } };
    struct mmsghdr rmsgs[2];
    memset(rmsgs, 0, sizeof(rmsgs));
    for (int i = 0; i < 2; i++) {
        rmsgs[i].msg_hdr.msg_iov = &riov[i];
        rmsgs[i].msg_hdr.msg_iovlen = 1;
        rmsgs[i].msg_hdr.msg_control = rctl[i];
        rmsgs[i].msg_hdr.msg_controllen = sizeof(rctl[i]);
    }
    long received = syscall(SYS_recvmmsg, b, rmsgs, 2, MSG_DONTWAIT, NULL);
    CHECK(received == 2, "recvmmsg received %ld messages", received);
    for (int i = 0; i < 2; i++) {
        struct cmsghdr *c = CMSG_FIRSTHDR(&rmsgs[i].msg_hdr);
        CHECK(rmsgs[i].msg_len == 1 &&
              data[i][0] == "AB"[i], "message %d has the wrong bytes", i);
        CHECK(c != NULL && c->cmsg_type == SCM_RIGHTS && c->cmsg_len == CMSG_LEN(sizeof(int)),
              "message %d has no descriptor", i);
        int fd;
        memcpy(&fd, CMSG_DATA(c), sizeof(fd));
        CHECK(is_read_end_of(fd, &p[i]), "the descriptor of message %d is wrong", i);
        close(fd);
    }
    close_files(6, a, b, p[0].rd, p[0].wr, p[1].rd, p[1].wr);
    return 0;
}

// ---------------------------------------------------------------------------------------------
// Credentials of connections that are not accepted yet, the slots of the queue, the limit of the
// descriptors of a send, and the credentials of a thread
// ---------------------------------------------------------------------------------------------

// Bytes that a client sends before accept() returns the connection carry its credentials, whether
// the sockets ask for them or not: Linux records them for a connection whose socket has no file
// yet. The receiver gets them if it has SO_PASSCRED when it receives, which it may set on the
// listener (the accepted socket inherits it), or on the accepted socket after accept().
static int test_credentials_of_bytes_sent_before_accept(void) {
    static const char *names[] = {
        "SO_PASSCRED on the listener", "SO_PASSCRED on the client", "SO_PASSCRED set after accept",
        "no SO_PASSCRED",
    };
    for (int variant = 0; variant < 4; variant++) {
        struct sockaddr_un addr;
        socklen_t addr_len;
        int l = listener(&addr, &addr_len);
        CHECK(l >= 0, "failed to listen");
        if (variant == 0) {
            CHECK(set_int_opt(l, SO_PASSCRED, 1) == 0, "failed to set SO_PASSCRED");
        }
        int c = socket(AF_UNIX, SOCK_STREAM, 0);
        CHECK(c >= 0, "failed to create a socket");
        if (variant == 1) {
            CHECK(set_int_opt(c, SO_PASSCRED, 1) == 0, "failed to set SO_PASSCRED");
        }
        CHECK(connect(c, (struct sockaddr *)&addr, addr_len) == 0, "failed to connect");
        CHECK(write(c, "x", 1) == 1, "failed to write before accept");
        int a = accept(l, NULL, NULL);
        CHECK(a >= 0, "failed to accept");
        set_timeout(a, RECV_TIMEOUT_MS);
        if (variant == 2) {
            CHECK(set_int_opt(a, SO_PASSCRED, 1) == 0, "failed to set SO_PASSCRED");
        }
        got_t got;
        recv_ctl(a, 8, 128, 0, &got);
        CHECK_GOT(&got, 1, "x");
        if (variant == 0 || variant == 2) {
            CHECK(got.has_cred, "%s: no credentials", names[variant]);
            CHECK(check_own_cred(&got, names[variant]) == 0,
                  "%s: wrong credentials for bytes sent before accept", names[variant]);
        } else {
            CHECK(!got.has_cred, "%s: credentials that nobody asked for", names[variant]);
        }
        // the bytes that follow accept() are the same
        CHECK(write(c, "y", 1) == 1, "failed to write after accept");
        recv_ctl(a, 8, 128, 0, &got);
        CHECK_GOT(&got, 1, "y");
        if (variant == 0 || variant == 2) {
            CHECK(check_own_cred(&got, names[variant]) == 0, "%s: wrong credentials after accept",
                  names[variant]);
        }
        close_files(3, a, c, l);
    }
    return 0;
}

// A connection that is reset, whose reader has bytes and a message with a descriptor queued: a
// receive that waits for all its bytes stops after the message with the descriptor, as always, and
// does not look at the queue again, so it does not take the error of the connection with it. The
// next receive gets the error, and the one after it the end of the file. (Without a descriptor, the
// call that finds the queue empty takes the error: see test_waitall_takes_the_reset of the test
// unix_socket_events.)
static int test_waitall_that_stops_at_a_descriptor_does_not_take_the_reset(void) {
    for (int kind = 0; kind < NUM_PAIR_KINDS; kind++) {
        int a, b;
        CHECK(create_pair(kind, &a, &b) == 0, "failed to create a %s pair", pair_names[kind]);
        pipe_t p;
        CHECK(make_pipe(&p) == 0, "failed to create a pipe");
        CHECK(write(a, "ab", 2) == 2 && send_fd(a, "c", 1, p.rd) == 1, "failed to send");
        CHECK(write(b, "u", 1) == 1, "failed to write");
        close(a);   // it has a byte that it did not read: b is reset
        got_t got;
        recv_ctl(b, 8, 128, MSG_WAITALL | MSG_DONTWAIT, &got);
        CHECK_GOT(&got, 3, "abc");
        CHECK(got.num_fds == 1, "%s: %d descriptors arrived", pair_names[kind], got.num_fds);
        close_got_fds(&got);
        recv_now(b, 8, 128, 0, &got);
        CHECK(got.ret == -ECONNRESET, "%s: the next receive returned %ld, not ECONNRESET",
              pair_names[kind], got.ret);
        recv_now(b, 8, 128, 0, &got);
        CHECK(got.ret == 0, "%s: the receive after the error returned %ld", pair_names[kind],
              got.ret);
        close_files(3, b, p.rd, p.wr);
    }
    return 0;
}

// ---------------------------------------------------------------------------------------------
// A child process that writes to the end of a socket pair that it has inherited
// ---------------------------------------------------------------------------------------------

static int child_write(const char *fd_arg) {
    int fd = atoi(fd_arg);
    return write(fd, "C", 1) == 1 ? 0 : 1;
}

static test_case_t test_cases[] = {
    TEST_CASE(test_rights_arrive),
    TEST_CASE(test_rights_after_the_sender_closed_the_descriptor),
    TEST_CASE(test_rights_of_a_file_that_is_open_only_in_the_message),
    TEST_CASE(test_rights_after_the_sender_thread_exited),
    TEST_CASE(test_rights_of_a_socket),
    TEST_CASE(test_rights_of_files_of_other_kinds),
    TEST_CASE(test_rights_several_descriptors),
    TEST_CASE(test_rights_that_are_not_delivered),
    TEST_CASE(test_rights_that_are_not_delivered_are_released),
    TEST_CASE(test_rights_in_the_queue_of_a_closed_socket_are_released),
    TEST_CASE(test_rights_after_close_and_shutdown_of_the_sender),
    TEST_CASE(test_rights_to_a_closed_peer_are_not_sent),
    TEST_CASE(test_socket_that_is_sent_to_itself),
    TEST_CASE(test_rights_without_bytes),
    TEST_CASE(test_plain_read_gets_only_the_bytes),
    TEST_CASE(test_receive_with_a_buffer_but_without_control_messages),
    TEST_CASE(test_boundaries_of_messages_with_descriptors),
    TEST_CASE(test_receive_into_several_buffers),
    TEST_CASE(test_peek_shows_the_descriptors),
    TEST_CASE(test_peek_looks_ahead_for_descriptors),
    TEST_CASE(test_receive_again_with_the_same_message),
    TEST_CASE(test_cloexec_of_received_descriptors),
    TEST_CASE(test_invalid_control_messages),
    TEST_CASE(test_passcred),
    TEST_CASE(test_passcred_with_peek_and_plain_reads),
    TEST_CASE(test_passcred_after_the_send_shows_nobody),
    TEST_CASE(test_passcred_of_the_sender),
    TEST_CASE(test_scm_credentials),
    TEST_CASE(test_scm_credentials_of_others),
    TEST_CASE(test_passcred_with_a_small_buffer),
    TEST_CASE(test_passcred_with_descriptors),
    TEST_CASE(test_passcred_at_the_end_of_the_file),
    TEST_CASE(test_passcred_of_an_accepted_socket),
    TEST_CASE(test_passcred_before_connect),
    TEST_CASE(test_passcred_of_two_senders),
    TEST_CASE(test_passcred_peek_does_not_look_past_another_sender),
    TEST_CASE(test_credentials_of_bytes_sent_before_accept),
    TEST_CASE(test_waitall_that_stops_at_a_descriptor_does_not_take_the_reset),
    TEST_CASE(test_nothing_is_left_behind),
    TEST_CASE(test_a_chain_of_sockets_is_released),
    TEST_CASE(test_queued_messages_are_not_bound_to_the_sender),
    TEST_CASE(test_the_queue_of_control_messages_is_limited),
    TEST_CASE(test_receive_that_waits_for_a_message_with_a_descriptor),
    TEST_CASE(test_blocked_send_with_a_descriptor),
    TEST_CASE(test_blocked_send_with_a_descriptor_to_a_peer_that_closes),
    TEST_CASE(test_descriptors_arrive_with_their_bytes_under_load),
    TEST_CASE(test_receivers_that_run_at_the_same_time),
    TEST_CASE(test_sendmmsg_and_recvmmsg_with_descriptors),
};

int main(int argc, const char *argv[]) {
    if (argc > 2 && strcmp(argv[1], "--child-write") == 0) {
        return child_write(argv[2]);
    }
    // the program that the test spawns: itself
    static char path[PATH_MAX];
    ssize_t path_len = readlink("/proc/self/exe", path, sizeof(path) - 1);
    self_path = path_len > 0 ? path : argv[0];
    // a write to a pipe that has no reader fails with EPIPE
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
        fflush(stdout);
    }
    fflush(stdout);
    return ret;
}
