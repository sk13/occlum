#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "test.h"

// ============================================================================
// recvmmsg() on the sockets of the host: UDP and TCP sockets (the unix sockets
// are tested by unix_socket_io)
// ============================================================================

#ifndef MSG_WAITFORONE
#define MSG_WAITFORONE 0x10000
#endif

#define MAX_MSGS    8
#define MAX_LEN     64

// The messages of one call: each has its own buffer and its own address
typedef struct {
    struct mmsghdr msgs[MAX_MSGS];
    struct iovec iov[MAX_MSGS];
    char bufs[MAX_MSGS][MAX_LEN];
    struct sockaddr_in from[MAX_MSGS];
} batch_t;

// Prepares n messages whose buffers are len bytes long, with or without a name
static void batch_init(batch_t *b, unsigned n, size_t len, int with_name) {
    memset(b, 0, sizeof(*b));
    for (unsigned i = 0; i < n; i++) {
        b->iov[i].iov_base = b->bufs[i];
        b->iov[i].iov_len = len;
        b->msgs[i].msg_hdr.msg_iov = &b->iov[i];
        b->msgs[i].msg_hdr.msg_iovlen = 1;
        if (with_name) {
            b->msgs[i].msg_hdr.msg_name = &b->from[i];
            b->msgs[i].msg_hdr.msg_namelen = sizeof(b->from[i]);
        }
    }
}

static long do_recvmmsg(int fd, batch_t *b, unsigned vlen, int flags,
                        struct timespec *timeout) {
    return syscall(SYS_recvmmsg, fd, b->msgs, vlen, flags, timeout);
}

// A UDP socket bound to 127.0.0.1 and a port that the system chooses
static int udp_socket(struct sockaddr_in *addr) {
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) {
        return -1;
    }
    memset(addr, 0, sizeof(*addr));
    addr->sin_family = AF_INET;
    addr->sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t len = sizeof(*addr);
    if (bind(fd, (struct sockaddr *)addr, sizeof(*addr)) < 0 ||
            getsockname(fd, (struct sockaddr *)addr, &len) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

// Lets the datagrams that were sent reach the queue: on a busy host, the delivery on the loopback
// may finish a little after sendto returns
static void settle(void) {
    usleep(50 * 1000);
}

// The datagram i has base + i bytes with the value 'a' + i
static int send_datagram(int fd, const struct sockaddr_in *to, unsigned i, size_t base) {
    char buf[MAX_LEN];
    size_t len = base + i;
    memset(buf, 'a' + i, len);
    if (sendto(fd, buf, len, 0, (const struct sockaddr *)to, sizeof(*to)) != (ssize_t)len) {
        THROW_ERROR("sendto failed");
    }
    return 0;
}

static int check_datagram(const batch_t *b, unsigned i, size_t base,
                          const struct sockaddr_in *from) {
    size_t len = base + i;
    if (b->msgs[i].msg_len != len) {
        THROW_ERROR("message %u has %u bytes instead of %zu", i, b->msgs[i].msg_len, len);
    }
    for (size_t k = 0; k < len; k++) {
        if (b->bufs[i][k] != (char)('a' + i)) {
            THROW_ERROR("message %u has a wrong byte at %zu", i, k);
        }
    }
    if (from != NULL) {
        const struct sockaddr_in *got = &b->from[i];
        if (b->msgs[i].msg_hdr.msg_namelen != sizeof(*got) ||
                got->sin_family != AF_INET || got->sin_port != from->sin_port ||
                got->sin_addr.s_addr != from->sin_addr.s_addr) {
            THROW_ERROR("message %u has the wrong name", i);
        }
    }
    return 0;
}

// ============================================================================
// Test cases
// ============================================================================

// Three datagrams are all received by one call, with their lengths and the
// address of the sender, and the next call finds nothing
static int test_udp_batch(void) {
    struct sockaddr_in rx_addr, tx_addr;
    int rx = udp_socket(&rx_addr), tx = udp_socket(&tx_addr);
    if (rx < 0 || tx < 0) {
        THROW_ERROR("failed to create the sockets");
    }
    for (unsigned i = 0; i < 3; i++) {
        if (send_datagram(tx, &rx_addr, i, 5) < 0) {
            return -1;
        }
    }
    settle();

    batch_t b;
    batch_init(&b, MAX_MSGS, MAX_LEN, 1);
    long n = do_recvmmsg(rx, &b, MAX_MSGS, MSG_DONTWAIT, NULL);
    if (n != 3) {
        THROW_ERROR("recvmmsg returns %ld instead of 3", n);
    }
    for (unsigned i = 0; i < 3; i++) {
        if (check_datagram(&b, i, 5, &tx_addr) < 0) {
            return -1;
        }
    }

    batch_init(&b, MAX_MSGS, MAX_LEN, 1);
    n = do_recvmmsg(rx, &b, MAX_MSGS, MSG_DONTWAIT, NULL);
    if (n != -1 || errno != EAGAIN) {
        THROW_ERROR("recvmmsg on an empty socket returns %ld, errno %d", n, errno);
    }
    close(rx);
    close(tx);
    return 0;
}

// The messages have no name if the call does not ask for it, and a vector that
// is shorter than the queue leaves the rest of it in order
static int test_udp_short_vector(void) {
    struct sockaddr_in rx_addr, tx_addr;
    int rx = udp_socket(&rx_addr), tx = udp_socket(&tx_addr);
    if (rx < 0 || tx < 0) {
        THROW_ERROR("failed to create the sockets");
    }
    for (unsigned i = 0; i < 3; i++) {
        if (send_datagram(tx, &rx_addr, i, 9) < 0) {
            return -1;
        }
    }
    settle();

    batch_t b;
    batch_init(&b, 2, MAX_LEN, 0);
    long n = do_recvmmsg(rx, &b, 2, MSG_DONTWAIT, NULL);
    if (n != 2 || check_datagram(&b, 0, 9, NULL) < 0 || check_datagram(&b, 1, 9, NULL) < 0) {
        THROW_ERROR("the first call returns %ld", n);
    }
    batch_init(&b, 2, MAX_LEN, 1);
    n = do_recvmmsg(rx, &b, 2, MSG_DONTWAIT, NULL);
    if (n != 1) {
        THROW_ERROR("the second call returns %ld instead of 1", n);
    }
    // The third datagram is the first one of this batch
    if (b.msgs[0].msg_len != 11 || b.bufs[0][0] != 'c') {
        THROW_ERROR("the third datagram is wrong");
    }
    close(rx);
    close(tx);
    return 0;
}

// A datagram that is longer than its buffer is cut, and the flag tells it
static int test_udp_truncation(void) {
    struct sockaddr_in rx_addr, tx_addr;
    int rx = udp_socket(&rx_addr), tx = udp_socket(&tx_addr);
    if (rx < 0 || tx < 0) {
        THROW_ERROR("failed to create the sockets");
    }
    if (send_datagram(tx, &rx_addr, 0, 20) < 0 || send_datagram(tx, &rx_addr, 1, 20) < 0) {
        return -1;
    }
    settle();

    batch_t b;
    batch_init(&b, 2, 8, 0);
    long n = do_recvmmsg(rx, &b, 2, MSG_DONTWAIT, NULL);
    if (n != 2 || b.msgs[0].msg_len != 8 || b.msgs[1].msg_len != 8) {
        THROW_ERROR("recvmmsg returns %ld, lengths %u and %u", n, b.msgs[0].msg_len,
                    b.msgs[1].msg_len);
    }
    if (!(b.msgs[0].msg_hdr.msg_flags & MSG_TRUNC) ||
            !(b.msgs[1].msg_hdr.msg_flags & MSG_TRUNC)) {
        THROW_ERROR("MSG_TRUNC is missing");
    }
    close(rx);
    close(tx);
    return 0;
}

// With MSG_WAITFORONE, only the first message waits: the call returns the
// datagrams that are there, as soon as there is one
static int test_udp_waitforone_queued(void) {
    struct sockaddr_in rx_addr, tx_addr;
    int rx = udp_socket(&rx_addr), tx = udp_socket(&tx_addr);
    if (rx < 0 || tx < 0) {
        THROW_ERROR("failed to create the sockets");
    }
    if (send_datagram(tx, &rx_addr, 0, 4) < 0 || send_datagram(tx, &rx_addr, 1, 4) < 0) {
        return -1;
    }
    settle();

    batch_t b;
    batch_init(&b, 4, MAX_LEN, 1);
    long n = do_recvmmsg(rx, &b, 4, MSG_WAITFORONE, NULL);
    if (n != 2 || check_datagram(&b, 0, 4, &tx_addr) < 0 ||
            check_datagram(&b, 1, 4, &tx_addr) < 0) {
        THROW_ERROR("recvmmsg returns %ld instead of 2", n);
    }
    close(rx);
    close(tx);
    return 0;
}

typedef struct {
    int fd;
    struct sockaddr_in to;
} sender_t;

static void *send_later(void *arg) {
    sender_t *s = arg;
    struct timespec delay = { 0, 200 * 1000 * 1000 };
    nanosleep(&delay, NULL);
    send_datagram(s->fd, &s->to, 0, 6);
    return NULL;
}

// With MSG_WAITFORONE and nothing queued, the call waits for the first
// datagram and returns with it, not with the whole vector
static int test_udp_waitforone_blocks(void) {
    struct sockaddr_in rx_addr, tx_addr;
    int rx = udp_socket(&rx_addr), tx = udp_socket(&tx_addr);
    if (rx < 0 || tx < 0) {
        THROW_ERROR("failed to create the sockets");
    }
    // A call that waits in vain fails after 5 s instead of hanging
    struct timeval limit = { 5, 0 };
    if (setsockopt(rx, SOL_SOCKET, SO_RCVTIMEO, &limit, sizeof(limit)) < 0) {
        THROW_ERROR("failed to set SO_RCVTIMEO");
    }
    sender_t s = { tx, rx_addr };
    pthread_t thread;
    if (pthread_create(&thread, NULL, send_later, &s) != 0) {
        THROW_ERROR("failed to create the thread");
    }

    batch_t b;
    batch_init(&b, 4, MAX_LEN, 1);
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    long n = do_recvmmsg(rx, &b, 4, MSG_WAITFORONE, NULL);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    pthread_join(thread, NULL);
    long ms = (t1.tv_sec - t0.tv_sec) * 1000 + (t1.tv_nsec - t0.tv_nsec) / 1000000;
    if (n != 1 || check_datagram(&b, 0, 6, &tx_addr) < 0) {
        THROW_ERROR("recvmmsg returns %ld instead of 1", n);
    }
    if (ms < 100) {
        THROW_ERROR("recvmmsg did not wait for the datagram (%ld ms)", ms);
    }
    close(rx);
    close(tx);
    return 0;
}

// The time of the call is checked after a message, as in Linux: the time that
// is left is written back, and a time of 0 ends the call after the first message
static int test_udp_timeout(void) {
    struct sockaddr_in rx_addr, tx_addr;
    int rx = udp_socket(&rx_addr), tx = udp_socket(&tx_addr);
    if (rx < 0 || tx < 0) {
        THROW_ERROR("failed to create the sockets");
    }
    for (unsigned i = 0; i < 2; i++) {
        if (send_datagram(tx, &rx_addr, i, 3) < 0) {
            return -1;
        }
    }
    settle();

    batch_t b;
    batch_init(&b, 2, MAX_LEN, 1);
    struct timespec timeout = { 5, 0 };
    long n = do_recvmmsg(rx, &b, 2, 0, &timeout);
    if (n != 2) {
        THROW_ERROR("recvmmsg with a time returns %ld instead of 2", n);
    }
    if (timeout.tv_sec > 5 || timeout.tv_sec < 3) {
        THROW_ERROR("the time that is left is %ld s", (long)timeout.tv_sec);
    }

    for (unsigned i = 0; i < 2; i++) {
        if (send_datagram(tx, &rx_addr, i, 3) < 0) {
            return -1;
        }
    }
    settle();
    batch_init(&b, 2, MAX_LEN, 1);
    timeout = (struct timespec){ 0, 0 };
    n = do_recvmmsg(rx, &b, 2, 0, &timeout);
    if (n != 1 || check_datagram(&b, 0, 3, &tx_addr) < 0) {
        THROW_ERROR("recvmmsg with the time 0 returns %ld instead of 1", n);
    }
    batch_init(&b, 2, MAX_LEN, 1);
    n = do_recvmmsg(rx, &b, 2, MSG_DONTWAIT, NULL);
    if (n != 1) {
        THROW_ERROR("the second datagram is gone (%ld)", n);
    }
    close(rx);
    close(tx);
    return 0;
}

// A connected TCP socket gives the bytes of its stream to the messages in
// turn, as recvmsg would
static int test_tcp_stream(void) {
    int listener = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t len = sizeof(addr);
    if (listener < 0 || bind(listener, (struct sockaddr *)&addr, sizeof(addr)) < 0 ||
            getsockname(listener, (struct sockaddr *)&addr, &len) < 0 ||
            listen(listener, 1) < 0) {
        THROW_ERROR("failed to create the listener");
    }
    int client = socket(AF_INET, SOCK_STREAM, 0);
    if (client < 0 || connect(client, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        THROW_ERROR("failed to connect");
    }
    int server = accept(listener, NULL, NULL);
    if (server < 0) {
        THROW_ERROR("failed to accept");
    }
    if (write(client, "abcdefgh", 8) != 8) {
        THROW_ERROR("failed to write");
    }
    // The bytes may arrive in pieces: wait until all of them can be read
    for (int tries = 0; tries < 100; tries++) {
        char peek[8];
        if (recv(server, peek, sizeof(peek), MSG_PEEK | MSG_DONTWAIT) == 8) {
            break;
        }
        usleep(10000);
    }

    batch_t b;
    batch_init(&b, 4, 3, 0);
    long n = do_recvmmsg(server, &b, 4, MSG_DONTWAIT, NULL);
    if (n != 3 || b.msgs[0].msg_len != 3 || b.msgs[1].msg_len != 3 || b.msgs[2].msg_len != 2) {
        THROW_ERROR("recvmmsg on the stream returns %ld", n);
    }
    if (memcmp(b.bufs[0], "abc", 3) != 0 || memcmp(b.bufs[1], "def", 3) != 0 ||
            memcmp(b.bufs[2], "gh", 2) != 0) {
        THROW_ERROR("recvmmsg on the stream gets wrong bytes");
    }
    n = do_recvmmsg(server, &b, 4, MSG_DONTWAIT, NULL);
    if (n != -1 || errno != EAGAIN) {
        THROW_ERROR("recvmmsg on the empty stream returns %ld, errno %d", n, errno);
    }
    close(server);
    close(client);
    close(listener);
    return 0;
}

// The errors of a call that cannot work
static int test_errors(void) {
    struct sockaddr_in addr;
    int fd = udp_socket(&addr);
    if (fd < 0) {
        THROW_ERROR("failed to create the socket");
    }
    batch_t b;
    batch_init(&b, 1, MAX_LEN, 0);

    long n = do_recvmmsg(fd, &b, 0, MSG_DONTWAIT, NULL);
    if (n != 0) {
        THROW_ERROR("recvmmsg with no messages returns %ld", n);
    }
    n = syscall(SYS_recvmmsg, 12345, b.msgs, 1, MSG_DONTWAIT, NULL);
    if (n != -1 || errno != EBADF) {
        THROW_ERROR("recvmmsg on a bad descriptor returns %ld, errno %d", n, errno);
    }
    n = syscall(SYS_recvmmsg, fd, NULL, 1, MSG_DONTWAIT, NULL);
    if (n != -1 || errno != EFAULT) {
        THROW_ERROR("recvmmsg without a vector returns %ld, errno %d", n, errno);
    }

    int fds[2];
    if (pipe(fds) < 0) {
        THROW_ERROR("failed to create a pipe");
    }
    n = do_recvmmsg(fds[0], &b, 1, MSG_DONTWAIT, NULL);
    if (n != -1 || errno != ENOTSOCK) {
        THROW_ERROR("recvmmsg on a pipe returns %ld, errno %d", n, errno);
    }
    n = do_recvmmsg(fds[0], &b, 0, MSG_DONTWAIT, NULL);
    if (n != -1 || errno != ENOTSOCK) {
        THROW_ERROR("recvmmsg on a pipe without messages returns %ld, errno %d", n, errno);
    }
    close(fds[0]);
    close(fds[1]);
    close(fd);
    return 0;
}

static test_case_t test_cases[] = {
    TEST_CASE(test_udp_batch),
    TEST_CASE(test_udp_short_vector),
    TEST_CASE(test_udp_truncation),
    TEST_CASE(test_udp_waitforone_queued),
    TEST_CASE(test_udp_waitforone_blocks),
    TEST_CASE(test_udp_timeout),
    TEST_CASE(test_tcp_stream),
    TEST_CASE(test_errors),
};

int main(void) {
    return test_suite_run(test_cases, ARRAY_SIZE(test_cases));
}
