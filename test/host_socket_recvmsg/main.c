#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "test.h"

// ============================================================================
// recvmsg() on the sockets of the host: the name of the message, which the
// call needs not to have
// ============================================================================

#define NAME_FILL   0x55

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

// A connected pair of TCP sockets on the loopback
static int tcp_pair(int *client, int *server) {
    int listener = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t len = sizeof(addr);
    if (listener < 0 || bind(listener, (struct sockaddr *)&addr, sizeof(addr)) < 0 ||
            getsockname(listener, (struct sockaddr *)&addr, &len) < 0 ||
            listen(listener, 1) < 0) {
        return -1;
    }
    *client = socket(AF_INET, SOCK_STREAM, 0);
    if (*client < 0 || connect(*client, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        return -1;
    }
    *server = accept(listener, NULL, NULL);
    close(listener);
    return *server < 0 ? -1 : 0;
}

// Lets what was sent reach the queue: the delivery may finish a little after the send
static void settle(void) {
    usleep(50 * 1000);
}

// The message of a stream socket is given to recvmsg again: the first call has set its length of
// the name to 0, as a stream socket has no address, and Linux takes the message the second time
static int test_stream_message_is_reused(void) {
    int client, server;
    if (tcp_pair(&client, &server) < 0) {
        THROW_ERROR("failed to create the sockets");
    }
    if (write(client, "abcdef", 6) != 6) {
        THROW_ERROR("failed to write");
    }
    settle();

    char buf[3];
    struct sockaddr_in name;
    struct iovec iov = { buf, sizeof(buf) };
    struct msghdr msg;
    memset(&msg, 0, sizeof(msg));
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_name = &name;
    msg.msg_namelen = sizeof(name);

    for (int i = 0; i < 2; i++) {
        memset(buf, 0, sizeof(buf));
        ssize_t n = recvmsg(server, &msg, MSG_DONTWAIT);
        if (n != 3 || memcmp(buf, i == 0 ? "abc" : "def", 3) != 0) {
            THROW_ERROR("call %d of recvmsg returns %zd, errno %d", i, n, errno);
        }
        if (msg.msg_namelen != 0) {
            THROW_ERROR("call %d leaves the name length %u instead of 0", i, msg.msg_namelen);
        }
    }
    close(client);
    close(server);
    return 0;
}

// The same with recvmmsg, whose messages the program gives back to it
static int test_stream_messages_are_reused_by_recvmmsg(void) {
    int client, server;
    if (tcp_pair(&client, &server) < 0) {
        THROW_ERROR("failed to create the sockets");
    }
    if (write(client, "abcdefgh", 8) != 8) {
        THROW_ERROR("failed to write");
    }
    settle();

    char bufs[2][2];
    struct sockaddr_in names[2];
    struct iovec iov[2];
    struct mmsghdr msgs[2];
    memset(msgs, 0, sizeof(msgs));
    for (int i = 0; i < 2; i++) {
        iov[i].iov_base = bufs[i];
        iov[i].iov_len = sizeof(bufs[i]);
        msgs[i].msg_hdr.msg_iov = &iov[i];
        msgs[i].msg_hdr.msg_iovlen = 1;
        msgs[i].msg_hdr.msg_name = &names[i];
        msgs[i].msg_hdr.msg_namelen = sizeof(names[i]);
    }
    for (int call = 0; call < 2; call++) {
        long n = syscall(SYS_recvmmsg, server, msgs, 2, MSG_DONTWAIT, NULL);
        if (n != 2 || memcmp(bufs[0], call == 0 ? "ab" : "ef", 2) != 0 ||
                memcmp(bufs[1], call == 0 ? "cd" : "gh", 2) != 0) {
            THROW_ERROR("call %d of recvmmsg returns %ld, errno %d", call, n, errno);
        }
    }
    close(client);
    close(server);
    return 0;
}

// A name that has no room: the call takes the datagram, writes nothing to the name and tells the
// length that the address needs
static int test_name_without_room(void) {
    struct sockaddr_in rx_addr, tx_addr;
    int rx = udp_socket(&rx_addr), tx = udp_socket(&tx_addr);
    if (rx < 0 || tx < 0) {
        THROW_ERROR("failed to create the sockets");
    }
    if (sendto(tx, "hello", 5, 0, (struct sockaddr *)&rx_addr, sizeof(rx_addr)) != 5) {
        THROW_ERROR("sendto failed");
    }
    settle();

    char buf[16];
    char name[32];
    memset(name, NAME_FILL, sizeof(name));
    struct iovec iov = { buf, sizeof(buf) };
    struct msghdr msg;
    memset(&msg, 0, sizeof(msg));
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_name = name;
    msg.msg_namelen = 0;
    ssize_t n = recvmsg(rx, &msg, MSG_DONTWAIT);
    if (n != 5 || memcmp(buf, "hello", 5) != 0) {
        THROW_ERROR("recvmsg returns %zd, errno %d", n, errno);
    }
    if (msg.msg_namelen != sizeof(struct sockaddr_in)) {
        THROW_ERROR("the length of the address is %u instead of %zu", msg.msg_namelen,
                    sizeof(struct sockaddr_in));
    }
    for (size_t i = 0; i < sizeof(name); i++) {
        if (name[i] != NAME_FILL) {
            THROW_ERROR("the name was written at %zu", i);
        }
    }
    close(rx);
    close(tx);
    return 0;
}

// No name with a length: the length does not matter, and the call leaves it alone
static int test_length_without_name(void) {
    struct sockaddr_in rx_addr, tx_addr;
    int rx = udp_socket(&rx_addr), tx = udp_socket(&tx_addr);
    if (rx < 0 || tx < 0) {
        THROW_ERROR("failed to create the sockets");
    }
    if (sendto(tx, "hello", 5, 0, (struct sockaddr *)&rx_addr, sizeof(rx_addr)) != 5) {
        THROW_ERROR("sendto failed");
    }
    settle();

    char buf[16];
    struct iovec iov = { buf, sizeof(buf) };
    struct msghdr msg;
    memset(&msg, 0, sizeof(msg));
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_name = NULL;
    msg.msg_namelen = 77;
    ssize_t n = recvmsg(rx, &msg, MSG_DONTWAIT);
    if (n != 5 || msg.msg_namelen != 77) {
        THROW_ERROR("UDP: recvmsg returns %zd, errno %d, length %u", n, errno, msg.msg_namelen);
    }
    close(rx);
    close(tx);

    int client, server;
    if (tcp_pair(&client, &server) < 0) {
        THROW_ERROR("failed to create the sockets");
    }
    if (write(client, "hello", 5) != 5) {
        THROW_ERROR("failed to write");
    }
    settle();
    msg.msg_namelen = 77;
    n = recvmsg(server, &msg, MSG_DONTWAIT);
    if (n != 5 || msg.msg_namelen != 77) {
        THROW_ERROR("TCP: recvmsg returns %zd, errno %d, length %u", n, errno, msg.msg_namelen);
    }
    close(client);
    close(server);
    return 0;
}

static test_case_t test_cases[] = {
    TEST_CASE(test_stream_message_is_reused),
    TEST_CASE(test_stream_messages_are_reused_by_recvmmsg),
    TEST_CASE(test_name_without_room),
    TEST_CASE(test_length_without_name),
};

int main(void) {
    return test_suite_run(test_cases, ARRAY_SIZE(test_cases));
}
