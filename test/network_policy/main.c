// Tests the network policy of test/Occlum.json. It allows this program
// (/bin/network_policy) only the addresses below, and all other programs
// everything, including network_policy_generic, a copy of this program.
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <spawn.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>
#include "test.h"

// Allowed for bind and connect on 127.0.0.1 (TCP and SCTP)
#define ALLOWED_PORT        8443
// Allowed for bind and connect on 127.0.0.1 (UDP), up to ALLOWED_UDP_PORT + 1
#define ALLOWED_UDP_PORT    9000
#define DENIED_PORT         8080

#ifndef AF_VSOCK
#define AF_VSOCK            40
#endif
#ifndef NETLINK_ROUTE
#define NETLINK_ROUTE       0
#endif
#define SOL_SCTP                    132
#define SCTP_SOCKOPT_BINDX_ADD      100
#define SCTP_SOCKOPT_CONNECTX       110
#define SCTP_SOCKOPT_CONNECTX3      111
#define SCTP_DSTADDRV4              7

#define EXPECT_DENIED(expr) do { \
    errno = 0; \
    if ((expr) != -1 || errno != EACCES) { \
        THROW_ERROR("%s should fail with EACCES", #expr); \
    } \
} while (0)

#define EXPECT_NOT_DENIED(expr) do { \
    errno = 0; \
    if ((expr) == -1 && errno == EACCES) { \
        THROW_ERROR("%s should not be denied", #expr); \
    } \
} while (0)

// ============================================================================
// Helper functions
// ============================================================================

static struct sockaddr_in addr4(const char *ip, int port) {
    struct sockaddr_in addr = { .sin_family = AF_INET, .sin_port = htons(port) };
    inet_pton(AF_INET, ip, &addr.sin_addr);
    return addr;
}

static struct sockaddr_in6 addr6(const char *ip, int port) {
    struct sockaddr_in6 addr = { .sin6_family = AF_INET6, .sin6_port = htons(port) };
    inet_pton(AF_INET6, ip, &addr.sin6_addr);
    return addr;
}

#define SA(addr) ((struct sockaddr *)&(addr))

static int reusable_socket(int domain, int type, int protocol) {
    int fd = socket(domain, type, protocol);
    int one = 1;
    if (fd >= 0) {
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    }
    return fd;
}

// ============================================================================
// Test cases
// ============================================================================

static int test_bind() {
    int fd = reusable_socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr = addr4("127.0.0.1", DENIED_PORT);
    EXPECT_DENIED(bind(fd, SA(addr), sizeof(addr)));
    // 0.0.0.0 is an address of its own, not a wildcard
    addr = addr4("0.0.0.0", ALLOWED_PORT);
    EXPECT_DENIED(bind(fd, SA(addr), sizeof(addr)));
    addr = addr4("127.0.0.1", ALLOWED_PORT);
    if (bind(fd, SA(addr), sizeof(addr)) < 0) {
        THROW_ERROR("bind to an allowed address failed");
    }
    close(fd);

    // [::1]:ALLOWED_PORT is allowed, but the host may not support IPv6
    fd = reusable_socket(AF_INET6, SOCK_STREAM, 0);
    if (fd >= 0) {
        struct sockaddr_in6 addr = addr6("::1", DENIED_PORT);
        EXPECT_DENIED(bind(fd, SA(addr), sizeof(addr)));
        addr = addr6("::1", ALLOWED_PORT);
        EXPECT_NOT_DENIED(bind(fd, SA(addr), sizeof(addr)));
        close(fd);
    }
    return 0;
}

static int test_listen_without_bind() {
    // Listening binds the socket to a port chosen by the host
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    EXPECT_DENIED(listen(fd, 1));
    close(fd);
    return 0;
}

static int test_tcp_connect() {
    int listen_fd = reusable_socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr = addr4("127.0.0.1", ALLOWED_PORT);
    if (bind(listen_fd, SA(addr), sizeof(addr)) < 0 || listen(listen_fd, 4) < 0) {
        THROW_ERROR("listen on an allowed address failed");
    }

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (connect(fd, SA(addr), sizeof(addr)) < 0) {
        THROW_ERROR("connect to an allowed address failed");
    }
    int accepted_fd = accept(listen_fd, NULL, NULL);
    char buf[4] = { 0 };
    if (accepted_fd < 0 || write(fd, "ping", 4) != 4 || read(accepted_fd, buf, 4) != 4 ||
            memcmp(buf, "ping", 4) != 0) {
        THROW_ERROR("the connection to an allowed address does not work");
    }
    close(accepted_fd);
    close(fd);

    fd = socket(AF_INET, SOCK_STREAM, 0);
    addr = addr4("127.0.0.1", DENIED_PORT);
    EXPECT_DENIED(connect(fd, SA(addr), sizeof(addr)));
    close(fd);

    // IPv4-mapped IPv6 addresses are IPv4 addresses
    fd = socket(AF_INET6, SOCK_STREAM, 0);
    if (fd >= 0) {
        struct sockaddr_in6 addr = addr6("::ffff:127.0.0.1", DENIED_PORT);
        EXPECT_DENIED(connect(fd, SA(addr), sizeof(addr)));
        // Without the scope ID, which Linux accepts
        EXPECT_DENIED(connect(fd, SA(addr), 24));
        addr = addr6("::1", ALLOWED_PORT);
        EXPECT_DENIED(connect(fd, SA(addr), sizeof(addr)));
        addr = addr6("::ffff:127.0.0.1", ALLOWED_PORT);
        EXPECT_NOT_DENIED(connect(fd, SA(addr), sizeof(addr)));
        close(fd);
    }
    close(listen_fd);
    return 0;
}

static int test_udp() {
    int rx_fd = reusable_socket(AF_INET, SOCK_DGRAM, 0);
    struct sockaddr_in rx_addr = addr4("127.0.0.1", ALLOWED_UDP_PORT);
    if (bind(rx_fd, SA(rx_addr), sizeof(rx_addr)) < 0) {
        THROW_ERROR("bind to an allowed address failed");
    }

    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    char buf[1];
    if (sendto(fd, "a", 1, 0, SA(rx_addr), sizeof(rx_addr)) != 1 ||
            recv(rx_fd, buf, 1, 0) != 1) {
        THROW_ERROR("sending to an allowed address failed");
    }
    struct sockaddr_in addr = addr4("127.0.0.1", ALLOWED_UDP_PORT + 2);
    EXPECT_DENIED(sendto(fd, "a", 1, 0, SA(addr), sizeof(addr)));

    struct iovec iov = { .iov_base = "a", .iov_len = 1 };
    struct msghdr msg = {
        .msg_name = &addr, .msg_namelen = sizeof(addr), .msg_iov = &iov, .msg_iovlen = 1,
    };
    EXPECT_DENIED(sendmsg(fd, &msg, 0));
    struct mmsghdr mmsg = { .msg_hdr = msg };
    EXPECT_DENIED(sendmmsg(fd, &mmsg, 1, 0));

    // Linux takes the AF_UNSPEC address of an IPv4 socket for an IPv4 address
    addr = addr4("127.0.0.1", ALLOWED_UDP_PORT);
    addr.sin_family = AF_UNSPEC;
    EXPECT_DENIED(sendto(fd, "a", 1, 0, SA(addr), sizeof(addr)));

    // An IPv4 option, which may route through other addresses
    char control[CMSG_SPACE(4)] = { 0 };
    struct cmsghdr *cmsg = (struct cmsghdr *)control;
    cmsg->cmsg_level = IPPROTO_IP;
    cmsg->cmsg_type = IP_RETOPTS;
    cmsg->cmsg_len = CMSG_LEN(4);
    msg.msg_name = &rx_addr;
    msg.msg_control = control;
    msg.msg_controllen = sizeof(control);
    EXPECT_DENIED(sendmsg(fd, &msg, 0));
    close(fd);

    // A connected socket
    fd = socket(AF_INET, SOCK_DGRAM, 0);
    addr = addr4("127.0.0.1", ALLOWED_UDP_PORT + 2);
    EXPECT_DENIED(connect(fd, SA(addr), sizeof(addr)));
    if (connect(fd, SA(rx_addr), sizeof(rx_addr)) < 0 || send(fd, "b", 1, 0) != 1 ||
            recv(rx_fd, buf, 1, 0) != 1 || buf[0] != 'b') {
        THROW_ERROR("sending with a socket connected to an allowed address failed");
    }
    // Connecting to AF_UNSPEC dissolves the association
    struct sockaddr unspec = { .sa_family = AF_UNSPEC };
    EXPECT_NOT_DENIED(connect(fd, &unspec, sizeof(unspec)));
    close(fd);

    // A network pattern (connect sends no packet with UDP)
    fd = socket(AF_INET, SOCK_DGRAM, 0);
    addr = addr4("10.1.2.3", 53);
    EXPECT_NOT_DENIED(connect(fd, SA(addr), sizeof(addr)));
    addr = addr4("11.0.0.1", 53);
    EXPECT_DENIED(connect(fd, SA(addr), sizeof(addr)));
    addr = addr4("10.1.2.3", 54);
    EXPECT_DENIED(connect(fd, SA(addr), sizeof(addr)));
    close(fd);

    close(rx_fd);
    return 0;
}

static int test_sockets_needing_raw() {
    EXPECT_DENIED(socket(AF_INET, SOCK_RAW, IPPROTO_ICMP));
    EXPECT_DENIED(socket(AF_INET6, SOCK_RAW, IPPROTO_ICMPV6));
    EXPECT_DENIED(socket(AF_PACKET, SOCK_RAW, htons(0x0003)));
    EXPECT_DENIED(socket(AF_VSOCK, SOCK_STREAM, 0));

    // AF_NETLINK and AF_UNIX sockets are not restricted
    int fd = socket(AF_NETLINK, SOCK_RAW, NETLINK_ROUTE);
    if (fd < 0) {
        THROW_ERROR("netlink socket failed");
    }
    close(fd);
    int fds[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) < 0) {
        THROW_ERROR("socketpair failed");
    }
    close_files(2, fds[0], fds[1]);
    return 0;
}

static int test_socket_options() {
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    // IP options may route through other addresses
    char options[4] = { 1, 1, 1, 0 };
    EXPECT_DENIED(setsockopt(fd, IPPROTO_IP, IP_OPTIONS, options, sizeof(options)));
    int one = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0) {
        THROW_ERROR("setsockopt failed");
    }
    close(fd);
    return 0;
}

static int test_sctp() {
    int fd = reusable_socket(AF_INET, SOCK_SEQPACKET, IPPROTO_SCTP);
    if (fd < 0) {
        printf("SCTP is not supported, skipped\n");
        return 0;
    }
    // sctp_bindx()
    struct sockaddr_in addrs[2] = {
        addr4("127.0.0.1", ALLOWED_PORT), addr4("127.0.0.1", DENIED_PORT)
    };
    EXPECT_DENIED(setsockopt(fd, SOL_SCTP, SCTP_SOCKOPT_BINDX_ADD, addrs, sizeof(addrs)));
    if (setsockopt(fd, SOL_SCTP, SCTP_SOCKOPT_BINDX_ADD, addrs, sizeof(addrs[0])) < 0) {
        THROW_ERROR("sctp_bindx to an allowed address failed");
    }
    close(fd);

    // sctp_connectx()
    fd = socket(AF_INET, SOCK_SEQPACKET, IPPROTO_SCTP);
    EXPECT_DENIED(setsockopt(fd, SOL_SCTP, SCTP_SOCKOPT_CONNECTX, addrs, sizeof(addrs)));
    char buf[64] = { 0 };
    socklen_t len = sizeof(buf);
    errno = 0;
    if (getsockopt(fd, SOL_SCTP, SCTP_SOCKOPT_CONNECTX3, buf, &len) != -1 ||
            errno != ENOPROTOOPT) {
        THROW_ERROR("SCTP_SOCKOPT_CONNECTX3 should fail with ENOPROTOOPT");
    }

    // An additional destination address with the port of the allowed destination
    char control[CMSG_SPACE(sizeof(struct in_addr))] = { 0 };
    struct cmsghdr *cmsg = (struct cmsghdr *)control;
    cmsg->cmsg_level = IPPROTO_SCTP;
    cmsg->cmsg_type = SCTP_DSTADDRV4;
    cmsg->cmsg_len = CMSG_LEN(sizeof(struct in_addr));
    inet_pton(AF_INET, "127.0.0.2", CMSG_DATA(cmsg));
    struct iovec iov = { .iov_base = "a", .iov_len = 1 };
    struct msghdr msg = {
        .msg_name = &addrs[0], .msg_namelen = sizeof(addrs[0]), .msg_iov = &iov, .msg_iovlen = 1,
        .msg_control = control, .msg_controllen = sizeof(control),
    };
    EXPECT_DENIED(sendmsg(fd, &msg, 0));
    close(fd);
    return 0;
}

// Returns the errno of connecting a child process to 127.0.0.1:DENIED_PORT
static int child_connect_errno(const char *path) {
    char *argv[] = { (char *)path, "try_connect", NULL };
    int pid, status;
    if (posix_spawn(&pid, path, NULL, NULL, argv, NULL) != 0 ||
            waitpid(pid, &status, 0) != pid || !WIFEXITED(status)) {
        return -1;
    }
    return WEXITSTATUS(status);
}

static int test_program_rules() {
    // Another program, to which the rules without a program apply
    int err = child_connect_errno("/bin/network_policy_generic");
    if (err == EACCES || err < 0) {
        THROW_ERROR("network_policy_generic should not be denied, errno = %d", err);
    }
    // This program with other paths
    if ((err = child_connect_errno("/bin/../bin/./network_policy")) != EACCES) {
        THROW_ERROR("/bin/../bin/./network_policy should be denied, errno = %d", err);
    }
    if (chdir("/bin") < 0) {
        THROW_ERROR("chdir failed");
    }
    err = child_connect_errno("network_policy");
    if (chdir("/") < 0 || err != EACCES) {
        THROW_ERROR("network_policy in /bin should be denied, errno = %d", err);
    }
    return 0;
}

// ============================================================================
// Test suite main
// ============================================================================

static test_case_t test_cases[] = {
    TEST_CASE(test_bind),
    TEST_CASE(test_listen_without_bind),
    TEST_CASE(test_tcp_connect),
    TEST_CASE(test_udp),
    TEST_CASE(test_sockets_needing_raw),
    TEST_CASE(test_socket_options),
    TEST_CASE(test_sctp),
    TEST_CASE(test_program_rules),
};

int main(int argc, const char *argv[]) {
    if (argc > 1 && strcmp(argv[1], "try_connect") == 0) {
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        struct sockaddr_in addr = addr4("127.0.0.1", DENIED_PORT);
        return connect(fd, SA(addr), sizeof(addr)) == 0 ? 0 : errno;
    }
    return test_suite_run(test_cases, ARRAY_SIZE(test_cases));
}
