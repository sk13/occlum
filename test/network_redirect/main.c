// Tests the redirects of the network policy of test/Occlum.json, which
// replace the TCP sockets of this program (/bin/network_redirect) with Unix
// sockets for these addresses:
//
// * bind and connect to 127.0.0.1:9100: /tmp/network_redirect_9100.sock
// * bind and connect to [::1]:9100: the abstract name "network-redirect-v6"
// * connect to 10.0.0.0/8: the abstract name "network-redirect-<ip>-<port>"
// * bind to 127.0.0.1:9200: the abstract name "network-redirect-9200"
//
// The program may also bind and connect to 127.0.0.1:9200, e.g., with UDP.
// All other programs may use all addresses, but have no redirects, including
// network_redirect_generic, a copy of this program.
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <pthread.h>
#include <spawn.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>
#include "test.h"

#define REDIRECTED_PORT     9100
#define REDIRECTED_PATH     "/tmp/network_redirect_9100.sock"
#define HOST_PORT           9200
#define DENIED_PORT         9101
#define EPHEMERAL_FIRST     32768

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

// Checks that the IPv4 address has the IP address and port, or a port of the
// ephemeral range if port is 0
static int check_addr4(const struct sockaddr_storage *storage, socklen_t len,
                       const char *ip, int port) {
    const struct sockaddr_in *addr = (const struct sockaddr_in *)storage;
    char text[INET_ADDRSTRLEN] = "";
    if (len != sizeof(*addr) || addr->sin_family != AF_INET) {
        THROW_ERROR("not an IPv4 address (family %d, length %u)", storage->ss_family, len);
    }
    inet_ntop(AF_INET, &addr->sin_addr, text, sizeof(text));
    int actual_port = ntohs(addr->sin_port);
    if (strcmp(text, ip) != 0 ||
            (port ? actual_port != port : actual_port < EPHEMERAL_FIRST)) {
        THROW_ERROR("address %s:%d, expected %s:%d", text, actual_port, ip, port);
    }
    return 0;
}

static int check_sockname(int fd, const char *ip, int port) {
    struct sockaddr_storage storage;
    socklen_t len = sizeof(storage);
    if (getsockname(fd, (struct sockaddr *)&storage, &len) < 0) {
        THROW_ERROR("getsockname failed");
    }
    return check_addr4(&storage, len, ip, port);
}

static int check_peername(int fd, const char *ip, int port) {
    struct sockaddr_storage storage;
    socklen_t len = sizeof(storage);
    if (getpeername(fd, (struct sockaddr *)&storage, &len) < 0) {
        THROW_ERROR("getpeername failed");
    }
    return check_addr4(&storage, len, ip, port);
}

static int get_int_opt(int fd, int level, int optname) {
    int value = -1;
    socklen_t len = sizeof(value);
    if (getsockopt(fd, level, optname, &value, &len) < 0 || len != sizeof(value)) {
        return -1;
    }
    return value;
}

// Sends a message from one socket and receives it with the other
static int exchange(int from, int to, const char *msg) {
    char buf[32] = "";
    if (write(from, msg, strlen(msg)) != (ssize_t)strlen(msg)) {
        THROW_ERROR("write failed");
    }
    if (read(to, buf, sizeof(buf) - 1) != (ssize_t)strlen(msg) || strcmp(buf, msg) != 0) {
        THROW_ERROR("read \"%s\", expected \"%s\"", buf, msg);
    }
    return 0;
}

static int listen_redirected(void) {
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    int one = 1;
    struct sockaddr_in addr = addr4("127.0.0.1", REDIRECTED_PORT);
    if (fd < 0 || setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0 ||
            bind(fd, SA(addr), sizeof(addr)) < 0 || listen(fd, 8) < 0) {
        return -1;
    }
    return fd;
}

// ============================================================================
// Test cases
// ============================================================================

static int test_bind_and_connect() {
    int listen_fd = listen_redirected();
    if (listen_fd < 0) {
        THROW_ERROR("listening on the redirected address failed");
    }
    if (access(REDIRECTED_PATH, F_OK) < 0) {
        THROW_ERROR("the redirect did not create %s", REDIRECTED_PATH);
    }
    // The socket at the fd is replaced, but still close-on-exec
    if (!(fcntl(listen_fd, F_GETFD) & FD_CLOEXEC)) {
        THROW_ERROR("the replaced socket lost FD_CLOEXEC");
    }
    if (check_sockname(listen_fd, "127.0.0.1", REDIRECTED_PORT) < 0) {
        THROW_ERROR("wrong local address of the listening socket");
    }
    if (get_int_opt(listen_fd, SOL_SOCKET, SO_DOMAIN) != AF_INET ||
            get_int_opt(listen_fd, SOL_SOCKET, SO_TYPE) != SOCK_STREAM ||
            get_int_opt(listen_fd, SOL_SOCKET, SO_PROTOCOL) != IPPROTO_TCP ||
            get_int_opt(listen_fd, SOL_SOCKET, SO_ACCEPTCONN) != 1) {
        THROW_ERROR("the listening socket does not look like a TCP socket");
    }

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    int one = 1;
    struct sockaddr_in addr = addr4("127.0.0.1", REDIRECTED_PORT);
    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) < 0) {
        THROW_ERROR("setting TCP_NODELAY failed");
    }
    if (connect(fd, SA(addr), sizeof(addr)) < 0) {
        THROW_ERROR("connecting to the redirected address failed");
    }
    // Options of the IP levels are accepted after the redirect, too
    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) < 0 ||
            get_int_opt(fd, IPPROTO_TCP, TCP_NODELAY) != 0) {
        THROW_ERROR("TCP_NODELAY of the redirected socket failed");
    }
    if (check_peername(fd, "127.0.0.1", REDIRECTED_PORT) < 0 ||
            check_sockname(fd, "127.0.0.1", 0) < 0) {
        THROW_ERROR("wrong addresses of the connected socket");
    }

    struct sockaddr_storage peer;
    socklen_t peer_len = sizeof(peer);
    int accepted_fd = accept(listen_fd, (struct sockaddr *)&peer, &peer_len);
    if (accepted_fd < 0) {
        THROW_ERROR("accept failed");
    }
    if (check_addr4(&peer, peer_len, "127.0.0.1", 0) < 0 ||
            check_sockname(accepted_fd, "127.0.0.1", REDIRECTED_PORT) < 0 ||
            check_peername(accepted_fd, "127.0.0.1", 0) < 0) {
        THROW_ERROR("wrong addresses of the accepted socket");
    }
    if (exchange(fd, accepted_fd, "ping") < 0 || exchange(accepted_fd, fd, "pong") < 0) {
        THROW_ERROR("exchanging data failed");
    }
    close(accepted_fd);
    close(fd);

    // Nothing listens once the listening socket is closed
    close(listen_fd);
    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (connect(fd, SA(addr), sizeof(addr)) == 0 || errno != ECONNREFUSED) {
        THROW_ERROR("connect without a listener should fail with ECONNREFUSED");
    }
    close(fd);
    return 0;
}

static int test_unix_listener() {
    // A Unix socket listens on the abstract name of the redirect for
    // 10.1.2.3:8080
    int unix_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    struct sockaddr_un unix_addr = { .sun_family = AF_UNIX };
    const char *name = "network-redirect-10.1.2.3-8080";
    memcpy(unix_addr.sun_path + 1, name, strlen(name));
    socklen_t unix_len = offsetof(struct sockaddr_un, sun_path) + 1 + strlen(name);
    if (bind(unix_fd, (struct sockaddr *)&unix_addr, unix_len) < 0 ||
            listen(unix_fd, 4) < 0) {
        THROW_ERROR("listening on the Unix socket failed");
    }

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr = addr4("10.1.2.3", 8080);
    if (connect(fd, SA(addr), sizeof(addr)) < 0) {
        THROW_ERROR("connecting to 10.1.2.3:8080 failed");
    }
    if (check_peername(fd, "10.1.2.3", 8080) < 0) {
        THROW_ERROR("wrong peer address");
    }
    int accepted_fd = accept(unix_fd, NULL, NULL);
    if (accepted_fd < 0) {
        THROW_ERROR("accept on the Unix socket failed");
    }
    if (exchange(fd, accepted_fd, "hello") < 0 || exchange(accepted_fd, fd, "world") < 0) {
        THROW_ERROR("exchanging data with the Unix socket failed");
    }
    close(accepted_fd);
    close(fd);
    close(unix_fd);

    // No listener for 10.1.2.3:8081
    fd = socket(AF_INET, SOCK_STREAM, 0);
    addr = addr4("10.1.2.3", 8081);
    if (connect(fd, SA(addr), sizeof(addr)) == 0 || errno != ECONNREFUSED) {
        THROW_ERROR("connect to 10.1.2.3:8081 should fail with ECONNREFUSED");
    }
    close(fd);
    return 0;
}

static int test_ipv6() {
    // The host may not support IPv6
    int listen_fd = socket(AF_INET6, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        return 0;
    }
    struct sockaddr_in6 addr = addr6("::1", REDIRECTED_PORT);
    if (bind(listen_fd, SA(addr), sizeof(addr)) < 0 || listen(listen_fd, 4) < 0) {
        THROW_ERROR("listening on [::1]:%d failed", REDIRECTED_PORT);
    }
    int fd = socket(AF_INET6, SOCK_STREAM, 0);
    if (connect(fd, SA(addr), sizeof(addr)) < 0) {
        THROW_ERROR("connecting to [::1]:%d failed", REDIRECTED_PORT);
    }
    struct sockaddr_in6 peer;
    socklen_t len = sizeof(peer);
    if (getpeername(fd, SA(peer), &len) < 0 || len != sizeof(peer) ||
            peer.sin6_family != AF_INET6 || ntohs(peer.sin6_port) != REDIRECTED_PORT ||
            memcmp(&peer.sin6_addr, &addr.sin6_addr, sizeof(addr.sin6_addr)) != 0) {
        THROW_ERROR("wrong peer address of the IPv6 socket");
    }
    if (get_int_opt(fd, SOL_SOCKET, SO_DOMAIN) != AF_INET6) {
        THROW_ERROR("the IPv6 socket has the wrong domain");
    }
    int accepted_fd = accept(listen_fd, NULL, NULL);
    if (accepted_fd < 0 || exchange(fd, accepted_fd, "six") < 0) {
        THROW_ERROR("IPv6 connection failed");
    }
    close(accepted_fd);
    close(fd);
    close(listen_fd);
    return 0;
}

static int test_nonblocking() {
    int listen_fd = listen_redirected();
    if (listen_fd < 0) {
        THROW_ERROR("listening on the redirected address failed");
    }
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    struct sockaddr_in addr = addr4("127.0.0.1", REDIRECTED_PORT);
    if (connect(fd, SA(addr), sizeof(addr)) < 0 && errno != EINPROGRESS) {
        THROW_ERROR("nonblocking connect failed");
    }
    if (!(fcntl(fd, F_GETFL) & O_NONBLOCK)) {
        THROW_ERROR("the replaced socket is not nonblocking");
    }
    struct pollfd pfd = { .fd = fd, .events = POLLOUT };
    if (poll(&pfd, 1, 1000) != 1 || !(pfd.revents & POLLOUT) ||
            get_int_opt(fd, SOL_SOCKET, SO_ERROR) != 0) {
        THROW_ERROR("the connected socket is not writable");
    }
    char buf[8];
    if (read(fd, buf, sizeof(buf)) != -1 || errno != EAGAIN) {
        THROW_ERROR("read without data should fail with EAGAIN");
    }

    // An epoll file monitors the socket after the redirect
    int accepted_fd = accept(listen_fd, NULL, NULL);
    int epfd = epoll_create1(0);
    struct epoll_event event = { .events = EPOLLIN, .data.fd = fd };
    if (accepted_fd < 0 || epfd < 0 || epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &event) < 0) {
        THROW_ERROR("setting up epoll failed");
    }
    if (write(accepted_fd, "x", 1) != 1 || epoll_wait(epfd, &event, 1, 1000) != 1 ||
            event.data.fd != fd || !(event.events & EPOLLIN)) {
        THROW_ERROR("epoll did not report the data");
    }
    close(epfd);
    close(accepted_fd);
    close(fd);
    close(listen_fd);
    return 0;
}

// Waits up to 3 s until the epoll file reports the events for the fd, and
// skips other events, e.g., EPOLLHUP of an unconnected socket, which Linux
// reports, too
static int wait_for_events(int epfd, int fd, uint32_t events) {
    for (int i = 0; i < 30; i++) {
        struct epoll_event event;
        int n = epoll_wait(epfd, &event, 1, 100);
        if (n == 1 && event.data.fd == fd && (event.events & events) == events) {
            return 0;
        }
    }
    return -1;
}

static int wait_epfd;
static int wait_fd;

// Waits for a connection like a thread of an event loop
static void *wait_for_connection(void *arg) {
    return (void *)(long)wait_for_events(wait_epfd, wait_fd, EPOLLIN);
}

static int test_epoll_before_redirect() {
    // Like Boost.Asio: the sockets are added to an epoll file, edge-triggered,
    // before they are bound or connected
    int epfd = epoll_create1(0);
    int listen_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    struct epoll_event event = { .events = EPOLLIN | EPOLLET, .data.fd = listen_fd };
    if (epfd < 0 || listen_fd < 0 || epoll_ctl(epfd, EPOLL_CTL_ADD, listen_fd, &event) < 0) {
        THROW_ERROR("adding the socket to the epoll file failed");
    }
    // A thread waits already when the socket is replaced
    pthread_t thread;
    wait_epfd = epfd;
    wait_fd = listen_fd;
    if (pthread_create(&thread, NULL, wait_for_connection, NULL) != 0) {
        THROW_ERROR("pthread_create failed");
    }
    usleep(100 * 1000);
    struct sockaddr_in addr = addr4("127.0.0.1", REDIRECTED_PORT);
    if (bind(listen_fd, SA(addr), sizeof(addr)) < 0 || listen(listen_fd, 4) < 0) {
        THROW_ERROR("listening on the redirected address failed");
    }

    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    event.events = EPOLLIN | EPOLLOUT | EPOLLET;
    event.data.fd = fd;
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &event) < 0) {
        THROW_ERROR("adding the client socket to the epoll file failed");
    }
    if (connect(fd, SA(addr), sizeof(addr)) < 0 && errno != EINPROGRESS) {
        THROW_ERROR("connecting to the redirected address failed");
    }
    void *ret;
    if (pthread_join(thread, &ret) != 0 || ret != NULL) {
        THROW_ERROR("the waiting thread got no EPOLLIN of the listening socket");
    }

    // The client socket is reported writable, and readable once data arrives
    if (wait_for_events(epfd, fd, EPOLLOUT) < 0) {
        THROW_ERROR("no EPOLLOUT of the connected socket");
    }
    int accepted_fd = accept(listen_fd, NULL, NULL);
    if (accepted_fd < 0 || write(accepted_fd, "y", 1) != 1) {
        THROW_ERROR("accepting or writing failed");
    }
    if (wait_for_events(epfd, fd, EPOLLIN) < 0) {
        THROW_ERROR("no EPOLLIN of the connected socket");
    }
    // The replaced socket is still registered: EPOLL_CTL_MOD works
    event.events = EPOLLIN;
    if (epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &event) < 0) {
        THROW_ERROR("EPOLL_CTL_MOD of the replaced socket failed");
    }
    close(accepted_fd);
    close(fd);
    close(listen_fd);
    close(epfd);
    return 0;
}

static volatile int stop_waiting;

// Waits on the epoll file in a loop, like the thread of an event loop, so
// that the replacements of sockets race with its epoll_wait()
static void *wait_in_loop(void *arg) {
    struct epoll_event events[8];
    while (!stop_waiting) {
        epoll_wait(wait_epfd, events, 8, 1);
    }
    return NULL;
}

static int test_epoll_replace_races() {
    // A socket replaced while another thread waits on the epoll file, then
    // modified and closed right away: EPOLL_CTL_MOD must find it, the closed
    // socket must not stay monitored (the next socket at the same fd gets
    // EEXIST from EPOLL_CTL_ADD then), and the peer must get end of file
    int listen_fd = listen_redirected();
    int epfd = epoll_create1(0);
    if (listen_fd < 0 || epfd < 0) {
        THROW_ERROR("listening or creating the epoll file failed");
    }
    pthread_t thread;
    wait_epfd = epfd;
    stop_waiting = 0;
    if (pthread_create(&thread, NULL, wait_in_loop, NULL) != 0) {
        THROW_ERROR("pthread_create failed");
    }

    int ret = 0;
    struct sockaddr_in addr = addr4("127.0.0.1", REDIRECTED_PORT);
    for (int i = 0; i < 300 && ret == 0; i++) {
        int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
        struct epoll_event event = { .events = EPOLLIN | EPOLLOUT | EPOLLET, .data.fd = fd };
        if (fd < 0 || epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &event) < 0) {
            printf("round %d: adding the socket failed: %s\n", i, strerror(errno));
            ret = -1;
            break;
        }
        if (connect(fd, SA(addr), sizeof(addr)) < 0 && errno != EINPROGRESS) {
            printf("round %d: connect failed: %s\n", i, strerror(errno));
            ret = -1;
        }
        event.events = EPOLLIN | EPOLLET;
        if (ret == 0 && epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &event) < 0) {
            printf("round %d: EPOLL_CTL_MOD failed: %s\n", i, strerror(errno));
            ret = -1;
        }
        close(fd);

        int accepted_fd = accept(listen_fd, NULL, NULL);
        struct pollfd pfd = { .fd = accepted_fd, .events = POLLIN };
        char c;
        if (accepted_fd < 0 || poll(&pfd, 1, 2000) != 1 || read(accepted_fd, &c, 1) != 0) {
            printf("round %d: no end of file at the peer\n", i);
            ret = -1;
        }
        close(accepted_fd);
    }

    stop_waiting = 1;
    pthread_join(thread, NULL);
    close(epfd);
    close(listen_fd);
    if (ret < 0) {
        THROW_ERROR("a replaced socket raced with epoll");
    }
    return 0;
}

static int test_other_sockets_and_addresses() {
    // Without a redirect or a bind pattern, binding is still denied
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr = addr4("127.0.0.1", DENIED_PORT);
    if (bind(fd, SA(addr), sizeof(addr)) == 0 || errno != EACCES) {
        THROW_ERROR("bind to 127.0.0.1:%d should fail with EACCES", DENIED_PORT);
    }
    if (connect(fd, SA(addr), sizeof(addr)) == 0 || errno != EACCES) {
        THROW_ERROR("connect to 127.0.0.1:%d should fail with EACCES", DENIED_PORT);
    }
    close(fd);

    // Redirects apply to TCP only: a UDP socket binds on the host
    fd = socket(AF_INET, SOCK_DGRAM, 0);
    addr = addr4("127.0.0.1", HOST_PORT);
    if (bind(fd, SA(addr), sizeof(addr)) < 0) {
        THROW_ERROR("binding a UDP socket to 127.0.0.1:%d failed", HOST_PORT);
    }
    if (check_sockname(fd, "127.0.0.1", HOST_PORT) < 0) {
        THROW_ERROR("wrong local address of the UDP socket");
    }
    close(fd);
    // ... and a UDP socket may not connect to 10.1.2.3:53, which a redirect
    // matches, but no connect pattern
    fd = socket(AF_INET, SOCK_DGRAM, 0);
    addr = addr4("10.1.2.3", 53);
    if (connect(fd, SA(addr), sizeof(addr)) == 0 || errno != EACCES) {
        THROW_ERROR("connecting a UDP socket to 10.1.2.3:53 should fail with EACCES");
    }
    close(fd);

    // A TCP socket bound to 127.0.0.1:HOST_PORT is redirected, not bound on
    // the host
    fd = socket(AF_INET, SOCK_STREAM, 0);
    addr = addr4("127.0.0.1", HOST_PORT);
    if (bind(fd, SA(addr), sizeof(addr)) < 0 || listen(fd, 1) < 0) {
        THROW_ERROR("binding a TCP socket to 127.0.0.1:%d failed", HOST_PORT);
    }
    struct sockaddr_un unix_addr;
    socklen_t len = sizeof(unix_addr);
    int unix_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    const char *name = "network-redirect-9200";
    memset(&unix_addr, 0, sizeof(unix_addr));
    unix_addr.sun_family = AF_UNIX;
    memcpy(unix_addr.sun_path + 1, name, strlen(name));
    len = offsetof(struct sockaddr_un, sun_path) + 1 + strlen(name);
    if (connect(unix_fd, (struct sockaddr *)&unix_addr, len) < 0) {
        THROW_ERROR("the redirected socket does not listen on the abstract name");
    }
    close(unix_fd);
    close(fd);
    return 0;
}

// Returns the errno of connecting a child process to the redirected address
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
    int listen_fd = listen_redirected();
    if (listen_fd < 0) {
        THROW_ERROR("listening on the redirected address failed");
    }
    // This program in a child process reaches the redirected socket
    int err = child_connect_errno("/bin/network_redirect");
    if (err != 0) {
        THROW_ERROR("network_redirect in a child failed with errno %d", err);
    }
    // Another program connects on the host, where nothing listens
    err = child_connect_errno("/bin/network_redirect_generic");
    if (err != ECONNREFUSED) {
        THROW_ERROR("network_redirect_generic should get ECONNREFUSED, errno = %d", err);
    }
    close(listen_fd);
    return 0;
}

// ============================================================================
// Test suite main
// ============================================================================

static test_case_t test_cases[] = {
    TEST_CASE(test_bind_and_connect),
    TEST_CASE(test_unix_listener),
    TEST_CASE(test_ipv6),
    TEST_CASE(test_nonblocking),
    TEST_CASE(test_epoll_before_redirect),
    TEST_CASE(test_epoll_replace_races),
    TEST_CASE(test_other_sockets_and_addresses),
    TEST_CASE(test_program_rules),
};

int main(int argc, const char *argv[]) {
    if (argc > 1 && strcmp(argv[1], "try_connect") == 0) {
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        struct sockaddr_in addr = addr4("127.0.0.1", REDIRECTED_PORT);
        return connect(fd, SA(addr), sizeof(addr)) == 0 ? 0 : errno;
    }
    return test_suite_run(test_cases, ARRAY_SIZE(test_cases));
}
