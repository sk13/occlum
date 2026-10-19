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
#include <stdatomic.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <time.h>
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

static long now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
}

static int set_timeout_ms(int fd, int optname, long ms) {
    struct timeval tv = { ms / 1000, (ms % 1000) * 1000 };
    return setsockopt(fd, SOL_SOCKET, optname, &tv, sizeof(tv));
}

// Returns the timeout that SO_RCVTIMEO or SO_SNDTIMEO of the socket has, in ms, or -1
static long get_timeout_ms(int fd, int optname) {
    struct timeval tv = { -1, -1 };
    socklen_t len = sizeof(tv);
    if (getsockopt(fd, SOL_SOCKET, optname, &tv, &len) < 0 || len != sizeof(tv)) {
        return -1;
    }
    return tv.tv_sec * 1000L + tv.tv_usec / 1000;
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

    // TCP has no files: nothing listens either if the file of the Unix socket is gone
    unlink(REDIRECTED_PATH);
    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (connect(fd, SA(addr), sizeof(addr)) == 0 || errno != ECONNREFUSED) {
        THROW_ERROR("connect without a socket file should fail with ECONNREFUSED");
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

// A socket that replaces a TCP socket takes the options of all levels, and
// returns the options of SOL_SOCKET that it was given, like a TCP socket. It
// does not fail for the options that a Unix socket would refuse, e.g., those of
// the IP levels and SO_REUSEPORT.
static int test_socket_options() {
    int listen_fd = listen_redirected();
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr = addr4("127.0.0.1", REDIRECTED_PORT);
    if (listen_fd < 0 || fd < 0 || connect(fd, SA(addr), sizeof(addr)) < 0) {
        THROW_ERROR("connecting to the redirected address failed");
    }
    int accepted_fd = accept(listen_fd, NULL, NULL);
    if (accepted_fd < 0) {
        THROW_ERROR("accept failed");
    }

    int sockets[] = { listen_fd, fd, accepted_fd };
    for (size_t i = 0; i < ARRAY_SIZE(sockets); i++) {
        int s = sockets[i];
        int one = 1;
        if (setsockopt(s, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) < 0 ||
                setsockopt(s, IPPROTO_TCP, TCP_KEEPIDLE, &one, sizeof(one)) < 0 ||
                setsockopt(s, IPPROTO_IP, IP_TTL, &one, sizeof(one)) < 0 ||
                setsockopt(s, IPPROTO_IP, IP_TOS, &one, sizeof(one)) < 0) {
            THROW_ERROR("an option of an IP level was refused by socket %zu", i);
        }
        // A TCP socket may reuse a port
        if (setsockopt(s, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one)) < 0 ||
                get_int_opt(s, SOL_SOCKET, SO_REUSEPORT) != 1) {
            THROW_ERROR("SO_REUSEPORT of socket %zu failed", i);
        }
        if (setsockopt(s, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof(one)) < 0 ||
                get_int_opt(s, SOL_SOCKET, SO_KEEPALIVE) != 1) {
            THROW_ERROR("SO_KEEPALIVE of socket %zu failed", i);
        }
        int size = 65536;
        if (setsockopt(s, SOL_SOCKET, SO_SNDBUF, &size, sizeof(size)) < 0 ||
                get_int_opt(s, SOL_SOCKET, SO_SNDBUF) != 2 * size) {
            THROW_ERROR("SO_SNDBUF of socket %zu failed", i);
        }
        struct timeval tv = { 3, 20000 }, got = { 0, 0 };
        socklen_t len = sizeof(got);
        if (setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0 ||
                getsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &got, &len) < 0 ||
                len != sizeof(got) || got.tv_sec != 3 || got.tv_usec != 20000) {
            THROW_ERROR("SO_RCVTIMEO of socket %zu failed", i);
        }
        // Options that the LibOS does not know are accepted, too, as the TCP
        // socket that it replaces may have them
        if (setsockopt(s, SOL_SOCKET, 9999, &one, sizeof(one)) < 0) {
            THROW_ERROR("an unknown option was refused by socket %zu", i);
        }
        // The socket still looks like a TCP socket
        if (get_int_opt(s, SOL_SOCKET, SO_DOMAIN) != AF_INET ||
                get_int_opt(s, SOL_SOCKET, SO_PROTOCOL) != IPPROTO_TCP) {
            THROW_ERROR("socket %zu does not look like a TCP socket", i);
        }
    }
    close_files(3, accepted_fd, fd, listen_fd);
    return 0;
}

struct connector {
    int fd;
    struct sockaddr_in addr;
    atomic_int done;
    int ret;
    int err;
};

static void *connect_in_thread(void *arg) {
    struct connector *connector = arg;
    errno = 0;
    connector->ret = connect(connector->fd, SA(connector->addr), sizeof(connector->addr));
    connector->err = errno;
    atomic_store(&connector->done, 1);
    return NULL;
}

// connect() to a redirected address whose backlog is full waits for an accept, as a TCP connect()
// does. The socket is replaced when the connection is made, not before, so that the thread that
// waits does not keep the other threads from using their file descriptors. A nonblocking socket
// fails with EAGAIN, as a Unix socket does
static int test_connect_waits_for_a_full_backlog() {
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr = addr4("127.0.0.1", REDIRECTED_PORT);
    if (listen_fd < 0 || bind(listen_fd, SA(addr), sizeof(addr)) < 0 ||
            listen(listen_fd, 0) < 0) {
        THROW_ERROR("listening on the redirected address failed");
    }
    // A backlog of 0 takes one connection
    int first = socket(AF_INET, SOCK_STREAM, 0);
    if (connect(first, SA(addr), sizeof(addr)) < 0) {
        THROW_ERROR("the first connection failed");
    }

    struct connector connector = { .fd = socket(AF_INET, SOCK_STREAM, 0), .addr = addr };
    pthread_t thread;
    if (connector.fd < 0 ||
            pthread_create(&thread, NULL, connect_in_thread, &connector) != 0) {
        THROW_ERROR("failed to start the thread that connects");
    }
    usleep(200 * 1000);
    if (atomic_load(&connector.done)) {
        THROW_ERROR("connect() to a full backlog did not wait: %d, errno %d", connector.ret,
                    connector.err);
    }

    // The other threads use their file descriptors meanwhile
    int p[2];
    int dup_fd = dup(listen_fd);
    if (pipe(p) < 0 || dup_fd < 0) {
        THROW_ERROR("the file table is not usable while connect() waits");
    }
    close(p[0]);
    close(p[1]);
    close(dup_fd);
    int nonblocking = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (connect(nonblocking, SA(addr), sizeof(addr)) == 0 || errno != EAGAIN) {
        THROW_ERROR("nonblocking connect() to a full backlog should fail with EAGAIN");
    }
    close(nonblocking);

    int accepted_fd = accept(listen_fd, NULL, NULL);
    if (accepted_fd < 0) {
        THROW_ERROR("accept failed");
    }
    for (int i = 0; i < 100 && !atomic_load(&connector.done); i++) {
        usleep(50 * 1000);
    }
    if (!atomic_load(&connector.done)) {
        THROW_ERROR("connect() to a full backlog did not go on after an accept");
    }
    pthread_join(thread, NULL);
    if (connector.ret != 0) {
        errno = connector.err;
        THROW_ERROR("connect() to a full backlog failed");
    }
    // The socket was replaced: it has the addresses of a connected TCP socket, and works
    int second_fd = accept(listen_fd, NULL, NULL);
    if (second_fd < 0 || check_peername(connector.fd, "127.0.0.1", REDIRECTED_PORT) < 0 ||
            exchange(connector.fd, second_fd, "late") < 0) {
        THROW_ERROR("the connection that waited does not work");
    }
    close_files(5, second_fd, accepted_fd, connector.fd, first, listen_fd);
    return 0;
}

// Waits until a thread has set its flag, for up to `ms`
static int wait_for_flag(atomic_int *done, int ms) {
    for (int i = 0; i < ms / 10 && !atomic_load(done); i++) {
        usleep(10 * 1000);
    }
    return atomic_load(done);
}

// SO_SNDTIMEO, which the program set on the TCP socket before connect(), ends the wait of connect() to
// a redirected address whose backlog is full: it fails with EAGAIN when the time is over (a TCP
// connect() fails with EINPROGRESS), and the socket connects later, when there is room. The socket
// that replaces it has the timeouts, and shows them
static int test_connect_timeout_on_a_full_backlog() {
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr = addr4("127.0.0.1", REDIRECTED_PORT);
    if (listen_fd < 0 || bind(listen_fd, SA(addr), sizeof(addr)) < 0 ||
            listen(listen_fd, 0) < 0) {
        THROW_ERROR("listening on the redirected address failed");
    }
    // A backlog of 0 takes one connection
    int first = socket(AF_INET, SOCK_STREAM, 0);
    if (first < 0 || connect(first, SA(addr), sizeof(addr)) < 0) {
        THROW_ERROR("the first connection failed");
    }

    struct connector connector = { .fd = socket(AF_INET, SOCK_STREAM, 0), .addr = addr };
    if (connector.fd < 0 || set_timeout_ms(connector.fd, SO_SNDTIMEO, 300) < 0 ||
            set_timeout_ms(connector.fd, SO_RCVTIMEO, 2000) < 0) {
        THROW_ERROR("setting the timeouts failed");
    }
    pthread_t thread;
    long start = now_ms();
    if (pthread_create(&thread, NULL, connect_in_thread, &connector) != 0) {
        THROW_ERROR("failed to start the thread that connects");
    }
    int done = wait_for_flag(&connector.done, 5000);
    if (!done) {
        // The timeout is lost: let the listener refuse the connection, which ends the wait
        close(listen_fd);
    }
    pthread_join(thread, NULL);
    long elapsed = now_ms() - start;
    if (!done) {
        THROW_ERROR("connect() with SO_SNDTIMEO of 300 ms still waits after %ld ms", elapsed);
    }
    if (connector.ret != -1 || (connector.err != EAGAIN && connector.err != EINPROGRESS)) {
        errno = connector.err;
        THROW_ERROR("connect() with a timeout on a full backlog returned %d", connector.ret);
    }
    if (elapsed < 250) {
        THROW_ERROR("connect() with SO_SNDTIMEO of 300 ms returned after %ld ms", elapsed);
    }

    // There is room after an accept, and the socket connects, and has the timeouts
    int accepted_fd = accept(listen_fd, NULL, NULL);
    if (accepted_fd < 0) {
        THROW_ERROR("accept failed");
    }
    if (connect(connector.fd, SA(addr), sizeof(addr)) < 0) {
        THROW_ERROR("connect() fails after the accept made room");
    }
    if (get_timeout_ms(connector.fd, SO_SNDTIMEO) != 300 ||
            get_timeout_ms(connector.fd, SO_RCVTIMEO) != 2000) {
        THROW_ERROR("the socket has the timeouts %ld and %ld ms, instead of 300 and 2000",
                    get_timeout_ms(connector.fd, SO_SNDTIMEO),
                    get_timeout_ms(connector.fd, SO_RCVTIMEO));
    }
    int second_fd = accept(listen_fd, NULL, NULL);
    if (second_fd < 0 || check_peername(connector.fd, "127.0.0.1", REDIRECTED_PORT) < 0 ||
            exchange(connector.fd, second_fd, "late") < 0) {
        THROW_ERROR("the connection after the timeout does not work");
    }
    close_files(5, second_fd, accepted_fd, connector.fd, first, listen_fd);
    return 0;
}

struct acceptor {
    int fd;
    atomic_int done;
    int ret;
    int err;
};

static void *accept_in_thread(void *arg) {
    struct acceptor *acceptor = arg;
    errno = 0;
    acceptor->ret = accept(acceptor->fd, NULL, NULL);
    acceptor->err = errno;
    atomic_store(&acceptor->done, 1);
    return NULL;
}

// SO_RCVTIMEO, which the program set on the TCP socket before bind(), ends the wait of accept() on the
// redirected listener: it fails with EAGAIN when the time is over. The listener has the timeout, and
// the sockets that it accepts have it, as the sockets that a TCP socket accepts do
static int test_accept_timeout() {
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr = addr4("127.0.0.1", REDIRECTED_PORT);
    if (listen_fd < 0 || set_timeout_ms(listen_fd, SO_RCVTIMEO, 300) < 0 ||
            set_timeout_ms(listen_fd, SO_SNDTIMEO, 1500) < 0 ||
            bind(listen_fd, SA(addr), sizeof(addr)) < 0 || listen(listen_fd, 4) < 0) {
        THROW_ERROR("listening on the redirected address failed");
    }

    struct acceptor acceptor = { .fd = listen_fd };
    pthread_t thread;
    long start = now_ms();
    if (pthread_create(&thread, NULL, accept_in_thread, &acceptor) != 0) {
        THROW_ERROR("failed to start the thread that accepts");
    }
    int done = wait_for_flag(&acceptor.done, 5000);
    int client_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (!done) {
        // The timeout is lost: a connection ends the wait
        connect(client_fd, SA(addr), sizeof(addr));
    }
    pthread_join(thread, NULL);
    long elapsed = now_ms() - start;
    if (!done) {
        THROW_ERROR("accept() with SO_RCVTIMEO of 300 ms still waits after %ld ms", elapsed);
    }
    if (acceptor.ret != -1 || acceptor.err != EAGAIN) {
        errno = acceptor.err;
        THROW_ERROR("accept() with a timeout and no connection returned %d", acceptor.ret);
    }
    if (elapsed < 250) {
        THROW_ERROR("accept() with SO_RCVTIMEO of 300 ms returned after %ld ms", elapsed);
    }
    if (get_timeout_ms(listen_fd, SO_RCVTIMEO) != 300 ||
            get_timeout_ms(listen_fd, SO_SNDTIMEO) != 1500) {
        THROW_ERROR("the listener has the timeouts %ld and %ld ms, instead of 300 and 1500",
                    get_timeout_ms(listen_fd, SO_RCVTIMEO),
                    get_timeout_ms(listen_fd, SO_SNDTIMEO));
    }

    // A connection that is queued is accepted at once, and the socket has the timeouts
    int accepted_fd = -1;
    if (connect(client_fd, SA(addr), sizeof(addr)) < 0 ||
            (accepted_fd = accept(listen_fd, NULL, NULL)) < 0) {
        THROW_ERROR("connecting to the listener and accepting failed");
    }
    if (get_timeout_ms(accepted_fd, SO_RCVTIMEO) != 300 ||
            get_timeout_ms(accepted_fd, SO_SNDTIMEO) != 1500) {
        THROW_ERROR("the accepted socket has the timeouts %ld and %ld ms, instead of 300 and 1500",
                    get_timeout_ms(accepted_fd, SO_RCVTIMEO),
                    get_timeout_ms(accepted_fd, SO_SNDTIMEO));
    }
    // The socket that connected did not set any
    long client_timeout = get_timeout_ms(client_fd, SO_RCVTIMEO);
    if (client_timeout != 0) {
        THROW_ERROR("the client socket has a timeout of %ld ms", client_timeout);
    }
    close_files(3, accepted_fd, client_fd, listen_fd);
    return 0;
}

// A read or write that may block, which a thread makes so that the test does not hang if the call
// does not return
struct blocked_io {
    int fd;
    int is_write;
    atomic_int done;
    ssize_t ret;
    int err;
    long elapsed;
};

static void *io_in_thread(void *arg) {
    struct blocked_io *io = arg;
    static char buf[65536];
    long start = now_ms();
    errno = 0;
    if (io->is_write) {
        // The writes go on until one of them waits for room: it ends with the bytes that it has
        // written, or with EAGAIN, when its timeout is over. A buffer takes 208 KiB at most.
        for (int i = 0; i < 64; i++) {
            long call_start = now_ms();
            io->ret = write(io->fd, buf, sizeof(buf));
            io->err = errno;
            if (io->ret != (ssize_t)sizeof(buf) || now_ms() - call_start > 100) {
                break;
            }
        }
    } else {
        io->ret = read(io->fd, buf, 1);
        io->err = errno;
    }
    io->elapsed = now_ms() - start;
    atomic_store(&io->done, 1);
    return NULL;
}

// Checks that a read or write on the socket gives up by itself when its timeout is over: a read
// fails with EAGAIN, a write returns the bytes that it has written, or fails with EAGAIN. `peer` is
// the other end of the connection, which is shut down if the call does not give up, to end it
static int check_times_out(const char *what, int fd, int peer, int is_write,
                           long timeout_ms) {
    struct blocked_io io = { .fd = fd, .is_write = is_write };
    pthread_t thread;
    if (pthread_create(&thread, NULL, io_in_thread, &io) != 0) {
        THROW_ERROR("failed to start the thread that blocks");
    }
    int done = wait_for_flag(&io.done, 5000);
    if (!done) {
        shutdown(peer, SHUT_RDWR);
    }
    pthread_join(thread, NULL);
    if (!done) {
        THROW_ERROR("%s is still blocked after 5 s, though its timeout is %ld ms", what,
                    timeout_ms);
    }
    int gave_up;
    if (is_write) {
        gave_up = io.ret >= 0 || io.err == EAGAIN;
    } else {
        gave_up = io.ret < 0 && io.err == EAGAIN;
    }
    if (!gave_up) {
        errno = io.err;
        THROW_ERROR("%s with a timeout of %ld ms returned %zd", what, timeout_ms, io.ret);
    }
    if (io.elapsed < timeout_ms - 50 || io.elapsed > timeout_ms + 3000) {
        THROW_ERROR("%s with a timeout of %ld ms gave up after %ld ms", what, timeout_ms,
                    io.elapsed);
    }
    return 0;
}

// SO_RCVTIMEO and SO_SNDTIMEO of a redirected socket end the waits of read() and write(), not only
// those of accept() and connect(): the socket that connects after the program has set them on its
// TCP socket has them, so has the socket that the program sets them on after it was replaced, and
// the socket that a redirected listener accepts has those of the listener
static int test_redirected_sockets_time_out() {
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr = addr4("127.0.0.1", REDIRECTED_PORT);
    if (listen_fd < 0 || set_timeout_ms(listen_fd, SO_RCVTIMEO, 300) < 0 ||
            set_timeout_ms(listen_fd, SO_SNDTIMEO, 300) < 0 ||
            bind(listen_fd, SA(addr), sizeof(addr)) < 0 || listen(listen_fd, 4) < 0) {
        THROW_ERROR("listening on the redirected address failed");
    }

    // The timeouts are those of the TCP socket that connects
    int client_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (client_fd < 0 || set_timeout_ms(client_fd, SO_RCVTIMEO, 300) < 0 ||
            set_timeout_ms(client_fd, SO_SNDTIMEO, 300) < 0 ||
            connect(client_fd, SA(addr), sizeof(addr)) < 0) {
        THROW_ERROR("connecting to the redirected address failed");
    }
    int accepted_fd = accept(listen_fd, NULL, NULL);
    if (accepted_fd < 0) {
        THROW_ERROR("accept failed");
    }
    if (check_times_out("read() of the socket that connected", client_fd, accepted_fd, 0,
                        300) < 0 ||
            check_times_out("read() of the socket that was accepted", accepted_fd, client_fd, 0,
                            300) < 0 ||
            check_times_out("write() of the socket that connected", client_fd, accepted_fd, 1,
                            300) < 0 ||
            check_times_out("write() of the socket that was accepted", accepted_fd, client_fd, 1,
                            300) < 0) {
        return -1;
    }
    close_files(2, accepted_fd, client_fd);

    // The program sets the timeouts after the socket was replaced
    client_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (client_fd < 0 || connect(client_fd, SA(addr), sizeof(addr)) < 0 ||
            set_timeout_ms(client_fd, SO_RCVTIMEO, 400) < 0 ||
            set_timeout_ms(client_fd, SO_SNDTIMEO, 400) < 0 ||
            (accepted_fd = accept(listen_fd, NULL, NULL)) < 0) {
        THROW_ERROR("the second connection failed");
    }
    if (check_times_out("read() of the socket with timeouts set after the redirect",
                        client_fd,
                        accepted_fd, 0, 400) < 0 ||
            check_times_out("write() of the socket with timeouts set after the redirect", client_fd,
                            accepted_fd, 1, 400) < 0) {
        return -1;
    }
    close_files(3, accepted_fd, client_fd, listen_fd);
    return 0;
}

// Returns the number of bytes that the socket takes before a write would block
static long fill_nonblocking(int fd) {
    static char buf[4096];
    int flags = fcntl(fd, F_GETFL);
    if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        return -1;
    }
    long total = 0;
    ssize_t n;
    while (total < (4L << 20) && (n = write(fd, buf, sizeof(buf))) > 0) {
        total += n;
    }
    fcntl(fd, F_SETFL, flags);
    return total;
}

// SO_SNDBUF of a redirected socket limits the bytes that it has in flight, as it does for a Unix
// socket, also if the socket is connected already, and the socket that a redirected listener
// accepts has the size of the listener, as the sockets that a TCP socket accepts do
static int test_redirected_sockets_limit_the_send_buffer() {
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr = addr4("127.0.0.1", REDIRECTED_PORT);
    // The options of the TCP socket are lost when it is replaced, but for the timeouts: the program
    // sets the size on the socket that replaced it
    int size = 4096;
    if (listen_fd < 0 || bind(listen_fd, SA(addr), sizeof(addr)) < 0 ||
            setsockopt(listen_fd, SOL_SOCKET, SO_SNDBUF, &size, sizeof(size)) < 0 ||
            listen(listen_fd, 4) < 0) {
        THROW_ERROR("listening on the redirected address failed");
    }
    int client_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (client_fd < 0 || connect(client_fd, SA(addr), sizeof(addr)) < 0) {
        THROW_ERROR("connecting to the redirected address failed");
    }
    int accepted_fd = accept(listen_fd, NULL, NULL);
    if (accepted_fd < 0) {
        THROW_ERROR("accept failed");
    }

    // The accepted socket shows the size of the listener, and is limited by it, the socket that
    // connected has the default size
    if (get_int_opt(accepted_fd, SOL_SOCKET, SO_SNDBUF) != 2 * size) {
        THROW_ERROR("the accepted socket shows a send buffer of %d bytes, not %d",
                    get_int_opt(accepted_fd, SOL_SOCKET, SO_SNDBUF), 2 * size);
    }
    long accepted_bytes = fill_nonblocking(accepted_fd);
    long client_bytes = fill_nonblocking(client_fd);
    if (accepted_bytes <= 0 || accepted_bytes > 50000) {
        THROW_ERROR("the accepted socket with a send buffer of %d bytes takes %ld bytes",
                    2 * size,
                    accepted_bytes);
    }
    if (client_bytes < 100000) {
        THROW_ERROR("the socket that connected takes only %ld bytes", client_bytes);
    }

    // A socket that is connected has the size that the program sets at once
    int second_fd = socket(AF_INET, SOCK_STREAM, 0), third_fd = -1;
    if (second_fd < 0 || connect(second_fd, SA(addr), sizeof(addr)) < 0 ||
            setsockopt(second_fd, SOL_SOCKET, SO_SNDBUF, &size, sizeof(size)) < 0 ||
            (third_fd = accept(listen_fd, NULL, NULL)) < 0) {
        THROW_ERROR("the second connection failed");
    }
    long second_bytes = fill_nonblocking(second_fd);
    if (second_bytes <= 0 || second_bytes > 50000) {
        THROW_ERROR("the socket with a send buffer of %d bytes, set after connect, takes %ld bytes",
                    2 * size, second_bytes);
    }
    close_files(5, third_fd, second_fd, accepted_fd, client_fd, listen_fd);
    return 0;
}

// The redirected sockets that are not connected have the errors of TCP: a receive and a shutdown
// fail with ENOTCONN, also if the socket listens, and FIONREAD fails only for a socket that
// listens. A connection that is closed with bytes that nobody reads is reset, as with TCP.
static int test_redirected_sockets_have_the_errors_of_tcp(void) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr = addr4("127.0.0.1", REDIRECTED_PORT);
    int one = 1, n = -1;
    char buf[16];
    if (fd < 0 || setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0 ||
            bind(fd, SA(addr), sizeof(addr)) < 0) {
        THROW_ERROR("binding to the redirected address failed");
    }
    if (recv(fd, buf, sizeof(buf), MSG_DONTWAIT) != -1 || errno != ENOTCONN) {
        THROW_ERROR("recv on a socket that is not connected should fail with ENOTCONN");
    }
    if (ioctl(fd, FIONREAD, &n) < 0 || n != 0) {
        THROW_ERROR("FIONREAD on a socket that is not connected should return 0");
    }
    if (shutdown(fd, SHUT_RD) != -1 || errno != ENOTCONN) {
        THROW_ERROR("shutdown of a socket that is not connected should fail with ENOTCONN");
    }
    if (listen(fd, 4) < 0) {
        THROW_ERROR("listen failed");
    }
    if (recv(fd, buf, sizeof(buf), MSG_DONTWAIT) != -1 || errno != ENOTCONN) {
        THROW_ERROR("recv on a listening socket should fail with ENOTCONN");
    }
    if (ioctl(fd, FIONREAD, &n) != -1 || errno != EINVAL) {
        THROW_ERROR("FIONREAD on a listening socket should fail with EINVAL");
    }

    int client_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (client_fd < 0 || connect(client_fd, SA(addr), sizeof(addr)) < 0) {
        THROW_ERROR("connecting to the redirected address failed");
    }
    int accepted_fd = accept(fd, NULL, NULL);
    if (accepted_fd < 0 || write(client_fd, "unread", 6) != 6) {
        THROW_ERROR("accept or write failed");
    }
    close(accepted_fd);
    if (recv(client_fd, buf, sizeof(buf), MSG_DONTWAIT) != -1 || errno != ECONNRESET) {
        THROW_ERROR("recv after the close with unread bytes should fail with ECONNRESET");
    }
    if (recv(client_fd, buf, sizeof(buf), MSG_DONTWAIT) != 0) {
        THROW_ERROR("recv after the reset should return the end of the file");
    }
    close(client_fd);
    close(fd);
    return 0;
}

// The file of the redirect is a socket file, like the file of any Unix socket: it cannot be opened,
// and it stays when the program closes the socket, so that the program binds to the address again
static int test_redirect_file_is_a_socket_file() {
    int listen_fd = listen_redirected();
    if (listen_fd < 0) {
        THROW_ERROR("listening on the redirected address failed");
    }
    struct stat st;
    if (stat(REDIRECTED_PATH, &st) < 0 || !S_ISSOCK(st.st_mode)) {
        THROW_ERROR("%s is not a socket file", REDIRECTED_PATH);
    }
    if (open(REDIRECTED_PATH, O_RDONLY) >= 0 || errno != ENXIO) {
        THROW_ERROR("opening %s should fail with ENXIO", REDIRECTED_PATH);
    }
    close(listen_fd);
    if (stat(REDIRECTED_PATH, &st) < 0 || !S_ISSOCK(st.st_mode)) {
        THROW_ERROR("the socket file is gone after the socket is closed");
    }
    listen_fd = listen_redirected();
    if (listen_fd < 0) {
        THROW_ERROR("listening on the redirected address again failed");
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
    TEST_CASE(test_socket_options),
    TEST_CASE(test_connect_waits_for_a_full_backlog),
    TEST_CASE(test_connect_timeout_on_a_full_backlog),
    TEST_CASE(test_accept_timeout),
    TEST_CASE(test_redirected_sockets_time_out),
    TEST_CASE(test_redirected_sockets_limit_the_send_buffer),
    TEST_CASE(test_redirected_sockets_have_the_errors_of_tcp),
    TEST_CASE(test_redirect_file_is_a_socket_file),
};

int main(int argc, const char *argv[]) {
    if (argc > 1 && strcmp(argv[1], "try_connect") == 0) {
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        struct sockaddr_in addr = addr4("127.0.0.1", REDIRECTED_PORT);
        return connect(fd, SA(addr), sizeof(addr)) == 0 ? 0 : errno;
    }
    return test_suite_run(test_cases, ARRAY_SIZE(test_cases));
}
