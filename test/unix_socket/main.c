#define _GNU_SOURCE
#include <sys/syscall.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/ioctl.h>
#include <poll.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <spawn.h>
#include <string.h>
#include <sys/epoll.h>
#include <pthread.h>
#include <signal.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#include <stddef.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

#include "test.h"
#include "kernel_heap.h"

#define ECHO_MSG "echo msg for unix_socket test"

int create_connected_sockets(int *sockets, char *sock_path) {
    int listen_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (listen_fd == -1) {
        THROW_ERROR("failed to create a unix socket");
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(struct sockaddr_un)); // Clear structure
    addr.sun_family = AF_UNIX;
    strcpy(addr.sun_path, sock_path);
    socklen_t addr_len = strlen(addr.sun_path) + sizeof(addr.sun_family) + 1;
    if (bind(listen_fd, (struct sockaddr *)&addr, addr_len) == -1) {
        close(listen_fd);
        THROW_ERROR("failed to bind");
    }

    if (listen(listen_fd, 5) == -1) {
        close(listen_fd);
        THROW_ERROR("failed to listen");
    }

    int client_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (client_fd == -1) {
        close(listen_fd);
        THROW_ERROR("failed to create a unix socket");
    }

    if (connect(client_fd, (struct sockaddr *)&addr, addr_len) == -1) {
        close(listen_fd);
        close(client_fd);
        THROW_ERROR("failed to connect");
    }

    int accepted_fd = accept(listen_fd, (struct sockaddr *)&addr, &addr_len);
    if (accepted_fd == -1) {
        close(listen_fd);
        close(client_fd);
        THROW_ERROR("failed to accept socket");
    }

    sockets[0] = client_fd;
    sockets[1] = accepted_fd;
    close(listen_fd);
    return 0;
}

int create_connceted_sockets_default(int *sockets) {
    return create_connected_sockets(sockets, "unix_socket_default_path");
}

int create_connected_sockets_then_rename(int *sockets) {
    char *socket_original_path = "/tmp/socket_tmp";
    char *socket_ready_path = "/tmp/.socket_tmp";
    int listen_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (listen_fd == -1) {
        THROW_ERROR("failed to create a unix socket");
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(struct sockaddr_un)); // Clear structure
    addr.sun_family = AF_UNIX;
    strcpy(addr.sun_path, socket_original_path);

    // About addr_len (from man page):
    // a UNIX domain socket can be bound to a null-terminated
    // filesystem pathname using bind(2).  When the address of
    // a pathname socket is returned (by one of the system
    // calls noted above), its length is:
    //  offsetof(struct sockaddr_un, sun_path) + strlen(sun_path) + 1
    socklen_t addr_len = strlen(addr.sun_path) + sizeof(addr.sun_family) + 1;
    if (bind(listen_fd, (struct sockaddr *)&addr, addr_len) == -1) {
        close(listen_fd);
        THROW_ERROR("failed to bind");
    }

    if (listen(listen_fd, 5) == -1) {
        close(listen_fd);
        THROW_ERROR("failed to listen");
    }

    // rename to new path
    unlink(socket_ready_path);
    if (rename(socket_original_path, socket_ready_path) < 0) {
        THROW_ERROR("failed to rename");
    }

    int client_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (client_fd == -1) {
        close(listen_fd);
        THROW_ERROR("failed to create a unix socket");
    }

    struct sockaddr_un addr_client;
    memset(&addr_client, 0, sizeof(struct sockaddr_un)); // Clear structure
    addr_client.sun_family = AF_UNIX;
    strcpy(addr_client.sun_path, "/proc/self/root");
    strcat(addr_client.sun_path, socket_ready_path);

    socklen_t client_addr_len = strlen(addr_client.sun_path) + sizeof(
                                    addr_client.sun_family) + 1;
    if (connect(client_fd, (struct sockaddr *)&addr_client, client_addr_len) == -1) {
        close(listen_fd);
        close(client_fd);
        THROW_ERROR("failed to connect");
    }

    int accepted_fd = accept(listen_fd, (struct sockaddr *)&addr_client, &client_addr_len);
    if (accepted_fd == -1) {
        close(listen_fd);
        close(client_fd);
        THROW_ERROR("failed to accept socket");
    }

    sockets[0] = client_fd;
    sockets[1] = accepted_fd;
    close(listen_fd);
    return 0;
}

int verify_child_echo(int *connected_sockets) {
    const char *child_prog = "/bin/hello_world";
    const char *child_argv[3] = {child_prog, ECHO_MSG, NULL};
    int child_pid;
    posix_spawn_file_actions_t file_actions;

    posix_spawn_file_actions_init(&file_actions);
    posix_spawn_file_actions_adddup2(&file_actions, connected_sockets[0], STDOUT_FILENO);
    posix_spawn_file_actions_addclose(&file_actions, connected_sockets[1]);

    if (posix_spawn(&child_pid, child_prog, &file_actions,
                    NULL, (char *const *)child_argv, NULL) < 0) {
        THROW_ERROR("failed to spawn a child process");
    }

    struct pollfd polls[] = {
        {.fd = connected_sockets[1], .events = POLLIN},
    };

    // Test for blocking poll, poll will be only interrupted by sigchld
    // if socket does not support waking up a sleeping poller
    int ret = poll(polls, 1, -1);
    if (ret < 0) {
        THROW_ERROR("failed to poll");
    }

    char actual_str[32] = {0};
    ssize_t len = read(connected_sockets[1], actual_str, 32);
    if (len != sizeof(ECHO_MSG) || strncmp(actual_str, ECHO_MSG, strlen(ECHO_MSG)) != 0) {
        printf("data read is :%s\n", actual_str);
        THROW_ERROR("received string is not as expected");
    }

    int status = 0;
    if (wait4(child_pid, &status, 0, NULL) < 0) {
        THROW_ERROR("failed to wait4 the child process");
    }

    return 0;
}

int verify_connection(int src_sock, int dest_sock) {
    char buf[1024];
    int i;
    for (i = 0; i < 100; i++) {
        if (i % 2 == 0) {
            if (write(src_sock, ECHO_MSG, sizeof(ECHO_MSG)) < 0) {
                THROW_ERROR("writing server message");
            }
        } else {
            if (sendto(src_sock, ECHO_MSG, sizeof(ECHO_MSG), 0, NULL, 0) < 0) {
                THROW_ERROR("sendto server message");
            }
        }

        if (read(dest_sock, buf, 1024) < 0) {
            THROW_ERROR("reading server message");
        }

        if (strncmp(buf, ECHO_MSG, sizeof(ECHO_MSG)) != 0) {
            THROW_ERROR("msg received mismatch");
        }
    }
    return 0;
}

// this value should not be too large as one pair consumes 2MB memory
#define PAIR_NUM 15

int test_multiple_socketpairs() {
    int sockets[PAIR_NUM][2];
    int i;
    int ret = 0;

    for (i = 0; i < PAIR_NUM; i++) {
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, sockets[i]) < 0) {
            THROW_ERROR("opening stream socket pair");
        }

        if (verify_connection(sockets[i][0], sockets[i][1]) < 0) {
            ret = -1;
            goto cleanup;
        }

        if (verify_connection(sockets[i][1], sockets[i][0]) < 0) {
            ret = -1;
            goto cleanup;
        }
    }
    i--;
cleanup:
    for (; i >= 0; i--) {
        close(sockets[i][0]);
        close(sockets[i][1]);
    }
    return ret;
}

int socketpair_default(int *sockets) {
    return socketpair(AF_UNIX, SOCK_STREAM, 0, sockets);
}

typedef int (*create_connection_func_t)(int *);
int test_connected_sockets_inter_process(create_connection_func_t fn) {
    int ret = 0;
    int sockets[2];
    if (fn(sockets) < 0) {
        return -1;
    }

    ret = verify_child_echo(sockets);

    close(sockets[0]);
    close(sockets[1]);
    return ret;
}

int test_unix_socket_inter_process() {
    return test_connected_sockets_inter_process(socketpair_default);
}

int test_socketpair_inter_process() {
    return test_connected_sockets_inter_process(create_connceted_sockets_default);
}

// To emulate JVM bahaviour on UDS
int test_unix_socket_rename() {
    return test_connected_sockets_inter_process(create_connected_sockets_then_rename);
}

int test_poll() {
    int socks[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, socks) < 0) {
        THROW_ERROR("socketpair failed");
    }

    if (write(socks[0], "not today\n", 10) < 0) {
        THROW_ERROR("failed to write to socket");
    }

    struct pollfd polls[] = {
        {.fd = socks[0], .events = POLLOUT},
        {.fd = socks[1], .events = POLLIN},
    };

    int ret = poll(polls, 2, 5000);
    if (ret <= 0) {
        THROW_ERROR("poll error");
    }
    if (((polls[0].revents & POLLOUT) && (polls[1].revents & POLLIN)) == 0) {
        printf("%d %d\n", polls[0].revents, polls[1].revents);
        THROW_ERROR("wrong return events");
    }
    return 0;
}

int test_getname() {
    char name[] = "unix_socket_path";
    int sock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock == -1) {
        THROW_ERROR("failed to create a unix socket");
    }

    struct sockaddr_un addr = {0};
    memset(&addr, 0, sizeof(struct sockaddr_un)); // Clear structure
    addr.sun_family = AF_UNIX;
    strcpy(addr.sun_path, name);
    socklen_t addr_len = strlen(addr.sun_path) + sizeof(addr.sun_family) + 1;
    if (bind(sock, (struct sockaddr *)&addr, addr_len) == -1) {
        close(sock);
        THROW_ERROR("failed to bind");
    }

    struct sockaddr_un ret_addr = {0};
    socklen_t ret_addr_len = sizeof(ret_addr);

    if (getsockname(sock, (struct sockaddr *)&ret_addr, &ret_addr_len) < 0) {
        close(sock);
        THROW_ERROR("failed to getsockname");
    }

    if (ret_addr_len != addr_len || strcmp(ret_addr.sun_path, name) != 0) {
        close(sock);
        THROW_ERROR("got name mismatched");
    }

    close(sock);
    return 0;
}

int test_ioctl_fionread() {
    int ret = 0;
    int sockets[2];
    ret = socketpair(AF_UNIX, SOCK_STREAM, 0, sockets);
    if (ret < 0) {
        THROW_ERROR("failed to create a unix socket");
    }

    const char *child_prog = "/bin/hello_world";
    const char *child_argv[3] = {child_prog, ECHO_MSG, NULL};
    int child_pid;
    posix_spawn_file_actions_t file_actions;

    posix_spawn_file_actions_init(&file_actions);
    posix_spawn_file_actions_adddup2(&file_actions, sockets[0], STDOUT_FILENO);
    posix_spawn_file_actions_addclose(&file_actions, sockets[1]);

    if (posix_spawn(&child_pid, child_prog, &file_actions,
                    NULL, (char *const *)child_argv, NULL) < 0) {
        THROW_ERROR("failed to spawn a child process");
    }

    int status = 0;
    if (wait4(child_pid, &status, 0, NULL) < 0) {
        THROW_ERROR("failed to wait4 the child process");
    }

    // data should be ready
    int data_len_ready = 0;
    if (ioctl(sockets[1], FIONREAD, &data_len_ready) < 0) {
        THROW_ERROR("failed to ioctl with FIONREAD option");
    }

    // data_len_ready will include '\0'
    if (data_len_ready - 1 != strlen(ECHO_MSG)) {
        THROW_ERROR("ioctl FIONREAD value not match");
    }

    char actual_str[32] = {0};
    ssize_t len = read(sockets[1], actual_str, 32);
    if (len != sizeof(ECHO_MSG) || strncmp(actual_str, ECHO_MSG, strlen(ECHO_MSG)) != 0) {
        printf("data read is :%s\n", actual_str);
        THROW_ERROR("received string is not as expected");
    }

    return 0;
}

void *client_routine(void *arg) {
    // Sleep a while before connect
    sleep(3);
    printf("sleep is done\n");

    char *sock_path = "/tmp/test.sock";
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(struct sockaddr_un)); // Clear structure
    addr.sun_family = AF_UNIX;
    strcpy(addr.sun_path, sock_path);
    socklen_t addr_len = strlen(addr.sun_path) + sizeof(addr.sun_family) + 1;

    int client_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (client_fd == -1) {
        printf("failed to create a unix socket\n");
        return NULL;
    }

    if (connect(client_fd, (struct sockaddr *)&addr, addr_len) == -1) {
        close(client_fd);
        printf("failed to connect\n");
        return NULL;
    }

    printf("connect success\n");
    return NULL;
}

int test_epoll_wait() {
    char *sock_path = "/tmp/test.sock";
    int ret;
    struct epoll_event event;
    uint32_t interest_events = EPOLLIN | EPOLLOUT;
    struct epoll_event polled_events;
    pthread_t client_tid;
    struct sockaddr_un addr;

    int listen_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (listen_fd == -1) {
        THROW_ERROR("failed to create a unix socket");
    }

    memset(&addr, 0, sizeof(struct sockaddr_un)); // Clear structure
    addr.sun_family = AF_UNIX;
    strcpy(addr.sun_path, sock_path);
    socklen_t addr_len = strlen(addr.sun_path) + sizeof(addr.sun_family) + 1;
    unlink(addr.sun_path);
    if (bind(listen_fd, (struct sockaddr *)&addr, addr_len) == -1) {
        close(listen_fd);
        THROW_ERROR("failed to bind");
    }

    if (listen(listen_fd, 5) != 0) {
        THROW_ERROR("server_routine, error in listen");
    }

    int ep_fd = epoll_create1(0);
    if (ep_fd < 0) {
        THROW_ERROR("failed to create an epoll");
    }

    event.events = interest_events;
    event.data.u32 = listen_fd;
    ret = epoll_ctl(ep_fd, EPOLL_CTL_ADD, listen_fd, &event);
    if (ret < 0) {
        THROW_ERROR("failed to do epoll ctl");
    }

    if (pthread_create(&client_tid, NULL, client_routine, NULL)) {
        THROW_ERROR("Failure creating client thread");
    }

    // wait infinitely
    ret = epoll_wait(ep_fd, &polled_events, 1, -1);
    if (ret != 1) {
        THROW_ERROR("failed to do epoll wait");
    }

    if (polled_events.data.u32 != listen_fd) {
        THROW_ERROR("epoll wait returned wrong fd");
    }

    int newsock = accept(listen_fd, (struct sockaddr *)&addr, &addr_len);
    if (newsock == -1) {
        THROW_ERROR("server_routine, error in accept");
    }

    printf("accept done, new socket = %d\n", newsock);
    pthread_join(client_tid, NULL);
    return 0;
}

int client_sendmsg(int server_fd, char *buf) {
    int ret = 0;
    struct msghdr msg;
    struct iovec iov[1];
    msg.msg_name = NULL;
    msg.msg_namelen = 0;
    iov[0].iov_base = buf;
    iov[0].iov_len = strlen(buf);
    msg.msg_iov = iov;
    msg.msg_iovlen = 1;
    msg.msg_control = 0;
    msg.msg_controllen = 0;
    msg.msg_flags = 0;

    ret = sendmsg(server_fd, &msg, 0);
    if (ret <= 0) {
        THROW_ERROR("sendmsg failed");
    }

    msg.msg_iov = NULL;
    msg.msg_iovlen = 0;

    ret = sendmsg(server_fd, &msg, 0);
    if (ret != 0) {
        THROW_ERROR("empty sendmsg failed");
    }
    return ret;
}


int server_recvmsg(int client_fd) {
    int ret = 0;
    const int buf_size = 10;
    char buf[3][buf_size];
    struct msghdr msg;
    struct iovec iov[3];
    char result_buf[] = {ECHO_MSG}; // 30 bytes

    memset(&msg, 0, sizeof(struct msghdr));
    msg.msg_name  = NULL;
    msg.msg_namelen  = 0;
    iov[0].iov_base = buf[0];
    iov[0].iov_len = buf_size;
    iov[1].iov_base = buf[1];
    iov[1].iov_len = buf_size;
    iov[2].iov_base = buf[2];
    iov[2].iov_len = buf_size;
    msg.msg_iov = iov;
    msg.msg_iovlen = 3;
    msg.msg_control = 0;
    msg.msg_controllen = 0;
    msg.msg_flags = 0;

    ret = recvmsg(client_fd, &msg, 0);
    if (ret <= 0) {
        THROW_ERROR("recvmsg failed");
    } else {
        if (strncmp(buf[0], result_buf, buf_size) != 0 &&
                strncmp(buf[1], result_buf + buf_size, buf_size) != 0 &&
                strncmp(buf[0], result_buf + buf_size * 2, buf_size) != 0) {
            printf("recvmsg : %d, msg: %s,  %s, %s\n", ret, buf[0], buf[1], buf[2]);
            THROW_ERROR("msg recvmsg mismatch");
        }
    }
    return ret;
}

int test_sendmsg_recvmsg() {
    int ret = 0;
    char test_buf[] = {ECHO_MSG};
    int socks[2];

    ret = socketpair(AF_UNIX, SOCK_STREAM, 0, socks);
    if (ret < 0) {
        THROW_ERROR("socket pair create failed");
    }

    int server_fd = socks[0];
    int client_fd = socks[1];

    ret = client_sendmsg(server_fd, test_buf);
    if (ret < 0) {
        THROW_ERROR("client_sendmsg failed");
    }

    ret = server_recvmsg(client_fd);
    if (ret < 0) {
        THROW_ERROR("server_recvmsg failed");
    }

    return ret;
}

// Reads an int option into a buffer filled with a pattern, which must be
// overwritten
static int get_int_sockopt(int fd, int optname, int *value) {
    *value = 0x7f7f7f7f;
    socklen_t len = sizeof(*value);
    if (getsockopt(fd, SOL_SOCKET, optname, value, &len) < 0) {
        THROW_ERROR("getsockopt(%d) failed", optname);
    }
    if (len != sizeof(*value) || *value == 0x7f7f7f7f) {
        THROW_ERROR("getsockopt(%d) did not return a value", optname);
    }
    return 0;
}

static int check_int_sockopt(int fd, int optname, int expected) {
    int value;
    if (get_int_sockopt(fd, optname, &value) < 0) {
        return -1;
    }
    if (value != expected) {
        THROW_ERROR("getsockopt(%d) returned %d instead of %d", optname, value, expected);
    }
    return 0;
}

static int check_peercred(int fd, pid_t pid) {
    struct ucred cred;
    memset(&cred, 0x7f, sizeof(cred));
    socklen_t len = sizeof(cred);
    if (getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &cred, &len) < 0) {
        THROW_ERROR("getsockopt(SO_PEERCRED) failed");
    }
    if (len != sizeof(cred) || cred.pid != pid || cred.uid != getuid() ||
            cred.gid != getgid()) {
        THROW_ERROR("SO_PEERCRED returned pid %d, uid %u, gid %u", cred.pid, cred.uid,
                    cred.gid);
    }
    return 0;
}

static int check_sockopts(const char *path, int listen_fd, int client_fd,
                          int accepted_fd) {
    if (check_int_sockopt(listen_fd, SO_TYPE, SOCK_STREAM) < 0 ||
            check_int_sockopt(listen_fd, SO_DOMAIN, AF_UNIX) < 0 ||
            check_int_sockopt(listen_fd, SO_PROTOCOL, 0) < 0 ||
            check_int_sockopt(listen_fd, SO_ACCEPTCONN, 1) < 0 ||
            check_int_sockopt(client_fd, SO_ACCEPTCONN, 0) < 0 ||
            check_int_sockopt(client_fd, SO_ERROR, 0) < 0 ||
            check_int_sockopt(accepted_fd, SO_ERROR, 0) < 0 ||
            check_peercred(accepted_fd, getpid()) < 0 ||
            check_peercred(client_fd, getpid()) < 0 ||
            // The options that cannot be set have their default values
            check_int_sockopt(accepted_fd, SO_KEEPALIVE, 0) < 0 ||
            check_int_sockopt(accepted_fd, SO_REUSEADDR, 0) < 0) {
        return -1;
    }

    // The length is that of the value
    struct linger linger;
    memset(&linger, 0x7f, sizeof(linger));
    socklen_t len = sizeof(linger);
    if (getsockopt(accepted_fd, SOL_SOCKET, SO_LINGER, &linger, &len) < 0 ||
            len != sizeof(linger) || linger.l_onoff != 0) {
        THROW_ERROR("SO_LINGER returned length %u and l_onoff %d", len, linger.l_onoff);
    }
    long long wide = -1;
    len = sizeof(wide);
    if (getsockopt(accepted_fd, SOL_SOCKET, SO_KEEPALIVE, &wide, &len) < 0 ||
            len != sizeof(int)) {
        THROW_ERROR("SO_KEEPALIVE returned length %u for a longer buffer", len);
    }

    int bufsize;
    if (get_int_sockopt(accepted_fd, SO_SNDBUF, &bufsize) < 0) {
        return -1;
    }
    if (bufsize <= 0) {
        THROW_ERROR("SO_SNDBUF returned %d", bufsize);
    }

    // Linux fails with EINVAL if the buffer is longer than the address
    struct sockaddr_un peer = {0};
    socklen_t peer_len = offsetof(struct sockaddr_un, sun_path) + strlen(path) + 1;
    if (getsockopt(client_fd, SOL_SOCKET, SO_PEERNAME, &peer, &peer_len) < 0) {
        THROW_ERROR("getsockopt(SO_PEERNAME) failed");
    }
    if (peer.sun_family != AF_UNIX || strcmp(peer.sun_path, path) != 0) {
        THROW_ERROR("SO_PEERNAME did not return the path of the listener");
    }

    // Unix sockets have no options of other levels
    int nodelay;
    len = sizeof(nodelay);
    if (getsockopt(accepted_fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, &len) != -1 ||
            errno != EOPNOTSUPP) {
        THROW_ERROR("getsockopt(IPPROTO_TCP) did not fail with EOPNOTSUPP");
    }
    return 0;
}

int test_getsockopt() {
    const char *path = "/tmp/unix_socket_sockopt";
    struct sockaddr_un addr = {0};
    addr.sun_family = AF_UNIX;
    strcpy(addr.sun_path, path);
    socklen_t addr_len = strlen(addr.sun_path) + sizeof(addr.sun_family) + 1;

    int listen_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        THROW_ERROR("failed to create a unix socket");
    }
    if (bind(listen_fd, (struct sockaddr *)&addr, addr_len) < 0 || listen(listen_fd, 5) < 0) {
        close(listen_fd);
        THROW_ERROR("failed to listen");
    }

    // Like a non-blocking connect of an event loop, which checks SO_ERROR
    // once the socket becomes writable
    int client_fd = socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0);
    int accepted_fd = -1;
    if (client_fd < 0 ||
            (connect(client_fd, (struct sockaddr *)&addr, addr_len) < 0 && errno != EINPROGRESS) ||
            (accepted_fd = accept(listen_fd, NULL, NULL)) < 0) {
        printf("\t\tERROR: failed to connect: %s\n", strerror(errno));
        close(listen_fd);
        close(client_fd);
        unlink(path);
        return -1;
    }

    int ret = check_sockopts(path, listen_fd, client_fd, accepted_fd);
    close(accepted_fd);
    close(client_fd);
    close(listen_fd);
    unlink(path);
    return ret;
}

// Like Linux, listen() takes a negative backlog for the maximum one
int test_listen_negative_backlog() {
    const char *path = "/tmp/unix_socket_backlog";
    struct sockaddr_un addr = {0};
    addr.sun_family = AF_UNIX;
    strcpy(addr.sun_path, path);
    socklen_t addr_len = strlen(addr.sun_path) + sizeof(addr.sun_family) + 1;

    int listen_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        THROW_ERROR("failed to create a unix socket");
    }
    if (bind(listen_fd, (struct sockaddr *)&addr, addr_len) < 0 ||
            listen(listen_fd, -1) < 0) {
        close(listen_fd);
        unlink(path);
        THROW_ERROR("failed to listen with a negative backlog");
    }

    int client_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    int ret = 0;
    if (client_fd < 0 || connect(client_fd, (struct sockaddr *)&addr, addr_len) < 0) {
        printf("\t\tERROR: failed to connect: %s\n", strerror(errno));
        ret = -1;
    }
    close(client_fd);
    close(listen_fd);
    unlink(path);
    return ret;
}

static int check_unnamed(struct sockaddr_un *addr, socklen_t len, const char *call) {
    if (len != sizeof(sa_family_t) || addr->sun_family != AF_UNIX) {
        THROW_ERROR("%s returned length %u and family %d for an unnamed peer", call, len,
                    addr->sun_family);
    }
    return 0;
}

// Like Linux, accept() and getpeername() return an address of the family
// AF_UNIX without a path for an unnamed peer, i.e., an unbound client, and
// recvfrom() returns no address
static int check_unnamed_peer(int listen_fd, int client_fd, int *accepted_fd) {
    struct sockaddr_un peer;
    memset(&peer, 0x7f, sizeof(peer));
    socklen_t len = sizeof(peer);
    *accepted_fd = accept(listen_fd, (struct sockaddr *)&peer, &len);
    if (*accepted_fd < 0) {
        THROW_ERROR("failed to accept");
    }
    if (check_unnamed(&peer, len, "accept") < 0) {
        return -1;
    }

    memset(&peer, 0x7f, sizeof(peer));
    len = sizeof(peer);
    if (getpeername(*accepted_fd, (struct sockaddr *)&peer, &len) < 0) {
        THROW_ERROR("getpeername failed");
    }
    if (check_unnamed(&peer, len, "getpeername") < 0) {
        return -1;
    }

    char c = 'x';
    if (write(client_fd, &c, 1) != 1) {
        THROW_ERROR("failed to write");
    }
    len = sizeof(peer);
    if (recvfrom(*accepted_fd, &c, 1, 0, (struct sockaddr *)&peer, &len) != 1) {
        THROW_ERROR("failed to receive");
    }
    if (len != 0) {
        THROW_ERROR("recvfrom returned an address of length %u for an unnamed peer", len);
    }
    return 0;
}

int test_unnamed_peer_address() {
    const char *path = "/tmp/unix_socket_unnamed";
    struct sockaddr_un addr = {0};
    addr.sun_family = AF_UNIX;
    strcpy(addr.sun_path, path);
    socklen_t addr_len = strlen(addr.sun_path) + sizeof(addr.sun_family) + 1;

    int listen_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        THROW_ERROR("failed to create a unix socket");
    }
    if (bind(listen_fd, (struct sockaddr *)&addr, addr_len) < 0 || listen(listen_fd, 5) < 0) {
        close(listen_fd);
        unlink(path);
        THROW_ERROR("failed to listen");
    }
    int client_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (client_fd < 0 || connect(client_fd, (struct sockaddr *)&addr, addr_len) < 0) {
        printf("\t\tERROR: failed to connect: %s\n", strerror(errno));
        close(client_fd);
        close(listen_fd);
        unlink(path);
        return -1;
    }

    int accepted_fd = -1;
    int ret = check_unnamed_peer(listen_fd, client_fd, &accepted_fd);
    if (accepted_fd >= 0) {
        close(accepted_fd);
    }
    close(client_fd);
    close(listen_fd);
    unlink(path);
    return ret;
}

#define ACCEPT_WHILE_WRITING_CONNECTIONS 2000
#define ACCEPT_WHILE_WRITING_WRITES 64

static void *accept_and_read(void *arg) {
    int listen_fd = *(int *)arg;
    for (int i = 0; i < ACCEPT_WHILE_WRITING_CONNECTIONS; i++) {
        int fd = accept(listen_fd, NULL, NULL);
        if (fd < 0) {
            return (void *) -1;
        }
        char buf[ACCEPT_WHILE_WRITING_WRITES];
        while (read(fd, buf, sizeof(buf)) > 0) {
        }
        close(fd);
    }
    return NULL;
}

// A client writes while the server accepts its connection, which must not
// race with the setup of the accepted socket
int test_accept_while_writing() {
    const char *path = "/tmp/unix_socket_accept_while_writing";
    struct sockaddr_un addr = {0};
    addr.sun_family = AF_UNIX;
    strcpy(addr.sun_path, path);
    socklen_t addr_len = strlen(addr.sun_path) + sizeof(addr.sun_family) + 1;

    int listen_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        THROW_ERROR("failed to create a unix socket");
    }
    // A backlog for all connections, as connect() fails if it is full
    if (bind(listen_fd, (struct sockaddr *)&addr, addr_len) < 0 ||
            listen(listen_fd, ACCEPT_WHILE_WRITING_CONNECTIONS) < 0) {
        close(listen_fd);
        unlink(path);
        THROW_ERROR("failed to listen");
    }
    pthread_t server;
    if (pthread_create(&server, NULL, accept_and_read, &listen_fd) != 0) {
        close(listen_fd);
        unlink(path);
        THROW_ERROR("failed to create the server thread");
    }

    int ret = 0;
    for (int i = 0; i < ACCEPT_WHILE_WRITING_CONNECTIONS && ret == 0; i++) {
        int fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (fd < 0 || connect(fd, (struct sockaddr *)&addr, addr_len) < 0) {
            printf("\t\tERROR: failed to connect: %s\n", strerror(errno));
            ret = -1;
        }
        for (int j = 0; j < ACCEPT_WHILE_WRITING_WRITES && ret == 0; j++) {
            if (write(fd, "x", 1) != 1) {
                printf("\t\tERROR: failed to write: %s\n", strerror(errno));
                ret = -1;
            }
        }
        close(fd);
    }

    void *server_ret = NULL;
    if (ret == 0) {
        pthread_join(server, &server_ret);
    }
    close(listen_fd);
    unlink(path);
    if (ret == 0 && server_ret != NULL) {
        THROW_ERROR("the server failed to accept");
    }
    return ret;
}

#define WRITE_AFTER_ACCEPT_CONNECTIONS 2000

static void *accept_and_write(void *arg) {
    int listen_fd = *(int *)arg;
    for (int i = 0; i < WRITE_AFTER_ACCEPT_CONNECTIONS; i++) {
        int fd = accept(listen_fd, NULL, NULL);
        if (fd < 0 || write(fd, "x", 1) != 1) {
            return (void *) -1;
        }
        char c;
        read(fd, &c, 1); // until the client closes
        close(fd);
    }
    return NULL;
}

// The server writes as soon as it accepts; the client, whose socket is in an
// epoll file already before connect(), must get the event
int test_epoll_in_after_connect() {
    const char *path = "/tmp/unix_socket_write_after_accept";
    struct sockaddr_un addr = {0};
    addr.sun_family = AF_UNIX;
    strcpy(addr.sun_path, path);
    socklen_t addr_len = strlen(addr.sun_path) + sizeof(addr.sun_family) + 1;

    int listen_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    int epfd = epoll_create1(0);
    if (listen_fd < 0 || epfd < 0) {
        THROW_ERROR("failed to create a unix socket or an epoll file");
    }
    if (bind(listen_fd, (struct sockaddr *)&addr, addr_len) < 0 ||
            listen(listen_fd, WRITE_AFTER_ACCEPT_CONNECTIONS) < 0) {
        close(listen_fd);
        close(epfd);
        unlink(path);
        THROW_ERROR("failed to listen");
    }
    pthread_t server;
    if (pthread_create(&server, NULL, accept_and_write, &listen_fd) != 0) {
        close(listen_fd);
        close(epfd);
        unlink(path);
        THROW_ERROR("failed to create the server thread");
    }

    int ret = 0;
    for (int i = 0; i < WRITE_AFTER_ACCEPT_CONNECTIONS && ret == 0; i++) {
        int fd = socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0);
        struct epoll_event event = { .events = EPOLLIN, .data.fd = fd };
        if (fd < 0 || epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &event) < 0 ||
                connect(fd, (struct sockaddr *)&addr, addr_len) < 0) {
            printf("\t\tERROR: failed to connect: %s\n", strerror(errno));
            ret = -1;
        } else if (epoll_wait(epfd, &event, 1, 2000) != 1) {
            printf("\t\tERROR: no event of connection %d after 2 s\n", i);
            ret = -1;
        }
        close(fd);
    }

    void *server_ret = NULL;
    if (ret == 0) {
        pthread_join(server, &server_ret);
    }
    close(listen_fd);
    close(epfd);
    unlink(path);
    if (ret == 0 && server_ret != NULL) {
        THROW_ERROR("the server failed to accept or write");
    }
    return ret;
}

// The interrupter thread sends SIGUSR2 to the main thread every 2 ms. The signal
// handler is installed without SA_RESTART, so blocking calls fail with EINTR.
static volatile int g_interrupter_run;
static pthread_t g_interrupter;
static pid_t g_main_tid;

static void on_sigusr2(int sig) {
}

static void *interrupter_main(void *arg) {
    struct timespec ts = { 0, 2000000 };
    while (g_interrupter_run) {
        nanosleep(&ts, NULL);
        if (g_interrupter_run) {
            syscall(SYS_tgkill, getpid(), g_main_tid, SIGUSR2);
        }
    }
    return NULL;
}

static int start_interrupter(void) {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_sigusr2;
    if (sigaction(SIGUSR2, &sa, NULL) < 0) {
        THROW_ERROR("failed to install the signal handler");
    }
    g_main_tid = syscall(SYS_gettid);
    g_interrupter_run = 1;
    if (pthread_create(&g_interrupter, NULL, interrupter_main, NULL) != 0) {
        THROW_ERROR("failed to create the interrupter thread");
    }
    return 0;
}

static void stop_interrupter(void) {
    g_interrupter_run = 0;
    pthread_join(g_interrupter, NULL);
}

// Fills an abstract address, i.e., one whose name follows a null byte, and
// returns its length
static socklen_t abstract_addr(struct sockaddr_un *addr, const char *name) {
    memset(addr, 0, sizeof(*addr));
    addr->sun_family = AF_UNIX;
    strcpy(addr->sun_path + 1, name);
    return offsetof(struct sockaddr_un, sun_path) + 1 + strlen(name);
}

static int abstract_bind(int fd, const char *name) {
    struct sockaddr_un addr;
    socklen_t addr_len = abstract_addr(&addr, name);
    return bind(fd, (struct sockaddr *)&addr, addr_len);
}

static int abstract_connect(int fd, const char *name) {
    struct sockaddr_un addr;
    socklen_t addr_len = abstract_addr(&addr, name);
    return connect(fd, (struct sockaddr *)&addr, addr_len);
}

// Returns a socket that listens on the abstract address, or -1
static int abstract_listener(const char *name, int flags) {
    int fd = socket(AF_UNIX, SOCK_STREAM | flags, 0);
    if (fd < 0) {
        printf("\t\tERROR: failed to create a unix socket: %s\n", strerror(errno));
        return -1;
    }
    if (abstract_bind(fd, name) < 0 || listen(fd, 16) < 0) {
        printf("\t\tERROR: failed to listen on %s: %s\n", name, strerror(errno));
        close(fd);
        return -1;
    }
    return fd;
}

static int check_accept_fails(int listen_fd, int expected_errno) {
    int fd = accept(listen_fd, NULL, NULL);
    if (fd >= 0) {
        close(fd);
        THROW_ERROR("accept succeeded without a connection");
    }
    if (errno != expected_errno) {
        printf("\t\tERROR: accept failed with errno %d (%s), expected %d (%s)\n",
               errno, strerror(errno), expected_errno, strerror(expected_errno));
        return -1;
    }
    return 0;
}

// Connects to the listening socket, and accepts the connection
static int check_connection(int listen_fd, const char *name) {
    int client_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (client_fd < 0 || abstract_connect(client_fd, name) < 0) {
        printf("\t\tERROR: failed to connect to %s: %s\n", name, strerror(errno));
        close(client_fd);
        return -1;
    }
    int accepted_fd = accept(listen_fd, NULL, NULL);
    if (accepted_fd < 0) {
        printf("\t\tERROR: failed to accept a connection to %s: %s\n", name, strerror(errno));
        close(client_fd);
        return -1;
    }
    close_files(2, client_fd, accepted_fd);
    return 0;
}

struct delayed_client {
    const char *name;
    int fd;
};

// Connects after 100 ms, which are four periods of the interrupts of the PAL
static void *delayed_client_main(void *arg) {
    struct delayed_client *client = arg;
    usleep(100000);
    client->fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (client->fd < 0 || abstract_connect(client->fd, client->name) < 0) {
        close(client->fd);
        client->fd = -1;
    }
    return NULL;
}

// A blocking accept() that no signal interrupts waits for the connection
static int check_accept_waits(int listen_fd, const char *name) {
    struct delayed_client client = { .name = name, .fd = -1 };
    pthread_t thread;
    if (pthread_create(&thread, NULL, delayed_client_main, &client) != 0) {
        THROW_ERROR("failed to create the client thread");
    }
    int accepted_fd = accept(listen_fd, NULL, NULL);
    int accept_errno = errno;
    pthread_join(thread, NULL);
    if (client.fd < 0) {
        printf("\t\tERROR: the client thread failed to connect to %s\n", name);
        close(accepted_fd);
        return -1;
    }
    close(client.fd);
    if (accepted_fd < 0) {
        printf("\t\tERROR: failed to accept a connection that arrives while waiting: %s\n",
               strerror(accept_errno));
        return -1;
    }
    close(accepted_fd);
    return 0;
}

#define INTERRUPTED_ACCEPTS 5

static int check_accept_errnos(int listen_fd, int nonblocking_fd) {
    if (check_accept_fails(nonblocking_fd, EAGAIN) < 0) {
        return -1;
    }
    // Switch to non-blocking mode and back
    int flags = fcntl(listen_fd, F_GETFL);
    if (fcntl(listen_fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        THROW_ERROR("failed to set O_NONBLOCK");
    }
    if (check_accept_fails(listen_fd, EAGAIN) < 0) {
        return -1;
    }
    if (fcntl(listen_fd, F_SETFL, flags) < 0) {
        THROW_ERROR("failed to clear O_NONBLOCK");
    }

    if (start_interrupter() < 0) {
        return -1;
    }
    int ret = 0;
    for (int i = 0; i < INTERRUPTED_ACCEPTS && ret == 0; i++) {
        ret = check_accept_fails(listen_fd, EINTR);
    }
    stop_interrupter();
    return ret;
}

// Like Linux, accept() on a listening socket that has no connection fails with
// EAGAIN if the socket is non-blocking, and with EINTR if it is blocking and a
// signal handler interrupts it
int test_accept_interrupted() {
    const char *name = "unix_socket_accept_interrupted";
    int listen_fd = abstract_listener(name, 0);
    int nonblocking_fd = abstract_listener("unix_socket_accept_nonblocking", SOCK_NONBLOCK);
    if (listen_fd < 0 || nonblocking_fd < 0) {
        return -1;
    }

    int ret = check_accept_errnos(listen_fd, nonblocking_fd);
    if (ret == 0) {
        // The interrupted calls did not harm the listening socket
        ret = check_accept_waits(listen_fd, name);
    }
    if (ret == 0) {
        ret = check_connection(listen_fd, name);
    }
    close_files(2, listen_fd, nonblocking_fd);
    return ret;
}

// How a connected socket gets the name that it binds
enum bind_kind {
    BIND_THEN_CONNECT,
    CONNECT_THEN_BIND,
    SOCKETPAIR_THEN_BIND,
    NUM_BIND_KINDS,
};

// Returns a socket that is connected and has bound the abstract address `name`,
// in the given order, and sets `peer_fd` to the other end of the connection,
// which is accepted from the listening socket for the first two kinds
static int create_bound_connected(enum bind_kind kind, int listen_fd,
                                  const char *listen_name,
                                  const char *name, int *peer_fd) {
    int fd;
    if (kind == SOCKETPAIR_THEN_BIND) {
        int fds[2];
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) < 0) {
            THROW_ERROR("failed to create a socket pair");
        }
        fd = fds[0];
        *peer_fd = fds[1];
    } else {
        fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (fd < 0) {
            THROW_ERROR("failed to create a unix socket");
        }
        if (kind == BIND_THEN_CONNECT && abstract_bind(fd, name) < 0) {
            close(fd);
            THROW_ERROR("failed to bind %s before connect", name);
        }
        if (abstract_connect(fd, listen_name) < 0) {
            close(fd);
            THROW_ERROR("failed to connect to %s", listen_name);
        }
        *peer_fd = accept(listen_fd, NULL, NULL);
        if (*peer_fd < 0) {
            close(fd);
            THROW_ERROR("failed to accept the connection");
        }
    }
    if (kind != BIND_THEN_CONNECT && abstract_bind(fd, name) < 0) {
        close_files(2, fd, *peer_fd);
        THROW_ERROR("failed to bind %s after connect", name);
    }
    return fd;
}

// Binding the address fails with EADDRINUSE
static int check_name_in_use(const char *name) {
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        THROW_ERROR("failed to create a unix socket");
    }
    int ret = abstract_bind(fd, name);
    if (ret == 0 || errno != EADDRINUSE) {
        printf("\t\tERROR: bind to %s returned %d with errno %d (%s), expected EADDRINUSE\n",
               name, ret, errno, strerror(errno));
        ret = -1;
    } else {
        ret = 0;
    }
    close(fd);
    return ret;
}

// The abstract address can be bound
static int check_name_free(const char *name, enum bind_kind kind) {
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        THROW_ERROR("failed to create a unix socket");
    }
    int ret = 0;
    if (abstract_bind(fd, name) < 0) {
        printf("\t\tERROR: bind to %s failed with errno %d (%s) after the socket that had bound it "
               "(kind %d) was closed\n", name, errno, strerror(errno), kind);
        ret = -1;
    }
    close(fd);
    return ret;
}

static int check_names_released(int listen_fd, const char *listen_name) {
    const char *name = "unix_socket_released_name";
    for (enum bind_kind kind = 0; kind < NUM_BIND_KINDS; kind++) {
        int peer_fd;
        int fd = create_bound_connected(kind, listen_fd, listen_name, name, &peer_fd);
        if (fd < 0) {
            return -1;
        }
        // The socket owns the name as long as it is open
        if (check_name_in_use(name) < 0) {
            close_files(2, fd, peer_fd);
            return -1;
        }
        close_files(2, fd, peer_fd);
        if (check_name_free(name, kind) < 0) {
            return -1;
        }
    }

    // The socket that accept() returns has the name of the listening socket,
    // but the latter owns it
    int client_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (client_fd < 0 || abstract_connect(client_fd, listen_name) < 0) {
        printf("\t\tERROR: failed to connect to %s: %s\n", listen_name, strerror(errno));
        close(client_fd);
        return -1;
    }
    int accepted_fd = accept(listen_fd, NULL, NULL);
    if (accepted_fd < 0) {
        printf("\t\tERROR: failed to accept the connection: %s\n", strerror(errno));
        close(client_fd);
        return -1;
    }
    // It has an address already, so it cannot bind another one
    int ret = abstract_bind(accepted_fd, "unix_socket_released_accepted");
    if (ret == 0 || errno != EINVAL) {
        printf("\t\tERROR: bind of an accepted socket returned %d with errno %d (%s), expected "
               "EINVAL\n", ret, errno, strerror(errno));
        close_files(2, client_fd, accepted_fd);
        return -1;
    }
    close_files(2, client_fd, accepted_fd);
    if (check_name_in_use(listen_name) < 0) {
        return -1;
    }
    return check_connection(listen_fd, listen_name);
}

// Like Linux, a socket that bound an abstract address releases it when it is
// closed, no matter if it was connected, and whether it bound before or after
// it connected. The socket that a listening socket accepts does not own the
// address of the listening socket.
int test_name_released_after_connect() {
    const char *listen_name = "unix_socket_released_listener";
    int listen_fd = abstract_listener(listen_name, 0);
    if (listen_fd < 0) {
        return -1;
    }
    int ret = check_names_released(listen_fd, listen_name);
    close(listen_fd);
    return ret;
}

#define BIND_CONNECT_CYCLES 1000
#define BIND_CONNECT_WARMUP_CYCLES 10
// A cycle leaked 70 bytes or more, so the unfixed growth is 70 kB. The heap is
// read in kB, so the noise of an allocation that is in flight when it is read
// is 1 kB at most.
#define BIND_CONNECT_LIMIT_KB 4

// A socket that bound an address and connected leaked its entry in the
// address space of the unix sockets. Each cycle needs a new name to detect it:
// the leaked entry blocks the name.
static int bind_connect_cycles(int listen_fd, const char *listen_name, int first,
                               int count) {
    for (int i = first; i < first + count; i++) {
        char name[64];
        snprintf(name, sizeof(name), "unix_socket_cycle_%d", i);
        int peer_fd;
        int fd = create_bound_connected(i % NUM_BIND_KINDS, listen_fd, listen_name, name,
                                        &peer_fd);
        if (fd < 0) {
            return -1;
        }
        close_files(2, fd, peer_fd);
    }
    return 0;
}

static int check_bind_connect_leaks_nothing(int listen_fd, const char *listen_name) {
    if (bind_connect_cycles(listen_fd, listen_name, 0, BIND_CONNECT_WARMUP_CYCLES) < 0) {
        return -1;
    }
    long before = kernel_heap_in_use();
    if (before == -2) {
        printf("\t\tSKIPPED: the kernel heap monitor is not enabled\n");
        return 0;
    }
    if (before < 0) {
        THROW_ERROR("failed to get the kernel heap in use");
    }
    if (bind_connect_cycles(listen_fd, listen_name, BIND_CONNECT_WARMUP_CYCLES,
                            BIND_CONNECT_CYCLES) < 0) {
        return -1;
    }
    long after = kernel_heap_in_use();
    if (after - before > BIND_CONNECT_LIMIT_KB) {
        printf("\t\tERROR: the kernel heap grew by %ld kB in %d cycles (limit %d kB)\n",
               after - before, BIND_CONNECT_CYCLES, BIND_CONNECT_LIMIT_KB);
        return -1;
    }
    return 0;
}

int test_bind_connect_leaks_nothing() {
    const char *listen_name = "unix_socket_cycle_listener";
    int listen_fd = abstract_listener(listen_name, 0);
    if (listen_fd < 0) {
        return -1;
    }
    int ret = check_bind_connect_leaks_nothing(listen_fd, listen_name);
    close(listen_fd);
    return ret;
}

static test_case_t test_cases[] = {
    TEST_CASE(test_unix_socket_inter_process),
    TEST_CASE(test_socketpair_inter_process),
    TEST_CASE(test_multiple_socketpairs),
    TEST_CASE(test_poll),
    TEST_CASE(test_getname),
    TEST_CASE(test_ioctl_fionread),
    TEST_CASE(test_unix_socket_rename),
    TEST_CASE(test_epoll_wait),
    TEST_CASE(test_sendmsg_recvmsg),
    TEST_CASE(test_getsockopt),
    TEST_CASE(test_listen_negative_backlog),
    TEST_CASE(test_unnamed_peer_address),
    TEST_CASE(test_accept_while_writing),
    TEST_CASE(test_epoll_in_after_connect),
    TEST_CASE(test_accept_interrupted),
    TEST_CASE(test_name_released_after_connect),
    TEST_CASE(test_bind_connect_leaks_nothing),
};

int main(int argc, const char *argv[]) {
    return test_suite_run(test_cases, ARRAY_SIZE(test_cases));
}
