#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <unistd.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <spawn.h>
#include <string.h>
#include <errno.h>
#include <stdarg.h>
#include <pthread.h>
#include "test.h"
#include "kernel_heap.h"

// ============================================================================
// Helper definition
// ============================================================================

#define MAXEVENTS 64
#define TEST_DATA 678

struct thread_arg {
    pthread_t tid;
    int fd;
    uint64_t data;
};

// ============================================================================
// Helper functions
// ============================================================================

static void *thread_child(void *arg) {
    struct thread_arg *child_arg = arg;

    printf("epoll_wait 1...\n");
    struct epoll_event events[MAXEVENTS] = {0};
    int nfds = epoll_wait(child_arg->fd, events, MAXEVENTS, -1);
    if (nfds < 0) {
        return (void *) -1;
    }
    printf("epoll_wait 1 success.\n");

    sleep(1);

    printf("epoll_wait 2...\n");
    nfds = epoll_wait(child_arg->fd, events, MAXEVENTS, -1);
    if (nfds < 0) {
        return (void *) -1;
    }
    printf("epoll_wait 2 success.\n");
    return NULL;
}

int create_child(struct thread_arg *arg) {
    pthread_attr_t attr;
    if (pthread_attr_init(&attr) != 0) {
        THROW_ERROR("failed to initialize attribute");
    }

    if (pthread_create(&(arg->tid), &attr, &thread_child, arg) != 0) {
        if (pthread_attr_destroy(&attr) != 0) {
            THROW_ERROR("failed to destroy attr");
        }
        THROW_ERROR("failed to create the thread");
    }

    if (pthread_attr_destroy(&attr) != 0) {
        THROW_ERROR("failed to destroy attr");
    }

    return 0;
}

// This test intends to test that the epoll_wait can be waken epoll_ctl
int test_epoll_ctl_main(int end_fd_1, int end_fd_2) {
    uint64_t data = TEST_DATA;
    struct thread_arg child_arg;

    int epfd = epoll_create1(0);
    if (epfd == -1) {
        THROW_ERROR("epoll_create failed");
    }

    // watch for end_fd_1
    struct epoll_event event;
    event.data.fd = end_fd_1;
    event.events = EPOLLIN | EPOLLET;
    int ret = epoll_ctl(epfd, EPOLL_CTL_ADD, end_fd_1, &event);
    if (ret == -1) {
        close(epfd);
        THROW_ERROR("epoll_ctl add failed");
    }

    // write to end_fd_2
    int write_size = write(end_fd_2, &data, sizeof(data));
    if (write_size < 0) {
        THROW_ERROR("failed to write an eventfd");
    }

    child_arg.data = 0;
    child_arg.fd = epfd;
    child_arg.tid = 0;
    if (create_child(&child_arg) != 0) {
        close(epfd);
        THROW_ERROR("failed to create children");
    }

    // wait for child thread to start second time epoll_wait
    sleep(3);

    printf("second time epoll ctl\n");
    ret = epoll_ctl(epfd, EPOLL_CTL_MOD, end_fd_1, &event);
    if (ret == -1) {
        close(epfd);
        THROW_ERROR("epoll_ctl mod failed");
    }

    pthread_join(child_arg.tid, NULL);
    close(epfd);

    return 0;
}

// ============================================================================
// Test cases for anonymous mmap
// ============================================================================

int test_epoll_ctl_uds() {
    int sockets[2];

    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) < 0) {
        THROW_ERROR("opening stream socket pair");
    }

    int ret = test_epoll_ctl_main(sockets[0], sockets[1]);
    close(sockets[0]);
    close(sockets[1]);

    if (ret < 0) {
        THROW_ERROR("epoll ctl test eventfd failure");
    }

    return 0;
}

int test_epoll_ctl_eventfd() {
    int event_fd = eventfd(0, EFD_NONBLOCK);
    if (event_fd < 0) {
        THROW_ERROR("failed to create an eventfd");
    }

    int ret = test_epoll_ctl_main(event_fd, event_fd);
    close(event_fd);

    if (ret < 0) {
        THROW_ERROR("epoll ctl test eventfd failure");
    }
    return 0;
}

// Like closing the fd, replacing a monitored file with dup2() removes it from
// the epoll file, so that the fd can be added again with the new file. This
// is how an LD_PRELOAD library such as ip2unix replaces a socket.
int test_epoll_ctl_after_dup2() {
    int ret = -1;
    int epfd = epoll_create1(0);
    int old_fd = eventfd(1, EFD_NONBLOCK); // readable
    int new_fd = eventfd(0, EFD_NONBLOCK); // not readable
    if (epfd < 0 || old_fd < 0 || new_fd < 0) {
        THROW_ERROR("failed to create the files");
    }

    struct epoll_event event = { .events = EPOLLIN, .data.u64 = 1 };
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, old_fd, &event) < 0) {
        printf("\t\tERROR: epoll_ctl add failed: %s\n", strerror(errno));
        goto out;
    }
    if (dup2(new_fd, old_fd) != old_fd) {
        printf("\t\tERROR: dup2 failed: %s\n", strerror(errno));
        goto out;
    }
    close(new_fd);
    new_fd = -1;

    struct epoll_event events[MAXEVENTS];
    int nfds = epoll_wait(epfd, events, MAXEVENTS, 0);
    if (nfds != 0) {
        printf("\t\tERROR: epoll_wait returned %d instead of no event for the replaced file\n",
               nfds);
        goto out;
    }

    event.data.u64 = 2;
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, old_fd, &event) < 0) {
        printf("\t\tERROR: epoll_ctl add after dup2 failed: %s\n", strerror(errno));
        goto out;
    }
    uint64_t one = 1;
    if (write(old_fd, &one, sizeof(one)) != sizeof(one)) {
        printf("\t\tERROR: failed to write the eventfd: %s\n", strerror(errno));
        goto out;
    }
    nfds = epoll_wait(epfd, events, MAXEVENTS, 0);
    if (nfds != 1 || events[0].data.u64 != 2) {
        printf("\t\tERROR: epoll_wait returned %d events instead of one of the new file\n", nfds);
        goto out;
    }
    ret = 0;

out:
    close(epfd);
    close(old_fd);
    if (new_fd >= 0) {
        close(new_fd);
    }
    return ret;
}

// Closing a socket that is in an epoll file, and in its ready list, closes it
// right away: its peer gets end of file without another epoll_wait()
int test_epoll_close_ready_socket() {
    int sockets[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) < 0) {
        THROW_ERROR("failed to create a socket pair");
    }
    int epfd = epoll_create1(0);
    struct epoll_event event = { .events = EPOLLIN, .data.fd = sockets[0] };
    char c = 'x';
    int ret = -1;
    if (epfd < 0 || epoll_ctl(epfd, EPOLL_CTL_ADD, sockets[0], &event) < 0 ||
            write(sockets[1], &c, 1) != 1) {
        printf("\t\tERROR: failed to set up: %s\n", strerror(errno));
        goto out;
    }
    // Level-triggered, so the socket stays in the ready list. Without unread
    // data, as closing the socket would reset the connection otherwise.
    if (epoll_wait(epfd, &event, 1, 1000) != 1 || read(sockets[0], &c, 1) != 1) {
        printf("\t\tERROR: no event or no data\n");
        goto out;
    }
    close(sockets[0]);
    sockets[0] = -1;

    struct pollfd pfd = { .fd = sockets[1], .events = POLLIN };
    if (poll(&pfd, 1, 1000) != 1 || read(sockets[1], &c, 1) != 0) {
        printf("\t\tERROR: the peer did not get end of file\n");
        goto out;
    }
    ret = 0;

out:
    if (sockets[0] >= 0) {
        close(sockets[0]);
    }
    close(sockets[1]);
    close(epfd);
    return ret;
}

// ============================================================================
// Test cases: closing a monitored file removes it from the epoll file
// ============================================================================

// Closes one end of a socket pair that the epoll file monitors. That removes the
// socket from the epoll file, so that the other end gets end of file at once.
// Otherwise the epoll file keeps the socket open.
static int check_close_monitored_socket(int epfd) {
    int sockets[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) < 0) {
        THROW_ERROR("failed to create a socket pair");
    }
    int ret = -1;
    struct epoll_event event = { .events = EPOLLIN, .data.fd = sockets[0] };
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, sockets[0], &event) < 0) {
        printf("\t\tERROR: epoll_ctl add failed: %s\n", strerror(errno));
        goto out;
    }
    close(sockets[0]);
    sockets[0] = -1;

    char c;
    struct pollfd pfd = { .fd = sockets[1], .events = POLLIN };
    if (poll(&pfd, 1, 1000) != 1 || read(sockets[1], &c, 1) != 0) {
        printf("\t\tERROR: the peer did not get end of file\n");
        goto out;
    }
    ret = 0;

out:
    if (sockets[0] >= 0) {
        close(sockets[0]);
    }
    close(sockets[1]);
    return ret;
}

int test_epoll_close_monitored_socket() {
    int epfd = epoll_create1(0);
    if (epfd < 0) {
        THROW_ERROR("failed to create the epoll file");
    }
    int ret = check_close_monitored_socket(epfd);
    close(epfd);
    return ret;
}

// The epoll file is open under another fd, too. Closing one of the fds must
// not stop the file table from telling the epoll file about the closed files.
int test_epoll_close_monitored_socket_after_closing_epfd() {
    int epfd = epoll_create1(0);
    int epfd2 = dup(epfd);
    if (epfd < 0 || epfd2 < 0) {
        THROW_ERROR("failed to create the epoll file");
    }
    close(epfd);
    int ret = check_close_monitored_socket(epfd2);
    close(epfd2);
    return ret;
}

int test_epoll_close_monitored_socket_after_closing_dup() {
    int epfd = epoll_create1(0);
    int epfd2 = fcntl(epfd, F_DUPFD, 100);
    if (epfd < 0 || epfd2 < 100) {
        THROW_ERROR("failed to create the epoll file");
    }
    close(epfd2);
    int ret = check_close_monitored_socket(epfd);
    close(epfd);
    return ret;
}

// Only the close of the last fd ends it, not that of the second to last
int test_epoll_close_monitored_socket_after_closing_two_of_three_fds() {
    int epfd = epoll_create1(0);
    int epfd2 = dup(epfd);
    int epfd3 = dup(epfd);
    if (epfd < 0 || epfd2 < 0 || epfd3 < 0) {
        THROW_ERROR("failed to create the epoll file");
    }
    close(epfd2);
    close(epfd);
    int ret = check_close_monitored_socket(epfd3);
    close(epfd3);
    return ret;
}

// The fd of a closed socket can be added to the epoll file again, like the next
// socket that gets the same fd number
int test_epoll_close_monitored_socket_reuse_fd() {
    int epfd = epoll_create1(0);
    int epfd2 = dup(epfd);
    if (epfd < 0 || epfd2 < 0) {
        THROW_ERROR("failed to create the epoll file");
    }
    close(epfd);
    int ret = -1;
    for (int i = 0; i < 3; i++) {
        int sockets[2];
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) < 0) {
            printf("\t\tERROR: failed to create a socket pair: %s\n", strerror(errno));
            goto out;
        }
        struct epoll_event event = { .events = EPOLLIN, .data.fd = sockets[0] };
        int added = epoll_ctl(epfd2, EPOLL_CTL_ADD, sockets[0], &event);
        close_files(2, sockets[0], sockets[1]);
        if (added < 0) {
            printf("\t\tERROR: epoll_ctl add failed in round %d: %s\n", i, strerror(errno));
            goto out;
        }
    }
    ret = 0;

out:
    close(epfd2);
    return ret;
}

// An epoll file that does not hear about the closed sockets keeps them open,
// with their ring buffers (416 kB for a pair of unix stream sockets), until
// it is closed, so the kernel heap runs out after some hundred pairs.
#define CLOSED_PAIRS 20
#define CLOSED_PAIRS_LIMIT_KB 256

int test_epoll_close_monitored_socket_no_leak() {
    int epfd = epoll_create1(0);
    int epfd2 = dup(epfd);
    if (epfd < 0 || epfd2 < 0) {
        THROW_ERROR("failed to create the epoll file");
    }
    close(epfd);

    int ret = -1;
    int sockets[CLOSED_PAIRS][2];
    int num_pairs = 0;
    long before = kernel_heap_in_use();
    if (before == -2) {
        printf("\t\tSKIPPED: the kernel heap monitor is not enabled\n");
        ret = 0;
        goto out;
    }
    if (before < 0) {
        printf("\t\tERROR: failed to get the kernel heap in use\n");
        goto out;
    }
    // Different fds, so that no new socket replaces an entry of the epoll file
    for (; num_pairs < CLOSED_PAIRS; num_pairs++) {
        struct epoll_event event = { .events = EPOLLIN };
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, sockets[num_pairs]) < 0) {
            printf("\t\tERROR: failed to create a socket pair: %s\n", strerror(errno));
            goto out;
        }
        event.data.fd = sockets[num_pairs][0];
        if (epoll_ctl(epfd2, EPOLL_CTL_ADD, sockets[num_pairs][0], &event) < 0) {
            printf("\t\tERROR: epoll_ctl add failed: %s\n", strerror(errno));
            close_files(2, sockets[num_pairs][0], sockets[num_pairs][1]);
            goto out;
        }
    }
    ret = 0;

out:
    for (int i = 0; i < num_pairs; i++) {
        close_files(2, sockets[i][0], sockets[i][1]);
    }
    if (ret == 0 && before >= 0) {
        long after = kernel_heap_in_use();
        if (after < 0) {
            printf("\t\tERROR: failed to get the kernel heap in use\n");
            ret = -1;
        } else if (after - before > CLOSED_PAIRS_LIMIT_KB) {
            printf("\t\tERROR: the kernel heap grew by %ld kB with %d closed socket pairs "
                   "(limit %d kB)\n", after - before, CLOSED_PAIRS, CLOSED_PAIRS_LIMIT_KB);
            ret = -1;
        }
    }
    close(epfd2);
    return ret;
}

// ============================================================================
// Test cases: the files of a process that ends are removed from the epoll files
// ============================================================================

// A process that ends closes all its files. The epoll files that outlive it,
// like the one that a child it spawned inherited, get to know which fds were
// closed, and drop the files. The owner process has gaps in its fd numbers, so
// that the number of a fd is more than its index among the open fds.
#define OWNER_CTL_FD 50
#define OWNER_SOCKET_FD_A 60
#define OWNER_SOCKET_FD_B 70
#define OWNER_GO_FD 80
// The owner has 8 fds (0, 1, 2, the epoll file, and the four above), but not
// this one, which is the number of a socket of the holder
#define HOLDER_SOCKET_FD 6
#define HOLDER_DATA 678

static const char *g_self_path = "/bin/epoll";

// The process that the test spawns. It makes an epoll file that monitors two
// sockets, spawns the holder, which inherits the epoll file, waits until the
// test tells it to end, and ends. It ends with exit, or, if die_in_vfork_child
// is set, with a vforked child that a signal kills, which takes the process
// with it, and nobody returns to the parent. The sockets are not inherited.
static int owner_main(int die_in_vfork_child) {
    int epfd = epoll_create1(0);
    if (epfd < 0) {
        return 1;
    }
    int socket_fds[] = { OWNER_SOCKET_FD_A, OWNER_SOCKET_FD_B };
    for (int i = 0; i < 2; i++) {
        struct epoll_event event = { .events = EPOLLIN, .data.fd = socket_fds[i] };
        if (fcntl(socket_fds[i], F_SETFD, FD_CLOEXEC) < 0 ||
                epoll_ctl(epfd, EPOLL_CTL_ADD, socket_fds[i], &event) < 0) {
            return 2;
        }
    }
    if (fcntl(OWNER_GO_FD, F_SETFD, FD_CLOEXEC) < 0) {
        return 3;
    }
    pid_t holder;
    char epfd_arg[16];
    snprintf(epfd_arg, sizeof(epfd_arg), "%d", epfd);
    char *argv[] = { (char *)g_self_path, "--holder", epfd_arg, NULL };
    if (posix_spawn(&holder, g_self_path, NULL, NULL, argv, NULL) != 0) {
        return 4;
    }

    char c;
    if (read(OWNER_GO_FD, &c, 1) != 1) {
        return 5;
    }
    if (die_in_vfork_child) {
        if (vfork() == 0) {
            kill(getpid(), SIGKILL);
            _exit(1);
        }
        kill(getpid(), SIGKILL);
        return 6;
    }
    return 0;
}

// The process that holds the epoll file of the owner open, until the test tells
// it to end. It adds a socket of its own to the epoll file, which the end of the
// owner must not remove.
static int holder_main(int epfd) {
    int sockets[2];
    struct epoll_event event = { .events = EPOLLIN, .data.u32 = HOLDER_DATA };
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) < 0 ||
            dup2(sockets[0], HOLDER_SOCKET_FD) != HOLDER_SOCKET_FD ||
            write(sockets[1], "x", 1) != 1 ||
            epoll_ctl(epfd, EPOLL_CTL_ADD, HOLDER_SOCKET_FD, &event) < 0) {
        return 1;
    }
    char c;
    if (write(OWNER_CTL_FD, "R", 1) != 1 || read(OWNER_CTL_FD, &c, 1) != 1 || c != 'q') {
        return 2;
    }
    struct epoll_event ready;
    c = epoll_wait(epfd, &ready, 1, 3000) == 1 && ready.data.u32 == HOLDER_DATA ? 'A' : 'F';
    return write(OWNER_CTL_FD, &c, 1) == 1 ? 0 : 3;
}

static int check_end_of_file(int fd) {
    char c;
    struct pollfd pfd = { .fd = fd, .events = POLLIN };
    return poll(&pfd, 1, 3000) == 1 && read(fd, &c, 1) == 0 ? 0 : -1;
}

static int check_closed_when_owner_ends(int die_in_vfork_child) {
    int ctl[2], peer_a[2], peer_b[2], go[2];
    if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, ctl) < 0 ||
            socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, peer_a) < 0 ||
            socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, peer_b) < 0 ||
            socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, go) < 0) {
        THROW_ERROR("failed to create the socket pairs");
    }
    // The owner gets the control socket, the go socket, and one end of each of the other pairs
    posix_spawn_file_actions_t file_actions;
    posix_spawn_file_actions_init(&file_actions);
    posix_spawn_file_actions_adddup2(&file_actions, ctl[1], OWNER_CTL_FD);
    posix_spawn_file_actions_adddup2(&file_actions, peer_a[0], OWNER_SOCKET_FD_A);
    posix_spawn_file_actions_adddup2(&file_actions, peer_b[0], OWNER_SOCKET_FD_B);
    posix_spawn_file_actions_adddup2(&file_actions, go[1], OWNER_GO_FD);
    pid_t owner;
    char *mode = die_in_vfork_child ? "--owner-vfork" : "--owner";
    char *argv[] = { (char *)g_self_path, mode, NULL };
    int spawned = posix_spawn(&owner, g_self_path, &file_actions, NULL, argv, NULL);
    posix_spawn_file_actions_destroy(&file_actions);
    close_files(4, ctl[1], peer_a[0], peer_b[0], go[1]);

    int ret = -1;
    if (spawned != 0) {
        printf("\t\tERROR: failed to spawn the owner: %s\n", strerror(spawned));
        goto out;
    }
    // The holder is running, and has the epoll file
    char c = 0;
    struct pollfd pfd = { .fd = ctl[0], .events = POLLIN };
    if (poll(&pfd, 1, 5000) != 1 || read(ctl[0], &c, 1) != 1 || c != 'R') {
        printf("\t\tERROR: the holder did not start\n");
        goto out;
    }
    if (send(go[0], "g", 1, MSG_NOSIGNAL) != 1) {
        printf("\t\tERROR: the owner ended before the holder started\n");
        goto out;
    }
    int status = 0;
    if (waitpid(owner, &status, 0) != owner ||
            (die_in_vfork_child ? !WIFSIGNALED(status) || WTERMSIG(status) != SIGKILL :
             !WIFEXITED(status) || WEXITSTATUS(status) != 0)) {
        printf("\t\tERROR: the owner did not end as expected (status 0x%x)\n", status);
        goto out;
    }

    // The sockets that the owner had are closed, and removed from the epoll file
    if (check_end_of_file(peer_a[1]) < 0 || check_end_of_file(peer_b[1]) < 0) {
        printf("\t\tERROR: the peer did not get end of file after the end of the owner\n");
        goto out;
    }

    // The holder has been alive all the time, and held the epoll file. The socket
    // that it added is still in the epoll file.
    if (send(ctl[0], "q", 1, MSG_NOSIGNAL) != 1 || poll(&pfd, 1, 5000) != 1 ||
            read(ctl[0], &c, 1) != 1) {
        printf("\t\tERROR: the holder ended before the test did\n");
        goto out;
    }
    if (c != 'A') {
        printf("\t\tERROR: the socket of the holder is not in the epoll file any more\n");
        goto out;
    }
    ret = 0;

out:
    // The holder ends when it sees that the control socket is closed
    close_files(4, ctl[0], peer_a[1], peer_b[1], go[0]);
    return ret;
}

int test_epoll_close_monitored_sockets_at_exit() {
    return check_closed_when_owner_ends(0);
}

int test_epoll_close_monitored_sockets_at_death_in_vfork_child() {
    return check_closed_when_owner_ends(1);
}

// ============================================================================
// Test cases: epoll_wait does not leak kernel heap
// ============================================================================

// Every call to epoll_wait used to leave a waker in the wait queue of the epoll
// file, which was only emptied by an event on the epoll file. So the kernel
// heap grew with every call on an epoll file without events (about 42 bytes
// per call), until the enclave ran out of heap.
static int check_epoll_wait_leak(int epfd, int timeout_ms, int expected_nfds, int calls,
                                 long limit_kb) {
    struct epoll_event event;
    // Let the allocations that stay settle
    for (int i = 0; i < 1000; i++) {
        if (epoll_wait(epfd, &event, 1, timeout_ms) != expected_nfds) {
            THROW_ERROR("unexpected result of epoll_wait");
        }
    }
    long before = kernel_heap_in_use();
    if (before == -2) {
        printf("\t\tSKIPPED: the kernel heap monitor is not enabled\n");
        return 0;
    }
    if (before < 0) {
        THROW_ERROR("failed to get the kernel heap in use");
    }
    for (int i = 0; i < calls; i++) {
        if (epoll_wait(epfd, &event, 1, timeout_ms) != expected_nfds) {
            THROW_ERROR("unexpected result of epoll_wait");
        }
    }
    long after = kernel_heap_in_use();
    if (after - before > limit_kb) {
        THROW_ERROR("the kernel heap grew by %ld kB in %d calls (limit %ld kB)",
                    after - before, calls, limit_kb);
    }
    return 0;
}

static int create_epoll_eventfd(int *epfd, int *efd) {
    *efd = eventfd(0, EFD_NONBLOCK);
    *epfd = epoll_create1(0);
    struct epoll_event event = { .events = EPOLLIN, .data.fd = *efd };
    if (*efd < 0 || *epfd < 0 || epoll_ctl(*epfd, EPOLL_CTL_ADD, *efd, &event) < 0) {
        THROW_ERROR("failed to set up the epoll file");
    }
    return 0;
}

// No event and no waiting: the call returns right away
int test_epoll_wait_idle_no_leak() {
    int epfd, efd;
    if (create_epoll_eventfd(&epfd, &efd) < 0) {
        return -1;
    }
    int ret = check_epoll_wait_leak(epfd, 0, 0, 200000, 1024);
    close(efd);
    close(epfd);
    return ret;
}

// A level-triggered event of a LibOS file (not a host file) that stays ready: the
// call returns right away with the event
int test_epoll_wait_ready_no_leak() {
    int pipe_fds[2];
    if (pipe(pipe_fds) < 0) {
        THROW_ERROR("failed to create the pipe");
    }
    int epfd = epoll_create1(0);
    struct epoll_event event = { .events = EPOLLIN, .data.fd = pipe_fds[0] };
    if (epfd < 0 || epoll_ctl(epfd, EPOLL_CTL_ADD, pipe_fds[0], &event) < 0 ||
            write(pipe_fds[1], "x", 1) != 1) {
        THROW_ERROR("failed to set up the epoll file");
    }
    int ret = check_epoll_wait_leak(epfd, 0, 1, 100000, 1024);
    close_files(3, pipe_fds[0], pipe_fds[1], epfd);
    return ret;
}

// No event: the call sleeps until the timeout is up
int test_epoll_wait_timeout_no_leak() {
    int epfd, efd;
    if (create_epoll_eventfd(&epfd, &efd) < 0) {
        return -1;
    }
    int ret = check_epoll_wait_leak(epfd, 1, 0, 2000, 32);
    close(efd);
    close(epfd);
    return ret;
}

#define PING_PONG_ROUNDS 5000

struct ping_pong_arg {
    int epfd;
    int in_fd;
    int out_fd;
};

// Waits for an event on in_fd, reads it, and writes out_fd unless it is -1
static int ping_pong_step(int epfd, int in_fd, int out_fd) {
    struct epoll_event event;
    char c;
    if (epoll_wait(epfd, &event, 1, 5000) != 1) {
        return -1;
    }
    if (read(in_fd, &c, 1) != 1) {
        return -1;
    }
    if (out_fd >= 0 && write(out_fd, "x", 1) != 1) {
        return -1;
    }
    return 0;
}

static void *ping_pong_child(void *_arg) {
    struct ping_pong_arg *arg = _arg;
    for (int i = 0; i < PING_PONG_ROUNDS; i++) {
        if (ping_pong_step(arg->epfd, arg->in_fd, arg->out_fd) < 0) {
            return (void *) -1;
        }
    }
    return NULL;
}

static int create_epoll_pipe(int *epfd, int pipe_fds[2]) {
    *epfd = epoll_create1(0);
    struct epoll_event event = { .events = EPOLLIN, .data.fd = 0 };
    if (pipe(pipe_fds) < 0 || *epfd < 0) {
        THROW_ERROR("failed to create the epoll file or the pipe");
    }
    event.data.fd = pipe_fds[0];
    if (epoll_ctl(*epfd, EPOLL_CTL_ADD, pipe_fds[0], &event) < 0) {
        THROW_ERROR("failed to add the pipe to the epoll file");
    }
    return 0;
}

// Two threads wake up each other through epoll files in turn, so that every
// epoll_wait sleeps until the other thread has written. The events are those of
// LibOS files, which wake up the sleeping thread through the wait queue of the
// epoll file. Waiters are removed from the wait queue when the waits end, which
// must not lose the wake-ups of the other waits.
int test_epoll_wait_ping_pong() {
    int epfd_a, epfd_b, pipe_a[2], pipe_b[2];
    if (create_epoll_pipe(&epfd_a, pipe_a) < 0 || create_epoll_pipe(&epfd_b, pipe_b) < 0) {
        return -1;
    }
    // The child reads pipe a and writes pipe b, the main thread the other way around
    struct ping_pong_arg child_arg = { .epfd = epfd_a, .in_fd = pipe_a[0], .out_fd = pipe_b[1] };
    pthread_t child;
    if (pthread_create(&child, NULL, ping_pong_child, &child_arg) != 0) {
        THROW_ERROR("failed to create the thread");
    }

    int failed = 0;
    for (int i = 0; i < PING_PONG_ROUNDS && !failed; i++) {
        if (write(pipe_a[1], "x", 1) != 1 || ping_pong_step(epfd_b, pipe_b[0], -1) < 0) {
            failed = 1;
        }
    }
    // A child that is stuck in epoll_wait gives up after 5 seconds
    void *child_ret = NULL;
    pthread_join(child, &child_ret);
    if (failed || child_ret != NULL) {
        THROW_ERROR("a thread was not woken up");
    }
    close_files(6, pipe_a[0], pipe_a[1], pipe_b[0], pipe_b[1], epfd_a, epfd_b);
    return 0;
}

static void *sleeping_waiter(void *arg) {
    int epfd = *(int *) arg;
    struct epoll_event event;
    return epoll_wait(epfd, &event, 1, 5000) == 1 ? NULL : (void *) -1;
}

// Two threads wait on one epoll file. The waits of one thread end without an
// event again and again, which dequeues its waiter from the wait queue of the epoll
// file each time. That must not dequeue the waiter of the other thread, which
// is still asleep and has to be woken up by the event.
int test_epoll_wait_two_waiters() {
    int epfd, pipe_fds[2];
    if (create_epoll_pipe(&epfd, pipe_fds) < 0) {
        return -1;
    }
    pthread_t sleeper;
    if (pthread_create(&sleeper, NULL, sleeping_waiter, &epfd) != 0) {
        THROW_ERROR("failed to create the thread");
    }
    // Let the thread fall asleep
    usleep(200 * 1000);

    int failed = 0;
    struct epoll_event event;
    for (int i = 0; i < 20 && !failed; i++) {
        failed = epoll_wait(epfd, &event, 1, 10) != 0;
    }
    if (write(pipe_fds[1], "x", 1) != 1) {
        failed = 1;
    }
    // A thread that is not woken up gives up after 5 seconds
    void *sleeper_ret = NULL;
    pthread_join(sleeper, &sleeper_ret);
    if (failed || sleeper_ret != NULL) {
        THROW_ERROR("the sleeping thread was not woken up");
    }
    close_files(3, pipe_fds[0], pipe_fds[1], epfd);
    return 0;
}

// ============================================================================
// Test suite main
// ============================================================================

static test_case_t test_cases[] = {
    TEST_CASE(test_epoll_ctl_eventfd),
    TEST_CASE(test_epoll_ctl_uds),
    TEST_CASE(test_epoll_ctl_after_dup2),
    TEST_CASE(test_epoll_close_ready_socket),
    TEST_CASE(test_epoll_close_monitored_socket),
    TEST_CASE(test_epoll_close_monitored_socket_after_closing_epfd),
    TEST_CASE(test_epoll_close_monitored_socket_after_closing_dup),
    TEST_CASE(test_epoll_close_monitored_socket_after_closing_two_of_three_fds),
    TEST_CASE(test_epoll_close_monitored_socket_reuse_fd),
    TEST_CASE(test_epoll_close_monitored_socket_no_leak),
    TEST_CASE(test_epoll_close_monitored_sockets_at_exit),
    TEST_CASE(test_epoll_close_monitored_sockets_at_death_in_vfork_child),
    TEST_CASE(test_epoll_wait_idle_no_leak),
    TEST_CASE(test_epoll_wait_ready_no_leak),
    TEST_CASE(test_epoll_wait_timeout_no_leak),
    TEST_CASE(test_epoll_wait_ping_pong),
    TEST_CASE(test_epoll_wait_two_waiters),
};

int main(int argc, const char *argv[]) {
    if (argc > 1 && strcmp(argv[1], "--owner") == 0) {
        return owner_main(0);
    }
    if (argc > 1 && strcmp(argv[1], "--owner-vfork") == 0) {
        return owner_main(1);
    }
    if (argc > 2 && strcmp(argv[1], "--holder") == 0) {
        return holder_main(atoi(argv[2]));
    }
    return test_suite_run(test_cases, ARRAY_SIZE(test_cases));
}
