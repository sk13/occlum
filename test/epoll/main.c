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
#include <stdlib.h>
#include <stdio.h>
#include <spawn.h>
#include <string.h>
#include <errno.h>
#include <stdarg.h>
#include <pthread.h>
#include "test.h"

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
// Test suite main
// ============================================================================

static test_case_t test_cases[] = {
    TEST_CASE(test_epoll_ctl_eventfd),
    TEST_CASE(test_epoll_ctl_uds),
    TEST_CASE(test_epoll_ctl_after_dup2),
    TEST_CASE(test_epoll_close_ready_socket),
};

int main() {
    return test_suite_run(test_cases, ARRAY_SIZE(test_cases));
}
