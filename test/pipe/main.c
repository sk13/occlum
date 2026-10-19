#define _GNU_SOURCE
#include <errno.h>
#include <sys/epoll.h>
#include <sys/select.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <spawn.h>
#include <string.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <sys/uio.h>

#include "test.h"

// ============================================================================
// Helper function
// ============================================================================
static void free_pipe(int *pipe) {
    close(pipe[0]);
    close(pipe[1]);
}

// ============================================================================
// Test cases
// ============================================================================
int test_fstat() {
    int pipe_fds[2];
    struct stat stat_bufs[2];
    if (pipe(pipe_fds) < 0) {
        THROW_ERROR("failed to create a pipe");
    }
    if (fstat(pipe_fds[0], &stat_bufs[0]) < 0 || fstat(pipe_fds[1], &stat_bufs[1]) < 0) {
        free_pipe(pipe_fds);
        THROW_ERROR("failed to fstat pipe fd");
    }
    free_pipe(pipe_fds);
    if (!S_ISFIFO(stat_bufs[0].st_mode) || !S_ISFIFO(stat_bufs[1].st_mode)) {
        THROW_ERROR("failed to check the pipe st_mode");
    }
    return 0;
}

int test_fcntl_get_flags() {
    int pipe_fds[2];
    if (pipe(pipe_fds) < 0) {
        THROW_ERROR("failed to create a pipe");
    }

    if ((fcntl(pipe_fds[0], F_GETFL, 0) != O_RDONLY) ||
            (fcntl(pipe_fds[1], F_GETFL, 0) != O_WRONLY)) {
        free_pipe(pipe_fds);
        THROW_ERROR("fcntl get flags failed");
    }

    free_pipe(pipe_fds);
    return 0;
}

int test_fcntl_set_flags() {
    int pipe_fds[2];
    if (pipe(pipe_fds) < 0) {
        THROW_ERROR("failed to create a pipe");
    }

    fcntl(pipe_fds[0], F_SETFL, O_NONBLOCK);
    if ((fcntl(pipe_fds[0], F_GETFL, 0) != (O_NONBLOCK | O_RDONLY)) ||
            (fcntl(pipe_fds[1], F_GETFL, 0) !=  O_WRONLY)) {
        free_pipe(pipe_fds);
        THROW_ERROR("fcntl set flags failed");
    }

    free_pipe(pipe_fds);
    return 0;
}

int test_create_with_flags() {
    int pipe_fds[2];
    if (pipe2(pipe_fds, O_NONBLOCK) < 0) {
        THROW_ERROR("failed to create a pipe");
    }

    if ((fcntl(pipe_fds[0], F_GETFL, 0) != (O_NONBLOCK | O_RDONLY)) ||
            (fcntl(pipe_fds[1], F_GETFL, 0) != (O_NONBLOCK | O_WRONLY))) {
        free_pipe(pipe_fds);
        THROW_ERROR("create flags failed\n");
    }

    free_pipe(pipe_fds);
    return 0;
}

int test_select_timeout() {
    fd_set rfds;

    int pipe_fds[2];
    if (pipe(pipe_fds) < 0) {
        THROW_ERROR("failed to create a pipe");
    }

    struct timeval tv = { .tv_sec = 1, .tv_usec = 0 };

    FD_ZERO(&rfds);
    FD_SET(pipe_fds[0], &rfds);
    struct timeval tv_start, tv_end;
    gettimeofday(&tv_start, NULL);
    select(pipe_fds[0] + 1, &rfds, NULL, NULL, &tv);
    gettimeofday(&tv_end, NULL);
    double total_s = tv_end.tv_sec - tv_start.tv_sec;
    if (total_s < 1) {
        printf("time consumed is %f\n",
               total_s + (double)(tv_end.tv_usec - tv_start.tv_usec) / 1000000);
        THROW_ERROR("select timer does not work correctly");
    }

    free_pipe(pipe_fds);
    return 0;
}

int test_epoll_timeout() {
    int pipe_fds[2];
    if (pipe(pipe_fds) < 0) {
        THROW_ERROR("failed to create a pipe");
    }
    int pipe_read_fd = pipe_fds[0];
    int pipe_write_fd = pipe_fds[1];

    int ep_fd = epoll_create1(0);
    if (ep_fd < 0) {
        THROW_ERROR("failed to create an epoll");
    }

    int ret;
    struct epoll_event event;

    event.events = EPOLLIN; // we want the write end to be readable
    event.data.u32 = pipe_write_fd;
    ret = epoll_ctl(ep_fd, EPOLL_CTL_ADD, pipe_write_fd, &event);
    if (ret < 0) {
        THROW_ERROR("failed to do epoll ctl");
    }

    event.events = EPOLLOUT; // we want the read end to be writable
    event.data.u32 = pipe_read_fd;
    ret = epoll_ctl(ep_fd, EPOLL_CTL_ADD, pipe_read_fd, &event);
    if (ret < 0) {
        THROW_ERROR("failed to do epoll ctl");
    }

    // We are waiting for the write end to be readable or the read end to be
    // writable, which can never happen. So the epoll_wait must end with
    // timeout.
    errno = 0;
    struct epoll_event events[2];
    ret = epoll_wait(ep_fd, events, ARRAY_SIZE(events), 10 /* ms */);
    if (ret != 0 || errno != 0) {
        THROW_ERROR("failed to do epoll ctl");
    }

    free_pipe(pipe_fds);
    close(ep_fd);
    return 0;
}

int test_poll_timeout() {
    // Start the timer
    struct timeval tv_start, tv_end;
    gettimeofday(&tv_start, NULL);
    int fds[2];
    if (pipe(fds) < 0) {
        THROW_ERROR("pipe failed");
    }
    struct pollfd polls[] = {
        { .fd = fds[0], .events = POLLOUT },
        { .fd = fds[1], .events = POLLIN }
    };

    poll(polls, 2, 1000);
    // Stop the timer
    gettimeofday(&tv_end, NULL);
    double total_s = tv_end.tv_sec - tv_start.tv_sec;
    if ((int)total_s < 1) {
        printf("time consumed is %f\n",
               total_s + (double)(tv_end.tv_usec - tv_start.tv_usec) / 1000000);
        THROW_ERROR("poll timer does not work correctly");
    }
    return 0;
}

int test_select_no_timeout() {
    fd_set wfds;
    int ret = 0;

    int pipe_fds[2];
    if (pipe(pipe_fds) < 0) {
        THROW_ERROR("failed to create a pipe");
    }

    FD_ZERO(&wfds);
    FD_SET(pipe_fds[1], &wfds);
    ret = select(pipe_fds[1] + 1, NULL, &wfds, NULL, NULL);
    if (ret != 1) {
        free_pipe(pipe_fds);
        THROW_ERROR("select failed");
    }

    if (FD_ISSET(pipe_fds[1], &wfds) == 0) {
        free_pipe(pipe_fds);
        THROW_ERROR("bad select return");
    }

    free_pipe(pipe_fds);
    return 0;
}

int test_poll_no_timeout() {
    int pipe_fds[2];
    if (pipe(pipe_fds) < 0) {
        THROW_ERROR("failed to create a pipe");
    }
    struct pollfd polls[] = {
        { .fd = pipe_fds[0], .events = POLLIN },
        { .fd = pipe_fds[1], .events = POLLOUT },
        { .fd = pipe_fds[1], .events = POLLOUT },
    };
    int ret = poll(polls, 3, -1);
    if (ret < 0) { THROW_ERROR("poll error"); }

    if (polls[0].revents != 0 || (polls[1].revents & POLLOUT) == 0 ||
            (polls[2].revents & POLLOUT) == 0 || ret != 2) { THROW_ERROR("wrong return events"); }
    return 0;
}

int test_epoll_no_timeout() {
    int pipe_fds[2];
    if (pipe(pipe_fds) < 0) {
        THROW_ERROR("failed to create a pipe");
    }
    int pipe_read_fd = pipe_fds[0];
    int pipe_write_fd = pipe_fds[1];

    int ep_fd = epoll_create1(0);
    if (ep_fd < 0) {
        THROW_ERROR("failed to create an epoll");
    }

    int ret;
    struct epoll_event event;

    event.events = EPOLLOUT; // writable
    event.data.u32 = pipe_write_fd;
    ret = epoll_ctl(ep_fd, EPOLL_CTL_ADD, pipe_write_fd, &event);
    if (ret < 0) {
        THROW_ERROR("failed to do epoll ctl");
    }

    event.events = EPOLLIN; // readable
    event.data.u32 = pipe_read_fd;
    ret = epoll_ctl(ep_fd, EPOLL_CTL_ADD, pipe_read_fd, &event);
    if (ret < 0) {
        THROW_ERROR("failed to do epoll ctl");
    }

    struct epoll_event events[2];
    ret = epoll_wait(ep_fd, events, ARRAY_SIZE(events), -1);
    // pipe_write_fd is ready, while pipe_read_fd is not
    if (ret != 1) {
        THROW_ERROR("failed to do epoll ctl");
    }

    free_pipe(pipe_fds);
    close(ep_fd);
    return 0;
}

int test_select_read_write() {
    int pipe_fds[2];
    if (pipe(pipe_fds) < 0) {
        THROW_ERROR("failed to create a pipe");
    }

    int pipe_rd_fd = pipe_fds[0];
    int pipe_wr_fd = pipe_fds[1];

    posix_spawn_file_actions_t file_actions;
    posix_spawn_file_actions_init(&file_actions);
    posix_spawn_file_actions_adddup2(&file_actions, pipe_wr_fd, STDOUT_FILENO);
    posix_spawn_file_actions_addclose(&file_actions, pipe_rd_fd);

    const char *msg = "Echo!\n";
    const char *child_prog = "/bin/hello_world";
    const char *child_argv[3] = { child_prog, msg, NULL };
    int child_pid;
    if (posix_spawn(&child_pid, child_prog, &file_actions,
                    NULL, (char *const *)child_argv, NULL) < 0) {
        THROW_ERROR("failed to spawn a child process");
    }
    close(pipe_wr_fd);

    const char *expected_str = msg;
    size_t expected_len = strlen(expected_str);
    char actual_str[32] = {0};
    fd_set rfds;

    FD_ZERO(&rfds);
    FD_SET(pipe_fds[0], &rfds);
    if (select(pipe_fds[0] + 1, &rfds, NULL, NULL, NULL) <= 0) {
        free_pipe(pipe_fds);
        THROW_ERROR("select failed");
    }

    if (read(pipe_rd_fd, actual_str, sizeof(actual_str) - 1) < 0) {
        THROW_ERROR("reading pipe failed");
    };

    if (strncmp(expected_str, actual_str, expected_len) != 0) {
        THROW_ERROR("received string is not as expected");
    }

    close(pipe_rd_fd);

    int status = 0;
    if (wait4(child_pid, &status, 0, NULL) < 0) {
        THROW_ERROR("failed to wait4 the child process");
    }
    return 0;
}

int test_ioctl_fionread() {
    int pipe_fds[2];
    if (pipe(pipe_fds) < 0) {
        THROW_ERROR("failed to create a pipe");
    }

    int pipe_rd_fd = pipe_fds[0];
    int pipe_wr_fd = pipe_fds[1];

    posix_spawn_file_actions_t file_actions;
    posix_spawn_file_actions_init(&file_actions);
    posix_spawn_file_actions_adddup2(&file_actions, pipe_wr_fd, STDOUT_FILENO);
    posix_spawn_file_actions_addclose(&file_actions, pipe_rd_fd);

    const char *msg = "Echo!\n";
    const char *child_prog = "/bin/hello_world";
    const char *child_argv[3] = { child_prog, msg, NULL };
    int child_pid;
    if (posix_spawn(&child_pid, child_prog, &file_actions,
                    NULL, (char *const *)child_argv, NULL) < 0) {
        THROW_ERROR("failed to spawn a child process");
    }
    int status = 0;
    if (wait4(child_pid, &status, 0, NULL) < 0) {
        THROW_ERROR("failed to wait4 the child process");
    }

    close(pipe_wr_fd);

    const char *expected_str = msg;
    size_t expected_len = strlen(expected_str);
    char actual_str[32] = {0};

    int data_len_ready = 0;
    if ( ioctl(pipe_rd_fd, FIONREAD, &data_len_ready) < 0 ) {
        THROW_ERROR("ioctl FIONREAD failed");
    }

    // data_len_ready will include '\0'
    if (data_len_ready - 1 != expected_len) {
        THROW_ERROR("ioctl FIONREAD value not match");
    }

    if (read(pipe_rd_fd, actual_str, sizeof(actual_str) - 1) < 0) {
        THROW_ERROR("reading pipe failed");
    };

    if (strncmp(expected_str, actual_str, expected_len) != 0) {
        THROW_ERROR("received string is not as expected");
    }

    close(pipe_rd_fd);
    return 0;
}

// A thread that reads from a pipe until it has got `total` bytes, which have the values of a
// pattern, or the pipe is empty and closed
typedef struct {
    int fd;
    size_t total;
    size_t chunk;
    useconds_t delay_us;    // the time before the first read
    useconds_t period_us;   // the time after each read
    size_t got;
    size_t bad_pos;         // the position of the first byte that is wrong
} reader_t;

static char pattern_byte(size_t pos) {
    return (char)(pos * 7 + pos / 251);
}

static void *reader_main(void *arg) {
    reader_t *r = arg;
    char *buf = malloc(r->chunk);
    r->bad_pos = r->total;
    usleep(r->delay_us);
    while (r->got < r->total) {
        ssize_t n = read(r->fd, buf, r->chunk);
        if (n <= 0) {
            break;
        }
        for (ssize_t i = 0; i < n && r->bad_pos == r->total; i++) {
            if (buf[i] != pattern_byte(r->got + i)) {
                r->bad_pos = r->got + i;
            }
        }
        r->got += n;
        if (r->period_us > 0) {
            usleep(r->period_us);
        }
    }
    free(buf);
    return NULL;
}

// A write to a blocking pipe waits for the reader until everything is written
int test_write_waits_until_all_bytes_are_written() {
    const size_t total = 3 * 1024 * 1024;
    char *data = malloc(total);
    for (size_t i = 0; i < total; i++) {
        data[i] = pattern_byte(i);
    }

    for (int vectored = 0; vectored < 2; vectored++) {
        int pipe_fds[2];
        if (pipe(pipe_fds) < 0) {
            THROW_ERROR("failed to create a pipe");
        }
        reader_t reader = { .fd = pipe_fds[0], .total = total, .chunk = 65536,
                            .delay_us = 100000
                          };
        pthread_t thread;
        if (pthread_create(&thread, NULL, reader_main, &reader) != 0) {
            THROW_ERROR("failed to create a thread");
        }

        ssize_t ret;
        if (vectored) {
            struct iovec iov[2] = { { data, total / 3 }, { data + total / 3, total - total / 3 } };
            ret = writev(pipe_fds[1], iov, 2);
        } else {
            ret = write(pipe_fds[1], data, total);
        }
        if (ret != (ssize_t)total) {
            THROW_ERROR("%s returned %ld, instead of %zu", vectored ? "writev" : "write", (long)ret,
                        total);
        }
        pthread_join(thread, NULL);
        if (reader.got != total || reader.bad_pos != total) {
            THROW_ERROR("the reader got %zu bytes, the first wrong one is at %zu", reader.got,
                        reader.bad_pos);
        }
        free_pipe(pipe_fds);
    }
    free(data);
    return 0;
}

// A write to a nonblocking pipe writes what fits, and fails with EAGAIN if nothing does
int test_nonblocking_write_is_partial() {
    const size_t total = 4 * 1024 * 1024;
    char *data = malloc(total);
    memset(data, 'x', total);
    int pipe_fds[2];
    if (pipe2(pipe_fds, O_NONBLOCK) < 0) {
        THROW_ERROR("failed to create a pipe");
    }

    ssize_t ret = write(pipe_fds[1], data, total);
    if (ret <= 0 || ret >= (ssize_t)total) {
        THROW_ERROR("write returned %ld", (long)ret);
    }
    if (write(pipe_fds[1], data, 100) != -1 || errno != EAGAIN) {
        THROW_ERROR("write to a full pipe does not fail with EAGAIN");
    }
    size_t drained = 0;
    ssize_t n;
    while ((n = read(pipe_fds[0], data, total)) > 0) {
        drained += n;
    }
    if (drained != (size_t)ret) {
        THROW_ERROR("%zu bytes were written, %zu are read", (size_t)ret, drained);
    }
    free_pipe(pipe_fds);
    free(data);
    return 0;
}

// A write of up to PIPE_BUF bytes is not interleaved with another write, also if the pipe is full
// but for a few bytes. The pipe is full when the writers start, and the reader takes 1000 bytes at a
// time, so the room is never a multiple of a record: a write that takes the part of its bytes that
// fits, and waits for the rest, shares the room with the other writer and interleaves the records
// within the first reads.
typedef struct {
    int fd;
    char value;
    int count;
} writer_t;

static void *writer_main(void *arg) {
    writer_t *w = arg;
    char record[4096];
    memset(record, w->value, sizeof(record));
    for (int i = 0; i < w->count; i++) {
        if (write(w->fd, record, sizeof(record)) != (ssize_t)sizeof(record)) {
            break;
        }
    }
    return NULL;
}

int test_small_writes_are_atomic() {
    const int count = 300;
    const size_t records_len = 2 * count * 4096;
    int pipe_fds[2];
    if (pipe(pipe_fds) < 0) {
        THROW_ERROR("failed to create a pipe");
    }

    // Fill the pipe with 'F', whatever its capacity is
    char record[4096];
    memset(record, 'F', sizeof(record));
    size_t filler = 0;
    ssize_t n;
    fcntl(pipe_fds[1], F_SETFL, fcntl(pipe_fds[1], F_GETFL) | O_NONBLOCK);
    while ((n = write(pipe_fds[1], record, sizeof(record))) > 0) {
        filler += n;
    }
    while ((n = write(pipe_fds[1], record, 1)) > 0) {
        filler += n;
    }
    if (errno != EAGAIN || filler < sizeof(record)) {
        THROW_ERROR("failed to fill the pipe");
    }
    fcntl(pipe_fds[1], F_SETFL, fcntl(pipe_fds[1], F_GETFL) & ~O_NONBLOCK);

    writer_t writers[2] = { { pipe_fds[1], 'A', count }, { pipe_fds[1], 'B', count } };
    pthread_t threads[2];
    for (int i = 0; i < 2; i++) {
        if (pthread_create(&threads[i], NULL, writer_main, &writers[i]) != 0) {
            THROW_ERROR("failed to create a thread");
        }
    }
    // Both writers wait for room now
    usleep(100000);

    size_t total = filler + records_len;
    char *data = malloc(total);
    size_t got = 0;
    while (got < total) {
        // A writer that gives up after a short write leaves no more bytes to come
        struct pollfd pfd = { pipe_fds[0], POLLIN, 0 };
        if (poll(&pfd, 1, 10000) != 1) {
            THROW_ERROR("no bytes arrive after %zu bytes", got);
        }
        n = read(pipe_fds[0], data + got, total - got < 1000 ? total - got : 1000);
        if (n <= 0) {
            THROW_ERROR("failed to read after %zu bytes", got);
        }
        got += n;
    }
    for (int i = 0; i < 2; i++) {
        pthread_join(threads[i], NULL);
    }

    for (size_t pos = 0; pos < filler; pos++) {
        if (data[pos] != 'F') {
            THROW_ERROR("a wrong byte at %zu, in the filler", pos);
        }
    }
    int records[2] = { 0, 0 };
    for (size_t pos = filler; pos < total; pos += 4096) {
        char value = data[pos];
        if (value != 'A' && value != 'B') {
            THROW_ERROR("a wrong byte at %zu", pos);
        }
        for (size_t i = 1; i < 4096; i++) {
            if (data[pos + i] != value) {
                THROW_ERROR("the record at %zu is interleaved with another write", pos);
            }
        }
        records[value - 'A']++;
    }
    if (records[0] != count || records[1] != count) {
        THROW_ERROR("%d and %d records arrive, instead of %d", records[0], records[1], count);
    }
    free(data);
    free_pipe(pipe_fds);
    return 0;
}

// A pipe is writable if a write of PIPE_BUF bytes does not wait for room, which a pipe that is full
// but for a few bytes has not got
int test_pipe_is_writable_if_an_atomic_write_fits() {
    int pipe_fds[2];
    char buf[4096] = { 0 };
    if (pipe2(pipe_fds, O_NONBLOCK) < 0) {
        THROW_ERROR("failed to create a pipe");
    }
    while (write(pipe_fds[1], buf, sizeof(buf)) > 0) {
    }
    if (errno != EAGAIN) {
        THROW_ERROR("failed to fill the pipe");
    }

    struct pollfd pfd = { pipe_fds[1], POLLOUT, 0 };
    if (poll(&pfd, 1, 0) != 0) {
        THROW_ERROR("a full pipe is writable");
    }
    // A few bytes of room is no room for a write that is atomic
    if (read(pipe_fds[0], buf, 100) != 100) {
        THROW_ERROR("failed to read");
    }
    if (poll(&pfd, 1, 0) != 0 || write(pipe_fds[1], buf, sizeof(buf)) != -1 ||
            errno != EAGAIN) {
        THROW_ERROR("a pipe with a few bytes of room is writable");
    }
    // Room for a record
    if (read(pipe_fds[0], buf, sizeof(buf)) != sizeof(buf) ||
            read(pipe_fds[0], buf, 4000) != 4000) {
        THROW_ERROR("failed to read");
    }
    pfd.revents = 0;
    if (poll(&pfd, 1, 0) != 1 || !(pfd.revents & POLLOUT)) {
        THROW_ERROR("a pipe with room for a record is not writable");
    }
    if (write(pipe_fds[1], buf, sizeof(buf)) != sizeof(buf)) {
        THROW_ERROR("write does not write the record");
    }
    free_pipe(pipe_fds);
    return 0;
}

// A write to a pipe that no process reads fails with EPIPE, and raises SIGPIPE (unless a write
// has no bytes, which does nothing)
static atomic_int sigpipes;

static void on_sigpipe(int sig) {
    atomic_fetch_add(&sigpipes, 1);
}

int test_write_to_a_pipe_nobody_reads_raises_sigpipe() {
    struct sigaction sa, old;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_sigpipe;
    sigaction(SIGPIPE, &sa, &old);
    atomic_store(&sigpipes, 0);

    int pipe_fds[2];
    if (pipe(pipe_fds) < 0) {
        THROW_ERROR("failed to create a pipe");
    }
    close(pipe_fds[0]);
    struct iovec iov = { "x", 1 };
    if (write(pipe_fds[1], "x", 1) != -1 || errno != EPIPE) {
        THROW_ERROR("write does not fail with EPIPE");
    }
    if (atomic_load(&sigpipes) != 1) {
        THROW_ERROR("write raised %d SIGPIPE", atomic_load(&sigpipes));
    }
    if (writev(pipe_fds[1], &iov, 1) != -1 || errno != EPIPE || atomic_load(&sigpipes) != 2) {
        THROW_ERROR("writev does not fail with EPIPE and SIGPIPE");
    }
    if (write(pipe_fds[1], "x", 0) != 0 || atomic_load(&sigpipes) != 2) {
        THROW_ERROR("write of no bytes does not return 0");
    }

    // Ignored, the signal is not raised
    signal(SIGPIPE, SIG_IGN);
    if (write(pipe_fds[1], "x", 1) != -1 || errno != EPIPE || atomic_load(&sigpipes) != 2) {
        THROW_ERROR("write does not fail with EPIPE");
    }
    close(pipe_fds[1]);
    sigaction(SIGPIPE, &old, NULL);
    return 0;
}

// The readers go while a write waits for room: the write returns the bytes that it has written,
// and raises SIGPIPE, too
static void *closer_main(void *arg) {
    usleep(300000);
    close(*(int *)arg);
    return NULL;
}

int test_sigpipe_when_the_readers_go_during_a_write() {
    const size_t total = 4 * 1024 * 1024;
    char *data = calloc(1, total);
    struct sigaction sa, old;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_sigpipe;
    sigaction(SIGPIPE, &sa, &old);
    atomic_store(&sigpipes, 0);

    int pipe_fds[2];
    if (pipe(pipe_fds) < 0) {
        THROW_ERROR("failed to create a pipe");
    }
    pthread_t thread;
    if (pthread_create(&thread, NULL, closer_main, &pipe_fds[0]) != 0) {
        THROW_ERROR("failed to create a thread");
    }
    ssize_t ret = write(pipe_fds[1], data, total);
    pthread_join(thread, NULL);
    sigaction(SIGPIPE, &old, NULL);
    if (ret <= 0 || ret >= (ssize_t)total) {
        THROW_ERROR("write returned %ld", (long)ret);
    }
    if (atomic_load(&sigpipes) != 1) {
        THROW_ERROR("write raised %d SIGPIPE", atomic_load(&sigpipes));
    }
    close(pipe_fds[1]);
    free(data);
    return 0;
}

// ============================================================================
// Test suite
// ============================================================================
static test_case_t test_cases[] = {
    TEST_CASE(test_fstat),
    TEST_CASE(test_fcntl_get_flags),
    TEST_CASE(test_fcntl_set_flags),
    TEST_CASE(test_create_with_flags),
    TEST_CASE(test_select_timeout),
    TEST_CASE(test_poll_timeout),
    TEST_CASE(test_epoll_timeout),
    TEST_CASE(test_select_no_timeout),
    TEST_CASE(test_poll_no_timeout),
    TEST_CASE(test_epoll_no_timeout),
    TEST_CASE(test_select_read_write),
    TEST_CASE(test_ioctl_fionread),
    TEST_CASE(test_write_waits_until_all_bytes_are_written),
    TEST_CASE(test_nonblocking_write_is_partial),
    TEST_CASE(test_small_writes_are_atomic),
    TEST_CASE(test_pipe_is_writable_if_an_atomic_write_fits),
    TEST_CASE(test_write_to_a_pipe_nobody_reads_raises_sigpipe),
    TEST_CASE(test_sigpipe_when_the_readers_go_during_a_write),
};

int main(int argc, const char *argv[]) {
    return test_suite_run(test_cases, ARRAY_SIZE(test_cases));
}
