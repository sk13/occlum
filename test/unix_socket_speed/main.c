// What a call that asks a socket or a pipe about its state does while another thread moves a lot of
// bytes through it in one call. The call that moves the bytes holds the lock of its end of the
// channel while it copies them, and the LibOS used to take the same lock to answer poll() or
// ioctl(FIONREAD), so the answer came when the copy was over. The ends of a channel that moves bytes
// through a socket in a bulk transfer held each other up the same way, as every push and every pop
// tells the other end's events, which are the answer of poll(), so a bulk transfer through a socket
// was slower than one through a pipe for that reason. A call that only reads the counters of the
// queue takes the time that it takes without the copy.
//
// The sockets are given a buffer of megabytes, so that one call copies for milliseconds, with
// SO_SNDBUFFORCE, which Linux allows root only. Without the privilege the test takes the greatest
// buffer that SO_SNDBUF sets, which does not copy long on Linux, and the test has little to
// measure there. A thread asks the socket again and again meanwhile (poll(), ioctl(FIONREAD), or
// setsockopt() of the size of the send buffer, which changes a limit of the queue that the LibOS
// used to change with the lock of the end that the call has), and the test counts the answers
// that come while the call copies: there are hundreds if the call does not wait, and one or none
// if it does. The test does not look at the time that a call takes, which varies with the load of
// the machine.
//
// Usage: unix_socket_speed [test case name | --list] [-v]
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "test.h"

#define MIB             (1024L * 1024L)
#define BYTES           (8 * MIB)       // the size of the call that copies, at most
#define MIN_COPY_NS     1500000L        // a call that is faster copies too little to be tested
#define MIN_ANSWERS     10              // the answers that come while the call copies
#define MAX_STAMPS      (1L << 20)
#define TRIES           3

// What the thread that asks does
enum { ASK_POLL, ASK_FIONREAD, ASK_SNDBUF };

#define CHECK(cond, fmt, ...) do { \
        if (!(cond)) { \
            THROW_ERROR(fmt, ##__VA_ARGS__); \
        } \
    } while (0)

static int verbose;

static long now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000000000L + ts.tv_nsec;
}

// A thread that asks a file descriptor about its state until it is told to stop, with poll(), with
// ioctl(FIONREAD), or by setting the size of the send buffer again, and notes the times at which
// the answers come
typedef struct {
    int fd;
    short events;
    int ask;
    atomic_int stop;
    atomic_long answers;
    long *stamps;
    pthread_t thread;
} observer_t;

static void *observer_main(void *arg) {
    observer_t *o = arg;
    struct pollfd pfd = { o->fd, o->events, 0 };
    for (int i = 0; !atomic_load(&o->stop); i++) {
        int bytes = 2 * BYTES + (i & 1) * 4096;
        if (o->ask == ASK_FIONREAD) {
            ioctl(o->fd, FIONREAD, &bytes);
        } else if (o->ask == ASK_SNDBUF) {
            // The size that the socket has already, or a little more
            setsockopt(o->fd, SOL_SOCKET, SO_SNDBUFFORCE, &bytes, sizeof(bytes));
        } else {
            poll(&pfd, 1, 0);
        }
        long n = atomic_load(&o->answers);
        if (n < MAX_STAMPS) {
            o->stamps[n] = now_ns();
            atomic_store(&o->answers, n + 1);
        }
    }
    return NULL;
}

static int observer_start(observer_t *o, int fd, short events, int ask) {
    memset(o, 0, sizeof(*o));
    o->fd = fd;
    o->events = events;
    o->ask = ask;
    o->stamps = malloc(MAX_STAMPS * sizeof(long));
    if (o->stamps == NULL || pthread_create(&o->thread, NULL, observer_main, o) != 0) {
        return -1;
    }
    // It has asked a few times
    while (atomic_load(&o->answers) < 10) {
        sched_yield();
    }
    return 0;
}

// Stops the observer, and returns the number of the answers that came between the times
static long observer_stop(observer_t *o, long from, long to) {
    atomic_store(&o->stop, 1);
    pthread_join(o->thread, NULL);
    long count = 0;
    for (long i = 0; i < atomic_load(&o->answers); i++) {
        if (o->stamps[i] > from && o->stamps[i] < to) {
            count++;
        }
    }
    free(o->stamps);
    return count;
}

// Makes a socket pair whose first socket may queue the bytes of a call, and returns the number of
// the bytes that the test may move, or 0 if the buffer is too small to be worth a test, or -1 on
// an error
static long make_pair_with_big_buffer(int sv[2]) {
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) != 0) {
        return -1;
    }
    int size = 2 * BYTES;
    if (setsockopt(sv[0], SOL_SOCKET, SO_SNDBUFFORCE, &size, sizeof(size)) == 0) {
        return BYTES;
    }
    // The size that the system lets the process set, of which the bytes take a part
    size = 1 << 30;
    socklen_t len = sizeof(size);
    if (setsockopt(sv[0], SOL_SOCKET, SO_SNDBUF, &size, len) < 0 ||
            getsockopt(sv[0], SOL_SOCKET, SO_SNDBUF, &size, &len) < 0) {
        return -1;
    }
    long bytes = size / 4;
    return bytes >= MIB ? (bytes < BYTES ? bytes : BYTES) : 0;
}

// The observer must be able to ask more than a few times while the call copies. If the call is
// quick, the machine is fast and the test has nothing to measure.
static int check_answers(const char *what, long answers, long call_ns, long moved,
                         long bytes) {
    if (verbose) {
        printf("\t\t%s: %ld answers came in the %ld us in which %ld MiB were copied\n", what,
               answers, call_ns / 1000, moved / MIB);
    }
    CHECK(moved == bytes, "%s: %ld bytes were moved, instead of %ld", what, moved, bytes);
    if (call_ns < MIN_COPY_NS) {
        return 0;
    }
    CHECK(answers >= MIN_ANSWERS,
          "%s: only %ld answers came in the %ld us in which a call copied "
          "%ld MiB, the others waited for the copy", what, answers, call_ns / 1000,
          moved / MIB);
    return 0;
}

// The call asks the end of the socket that a thread reads from, or the one that it writes to
static int ask_while_a_call_copies(int ask, int reads) {
    int sv[2];
    long bytes = make_pair_with_big_buffer(sv);
    CHECK(bytes >= 0, "failed to create a socket pair");
    if (bytes == 0) {
        return 0;
    }
    char *buf = malloc(bytes);
    memset(buf, 'x', bytes);
    if (reads) {
        // The bytes that the call reads are there
        long done = 0;
        while (done < bytes) {
            ssize_t n = send(sv[0], buf + done, bytes - done, MSG_DONTWAIT);
            CHECK(n > 0, "failed to write the bytes that are to be read");
            done += n;
        }
    }

    observer_t observer;
    CHECK(observer_start(&observer, reads ? sv[1] : sv[0], reads ? POLLIN : POLLOUT,
                         ask) == 0, "failed to start the observer");
    usleep(10000);
    long start = now_ns();
    ssize_t n = reads ? read(sv[1], buf, bytes) : write(sv[0], buf, bytes);
    long end = now_ns();
    long answers = observer_stop(&observer, start, end);
    CHECK(n > 0, "failed to %s", reads ? "read" : "write");
    const char *what = ask == ASK_FIONREAD ? "FIONREAD" : ask == ASK_SNDBUF ? "SO_SNDBUF" :
                       (reads ? "poll of the reader" : "poll of the writer");
    int ret = check_answers(what, answers, end - start, n, bytes);
    close(sv[0]);
    close(sv[1]);
    free(buf);
    return ret;
}

// The same for a pipe, which holds 1 MiB at the most: a read of it is quick, so only a few answers
// can come, and FIONREAD gives many in the time
static int ask_a_pipe_while_a_call_reads(void) {
    int fds[2];
    CHECK(pipe2(fds, O_NONBLOCK) == 0, "failed to create a pipe");
    long bytes = 0;
    int size = 1 << 20;
    char *buf = malloc(size);
    memset(buf, 'x', size);
    // Fill the pipe, which takes 1 MiB in the LibOS and 64 KiB on Linux
    while (bytes < size) {
        ssize_t n = write(fds[1], buf + bytes, size - bytes);
        if (n <= 0) {
            break;
        }
        bytes += n;
    }
    fcntl(fds[0], F_SETFL, fcntl(fds[0], F_GETFL) & ~O_NONBLOCK);
    observer_t observer;
    CHECK(observer_start(&observer, fds[0], POLLIN, ASK_FIONREAD) == 0,
          "failed to start the observer");
    usleep(10000);
    long start = now_ns();
    ssize_t n = read(fds[0], buf, size);
    long end = now_ns();
    long answers = observer_stop(&observer, start, end);
    CHECK(n > 0, "failed to read");
    if (verbose) {
        printf("\t\tFIONREAD of a pipe: %ld answers came in the %ld us in which %zd KiB were "
               "copied\n", answers, (end - start) / 1000, n / 1024);
    }
    // The copy of 1 MiB takes a few hundred microseconds, in which hundreds of answers come
    if (end - start >= 200000) {
        CHECK(answers >= MIN_ANSWERS, "only %ld answers came in the %ld us in which a call "
              "copied %zd KiB, the others waited for the copy", answers, (end - start) / 1000,
              n / 1024);
    }
    close(fds[0]);
    close(fds[1]);
    free(buf);
    return 0;
}

// A moment of the machine that is slow for the test, not for the LibOS, is not a failure: it has
// to happen in all the tries
static int try_again(int (*test)(int, int), int ask, int reads) {
    // The observer has to run while the call copies
    if (sysconf(_SC_NPROCESSORS_ONLN) < 2) {
        return 0;
    }
    int ret = -1;
    for (int i = 0; i < TRIES && ret != 0; i++) {
        ret = test(ask, reads);
    }
    return ret;
}

static int test_poll_does_not_wait_for_a_read(void) {
    return try_again(ask_while_a_call_copies, ASK_POLL, 1);
}

static int test_poll_does_not_wait_for_a_write(void) {
    return try_again(ask_while_a_call_copies, ASK_POLL, 0);
}

static int test_fionread_does_not_wait_for_a_read(void) {
    return try_again(ask_while_a_call_copies, ASK_FIONREAD, 1);
}

static int test_sndbuf_does_not_wait_for_a_write(void) {
    return try_again(ask_while_a_call_copies, ASK_SNDBUF, 0);
}

static int pipe_wrapper(int ask, int reads) {
    return ask_a_pipe_while_a_call_reads();
}

static int test_fionread_of_a_pipe_does_not_wait_for_a_read(void) {
    return try_again(pipe_wrapper, ASK_FIONREAD, 1);
}

static test_case_t test_cases[] = {
    TEST_CASE(test_poll_does_not_wait_for_a_read),
    TEST_CASE(test_poll_does_not_wait_for_a_write),
    TEST_CASE(test_fionread_does_not_wait_for_a_read),
    TEST_CASE(test_sndbuf_does_not_wait_for_a_write),
    TEST_CASE(test_fionread_of_a_pipe_does_not_wait_for_a_read),
};

int main(int argc, const char *argv[]) {
    test_case_t selected[ARRAY_SIZE(test_cases)];
    test_case_t *cases = test_cases;
    int num_cases = ARRAY_SIZE(test_cases);
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-v") == 0) {
            verbose = 1;
        } else if (strcmp(argv[i], "--list") == 0) {
            for (size_t j = 0; j < ARRAY_SIZE(test_cases); j++) {
                printf("%s\n", test_cases[j].name);
            }
            return 0;
        } else {
            num_cases = 0;
            for (size_t j = 0; j < ARRAY_SIZE(test_cases); j++) {
                if (strcmp(argv[i], test_cases[j].name) == 0) {
                    selected[num_cases++] = test_cases[j];
                }
            }
            if (num_cases == 0) {
                printf("no such test case: %s\n", argv[i]);
                return 2;
            }
            cases = selected;
        }
    }
    return test_suite_run(cases, num_cases);
}
