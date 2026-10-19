// NOT PART OF THE DEFAULT TESTS (it is not in TESTS of test/Makefile). This test drives the kernel
// heap to its limit, and the LibOS allocates infallibly everywhere else: any allocation that fails
// while the heap is full, whether in this process or in a thread of the LibOS, aborts the enclave.
// On a machine that runs other work (CPU or memory pressure) it did abort the enclave once. The
// other test cases of unix_socket_memory keep the heap far from its limit. Run this one by itself,
// when the machine is quiet:
//
//     make -C test test TESTS=unix_socket_memory_exhaust
//
// The kernel heap runs out of memory because of queued data: a write to a socket or a pipe that
// finds no memory for its data fails with ENOMEM, it does not abort the enclave. The queues of the
// channels (unix sockets, pipes) allocate their chunks of data fallibly, and report it. A writev
// that has queued its first part when it finds no memory for the second one returns the size of
// the first part, so that the caller does not write those bytes again.
//
// It fills sockets and pipes with data until a write fails, so it needs a kernel heap that a
// number of sockets and pipes within the limit of open descriptors can fill (about 190 socketpairs
// for the 40 MB heap of the test configuration). It reports that it is skipped otherwise, and when
// there is no kernel heap monitor (in Linux, and in a LibOS built without the feature).
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

#include "test.h"
#include "kernel_heap.h"

// The data that a socket (one direction) and a pipe take: what the LibOS lets them queue
#define SOCKET_BUF_BYTES    (208 * 1024)
#define PIPE_BUF_BYTES      (1024 * 1024)

// The heap that may stay in use after the objects were closed
#define RESIDUE_KB          32

#define WATCHDOG_SECS       300

static void *watchdog(void *arg) {
    sleep(WATCHDOG_SECS);
    printf("\t\tERROR: the test case is stuck for %d seconds\n", WATCHDOG_SECS);
    fflush(stdout);
    _exit(1);
    return NULL;
}

// The size of the kernel heap in kB, or -1
static long heap_total_kb(void) {
    char buf[512];
    int fd = open("/proc/meminfo", O_RDONLY);
    if (fd < 0) {
        return -1;
    }
    int len = read(fd, buf, sizeof(buf) - 1);
    close(fd);
    if (len <= 0) {
        return -1;
    }
    buf[len] = '\0';
    char *line = strstr(buf, "KernelHeapTotal:");
    long kb = -1;
    if (line == NULL || sscanf(line, "KernelHeapTotal: %ld", &kb) != 1) {
        return -1;
    }
    return kb;
}

static int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL);
    if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        THROW_ERROR("failed to make a descriptor non-blocking");
    }
    return 0;
}

// Makes a socketpair or a pipe, which is not full, and writes 100 bytes of 0x5a to the end
// fds[1], which the end fds[0] reads
static int make_twin(int use_pipes, int fds[2]) {
    int rc = use_pipes ? pipe(fds) : socketpair(AF_UNIX, SOCK_STREAM, 0, fds);
    if (rc < 0) {
        THROW_ERROR("failed to create a %s", use_pipes ? "pipe" : "socketpair");
    }
    unsigned char bytes[100];
    memset(bytes, 0x5a, sizeof(bytes));
    if (write(fds[1], bytes, sizeof(bytes)) != sizeof(bytes)) {
        THROW_ERROR("failed to write to a new %s", use_pipes ? "pipe" : "socketpair");
    }
    if (set_nonblocking(fds[0]) < 0 || set_nonblocking(fds[1]) < 0) {
        return -1;
    }
    return 0;
}

// Reads the `total` bytes of 0x5a that a twin holds, and checks that there are no more
static int read_all(int fd, long total) {
    unsigned char bytes[4096];
    long done = 0;
    while (done < total) {
        ssize_t r = read(fd, bytes, MIN((long)sizeof(bytes), total - done));
        if (r <= 0) {
            THROW_ERROR("read %zd bytes (%s) after %ld of %ld bytes", r,
                        r < 0 ? strerror(errno) : "end of file", done, total);
        }
        for (ssize_t i = 0; i < r; i++) {
            if (bytes[i] != 0x5a) {
                THROW_ERROR("byte %ld is 0x%x, not 0x5a", done + i, bytes[i]);
            }
        }
        done += r;
    }
    if (read(fd, bytes, sizeof(bytes)) >= 0 || errno != EAGAIN) {
        THROW_ERROR("there are more bytes than the %ld that were queued", total);
    }
    return 0;
}

// Fills socketpairs, or pipes, as far as they go, until a write finds no memory
static int exhaust_the_heap(int use_pipes) {
    long total_kb = heap_total_kb();
    long used_kb = kernel_heap_in_use();
    if (total_kb <= 0 || used_kb < 0) {
        printf("\t\tSKIPPED: the size of the kernel heap is unknown\n");
        return 0;
    }

    // The objects that it takes to fill the heap, with a margin; each of them takes two descriptors
    long per_object = use_pipes ? PIPE_BUF_BYTES : SOCKET_BUF_BYTES;
    long objects_max = (total_kb - used_kb) * 1024 / per_object + 16;
    struct rlimit nofile;
    if (getrlimit(RLIMIT_NOFILE, &nofile) < 0) {
        THROW_ERROR("failed to get the limit of open descriptors");
    }
    long fds_max = nofile.rlim_cur == RLIM_INFINITY ? 1 << 20 : (long)nofile.rlim_cur;
    if (objects_max * 2 > fds_max - 32) {
        printf("\t\tSKIPPED: %ld %s are needed to fill the kernel heap of %ld kB, and the limit "
               "of descriptors is %ld\n", objects_max, use_pipes ? "pipes" : "socketpairs",
               total_kb, fds_max);
        return 0;
    }

    int (*fds)[2] = calloc(objects_max, sizeof(*fds));
    unsigned char *buf = malloc(65536);
    if (fds == NULL || buf == NULL) {
        THROW_ERROR("failed to allocate");
    }
    memset(buf, 0x5a, 65536);
    long before = kernel_heap_in_use();
    // Two twins for the writev below, which hold 100 bytes each. Nothing can be created when the
    // heap is full
    int probe[2], target[2];
    if (make_twin(use_pipes, probe) < 0 || make_twin(use_pipes, target) < 0) {
        return -1;
    }
    long objects = 0;
    int failed = 0;
    int error = 0;
    size_t queued_total = 0;
    while (objects < objects_max && !failed) {
        int *pair = fds[objects];
        int rc = use_pipes ? pipe(pair) : socketpair(AF_UNIX, SOCK_STREAM, 0, pair);
        if (rc < 0) {
            error = errno;
            failed = 1;
            break;
        }
        objects++;
        set_nonblocking(pair[1]);
        // Fill one direction as far as it goes
        for (;;) {
            ssize_t r = write(pair[1], buf, 65536);
            if (r > 0) {
                queued_total += r;
                continue;
            }
            if (r < 0 && errno != EAGAIN) {
                error = errno;
                failed = 1;
            }
            break;
        }
    }
    // The heap is full: a writev whose first part fills the chunk that the twin holds, and whose
    // second part needs a new chunk, queues the first part and returns its size. It must not
    // fail with ENOMEM, as the caller would write the bytes that are queued again. The room in
    // the chunk of the target is the room in that of the probe, which tells it: a writev that
    // does not need a new chunk for its first byte is cut short where the chunk ends
    long room = -1;
    ssize_t written = -1;
    int writev_errno = 0;
    if (failed && error == ENOMEM) {
        struct iovec iov[2] = {{buf, 1}, {buf, 65536}};
        room = writev(probe[1], iov, 2);
        if (room > 0) {
            iov[0].iov_len = room;
            written = writev(target[1], iov, 2);
            writev_errno = errno;
        }
    }
    // Give the memory back at once
    for (long i = 0; i < objects; i++) {
        close(fds[i][0]);
        close(fds[i][1]);
    }
    free(fds);
    printf("\t\t%ld %s took %zu bytes, then %s\n", objects,
           use_pipes ? "pipes" : "socketpairs",
           queued_total, failed ? strerror(error) : "the loop ended");
    if (!failed) {
        THROW_ERROR("the heap of %ld kB was not exhausted by %ld objects", total_kb, objects);
    }
    if (error != ENOMEM) {
        THROW_ERROR("the failure is %s, not ENOMEM", strerror(error));
    }
    printf("\t\twritev of the room of a chunk (%ld bytes) and 64 KiB: %zd\n", room, written);
    if (room <= 0) {
        THROW_ERROR("the probe took %ld bytes with a full heap", room);
    }
    if (written != room) {
        THROW_ERROR("writev returned %zd (%s), not the %ld bytes that it queued", written,
                    written < 0 ? strerror(writev_errno) : "", room);
    }
    // And the twin holds what its writes queued, now that there is memory to read it
    if (read_all(target[0], 100 + room) < 0 || read_all(probe[0], 100 + room) < 0) {
        return -1;
    }
    close(target[0]);
    close(target[1]);
    close(probe[0]);
    close(probe[1]);
    free(buf);
    long after = kernel_heap_in_use();
    printf("\t\theap after closing the %s: %+ld kB\n", use_pipes ? "pipes" : "socketpairs",
           after - before);
    if (after - before > RESIDUE_KB) {
        THROW_ERROR("the kernel heap holds %ld kB more after the objects were closed",
                    after - before);
    }
    return 0;
}

static int test_socketpairs_fail_with_enomem(void) {
    return exhaust_the_heap(0);
}

static int test_pipes_fail_with_enomem(void) {
    return exhaust_the_heap(1);
}

static test_case_t test_cases[] = {
    TEST_CASE(test_socketpairs_fail_with_enomem),
    TEST_CASE(test_pipes_fail_with_enomem),
};

int main(int argc, const char *argv[]) {
    signal(SIGPIPE, SIG_IGN);
    // The output is complete if the LibOS aborts the process
    setvbuf(stdout, NULL, _IOLBF, 0);

    long kb = kernel_heap_in_use();
    if (kb < 0) {
        printf("\t\tno kernel heap monitor: nothing to test\n");
        return 0;
    }
    pthread_t thread;
    if (pthread_create(&thread, NULL, watchdog, NULL) == 0) {
        pthread_detach(thread);
    }
    return test_suite_run(test_cases, ARRAY_SIZE(test_cases)) < 0 ? 1 : 0;
}
