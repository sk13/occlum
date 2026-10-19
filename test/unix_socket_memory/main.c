// The memory of unix stream sockets and pipes. Linux accounts the memory of a connection by the data
// that is queued, so thousands of idle or pending connections are cheap. The LibOS used to allocate
// two ring buffers of 208 KiB for each connection, whether anything was queued or not (a pipe 1 MiB),
// so the 91st connection that nobody accepted aborted the enclave with its 40 MB kernel heap.
//
// The test cases read the kernel heap in use from /proc/meminfo (test/include/kernel_heap.h), and
// check that
//   - an idle socketpair, connection, pending connection or pipe costs a few kB,
//   - the memory follows the queued data, and returns when it is read or the sockets are closed
//     (besides the data, a queue keeps its last chunk and a few spare chunks for its writer, as
//     long as the writer is open: they are checked, too, and that they go away with the writer),
//   - the capacity of a socket buffer (208 KiB) and a pipe (1 MiB) is still there,
//   - 300 connections that nobody accepts do not abort the enclave,
//   - thousands of connections, socketpairs and pipes that carry data and are closed in any order
//     leave nothing behind, also when their data is read up to each border between two chunks,
//   - no data is lost, duplicated, or reordered when two threads move bulk data through a
//     connection, a socketpair or a pipe in random chunk sizes, also when the writer shuts
//     down in the middle of the transfer.
// The heap checks are skipped where there is no kernel heap monitor (in Linux, and in a LibOS built
// without the feature), the other test cases pass in Linux, too.
//
// The case that fills the kernel heap with queued data, to see that a write fails with ENOMEM and
// does not abort the enclave, is not a case of this test, since it can take the enclave down on a
// busy machine: it is the test unix_socket_memory_exhaust, which is not in the default tests.
//
// Usage: unix_socket_memory [test case name | --list]
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#include "test.h"
#include "kernel_heap.h"

#if defined(SGX_MODE_HW) || defined(SGX_MODE_SIM) || defined(SGX_MODE_HYPER)
#define IN_OCCLUM 1
#else
#define IN_OCCLUM 0
#endif

// What an idle connection, socketpair or pipe may cost. The LibOS used to take 428 kB for a
// connection and 1 MiB for a pipe, so the limit is no tight one
#define IDLE_LIMIT_BYTES    (16 * 1024)
// The memory that a socket with queued data may hold besides the data, from the first write
#define SLACK_BYTES         (48 * 1024)
// A queue (src/libos/src/fs/channel/ring.rs) keeps its data in chunks. The largest chunk is a
// sixteenth of the capacity between 16 and 64 KiB (16 KiB for a socket, 64 KiB for a pipe), and a
// queue that has carried data keeps its last chunk and spare chunks that the reader has read, for
// the writer: three quarters of the capacity, at most 256 KiB, as long as the writer is open
#define SOCKET_BUF_SIZE     (208 * 1024)
#define PIPE_BUF_SIZE       (1024 * 1024)

#define LARGEST_CHUNK_MIN   (16 * 1024)
#define LARGEST_CHUNK_MAX   (64 * 1024)
#define SPARE_BYTES_MAX     (256 * 1024)

// The heap that may stay in use after a test case allocated and freed many objects
#define RESIDUE_KB          32

#define WATCHDOG_SECS       300

static long largest_chunk(size_t capacity) {
    long chunk = LARGEST_CHUNK_MIN;
    while (chunk < LARGEST_CHUNK_MAX && (size_t)chunk < capacity / 16) {
        chunk *= 2;
    }
    return chunk;
}

// What a queue of fresh data may hold besides the data: two of its largest chunks, and some
static long slack_bytes(size_t capacity) {
    return 2 * largest_chunk(capacity) + 16 * 1024;
}

// What a queue that has carried data may hold besides its data: its last chunk, the spares, and some
static long kept_bytes(size_t capacity) {
    long spares = capacity / 4 * 3;
    if (spares > SPARE_BYTES_MAX) {
        spares = SPARE_BYTES_MAX;
    }
    return largest_chunk(capacity) + spares + 8 * 1024;
}

// ---------------------------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------------------------

static void *watchdog(void *arg) {
    sleep(WATCHDOG_SECS);
    printf("\t\tERROR: the test case is stuck for %d seconds\n", WATCHDOG_SECS);
    fflush(stdout);
    _exit(1);
    return NULL;
}

// The kernel heap in use in kB, or a negative number if it cannot be told
static long heap_kb(void) {
    return kernel_heap_in_use();
}

static int heap_usable;

static int check_heap_usable(void) {
    long kb = kernel_heap_in_use();
    if (kb >= 0) {
        heap_usable = 1;
        return 0;
    }
    heap_usable = 0;
    if (kb == -1 && IN_OCCLUM) {
        THROW_ERROR("failed to read the kernel heap in use");
    }
    printf("\t\tno kernel heap monitor: the heap checks are skipped\n");
    return 0;
}

// Returns the kernel heap that `count` objects hold on average since `before_kb`, in bytes
static long bytes_per_object(long before_kb, int count) {
    return (heap_kb() - before_kb) * 1024 / count;
}

// Checks that the heap is back at `before_kb` after the objects were closed
static int check_heap_returned(long before_kb, const char *what) {
    if (!heap_usable) {
        return 0;
    }
    long after = heap_kb();
    printf("\t\theap after closing %s: %+ld kB\n", what, after - before_kb);
    if (after - before_kb > RESIDUE_KB) {
        THROW_ERROR("the kernel heap holds %ld kB more after %s were closed (limit %d kB)",
                    after - before_kb, what, RESIDUE_KB);
    }
    return 0;
}

// A byte of the stream at an offset: it tells that a byte is lost, duplicated, or in the wrong place
static unsigned char pattern(size_t offset) {
    return (unsigned char)(offset * 131 + (offset >> 8) * 7 + (offset >> 20));
}

static void fill_pattern(unsigned char *buf, size_t offset, size_t len) {
    for (size_t i = 0; i < len; i++) {
        buf[i] = pattern(offset + i);
    }
}

// Returns the offset of the first wrong byte, or -1
static long find_wrong_byte(const unsigned char *buf, size_t offset, size_t len) {
    for (size_t i = 0; i < len; i++) {
        if (buf[i] != pattern(offset + i)) {
            return (long)(offset + i);
        }
    }
    return -1;
}

static unsigned next_random(unsigned *state) {
    *state = *state * 1103515245u + 12345u;
    return (*state >> 8) & 0xffffff;
}

static int unique_counter;

// A listening socket with an abstract name
static int make_listener(int backlog, struct sockaddr_un *addr, socklen_t *addr_len) {
    memset(addr, 0, sizeof(*addr));
    addr->sun_family = AF_UNIX;
    int n = snprintf(addr->sun_path + 1, sizeof(addr->sun_path) - 1,
                     "unix_socket_memory.%d.%d",
                     getpid(), unique_counter++);
    *addr_len = offsetof(struct sockaddr_un, sun_path) + 1 + n;
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        THROW_ERROR("failed to create a socket");
    }
    if (bind(fd, (struct sockaddr *)addr, *addr_len) < 0) {
        close(fd);
        THROW_ERROR("failed to bind");
    }
    if (listen(fd, backlog) < 0) {
        close(fd);
        THROW_ERROR("failed to listen");
    }
    return fd;
}

static int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL);
    if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        THROW_ERROR("failed to make a descriptor non-blocking");
    }
    return 0;
}

// Writes `len` bytes of the pattern from `offset` to a non-blocking descriptor until it is
// full, and returns how many it took. -1 if it failed. A write of PIPE_BUF bytes or less to a
// pipe takes all its bytes or none, so the room that is left, when a write fails with EAGAIN, is
// filled with single bytes.
static long fill_until_full(int fd, size_t offset, size_t limit) {
    unsigned char *buf = malloc(4093);
    size_t total = 0;
    size_t chunk = 4093;
    long ret = -1;
    while (total < limit) {
        size_t n = MIN(chunk, limit - total);
        fill_pattern(buf, offset + total, n);
        ssize_t r = write(fd, buf, n);
        if (r < 0 && errno == EAGAIN && chunk > 1) {
            chunk = 1;
            continue;
        }
        if (r < 0 && errno == EAGAIN) {
            ret = total;
            goto out;
        }
        if (r < 0) {
            goto out;
        }
        total += r;
    }
    ret = total;
out:
    free(buf);
    return ret;
}

// Reads from a non-blocking descriptor until it is empty, and checks the pattern. Returns how
// many bytes it got, or -1
static long drain_and_check(int fd, size_t offset) {
    unsigned char *buf = malloc(65536);
    size_t total = 0;
    long ret = -1;
    for (;;) {
        ssize_t r = read(fd, buf, 65536);
        if (r < 0 && errno == EAGAIN) {
            break;
        }
        if (r <= 0) {
            goto out;
        }
        long wrong = find_wrong_byte(buf, offset + total, r);
        if (wrong >= 0) {
            printf("\t\tERROR: byte %ld of the stream is wrong\n", wrong);
            goto out;
        }
        total += r;
    }
    ret = total;
out:
    free(buf);
    return ret;
}

// ---------------------------------------------------------------------------------------------
// Test cases
// ---------------------------------------------------------------------------------------------

#define PAIRS 100

static int test_socketpair_memory(void) {
    static int fds[PAIRS][2];
    long before = heap_kb();
    for (int i = 0; i < PAIRS; i++) {
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds[i]) < 0) {
            THROW_ERROR("failed to create socketpair %d", i);
        }
    }
    if (heap_usable) {
        long idle = bytes_per_object(before, PAIRS);
        printf("\t\tidle socketpair: %ld bytes\n", idle);
        if (idle > IDLE_LIMIT_BYTES) {
            THROW_ERROR("an idle socketpair holds %ld bytes of the kernel heap", idle);
        }
    }

    // Each end sends 100 bytes
    char out[100], in[100];
    for (int i = 0; i < PAIRS; i++) {
        memset(out, i, sizeof(out));
        for (int end = 0; end < 2; end++) {
            if (write(fds[i][end], out, sizeof(out)) != sizeof(out)) {
                THROW_ERROR("failed to write to socketpair %d", i);
            }
        }
    }
    if (heap_usable) {
        long busy = bytes_per_object(before, PAIRS);
        printf("\t\tsocketpair with 100 bytes in flight each way: %ld bytes\n", busy);
        if (busy > IDLE_LIMIT_BYTES) {
            THROW_ERROR("a socketpair with 200 bytes holds %ld bytes of the kernel heap", busy);
        }
    }
    for (int i = 0; i < PAIRS; i++) {
        for (int end = 0; end < 2; end++) {
            if (read(fds[i][end], in, sizeof(in)) != sizeof(in) || in[0] != (char)i) {
                THROW_ERROR("failed to read from socketpair %d", i);
            }
        }
    }
    for (int i = 0; i < PAIRS; i++) {
        close(fds[i][0]);
        close(fds[i][1]);
    }
    return check_heap_returned(before, "the socketpairs");
}

// n connections that nobody accepted, then accepted and closed, or dropped with the listener
static int pending_connections(int n, int close_listener_first) {
    int ret = -1;
    int *clients = malloc(n * sizeof(int));
    int *servers = malloc(n * sizeof(int));
    struct sockaddr_un addr;
    socklen_t addr_len;
    int listener = make_listener(n + 10, &addr, &addr_len);
    if (listener < 0) {
        goto out;
    }

    long before = heap_kb();
    for (int i = 0; i < n; i++) {
        clients[i] = socket(AF_UNIX, SOCK_STREAM, 0);
        if (clients[i] < 0 || connect(clients[i], (struct sockaddr *)&addr, addr_len) < 0) {
            printf("\t\tERROR: connection %d of %d failed: %s\n", i, n, strerror(errno));
            goto out;
        }
    }
    if (heap_usable) {
        long pending = bytes_per_object(before, n);
        printf("\t\tpending connection: %ld bytes (%d connections)\n", pending, n);
        if (pending > IDLE_LIMIT_BYTES) {
            printf("\t\tERROR: a pending connection holds %ld bytes of the kernel heap\n", pending);
            goto out;
        }
    }

    // The clients send a byte each, which is queued although nobody has accepted
    for (int i = 0; i < n; i++) {
        char byte = (char)i;
        if (write(clients[i], &byte, 1) != 1) {
            printf("\t\tERROR: failed to write to the pending connection %d\n", i);
            goto out;
        }
    }

    if (close_listener_first) {
        // The pending connections are dropped with the listener
        close(listener);
        for (int i = 0; i < n; i++) {
            close(clients[i]);
        }
    } else {
        // They are accepted in the order of their connection, and keep their data
        for (int i = 0; i < n; i++) {
            servers[i] = accept(listener, NULL, NULL);
            char byte = 0;
            if (servers[i] < 0 || read(servers[i], &byte, 1) != 1 || byte != (char)i) {
                printf("\t\tERROR: failed to accept connection %d, or its byte is wrong\n", i);
                goto out;
            }
        }
        if (heap_usable) {
            long accepted = bytes_per_object(before, n);
            printf("\t\taccepted connection: %ld bytes\n", accepted);
            if (accepted > IDLE_LIMIT_BYTES) {
                printf("\t\tERROR: an accepted connection holds %ld bytes of the kernel heap\n",
                       accepted);
                goto out;
            }
        }
        for (int i = 0; i < n; i++) {
            close(servers[i]);
            close(clients[i]);
        }
        close(listener);
    }
    if (check_heap_returned(before, "the connections") < 0) {
        goto out;
    }
    ret = 0;
out:
    // After a failure the descriptors stay open: the test case ends the process
    free(clients);
    free(servers);
    return ret;
}

static int test_pending_connections_memory(void) {
    if (pending_connections(100, 0) < 0) {
        return -1;
    }
    return pending_connections(100, 1);
}

// The LibOS aborted at the 91st connection that nobody accepted
static int test_many_pending_connections(void) {
    if (pending_connections(300, 0) < 0) {
        return -1;
    }
    return pending_connections(300, 1);
}

// The memory of data that nobody read is freed when the sockets are closed, whichever end goes first
static int test_close_with_queued_data(void) {
    enum { N = 60 };
    static int fds[N][2];
    unsigned char *buf = malloc(50000);
    long before = heap_kb();
    for (int round = 0; round < 2; round++) {
        for (int i = 0; i < N; i++) {
            if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds[i]) < 0) {
                THROW_ERROR("failed to create socketpair %d", i);
            }
            // 50000 bytes queued in one direction, 7 in the other
            fill_pattern(buf, 0, 50000);
            if (write(fds[i][0], buf, 50000) != 50000 || write(fds[i][1], buf, 7) != 7) {
                THROW_ERROR("failed to write to socketpair %d", i);
            }
        }
        if (heap_usable) {
            long per_pair = bytes_per_object(before, N);
            printf("\t\tsocketpair with 50007 bytes queued: %ld bytes\n", per_pair);
            if (per_pair < 50000 || per_pair > 50007 + SLACK_BYTES + IDLE_LIMIT_BYTES) {
                THROW_ERROR("a socketpair with 50007 queued bytes holds %ld bytes", per_pair);
            }
        }
        // Round 0 closes the writer of the big queue first, round 1 its reader first
        for (int i = 0; i < N; i++) {
            close(fds[i][round]);
        }
        for (int i = 0; i < N; i++) {
            if (round == 0) {
                // The data that was queued before the writer closed is still there
                unsigned char *in = malloc(50000);
                size_t got = 0;
                while (got < 50000) {
                    ssize_t r = read(fds[i][1], in + got, 50000 - got);
                    if (r <= 0) {
                        break;
                    }
                    got += r;
                }
                long wrong = find_wrong_byte(in, 0, got);
                free(in);
                if (got != 50000 || wrong >= 0) {
                    THROW_ERROR("read %zu bytes of socketpair %d after the peer closed, wrong "
                                "byte %ld", got, i, wrong);
                }
            }
            close(fds[i][1 - round]);
        }
        if (check_heap_returned(before, "the socketpairs with queued data") < 0) {
            return -1;
        }
    }
    free(buf);
    return 0;
}

// A connection that nobody accepted keeps what the client wrote, and the data is all there when
// the server accepts, for a few hundred kB in all
static int test_pending_connections_keep_data(void) {
    enum { N = 20, LEN = 100000 };
    int clients[N], servers[N];
    struct sockaddr_un addr;
    socklen_t addr_len;
    int listener = make_listener(N + 10, &addr, &addr_len);
    if (listener < 0) {
        return -1;
    }
    unsigned char *buf = malloc(LEN);
    long before = heap_kb();
    for (int i = 0; i < N; i++) {
        clients[i] = socket(AF_UNIX, SOCK_STREAM, 0);
        if (clients[i] < 0 || connect(clients[i], (struct sockaddr *)&addr, addr_len) < 0) {
            THROW_ERROR("failed to connect %d", i);
        }
        // Every client has its own data
        fill_pattern(buf, 1000 * i, LEN);
        if (write(clients[i], buf, LEN) != LEN) {
            THROW_ERROR("failed to write %d bytes to the pending connection %d", LEN, i);
        }
    }
    if (heap_usable) {
        long per_conn = bytes_per_object(before, N);
        printf("\t\tpending connection with %d bytes queued: %ld bytes\n", LEN, per_conn);
        if (per_conn > LEN + SLACK_BYTES) {
            THROW_ERROR("a pending connection with %d queued bytes holds %ld bytes", LEN,
                        per_conn);
        }
    }
    for (int i = 0; i < N; i++) {
        servers[i] = accept(listener, NULL, NULL);
        if (servers[i] < 0) {
            THROW_ERROR("failed to accept %d", i);
        }
        size_t got = 0;
        while (got < LEN) {
            ssize_t r = read(servers[i], buf + got, LEN - got);
            if (r <= 0) {
                THROW_ERROR("failed to read the data of the connection %d", i);
            }
            got += r;
        }
        long wrong = find_wrong_byte(buf, 1000 * i, LEN);
        if (wrong >= 0) {
            THROW_ERROR("the data of the connection %d is wrong at %ld", i, wrong);
        }
    }
    free(buf);
    for (int i = 0; i < N; i++) {
        close(servers[i]);
        close(clients[i]);
    }
    close(listener);
    return check_heap_returned(before, "the connections with data");
}

#define PIPES 200

static int test_pipe_memory(void) {
    static int fds[PIPES][2];
    long before = heap_kb();
    for (int i = 0; i < PIPES; i++) {
        if (pipe(fds[i]) < 0) {
            THROW_ERROR("failed to create pipe %d", i);
        }
    }
    if (heap_usable) {
        long idle = bytes_per_object(before, PIPES);
        printf("\t\tidle pipe: %ld bytes\n", idle);
        if (idle > IDLE_LIMIT_BYTES) {
            THROW_ERROR("an idle pipe holds %ld bytes of the kernel heap", idle);
        }
    }
    for (int i = 0; i < PIPES; i++) {
        char byte = (char)i;
        if (write(fds[i][1], &byte, 1) != 1) {
            THROW_ERROR("failed to write to pipe %d", i);
        }
    }
    for (int i = 0; i < PIPES; i++) {
        char byte = 0;
        if (read(fds[i][0], &byte, 1) != 1 || byte != (char)i) {
            THROW_ERROR("failed to read from pipe %d", i);
        }
    }
    for (int i = 0; i < PIPES; i++) {
        close(fds[i][0]);
        close(fds[i][1]);
    }
    return check_heap_returned(before, "the pipes");
}

// The memory follows the data: it grows with what is written, and is freed when the data is read
static int check_memory_follows_data(int wfd, int rfd, size_t capacity,
                                     const char *name) {
    if (set_nonblocking(wfd) < 0 || set_nonblocking(rfd) < 0) {
        return -1;
    }
    long before = heap_kb();

    // 100 KiB
    size_t some = 100 * 1024;
    long written = fill_until_full(wfd, 0, some);
    if (written != (long)some) {
        THROW_ERROR("%s: wrote %ld of %zu bytes", name, written, some);
    }
    if (heap_usable) {
        long held = (heap_kb() - before) * 1024;
        printf("\t\t%s with 100 KiB queued: %ld bytes\n", name, held);
        if (held < (long)some - 1024 || held > (long)some + slack_bytes(capacity)) {
            THROW_ERROR("%s holds %ld bytes of the kernel heap for %zu queued bytes", name, held,
                        some);
        }
    }

    // Read 90 KiB: most of the memory is free again
    size_t part = 90 * 1024;
    unsigned char *buf = malloc(part);
    size_t got = 0;
    while (got < part) {
        ssize_t r = read(rfd, buf, part - got);
        if (r <= 0 || find_wrong_byte(buf, got, r) >= 0) {
            free(buf);
            THROW_ERROR("%s: read failed, or the data is wrong", name);
        }
        got += r;
    }
    free(buf);
    if (heap_usable) {
        long held = (heap_kb() - before) * 1024;
        printf("\t\t%s with 10 KiB queued: %ld bytes\n", name, held);
        if (held > (long)(some - part) + kept_bytes(capacity)) {
            THROW_ERROR("%s holds %ld bytes of the kernel heap for %zu queued bytes", name, held,
                        some - part);
        }
    }

    // Fill it up: the capacity is the limit of the data that a socket or a pipe takes
    long more = fill_until_full(wfd, some, 4 * PIPE_BUF_SIZE);
    if (more < 0) {
        THROW_ERROR("%s: failed to fill up", name);
    }
    long queued = (long)(some - part) + more;
    printf("\t\t%s takes %ld bytes\n", name, queued);
    if (IN_OCCLUM && queued != (long)capacity) {
        THROW_ERROR("%s takes %ld bytes, not the capacity %zu", name, queued, capacity);
    }
    if (heap_usable) {
        long held = (heap_kb() - before) * 1024;
        printf("\t\t%s full: %ld bytes\n", name, held);
        if (held < queued - 1024 ||
                held > queued + slack_bytes(capacity) + kept_bytes(capacity)) {
            THROW_ERROR("%s holds %ld bytes of the kernel heap for %ld queued bytes", name, held,
                        queued);
        }
    }

    // Drain it
    long drained = drain_and_check(rfd, part);
    if (drained != queued) {
        THROW_ERROR("%s: drained %ld of %ld bytes", name, drained, queued);
    }
    if (heap_usable) {
        long held = (heap_kb() - before) * 1024;
        printf("\t\t%s drained: %ld bytes\n", name, held);
        if (held > kept_bytes(capacity)) {
            THROW_ERROR("%s holds %ld bytes of the kernel heap although it is empty", name, held);
        }
    }

    // And it goes on, in the same place of the stream
    char byte = 0;
    if (write(wfd, "x", 1) != 1 || read(rfd, &byte, 1) != 1 || byte != 'x') {
        THROW_ERROR("%s: failed to write and read again", name);
    }
    return 0;
}

static int test_socketpair_memory_follows_data(void) {
    int fds[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) < 0) {
        THROW_ERROR("failed to create a socketpair");
    }
    long before = heap_kb();
    int ret = check_memory_follows_data(fds[0], fds[1], SOCKET_BUF_SIZE, "socketpair");
    close(fds[0]);
    close(fds[1]);
    if (ret < 0) {
        return ret;
    }
    return check_heap_returned(before, "the socketpair");
}

static int test_connection_memory_follows_data(void) {
    struct sockaddr_un addr;
    socklen_t addr_len;
    int listener = make_listener(1, &addr, &addr_len);
    if (listener < 0) {
        return -1;
    }
    int client = socket(AF_UNIX, SOCK_STREAM, 0);
    if (client < 0 || connect(client, (struct sockaddr *)&addr, addr_len) < 0) {
        THROW_ERROR("failed to connect");
    }
    int server = accept(listener, NULL, NULL);
    if (server < 0) {
        THROW_ERROR("failed to accept");
    }
    close(listener);
    long before = heap_kb();
    // The server writes, the client reads: the other direction of the connection
    int ret = check_memory_follows_data(server, client, SOCKET_BUF_SIZE, "connection");
    close(client);
    close(server);
    if (ret < 0) {
        return ret;
    }
    return check_heap_returned(before, "the connection");
}

static int test_pipe_memory_follows_data(void) {
    int fds[2];
    if (pipe(fds) < 0) {
        THROW_ERROR("failed to create a pipe");
    }
    if (!IN_OCCLUM) {
        // A pipe of Linux holds 64 KiB, unless it is told to hold more
        fcntl(fds[1], F_SETPIPE_SZ, PIPE_BUF_SIZE);
    }
    long before = heap_kb();
    int ret = check_memory_follows_data(fds[1], fds[0], PIPE_BUF_SIZE, "pipe");
    close(fds[0]);
    close(fds[1]);
    if (ret < 0) {
        return ret;
    }
    return check_heap_returned(before, "the pipe");
}

// A queue that has carried data keeps its last chunk and a few spare chunks for its writer while
// both ends are open. It gives the spares back when the writer is done (shutdown(SHUT_WR) of a
// socket, closing a pipe), or when the reader is closed, and the reader gets the end of the stream
// in the first case. The writer fills the queue and the reader drains it, again and again.
enum { SOCKETPAIR, CONNECTION, PIPE };

// Creates the descriptors that a writer writes to and a reader reads from
static int make_pair(int kind, int *wfd, int *rfd) {
    if (kind == SOCKETPAIR) {
        int fds[2];
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) < 0) {
            THROW_ERROR("failed to create a socketpair");
        }
        *wfd = fds[0];
        *rfd = fds[1];
    } else if (kind == CONNECTION) {
        struct sockaddr_un addr;
        socklen_t addr_len;
        int listener = make_listener(1, &addr, &addr_len);
        if (listener < 0) {
            return -1;
        }
        int client = socket(AF_UNIX, SOCK_STREAM, 0);
        if (client < 0 || connect(client, (struct sockaddr *)&addr, addr_len) < 0) {
            THROW_ERROR("failed to connect");
        }
        int server = accept(listener, NULL, NULL);
        if (server < 0) {
            THROW_ERROR("failed to accept");
        }
        close(listener);
        *wfd = client;
        *rfd = server;
    } else {
        int fds[2];
        if (pipe(fds) < 0) {
            THROW_ERROR("failed to create a pipe");
        }
        if (!IN_OCCLUM) {
            fcntl(fds[1], F_SETPIPE_SZ, PIPE_BUF_SIZE);
        }
        *wfd = fds[1];
        *rfd = fds[0];
    }
    return 0;
}

static int check_memory_after_a_transfer(int kind, size_t capacity, const char *name) {
    for (int reader_closes = 0; reader_closes < 2; reader_closes++) {
        int wfd, rfd;
        if (make_pair(kind, &wfd, &rfd) < 0 || set_nonblocking(wfd) < 0 ||
                set_nonblocking(rfd) < 0) {
            return -1;
        }
        long before = heap_kb();
        unsigned char *wbuf = malloc(65536);
        unsigned char *rbuf = malloc(65536);
        size_t sent = 0, got = 0;
        for (int round = 0; round < 12; round++) {
            for (;;) {
                fill_pattern(wbuf, sent, 65536);
                ssize_t w = write(wfd, wbuf, 65536);
                if (w < 0 && errno == EAGAIN) {
                    break;
                }
                if (w < 0) {
                    THROW_ERROR("%s: failed to write", name);
                }
                sent += w;
            }
            for (;;) {
                ssize_t r = read(rfd, rbuf, 65536);
                if (r < 0 && errno == EAGAIN) {
                    break;
                }
                if (r <= 0 || find_wrong_byte(rbuf, got, r) >= 0) {
                    THROW_ERROR("%s: failed to read, or the data is wrong", name);
                }
                got += r;
            }
        }
        free(wbuf);
        free(rbuf);
        if (got != sent) {
            THROW_ERROR("%s: %zu bytes were written, %zu read", name, sent, got);
        }
        if (heap_usable) {
            long held = (heap_kb() - before) * 1024;
            printf("\t\t%s idle after %zu bytes: %ld bytes\n", name, sent, held);
            if (held > kept_bytes(capacity)) {
                THROW_ERROR("%s holds %ld bytes of the kernel heap although it is idle", name,
                            held);
            }
            // The spares are there: the checks below would find nothing to give back otherwise
            if (held <= 2 * largest_chunk(capacity)) {
                THROW_ERROR("%s holds only %ld bytes of the kernel heap after a transfer", name,
                            held);
            }
        }

        const char *what;
        if (reader_closes) {
            what = "the reader is closed";
            close(rfd);
        } else {
            what = "the writer is done";
            if (kind == PIPE) {
                close(wfd);
            } else if (shutdown(wfd, SHUT_WR) < 0) {
                THROW_ERROR("%s: failed to shut down the writer", name);
            }
        }
        if (heap_usable) {
            long held = (heap_kb() - before) * 1024;
            printf("\t\t%s after %s: %ld bytes\n", name, what, held);
            if (held > largest_chunk(capacity) + 8 * 1024) {
                THROW_ERROR("%s holds %ld bytes of the kernel heap after %s", name, held, what);
            }
        }
        if (!reader_closes) {
            char byte;
            if (read(rfd, &byte, 1) != 0) {
                THROW_ERROR("%s: the reader does not get the end of the stream", name);
            }
            close(rfd);
        }
        if (reader_closes || kind != PIPE) {
            close(wfd);
        }
        if (check_heap_returned(before, name) < 0) {
            return -1;
        }
    }
    return 0;
}

static int test_socketpair_memory_after_a_transfer(void) {
    return check_memory_after_a_transfer(SOCKETPAIR, SOCKET_BUF_SIZE, "socketpair");
}

static int test_connection_memory_after_a_transfer(void) {
    return check_memory_after_a_transfer(CONNECTION, SOCKET_BUF_SIZE, "connection");
}

static int test_pipe_memory_after_a_transfer(void) {
    return check_memory_after_a_transfer(PIPE, PIPE_BUF_SIZE, "pipe");
}

// Cycles of connections, socketpairs and pipes that carry a random amount of data, which is read in
// part, and are closed in a random order: a chunk that is not freed in one of these cases shows up
// as a growing heap after a few thousand cycles. The limit is tight: a connection leaks a chunk of
// 528 bytes for the 64 that are accepted, which is 8 bytes per cycle
#define CYCLES 4000
#define LEAK_LIMIT_KB 8

static int leak_cycle(int kind, int listener, const struct sockaddr_un *addr,
                      socklen_t addr_len,
                      unsigned *rng, unsigned char *buf) {
    int a, b;
    if (kind == 0) {
        int fds[2];
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) < 0) {
            THROW_ERROR("failed to create a socketpair");
        }
        a = fds[0];
        b = fds[1];
    } else if (kind == 1) {
        a = socket(AF_UNIX, SOCK_STREAM, 0);
        if (a < 0 || connect(a, (const struct sockaddr *)addr, addr_len) < 0) {
            THROW_ERROR("failed to connect");
        }
        b = accept(listener, NULL, NULL);
        if (b < 0) {
            THROW_ERROR("failed to accept");
        }
    } else {
        int fds[2];
        if (pipe(fds) < 0) {
            THROW_ERROR("failed to create a pipe");
        }
        a = fds[1];
        b = fds[0];
    }
    if (set_nonblocking(a) < 0 || set_nonblocking(b) < 0) {
        return -1;
    }
    size_t amounts[] = {next_random(rng) % 100, next_random(rng) % 3000, next_random(rng) % 40000,
                        next_random(rng) % 300000
                       };
    size_t n = amounts[next_random(rng) % 4];
    fill_pattern(buf, 0, n);
    ssize_t w = write(a, buf, n);
    if (w < 0 && errno != EAGAIN) {
        THROW_ERROR("failed to write");
    }
    if (w > 0) {
        // Read part of it
        size_t part = next_random(rng) % (w + 1);
        ssize_t r = part > 0 ? read(b, buf, part) : 0;
        if (r < 0 || (r > 0 && find_wrong_byte(buf, 0, r) >= 0)) {
            THROW_ERROR("failed to read, or the data is wrong");
        }
    }
    // And something in the other direction of a socket
    if (kind != 2 && next_random(rng) % 2) {
        ssize_t x = write(b, buf, next_random(rng) % 5000);
        (void)x;
    }
    if (next_random(rng) % 2) {
        close(a);
        close(b);
    } else {
        close(b);
        close(a);
    }
    return 0;
}

static int test_cycles_leak_nothing(void) {
    unsigned char *buf = malloc(300000);
    struct sockaddr_un addr;
    socklen_t addr_len;
    int listener = make_listener(100, &addr, &addr_len);
    if (listener < 0) {
        return -1;
    }
    unsigned rng = 4711;
    static const char *names[] = {"socketpairs", "connections", "pipes"};
    for (int kind = 0; kind < 3; kind++) {
        long before = heap_kb();
        for (int i = 0; i < CYCLES; i++) {
            if (leak_cycle(kind, listener, &addr, addr_len, &rng, buf) < 0) {
                printf("\t\tERROR: cycle %d of the %s failed\n", i, names[kind]);
                return -1;
            }
        }
        if (heap_usable) {
            long grown = heap_kb() - before;
            printf("\t\t%d cycles of %s: %+ld kB\n", CYCLES, names[kind], grown);
            if (grown > LEAK_LIMIT_KB) {
                THROW_ERROR("the kernel heap grew by %ld kB in %d cycles of %s (limit %d kB)", grown,
                            CYCLES, names[kind], LEAK_LIMIT_KB);
            }
        }
    }
    close(listener);
    free(buf);
    return 0;
}

// A queue gives up a chunk when its reader has read the last byte of it and the next chunk exists
// already. The reads of test_cycles_leak_nothing end at the border of a chunk by chance only, so
// these cycles read in pieces of 512 bytes, or of 511 and 1 byte in every other cycle (a read of
// one byte takes another way through the queue). 512 bytes are the smallest chunk, and the size of
// all the others is a multiple of it, so every border is at a multiple of 512 and a read ends at
// each of them. The data is written before anything is read, so that the queue has several chunks
// of different sizes, and a connection has its data queued before it is accepted. A chunk that is
// not freed at a border leaks a chunk per cycle, which is much more than the limit
#define BORDER_CYCLES 2000
#define BORDER_PIECE 512

static const size_t border_writes[] = {100, 1000, 100, 2000, 5000, 20};

// Reads the bytes of the pattern that are queued in `fd`, 512 at a time, or 511 and 1 at a time
static int read_in_pieces(int fd, size_t total, int split) {
    unsigned char piece[BORDER_PIECE];
    size_t offset = 0;
    while (offset < total) {
        size_t size = BORDER_PIECE;
        if (split) {
            size = offset % BORDER_PIECE == 0 ? BORDER_PIECE - 1 : 1;
        }
        size_t want = MIN(size, total - offset);
        ssize_t r = read(fd, piece, size);
        if (r != (ssize_t)want) {
            THROW_ERROR("a piece at %zu of %zu bytes is %zd bytes, not %zu", offset, total, r,
                        want);
        }
        long wrong = find_wrong_byte(piece, offset, r);
        if (wrong >= 0) {
            THROW_ERROR("byte %ld of the stream is wrong", wrong);
        }
        offset += r;
    }
    if (read(fd, piece, BORDER_PIECE) >= 0 || errno != EAGAIN) {
        THROW_ERROR("there are more bytes than the %zu that were written", total);
    }
    return 0;
}

// Writes the pattern in the writes of different sizes to `fd`, which takes all of them
static int write_in_pieces(int fd, const unsigned char *pattern_buf, size_t *total) {
    *total = 0;
    for (size_t i = 0; i < ARRAY_SIZE(border_writes); i++) {
        if (write(fd, pattern_buf + *total, border_writes[i]) != (ssize_t)border_writes[i]) {
            THROW_ERROR("failed to write %zu bytes", border_writes[i]);
        }
        *total += border_writes[i];
    }
    return 0;
}

static int border_cycle(int kind, int listener, const struct sockaddr_un *addr,
                        socklen_t addr_len, const unsigned char *pattern_buf, int split) {
    int a, b;
    size_t total;
    int ret = -1;
    if (kind == 0) {
        int fds[2];
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) < 0) {
            THROW_ERROR("failed to create a socketpair");
        }
        a = fds[0];
        b = fds[1];
        if (set_nonblocking(a) < 0 || write_in_pieces(a, pattern_buf, &total) < 0) {
            goto out;
        }
    } else if (kind == 1) {
        a = socket(AF_UNIX, SOCK_STREAM, 0);
        if (a < 0 || connect(a, (const struct sockaddr *)addr, addr_len) < 0) {
            THROW_ERROR("failed to connect");
        }
        // The client writes while its connection is not accepted
        if (set_nonblocking(a) < 0 || write_in_pieces(a, pattern_buf, &total) < 0) {
            close(a);
            return -1;
        }
        b = accept(listener, NULL, NULL);
        if (b < 0) {
            close(a);
            THROW_ERROR("failed to accept");
        }
    } else {
        int fds[2];
        if (pipe(fds) < 0) {
            THROW_ERROR("failed to create a pipe");
        }
        a = fds[1];
        b = fds[0];
        if (set_nonblocking(a) < 0 || write_in_pieces(a, pattern_buf, &total) < 0) {
            goto out;
        }
    }
    if (set_nonblocking(b) < 0 || read_in_pieces(b, total, split) < 0) {
        goto out;
    }
    if (kind != 2) {
        // The other direction of the socket
        if (write_in_pieces(b, pattern_buf, &total) < 0 || read_in_pieces(a, total, split) < 0) {
            goto out;
        }
    }
    ret = 0;
out:
    close(a);
    close(b);
    return ret;
}

static int test_chunk_borders_leak_nothing(void) {
    size_t total = 0;
    for (size_t i = 0; i < ARRAY_SIZE(border_writes); i++) {
        total += border_writes[i];
    }
    unsigned char *pattern_buf = malloc(total);
    fill_pattern(pattern_buf, 0, total);
    struct sockaddr_un addr;
    socklen_t addr_len;
    int listener = make_listener(100, &addr, &addr_len);
    if (listener < 0) {
        return -1;
    }
    static const char *names[] = {"socketpairs", "connections", "pipes"};
    for (int kind = 0; kind < 3; kind++) {
        long before = heap_kb();
        for (int i = 0; i < BORDER_CYCLES; i++) {
            if (border_cycle(kind, listener, &addr, addr_len, pattern_buf, i % 2) < 0) {
                printf("\t\tERROR: cycle %d of the %s failed\n", i, names[kind]);
                return -1;
            }
        }
        if (heap_usable) {
            long grown = heap_kb() - before;
            printf("\t\t%d cycles of %s: %+ld kB\n", BORDER_CYCLES, names[kind], grown);
            if (grown > LEAK_LIMIT_KB) {
                THROW_ERROR("the kernel heap grew by %ld kB in %d cycles of %s (limit %d kB)", grown,
                            BORDER_CYCLES, names[kind], LEAK_LIMIT_KB);
            }
        }
    }
    close(listener);
    free(pattern_buf);
    return 0;
}

// Two threads move `total` bytes through a descriptor pair in random chunk sizes
typedef struct {
    int fd;
    size_t total;
    size_t max_chunk;
    unsigned seed;
    int shutdown_socket;      // shut down the sending side when everything is sent
    int pause_every;          // sleeps now and then, to make the other side wait
    volatile size_t done;     // bytes sent or received
    volatile int stop;        // the writer stops when it is set
    int error;
} xfer_t;

static void maybe_pause(xfer_t *x, unsigned *rng) {
    if (x->pause_every > 0 && next_random(rng) % x->pause_every == 0) {
        usleep(next_random(rng) % 3000);
    }
}

static void *writer_thread(void *arg) {
    xfer_t *x = arg;
    unsigned rng = x->seed;
    unsigned char *buf = malloc(x->max_chunk);
    while (x->done < x->total && !x->stop) {
        size_t n = 1 + next_random(&rng) % x->max_chunk;
        n = MIN(n, x->total - x->done);
        fill_pattern(buf, x->done, n);
        size_t off = 0;
        while (off < n) {
            ssize_t r = write(x->fd, buf + off, n - off);
            if (r < 0 && errno == EINTR) {
                continue;
            }
            if (r < 0) {
                // The reader went away, or the socket was shut down in the middle: what was
                // written before is all that there is
                x->done += off;
                if (errno != EPIPE && errno != ECONNRESET) {
                    x->error = errno;
                }
                goto out;
            }
            off += r;
        }
        x->done += n;
        maybe_pause(x, &rng);
    }
out:
    free(buf);
    if (x->shutdown_socket) {
        shutdown(x->fd, SHUT_WR);
    } else {
        close(x->fd);
    }
    return NULL;
}

static void *reader_thread(void *arg) {
    xfer_t *x = arg;
    unsigned rng = x->seed ^ 0x5bd1e995;
    unsigned char *buf = malloc(x->max_chunk);
    for (;;) {
        size_t n = 1 + next_random(&rng) % x->max_chunk;
        ssize_t r = read(x->fd, buf, n);
        if (r < 0 && errno == EINTR) {
            continue;
        }
        if (r < 0) {
            x->error = errno;
            break;
        }
        if (r == 0) {
            break;
        }
        long wrong = find_wrong_byte(buf, x->done, r);
        if (wrong >= 0) {
            printf("\t\tERROR: byte %ld of the stream is wrong\n", wrong);
            x->error = EIO;
            break;
        }
        x->done += r;
        maybe_pause(x, &rng);
    }
    free(buf);
    return NULL;
}

// Moves the data and checks that the reader gets what the writer wrote, in order. If
// `shutdown_after_us` is more than 0, the main thread shuts down the writing end of the writer's
// socket after that time, so the writer fails and the reader gets as much as the writer wrote.
static int run_transfer(int wfd, int rfd, size_t total, size_t max_chunk, unsigned seed,
                        int is_socket, int pause_every, int shutdown_after_us) {
    xfer_t w = {.fd = wfd, .total = total, .max_chunk = max_chunk, .seed = seed,
                .shutdown_socket = is_socket, .pause_every = pause_every
               };
    xfer_t r = {.fd = rfd, .total = total, .max_chunk = max_chunk, .seed = seed,
                .pause_every = pause_every
               };
    pthread_t tw, tr;
    if (pthread_create(&tr, NULL, reader_thread, &r) != 0 ||
            pthread_create(&tw, NULL, writer_thread, &w) != 0) {
        THROW_ERROR("failed to create the threads");
    }
    if (shutdown_after_us > 0) {
        usleep(shutdown_after_us);
        // The writer is in the middle of its transfer, and does not stop by itself
        shutdown(wfd, SHUT_WR);
    }
    pthread_join(tw, NULL);
    pthread_join(tr, NULL);
    if (w.error || r.error) {
        THROW_ERROR("transfer of %zu bytes in chunks up to %zu failed: write errno %d, read "
                    "errno %d", total, max_chunk, w.error, r.error);
    }
    if (shutdown_after_us <= 0 && w.done != total) {
        THROW_ERROR("the writer sent %zu of %zu bytes", (size_t)w.done, total);
    }
    if (r.done != w.done) {
        THROW_ERROR("the writer sent %zu bytes, the reader got %zu (chunks up to %zu)",
                    (size_t)w.done, (size_t)r.done, max_chunk);
    }
    if (shutdown_after_us > 0) {
        printf("\t\tshut down after %d us: %zu of %zu bytes were sent\n", shutdown_after_us,
               (size_t)w.done, total);
    }
    return 0;
}

static const size_t chunk_sizes[] = {1, 100, 4096, 70000, 400000};

// Small chunks take many system calls, so they move less data
static size_t total_for_chunk(size_t chunk) {
    return MIN(24UL * 1024 * 1024, 30000 * chunk);
}

static int test_socketpair_bulk_integrity(void) {
    for (size_t i = 0; i < ARRAY_SIZE(chunk_sizes); i++) {
        int fds[2];
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) < 0) {
            THROW_ERROR("failed to create a socketpair");
        }
        // Every third transfer has threads that wait for each other now and then
        int ret = run_transfer(fds[0], fds[1], total_for_chunk(chunk_sizes[i]), chunk_sizes[i],
                               1234 + i, 1, i % 3 == 2 ? 50 : 0, 0);
        close(fds[0]);
        close(fds[1]);
        if (ret < 0) {
            return ret;
        }
    }
    return 0;
}

static int test_connection_bulk_integrity(void) {
    struct sockaddr_un addr;
    socklen_t addr_len;
    int listener = make_listener(4, &addr, &addr_len);
    if (listener < 0) {
        return -1;
    }
    for (size_t i = 0; i < ARRAY_SIZE(chunk_sizes); i++) {
        int client = socket(AF_UNIX, SOCK_STREAM, 0);
        if (client < 0 || connect(client, (struct sockaddr *)&addr, addr_len) < 0) {
            THROW_ERROR("failed to connect");
        }
        int server = accept(listener, NULL, NULL);
        if (server < 0) {
            THROW_ERROR("failed to accept");
        }
        // Client to server, then server to client
        int ret = run_transfer(client, server, total_for_chunk(chunk_sizes[i]), chunk_sizes[i],
                               99 + i, 1, i % 2 ? 50 : 0, 0);
        if (ret == 0) {
            ret = run_transfer(server, client, total_for_chunk(chunk_sizes[i]), chunk_sizes[i],
                               77 + i, 1, 0, 0);
        }
        close(client);
        close(server);
        if (ret < 0) {
            return ret;
        }
    }
    close(listener);
    return 0;
}

static int test_pipe_bulk_integrity(void) {
    for (size_t i = 0; i < ARRAY_SIZE(chunk_sizes); i++) {
        int fds[2];
        if (pipe(fds) < 0) {
            THROW_ERROR("failed to create a pipe");
        }
        // The writer closes the pipe when it is done: that is the end of the file for the reader
        int ret = run_transfer(fds[1], fds[0], total_for_chunk(chunk_sizes[i]), chunk_sizes[i],
                               4321 + i, 0, i % 3 == 1 ? 50 : 0, 0);
        close(fds[0]);
        if (ret < 0) {
            return ret;
        }
    }
    return 0;
}

// The writing end is shut down while the writer is in the middle of the transfer. The reader
// must get all the bytes that the writer got through, and then the end of the stream
static int test_shutdown_in_the_middle(void) {
    static const int delays_us[] = {1, 300, 2000, 10000, 40000};
    for (size_t i = 0; i < ARRAY_SIZE(delays_us); i++) {
        int fds[2];
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) < 0) {
            THROW_ERROR("failed to create a socketpair");
        }
        // More than they move in that time, and chunks of all sizes
        size_t chunk = i % 2 ? 4096 : 100000;
        int ret = run_transfer(fds[0], fds[1], 1024UL * 1024 * 1024, chunk, 555 + i, 1,
                               i % 3 == 0 ? 50 : 0, delays_us[i]);
        close(fds[0]);
        close(fds[1]);
        if (ret < 0) {
            return ret;
        }
    }
    return 0;
}

static test_case_t test_cases[] = {
    TEST_CASE(test_socketpair_memory),
    TEST_CASE(test_pending_connections_memory),
    TEST_CASE(test_many_pending_connections),
    TEST_CASE(test_pending_connections_keep_data),
    TEST_CASE(test_close_with_queued_data),
    TEST_CASE(test_pipe_memory),
    TEST_CASE(test_socketpair_memory_follows_data),
    TEST_CASE(test_connection_memory_follows_data),
    TEST_CASE(test_pipe_memory_follows_data),
    TEST_CASE(test_socketpair_memory_after_a_transfer),
    TEST_CASE(test_connection_memory_after_a_transfer),
    TEST_CASE(test_pipe_memory_after_a_transfer),
    TEST_CASE(test_cycles_leak_nothing),
    TEST_CASE(test_chunk_borders_leak_nothing),
    TEST_CASE(test_socketpair_bulk_integrity),
    TEST_CASE(test_connection_bulk_integrity),
    TEST_CASE(test_pipe_bulk_integrity),
    TEST_CASE(test_shutdown_in_the_middle),
};

int main(int argc, const char *argv[]) {
    // A write to a socket or a pipe that was shut down is a normal error here
    signal(SIGPIPE, SIG_IGN);
    // The output is complete if the LibOS aborts the process
    setvbuf(stdout, NULL, _IOLBF, 0);

    test_case_t selected[ARRAY_SIZE(test_cases)];
    test_case_t *cases = test_cases;
    int num_cases = ARRAY_SIZE(test_cases);
    if (argc > 1) {
        // Run only the test case with the name, or list the names
        int list = strcmp(argv[1], "--list") == 0;
        num_cases = 0;
        for (size_t i = 0; i < ARRAY_SIZE(test_cases); i++) {
            if (list) {
                printf("%s\n", test_cases[i].name);
            } else if (strcmp(argv[1], test_cases[i].name) == 0) {
                selected[num_cases++] = test_cases[i];
            }
        }
        if (list) {
            return 0;
        }
        if (num_cases == 0) {
            printf("no such test case: %s\n", argv[1]);
            return 2;
        }
        cases = selected;
    }

    pthread_t thread;
    if (pthread_create(&thread, NULL, watchdog, NULL) == 0) {
        pthread_detach(thread);
    }
    if (check_heap_usable() < 0) {
        return 1;
    }
    return test_suite_run(cases, num_cases) < 0 ? 1 : 0;
}
