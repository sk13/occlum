// The options of unix stream sockets: setsockopt() stores them and getsockopt() returns them, with
// the values and errors of Linux. The test passes there, too (gcc -I../include main.c), and what
// it expects of the LibOS is what Linux does:
//
//  - the options of SOL_SOCKET that a program sets are read back (flags, integers, SO_LINGER, the
//    timeouts, the sizes of the buffers, which are doubled), also through a dup() of the socket;
//  - the options of other levels (TCP_NODELAY, IP_TTL) fail with EOPNOTSUPP, an unknown option with
//    ENOPROTOOPT, a read-only option cannot be set (ENOPROTOOPT), a short value is EINVAL;
//  - an accepted socket takes over SO_PASSCRED and SO_PASSSEC from the listening socket, and no
//    other option;
//  - SO_PEERCRED and SO_PEERGROUPS have the credentials of the creator of a socket pair.
//
// Options whose Linux behavior depends on the privileges of the process (SO_DEBUG, SO_MARK,
// SO_SNDBUFFORCE) are only checked if Linux lets the process set them, as Occlum does for root.
// Timeouts are multiples of 20 ms, as Linux rounds them up to its clock tick (1 to 10 ms).
//
// Usage: unix_socket_options [test case name | --list]
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <unistd.h>

#include "kernel_heap.h"
#include "test.h"

// Options that some C libraries do not define
#define OPT_TIMESTAMP_OLD       29
#define OPT_TIMESTAMPNS_OLD     35
#define OPT_TIMESTAMP_NEW       63
#define OPT_TIMESTAMPNS_NEW     64
#define OPT_RCVTIMEO_NEW        66
#define OPT_SNDTIMEO_NEW        67
#define OPT_PEEK_OFF            42
#define OPT_PASSSEC             34
#define OPT_NO_CHECK            11
#define OPT_RXQ_OVFL            40
#define OPT_SELECT_ERR_QUEUE    45
#define OPT_LOCK_FILTER         44
#define OPT_BUSY_POLL           46
#define OPT_INCOMING_CPU        49
#define OPT_SNDBUFFORCE         32
#define OPT_RCVBUFFORCE         33
#define OPT_PEERSEC             31
#define OPT_PEERGROUPS          59
#define OPT_PEERPIDFD           77
#define OPT_MARK                36
#define OPT_REUSEPORT           15

#define DEFAULT_BUF_SIZE        212992
#define MIN_SNDBUF              4608
#define MIN_RCVBUF              2304

// ---------------------------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------------------------

// Sets an option. Returns 0, or the negative errno.
static int set_opt(int fd, int level, int opt, const void *val, socklen_t len) {
    return setsockopt(fd, level, opt, val, len) < 0 ? -errno : 0;
}

static int set_int(int fd, int opt, int val) {
    return set_opt(fd, SOL_SOCKET, opt, &val, sizeof(val));
}

// Reads an int option, which must write the whole int. Returns 0, or the negative errno.
static int get_int(int fd, int level, int opt, int *val) {
    socklen_t len = sizeof(*val);
    *val = 0x7f7f7f7f;
    if (getsockopt(fd, level, opt, val, &len) < 0) {
        return -errno;
    }
    return len == sizeof(*val) && *val != 0x7f7f7f7f ? 0 : -EMSGSIZE;
}

#define CHECK_SET(fd, opt, val) do { \
    int r_ = set_int(fd, opt, val); \
    if (r_ != 0) { \
        THROW_ERROR("setsockopt(" #opt ", %d) returned %d", (int)(val), r_); \
    } \
} while (0)

#define CHECK_SET_ERR(fd, opt, val, err) do { \
    int r_ = set_int(fd, opt, val); \
    if (r_ != -(err)) { \
        THROW_ERROR("setsockopt(" #opt ", %d) returned %d, expected %d", (int)(val), r_, -(err)); \
    } \
} while (0)

#define CHECK_GET(fd, opt, expected) do { \
    int v_; \
    int r_ = get_int(fd, SOL_SOCKET, opt, &v_); \
    if (r_ != 0) { \
        THROW_ERROR("getsockopt(" #opt ") returned %d", r_); \
    } \
    if (v_ != (expected)) { \
        THROW_ERROR("getsockopt(" #opt ") returned %d, expected %d", v_, (int)(expected)); \
    } \
} while (0)

#define CHECK_GET_ERR(fd, level, opt, err) do { \
    int v_; \
    int r_ = get_int(fd, level, opt, &v_); \
    if (r_ != -(err)) { \
        THROW_ERROR("getsockopt(" #opt ") returned %d, expected %d", r_, -(err)); \
    } \
} while (0)

#define CHECK(cond, ...) do { \
    if (!(cond)) { \
        THROW_ERROR(__VA_ARGS__); \
    } \
} while (0)

static int new_socket(void) {
    return socket(AF_UNIX, SOCK_STREAM, 0);
}

// An abstract address, which needs no file
static socklen_t make_addr(struct sockaddr_un *addr) {
    static int counter = 0;
    memset(addr, 0, sizeof(*addr));
    addr->sun_family = AF_UNIX;
    int name_len = snprintf(addr->sun_path + 1, sizeof(addr->sun_path) - 1,
                            "unix_socket_options.%d.%d", (int)getpid(), counter++);
    return offsetof(struct sockaddr_un, sun_path) + 1 + name_len;
}

// Creates a listening socket
static int new_listener(struct sockaddr_un *addr, socklen_t *addr_len) {
    int fd = new_socket();
    *addr_len = make_addr(addr);
    if (fd < 0 || bind(fd, (struct sockaddr *)addr, *addr_len) < 0 || listen(fd, 4) < 0) {
        if (fd >= 0) {
            close(fd);
        }
        return -1;
    }
    return fd;
}

// Connects a socket to the listener and accepts it. Returns the connected sockets.
static int connect_and_accept(int listen_fd, const struct sockaddr_un *addr,
                              socklen_t addr_len,
                              int *client_fd, int *accepted_fd) {
    int c = new_socket();
    if (c < 0 || connect(c, (const struct sockaddr *)addr, addr_len) < 0) {
        return -1;
    }
    int s = accept(listen_fd, NULL, NULL);
    if (s < 0) {
        close(c);
        return -1;
    }
    *client_fd = c;
    *accepted_fd = s;
    return 0;
}

// The size of the buffers of a new socket: the default of the system, which is that of the LibOS
static int default_buf_size(const char *file) {
    int size = DEFAULT_BUF_SIZE;
    FILE *f = fopen(file, "r");
    if (f != NULL) {
        if (fscanf(f, "%d", &size) != 1) {
            size = DEFAULT_BUF_SIZE;
        }
        fclose(f);
    }
    return size;
}

static int default_sndbuf(void) {
    return default_buf_size("/proc/sys/net/core/wmem_default");
}

static int default_rcvbuf(void) {
    return default_buf_size("/proc/sys/net/core/rmem_default");
}

static int set_timeout(int fd, int opt, long sec, long usec) {
    struct timeval tv = { .tv_sec = sec, .tv_usec = usec };
    return set_opt(fd, SOL_SOCKET, opt, &tv, sizeof(tv));
}

// Reads a timeout, which must have the length of a struct timeval
static int get_timeout(int fd, int opt, long *sec, long *usec) {
    struct timeval tv = { .tv_sec = 77, .tv_usec = 77 };
    socklen_t len = sizeof(tv);
    if (getsockopt(fd, SOL_SOCKET, opt, &tv, &len) < 0) {
        return -errno;
    }
    *sec = tv.tv_sec;
    *usec = tv.tv_usec;
    return len == sizeof(tv) ? 0 : -EMSGSIZE;
}

#define CHECK_TIMEOUT(fd, opt, sec, usec) do { \
    long s_, u_; \
    int r_ = get_timeout(fd, opt, &s_, &u_); \
    if (r_ != 0) { \
        THROW_ERROR("getsockopt(" #opt ") returned %d", r_); \
    } \
    if (s_ != (long)(sec) || u_ != (long)(usec)) { \
        THROW_ERROR("getsockopt(" #opt ") returned %ld s %ld us, expected %ld s %ld us", \
                    s_, u_, (long)(sec), (long)(usec)); \
    } \
} while (0)

static int get_linger(int fd, int *onoff, int *secs) {
    struct linger l = { .l_onoff = 77, .l_linger = 77 };
    socklen_t len = sizeof(l);
    if (getsockopt(fd, SOL_SOCKET, SO_LINGER, &l, &len) < 0) {
        return -errno;
    }
    *onoff = l.l_onoff;
    *secs = l.l_linger;
    return len == sizeof(l) ? 0 : -EMSGSIZE;
}

static int set_linger(int fd, int onoff, int secs) {
    struct linger l = { .l_onoff = onoff, .l_linger = secs };
    return set_opt(fd, SOL_SOCKET, SO_LINGER, &l, sizeof(l));
}

#define CHECK_LINGER(fd, onoff, secs) do { \
    int o_, s_; \
    int r_ = get_linger(fd, &o_, &s_); \
    if (r_ != 0) { \
        THROW_ERROR("getsockopt(SO_LINGER) returned %d", r_); \
    } \
    if (o_ != (onoff) || s_ != (secs)) { \
        THROW_ERROR("SO_LINGER is %d, %d, expected %d, %d", o_, s_, (int)(onoff), (int)(secs)); \
    } \
} while (0)

// The options that are on or off, with the name for the messages
static const struct {
    const char *name;
    int opt;
} flag_opts[] = {
    { "SO_KEEPALIVE", SO_KEEPALIVE },
    { "SO_REUSEADDR", SO_REUSEADDR },
    { "SO_OOBINLINE", SO_OOBINLINE },
    { "SO_BROADCAST", SO_BROADCAST },
    { "SO_DONTROUTE", SO_DONTROUTE },
    { "SO_PASSCRED", SO_PASSCRED },
    { "SO_PASSSEC", OPT_PASSSEC },
    { "SO_NO_CHECK", OPT_NO_CHECK },
    { "SO_RXQ_OVFL", OPT_RXQ_OVFL },
    { "SO_SELECT_ERR_QUEUE", OPT_SELECT_ERR_QUEUE },
};

// ---------------------------------------------------------------------------------------------
// Test cases
// ---------------------------------------------------------------------------------------------

// The values of the options of a new socket, which are those of Linux
static int check_default_options(int fd) {
    CHECK_GET(fd, SO_TYPE, SOCK_STREAM);
    CHECK_GET(fd, SO_DOMAIN, AF_UNIX);
    CHECK_GET(fd, SO_PROTOCOL, 0);
    CHECK_GET(fd, SO_ERROR, 0);
    CHECK_GET(fd, SO_ACCEPTCONN, 0);
    for (size_t i = 0; i < ARRAY_SIZE(flag_opts); i++) {
        int v;
        if (get_int(fd, SOL_SOCKET, flag_opts[i].opt, &v) != 0 || v != 0) {
            THROW_ERROR("%s of a new socket is %d", flag_opts[i].name, v);
        }
    }
    CHECK_GET(fd, SO_DEBUG, 0);
    CHECK_GET(fd, SO_REUSEPORT, 0);
    CHECK_GET(fd, OPT_TIMESTAMP_OLD, 0);
    CHECK_GET(fd, SO_PRIORITY, 0);
    CHECK_GET(fd, OPT_MARK, 0);
    CHECK_GET(fd, SO_RCVLOWAT, 1);
    CHECK_GET(fd, SO_SNDLOWAT, 1);
    CHECK_GET(fd, OPT_BUSY_POLL, 0);
    CHECK_GET(fd, OPT_INCOMING_CPU, -1);
    CHECK_GET(fd, OPT_PEEK_OFF, -1);
    CHECK_GET(fd, SO_SNDBUF, default_sndbuf());
    CHECK_GET(fd, SO_RCVBUF, default_rcvbuf());
    CHECK_LINGER(fd, 0, 0);
    CHECK_TIMEOUT(fd, SO_RCVTIMEO, 0, 0);
    CHECK_TIMEOUT(fd, SO_SNDTIMEO, 0, 0);
    // The socket is bound to no device: the name is empty
    char device[32];
    socklen_t len = sizeof(device);
    CHECK(getsockopt(fd, SOL_SOCKET, SO_BINDTODEVICE, device, &len) == 0 && len == 0,
          "SO_BINDTODEVICE of a new socket has %u bytes", (unsigned)len);
    return 0;
}

static int test_default_options(void) {
    int fd = new_socket();
    int sv[2];
    if (fd < 0 || socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) {
        THROW_ERROR("failed to create sockets");
    }
    if (check_default_options(fd) < 0 || check_default_options(sv[0]) < 0 ||
            check_default_options(sv[1]) < 0) {
        THROW_ERROR("a socket has other options than the default");
    }
    struct sockaddr_un addr;
    socklen_t addr_len;
    int listen_fd = new_listener(&addr, &addr_len);
    int client_fd, accepted_fd;
    if (listen_fd < 0 || connect_and_accept(listen_fd, &addr, addr_len, &client_fd,
                                            &accepted_fd) < 0) {
        THROW_ERROR("failed to connect");
    }
    // A listening socket is one, and the others of a connection are not
    CHECK_GET(listen_fd, SO_ACCEPTCONN, 1);
    CHECK_GET(client_fd, SO_ACCEPTCONN, 0);
    CHECK_GET(accepted_fd, SO_ACCEPTCONN, 0);
    if (check_default_options(client_fd) < 0 || check_default_options(accepted_fd) < 0) {
        THROW_ERROR("a connected socket has other options than the default");
    }
    close_files(5, fd, sv[0], sv[1], listen_fd, client_fd);
    close(accepted_fd);
    return 0;
}

static int test_flag_options(void) {
    int fd = new_socket();
    CHECK(fd >= 0, "failed to create a socket");
    for (size_t i = 0; i < ARRAY_SIZE(flag_opts); i++) {
        int opt = flag_opts[i].opt;
        const char *name = flag_opts[i].name;
        // Any value but 0 turns the option on, and the value that is read is 0 or 1
        static const int values[][2] = { {1, 1}, {0, 0}, {5, 1}, {0, 0}, {-1, 1}, {0, 0} };
        for (size_t k = 0; k < ARRAY_SIZE(values); k++) {
            int v;
            if (set_int(fd, opt, values[k][0]) != 0 || get_int(fd, SOL_SOCKET, opt, &v) != 0 ||
                    v != values[k][1]) {
                THROW_ERROR("%s set to %d reads %d, expected %d", name, values[k][0], v,
                            values[k][1]);
            }
        }
    }
    close(fd);
    return 0;
}

// SO_DEBUG needs a privilege (EACCES without it), and SO_REUSEPORT is refused on unix sockets by
// recent kernels, and set by old ones
static int test_flag_options_with_exceptions(void) {
    int fd = new_socket();
    CHECK(fd >= 0, "failed to create a socket");
    int r = set_int(fd, SO_DEBUG, 1);
    if (r == 0) {
        CHECK_GET(fd, SO_DEBUG, 1);
        CHECK_SET(fd, SO_DEBUG, 0);
        CHECK_GET(fd, SO_DEBUG, 0);
    } else if (r != -EACCES && r != -EPERM) {
        THROW_ERROR("setsockopt(SO_DEBUG) returned %d", r);
    }

    r = set_int(fd, OPT_REUSEPORT, 1);
    if (r == 0) {
        CHECK_GET(fd, OPT_REUSEPORT, 1);
    } else if (r != -EOPNOTSUPP) {
        THROW_ERROR("setsockopt(SO_REUSEPORT) returned %d", r);
    } else {
        // The failed call changes nothing
        CHECK_GET(fd, OPT_REUSEPORT, 0);
    }
    // Turning it off is allowed
    CHECK_SET(fd, OPT_REUSEPORT, 0);
    CHECK_GET(fd, OPT_REUSEPORT, 0);

    // The filter that is locked stays locked
    CHECK_SET(fd, OPT_LOCK_FILTER, 1);
    CHECK_GET(fd, OPT_LOCK_FILTER, 1);
    CHECK_SET_ERR(fd, OPT_LOCK_FILTER, 0, EPERM);
    CHECK_GET(fd, OPT_LOCK_FILTER, 1);
    close(fd);
    return 0;
}

static int test_int_options(void) {
    int fd = new_socket();
    CHECK(fd >= 0, "failed to create a socket");

    // Priorities that every process may set
    for (int prio = 0; prio <= 6; prio++) {
        CHECK_SET(fd, SO_PRIORITY, prio);
        CHECK_GET(fd, SO_PRIORITY, prio);
    }
    CHECK_SET(fd, SO_PRIORITY, 0);
    CHECK_GET(fd, SO_PRIORITY, 0);

    // The mark needs a privilege
    int r = set_int(fd, OPT_MARK, 0x1234);
    if (r == 0) {
        CHECK_GET(fd, OPT_MARK, 0x1234);
    } else if (r != -EPERM) {
        THROW_ERROR("setsockopt(SO_MARK) returned %d", r);
    }

    // The least number of bytes to receive is at least 1. A negative number is the greatest.
    CHECK_SET(fd, SO_RCVLOWAT, 7);
    CHECK_GET(fd, SO_RCVLOWAT, 7);
    CHECK_SET(fd, SO_RCVLOWAT, 0);
    CHECK_GET(fd, SO_RCVLOWAT, 1);
    CHECK_SET(fd, SO_RCVLOWAT, -1);
    CHECK_GET(fd, SO_RCVLOWAT, INT_MAX);
    CHECK_SET(fd, SO_RCVLOWAT, 100000);
    CHECK_GET(fd, SO_RCVLOWAT, 100000);
    CHECK_SET(fd, SO_RCVLOWAT, INT_MAX);
    CHECK_GET(fd, SO_RCVLOWAT, INT_MAX);
    // It cannot be set
    CHECK_SET_ERR(fd, SO_SNDLOWAT, 5, ENOPROTOOPT);
    CHECK_GET(fd, SO_SNDLOWAT, 1);

    CHECK_SET(fd, OPT_BUSY_POLL, 0);
    CHECK_GET(fd, OPT_BUSY_POLL, 0);
    CHECK_SET_ERR(fd, OPT_BUSY_POLL, -1, EINVAL);
    CHECK_GET(fd, OPT_BUSY_POLL, 0);

    CHECK_SET(fd, OPT_INCOMING_CPU, 2);
    CHECK_GET(fd, OPT_INCOMING_CPU, 2);
    CHECK_SET(fd, OPT_INCOMING_CPU, -1);
    CHECK_GET(fd, OPT_INCOMING_CPU, -1);

    CHECK_SET(fd, OPT_PEEK_OFF, 5);
    CHECK_GET(fd, OPT_PEEK_OFF, 5);
    CHECK_SET(fd, OPT_PEEK_OFF, -1);
    CHECK_GET(fd, OPT_PEEK_OFF, -1);
    close(fd);
    return 0;
}

// The four options of timestamps are one setting, of which the old and new options show parts
static int check_timestamps(int fd, int old, int old_ns, int new, int new_ns) {
    CHECK_GET(fd, OPT_TIMESTAMP_OLD, old);
    CHECK_GET(fd, OPT_TIMESTAMPNS_OLD, old_ns);
    int v;
    int r = get_int(fd, SOL_SOCKET, OPT_TIMESTAMP_NEW, &v);
    if (r == 0) {
        CHECK(v == new, "SO_TIMESTAMP_NEW is %d, expected %d", v, new);
        CHECK_GET(fd, OPT_TIMESTAMPNS_NEW, new_ns);
    } else if (r != -ENOPROTOOPT) {
        THROW_ERROR("getsockopt(SO_TIMESTAMP_NEW) returned %d", r);
    }
    return 0;
}

static int test_timestamp_options(void) {
    int fd = new_socket();
    CHECK(fd >= 0, "failed to create a socket");
    CHECK_SET(fd, OPT_TIMESTAMP_OLD, 1);
    CHECK(check_timestamps(fd, 1, 0, 0, 0) == 0, "SO_TIMESTAMP is wrong");
    CHECK_SET(fd, OPT_TIMESTAMPNS_OLD, 1);
    CHECK(check_timestamps(fd, 0, 1, 0, 0) == 0, "SO_TIMESTAMPNS is wrong");
    CHECK_SET(fd, OPT_TIMESTAMPNS_OLD, 0);
    CHECK(check_timestamps(fd, 0, 0, 0, 0) == 0, "turning off SO_TIMESTAMPNS is wrong");
    CHECK_SET(fd, OPT_TIMESTAMP_OLD, 5);
    CHECK(check_timestamps(fd, 1, 0, 0, 0) == 0, "SO_TIMESTAMP with 5 is wrong");
    CHECK_SET(fd, OPT_TIMESTAMP_OLD, 0);
    CHECK(check_timestamps(fd, 0, 0, 0, 0) == 0, "turning off SO_TIMESTAMP is wrong");

    // The options of 64-bit times exist since Linux 5.1
    int r = set_int(fd, OPT_TIMESTAMP_NEW, 1);
    if (r == 0) {
        CHECK(check_timestamps(fd, 0, 0, 1, 0) == 0, "SO_TIMESTAMP_NEW is wrong");
        CHECK_SET(fd, OPT_TIMESTAMPNS_NEW, 1);
        CHECK(check_timestamps(fd, 0, 0, 1, 1) == 0, "SO_TIMESTAMPNS_NEW is wrong");
        CHECK_SET(fd, OPT_TIMESTAMP_OLD, 1);
        CHECK(check_timestamps(fd, 1, 0, 0, 0) == 0, "SO_TIMESTAMP after the new one is wrong");
        CHECK_SET(fd, OPT_TIMESTAMP_OLD, 0);
        CHECK(check_timestamps(fd, 0, 0, 0, 0) == 0, "all timestamps off is wrong");
    } else if (r != -ENOPROTOOPT) {
        THROW_ERROR("setsockopt(SO_TIMESTAMP_NEW) returned %d", r);
    }
    close(fd);
    return 0;
}

static int test_linger(void) {
    int fd = new_socket();
    CHECK(fd >= 0, "failed to create a socket");
    CHECK_LINGER(fd, 0, 0);
    CHECK(set_linger(fd, 1, 5) == 0, "setting SO_LINGER failed");
    CHECK_LINGER(fd, 1, 5);
    // Any number but 0 turns it on
    CHECK(set_linger(fd, 7, 3) == 0, "setting SO_LINGER failed");
    CHECK_LINGER(fd, 1, 3);
    // Like Linux, the time stays if the option is turned off
    CHECK(set_linger(fd, 0, 9) == 0, "turning SO_LINGER off failed");
    CHECK_LINGER(fd, 0, 3);
    CHECK(set_linger(fd, 1, 100000) == 0, "setting SO_LINGER failed");
    CHECK_LINGER(fd, 1, 100000);
    CHECK(set_linger(fd, 1, 0) == 0, "setting SO_LINGER failed");
    CHECK_LINGER(fd, 1, 0);

    // The value is a struct linger: shorter ones fail, longer ones are fine
    struct linger l = { 1, 6 };
    for (socklen_t len = 0; len < sizeof(l); len++) {
        int r = set_opt(fd, SOL_SOCKET, SO_LINGER, &l, len);
        if (r != -EINVAL) {
            THROW_ERROR("SO_LINGER with %u bytes returned %d, expected EINVAL", (unsigned)len, r);
        }
    }
    CHECK_LINGER(fd, 1, 0);
    char longer[16];
    memcpy(longer, &l, sizeof(l));
    memset(longer + sizeof(l), 0xff, sizeof(longer) - sizeof(l));
    CHECK(set_opt(fd, SOL_SOCKET, SO_LINGER, longer, sizeof(longer)) == 0,
          "SO_LINGER with a longer value failed");
    CHECK_LINGER(fd, 1, 6);

    // A short buffer gets the first bytes, a long one only the struct
    memset(longer, 0x55, sizeof(longer));
    socklen_t len = 4;
    CHECK(getsockopt(fd, SOL_SOCKET, SO_LINGER, longer, &len) == 0 && len == 4 &&
          memcmp(longer, &l, 4) == 0 && longer[4] == 0x55, "SO_LINGER in 4 bytes is wrong");
    len = sizeof(longer);
    CHECK(getsockopt(fd, SOL_SOCKET, SO_LINGER, longer, &len) == 0 && len == sizeof(l) &&
          memcmp(longer, &l, sizeof(l)) == 0 && longer[sizeof(l)] == 0x55,
          "SO_LINGER in 16 bytes is wrong");
    close(fd);
    return 0;
}

// The size is doubled, and not less than a minimum
static int test_buffer_sizes(void) {
    int fd = new_socket();
    CHECK(fd >= 0, "failed to create a socket");
    static const struct {
        int set, snd, rcv;
    } cases[] = {
        { 0, MIN_SNDBUF, MIN_RCVBUF },
        { 1, MIN_SNDBUF, MIN_RCVBUF },
        { 2303, MIN_SNDBUF, 4606 },
        { 2304, MIN_SNDBUF, 4608 },
        { 2305, 4610, 4610 },
        { 4096, 8192, 8192 },
        { 10000, 20000, 20000 },
        { 65536, 131072, 131072 },
        { 100000, 200000, 200000 },
    };
    for (size_t i = 0; i < ARRAY_SIZE(cases); i++) {
        CHECK_SET(fd, SO_SNDBUF, cases[i].set);
        CHECK_GET(fd, SO_SNDBUF, cases[i].snd);
        CHECK_SET(fd, SO_RCVBUF, cases[i].set);
        CHECK_GET(fd, SO_RCVBUF, cases[i].rcv);
    }

    // The two sizes are independent
    CHECK_SET(fd, SO_SNDBUF, 65536);
    CHECK_SET(fd, SO_RCVBUF, 20000);
    CHECK_GET(fd, SO_SNDBUF, 131072);
    CHECK_GET(fd, SO_RCVBUF, 40000);
    CHECK_SET(fd, SO_RCVBUF, 4096);
    CHECK_GET(fd, SO_SNDBUF, 131072);
    CHECK_GET(fd, SO_RCVBUF, 8192);

    // Too great a size is not an error, but is limited, and a negative size is a great one
    CHECK_SET(fd, SO_SNDBUF, 1 << 30);
    int big;
    CHECK(get_int(fd, SOL_SOCKET, SO_SNDBUF, &big) == 0, "reading SO_SNDBUF failed");
    CHECK(big >= 2 * 100000 && big <= (1 << 30), "SO_SNDBUF of 1 GiB is %d", big);
    CHECK_SET(fd, SO_SNDBUF, 4096);
    CHECK_SET(fd, SO_SNDBUF, -1);
    CHECK_GET(fd, SO_SNDBUF, big);
    CHECK_SET(fd, SO_RCVBUF, 1 << 30);
    CHECK(get_int(fd, SOL_SOCKET, SO_RCVBUF, &big) == 0, "reading SO_RCVBUF failed");
    CHECK(big >= 2 * 100000 && big <= (1 << 30), "SO_RCVBUF of 1 GiB is %d", big);
    CHECK_SET(fd, SO_RCVBUF, 4096);
    CHECK_SET(fd, SO_RCVBUF, -1);
    CHECK_GET(fd, SO_RCVBUF, big);

    // Shorter values than an int fail
    int v = 4096;
    CHECK(set_opt(fd, SOL_SOCKET, SO_SNDBUF, &v, 3) == -EINVAL, "SO_SNDBUF with 3 bytes");
    close(fd);
    return 0;
}

// Root forces a size without a limit. The others cannot, which is fine.
static int test_forced_buffer_sizes(void) {
    int fd = new_socket();
    CHECK(fd >= 0, "failed to create a socket");
    int r = set_int(fd, OPT_SNDBUFFORCE, 4096);
    if (r == -EPERM) {
        printf("\t\tnote: SO_SNDBUFFORCE needs a privilege, skipped\n");
        close(fd);
        return 0;
    }
    CHECK(r == 0, "setsockopt(SO_SNDBUFFORCE) returned %d", r);
    CHECK_GET(fd, SO_SNDBUF, 8192);
    CHECK_SET(fd, OPT_SNDBUFFORCE, 1);
    CHECK_GET(fd, SO_SNDBUF, MIN_SNDBUF);
    // A size that SO_SNDBUF would limit
    CHECK_SET(fd, OPT_SNDBUFFORCE, 16 * 1024 * 1024);
    CHECK_GET(fd, SO_SNDBUF, 32 * 1024 * 1024);
    CHECK_SET(fd, OPT_RCVBUFFORCE, 4096);
    CHECK_GET(fd, SO_RCVBUF, 8192);
    CHECK_SET(fd, OPT_RCVBUFFORCE, 1);
    CHECK_GET(fd, SO_RCVBUF, MIN_RCVBUF);
    CHECK_SET(fd, OPT_RCVBUFFORCE, 16 * 1024 * 1024);
    CHECK_GET(fd, SO_RCVBUF, 32 * 1024 * 1024);
    CHECK_GET(fd, SO_SNDBUF, 32 * 1024 * 1024);
    // The forcing options cannot be read
    CHECK_GET_ERR(fd, SOL_SOCKET, OPT_SNDBUFFORCE, ENOPROTOOPT);
    CHECK_GET_ERR(fd, SOL_SOCKET, OPT_RCVBUFFORCE, ENOPROTOOPT);
    close(fd);
    return 0;
}

static int test_timeouts(void) {
    int fd = new_socket();
    CHECK(fd >= 0, "failed to create a socket");
    static const int opts[] = { SO_RCVTIMEO, SO_SNDTIMEO };
    static const int others[] = { SO_SNDTIMEO, SO_RCVTIMEO };
    for (size_t i = 0; i < 2; i++) {
        int opt = opts[i], other = others[i];
        CHECK_TIMEOUT(fd, opt, 0, 0);
        CHECK(set_timeout(fd, opt, 1, 500000) == 0, "setting a timeout failed");
        CHECK_TIMEOUT(fd, opt, 1, 500000);
        // The other timeout is not changed
        CHECK_TIMEOUT(fd, other, 0, 0);
        CHECK(set_timeout(fd, opt, 0, 20000) == 0, "setting a timeout failed");
        CHECK_TIMEOUT(fd, opt, 0, 20000);
        CHECK(set_timeout(fd, opt, 2, 140000) == 0, "setting a timeout failed");
        CHECK_TIMEOUT(fd, opt, 2, 140000);
        CHECK(set_timeout(fd, opt, 1L << 30, 0) == 0, "setting a long timeout failed");
        CHECK_TIMEOUT(fd, opt, 1L << 30, 0);
        // Zero is no timeout
        CHECK(set_timeout(fd, opt, 0, 0) == 0, "setting no timeout failed");
        CHECK_TIMEOUT(fd, opt, 0, 0);
        // A negative number of seconds is a timeout that has passed, which reads as no timeout,
        // like a timeout that is too long to be one
        CHECK(set_timeout(fd, opt, 3, 0) == 0, "setting a timeout failed");
        CHECK(set_timeout(fd, opt, -1, 0) == 0, "setting a negative timeout failed");
        CHECK_TIMEOUT(fd, opt, 0, 0);
        CHECK(set_timeout(fd, opt, 3, 0) == 0, "setting a timeout failed");
        CHECK(set_timeout(fd, opt, -5, 999999) == 0, "setting a negative timeout failed");
        CHECK_TIMEOUT(fd, opt, 0, 0);
        CHECK(set_timeout(fd, opt, 3, 0) == 0, "setting a timeout failed");
        CHECK(set_timeout(fd, opt, LONG_MAX, 0) == 0, "setting the longest timeout failed");
        CHECK_TIMEOUT(fd, opt, 0, 0);
        CHECK_TIMEOUT(fd, other, 0, 0);
    }

    // The options for a struct __kernel_sock_timeval, since Linux 5.1, are the same
    int r = set_timeout(fd, OPT_RCVTIMEO_NEW, 4, 40000);
    if (r == 0) {
        CHECK_TIMEOUT(fd, OPT_RCVTIMEO_NEW, 4, 40000);
        CHECK_TIMEOUT(fd, SO_RCVTIMEO, 4, 40000);
        CHECK(set_timeout(fd, OPT_SNDTIMEO_NEW, 5, 60000) == 0, "setting SO_SNDTIMEO_NEW failed");
        CHECK_TIMEOUT(fd, SO_SNDTIMEO, 5, 60000);
        CHECK(set_timeout(fd, SO_SNDTIMEO, 6, 80000) == 0, "setting a timeout failed");
        CHECK_TIMEOUT(fd, OPT_SNDTIMEO_NEW, 6, 80000);
    } else if (r != -ENOPROTOOPT) {
        THROW_ERROR("setsockopt(SO_RCVTIMEO_NEW) returned %d", r);
    }
    close(fd);
    return 0;
}

static int test_timeout_errors(void) {
    int fd = new_socket();
    CHECK(fd >= 0, "failed to create a socket");
    static const int opts[] = { SO_RCVTIMEO, SO_SNDTIMEO };
    for (size_t i = 0; i < 2; i++) {
        int opt = opts[i];
        CHECK(set_timeout(fd, opt, 3, 60000) == 0, "setting a timeout failed");
        // The microseconds must be less than a second, and not negative, whatever the seconds
        CHECK(set_timeout(fd, opt, 0, 1000000) == -EDOM, "1000000 microseconds");
        CHECK(set_timeout(fd, opt, 5, 1000000) == -EDOM, "5 s 1000000 microseconds");
        CHECK(set_timeout(fd, opt, 5, 5000000) == -EDOM, "5 s 5000000 microseconds");
        CHECK(set_timeout(fd, opt, 5, -1) == -EDOM, "5 s -1 microseconds");
        CHECK(set_timeout(fd, opt, -1, -1) == -EDOM, "-1 s -1 microseconds");
        CHECK(set_timeout(fd, opt, 5, LONG_MAX) == -EDOM, "microseconds of LONG_MAX");
        // A failed call changes nothing
        CHECK_TIMEOUT(fd, opt, 3, 60000);

        // A struct timeval is 16 bytes. Shorter values fail, longer ones are fine.
        struct timeval tv[2] = { { 7, 100000 }, { 9, 9 } };
        for (socklen_t len = 0; len < sizeof(tv[0]); len++) {
            int r = set_opt(fd, SOL_SOCKET, opt, tv, len);
            if (r != -EINVAL) {
                THROW_ERROR("timeout with %u bytes returned %d, expected EINVAL", (unsigned)len, r);
            }
        }
        CHECK_TIMEOUT(fd, opt, 3, 60000);
        CHECK(set_opt(fd, SOL_SOCKET, opt, tv, sizeof(tv)) == 0, "a longer timeout failed");
        CHECK_TIMEOUT(fd, opt, 7, 100000);
        CHECK(set_opt(fd, SOL_SOCKET, opt, NULL, sizeof(tv[0])) == -EFAULT, "a NULL timeout");

        // A short buffer gets the first bytes, a long one the struct
        long buf[4];
        memset(buf, 0x55, sizeof(buf));
        socklen_t len = 8;
        CHECK(getsockopt(fd, SOL_SOCKET, opt, buf, &len) == 0 && len == 8 && buf[0] == 7 &&
              buf[1] == 0x5555555555555555L, "timeout in 8 bytes is wrong");
        len = sizeof(buf);
        CHECK(getsockopt(fd, SOL_SOCKET, opt, buf, &len) == 0 && len == 16 && buf[0] == 7 &&
              buf[1] == 100000 && buf[2] == 0x5555555555555555L, "timeout in 32 bytes is wrong");
    }
    close(fd);
    return 0;
}

static int test_invalid_level(void) {
    int fd = new_socket();
    int sv[2];
    CHECK(fd >= 0 &&
          socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0, "failed to create sockets");
    static const struct {
        const char *name;
        int level, opt;
    } options[] = {
        { "TCP_NODELAY", IPPROTO_TCP, TCP_NODELAY },
        { "TCP_KEEPIDLE", IPPROTO_TCP, TCP_KEEPIDLE },
        { "IP_TTL", IPPROTO_IP, IP_TTL },
        { "IP_TOS", IPPROTO_IP, IP_TOS },
        { "IPV6_V6ONLY", IPPROTO_IPV6, IPV6_V6ONLY },
        { "level 9999", 9999, 1 },
        { "level 270", 270, 1 },
        { "level -1", -1, 1 },
    };
    int socks[] = { fd, sv[0] };
    for (size_t s = 0; s < 2; s++) {
        for (size_t i = 0; i < ARRAY_SIZE(options); i++) {
            int v = 1;
            int r = set_opt(socks[s], options[i].level, options[i].opt, &v, sizeof(v));
            if (r != -EOPNOTSUPP) {
                THROW_ERROR("setsockopt(%s) returned %d, expected EOPNOTSUPP", options[i].name, r);
            }
            r = get_int(socks[s], options[i].level, options[i].opt, &v);
            if (r != -EOPNOTSUPP) {
                THROW_ERROR("getsockopt(%s) returned %d, expected EOPNOTSUPP", options[i].name, r);
            }
            // The level is checked for a buffer of no bytes, too
            socklen_t len = 0;
            char buf[4];
            if (getsockopt(socks[s], options[i].level, options[i].opt, buf, &len) == 0 ||
                    errno != EOPNOTSUPP) {
                THROW_ERROR("getsockopt(%s) of 0 bytes did not fail with EOPNOTSUPP",
                            options[i].name);
            }
        }
    }
    close_files(3, fd, sv[0], sv[1]);
    return 0;
}

static int test_invalid_option(void) {
    int fd = new_socket();
    CHECK(fd >= 0, "failed to create a socket");
    static const int unknown[] = { 0, 1000, 9999, INT_MAX, -1 };
    for (size_t i = 0; i < ARRAY_SIZE(unknown); i++) {
        int v = 1;
        int r = set_opt(fd, SOL_SOCKET, unknown[i], &v, sizeof(v));
        if (r != -ENOPROTOOPT) {
            THROW_ERROR("setsockopt(%d) returned %d, expected ENOPROTOOPT", unknown[i], r);
        }
        CHECK_GET_ERR(fd, SOL_SOCKET, unknown[i], ENOPROTOOPT);
        // The option is checked for a buffer of no bytes, too
        socklen_t len = 0;
        char buf[4];
        if (getsockopt(fd, SOL_SOCKET, unknown[i], buf, &len) == 0 || errno != ENOPROTOOPT) {
            THROW_ERROR("getsockopt(%d) of 0 bytes did not fail with ENOPROTOOPT", unknown[i]);
        }
        // A value that is too short for an int is wrong, before the option is looked at
        r = set_opt(fd, SOL_SOCKET, unknown[i], &v, 2);
        if (r != -EINVAL) {
            THROW_ERROR("setsockopt(%d) of 2 bytes returned %d, expected EINVAL", unknown[i], r);
        }
    }
    close(fd);
    return 0;
}

// The options that cannot be set, which keep their values
static int test_read_only_options(void) {
    int fd = new_socket();
    CHECK(fd >= 0, "failed to create a socket");
    CHECK_SET_ERR(fd, SO_TYPE, SOCK_DGRAM, ENOPROTOOPT);
    CHECK_GET(fd, SO_TYPE, SOCK_STREAM);
    CHECK_SET_ERR(fd, SO_ERROR, 1, ENOPROTOOPT);
    CHECK_GET(fd, SO_ERROR, 0);
    CHECK_SET_ERR(fd, SO_ACCEPTCONN, 1, ENOPROTOOPT);
    CHECK_GET(fd, SO_ACCEPTCONN, 0);
    CHECK_SET_ERR(fd, SO_DOMAIN, AF_INET, ENOPROTOOPT);
    CHECK_GET(fd, SO_DOMAIN, AF_UNIX);
    CHECK_SET_ERR(fd, SO_PROTOCOL, IPPROTO_TCP, ENOPROTOOPT);
    CHECK_GET(fd, SO_PROTOCOL, 0);
    CHECK_SET_ERR(fd, SO_PEERCRED, 1, ENOPROTOOPT);
    CHECK_SET_ERR(fd, OPT_PEERSEC, 1, ENOPROTOOPT);
    CHECK_SET_ERR(fd, OPT_PEERGROUPS, 1, ENOPROTOOPT);
    CHECK_SET_ERR(fd, SO_PEERNAME, 1, ENOPROTOOPT);
    // They are read-only for a value that is too short, too, in the order of Linux: the length
    // first
    int v = 1;
    CHECK(set_opt(fd, SOL_SOCKET, SO_TYPE, &v, 2) == -EINVAL, "SO_TYPE with 2 bytes");
    close(fd);
    return 0;
}

static int test_set_optlen(void) {
    int fd = new_socket();
    CHECK(fd >= 0, "failed to create a socket");
    int v = 1;
    for (socklen_t len = 0; len < sizeof(v); len++) {
        int r = set_opt(fd, SOL_SOCKET, SO_KEEPALIVE, &v, len);
        if (r != -EINVAL) {
            THROW_ERROR("SO_KEEPALIVE with %u bytes returned %d, expected EINVAL", (unsigned)len,
                        r);
        }
    }
    CHECK_GET(fd, SO_KEEPALIVE, 0);
    // A longer value is fine: the first four bytes are the int
    char longer[8] = { 1, 0, 0, 0, 0xff, 0xff, 0xff, 0xff };
    CHECK(set_opt(fd, SOL_SOCKET, SO_KEEPALIVE, longer, sizeof(longer)) == 0, "8 bytes");
    CHECK_GET(fd, SO_KEEPALIVE, 1);
    // The pointer
    CHECK(set_opt(fd, SOL_SOCKET, SO_KEEPALIVE, NULL, sizeof(v)) == -EFAULT, "NULL value");
    CHECK(set_opt(fd, SOL_SOCKET, SO_KEEPALIVE, (void *)1, sizeof(v)) == -EFAULT,
          "bad value");
    CHECK_GET(fd, SO_KEEPALIVE, 1);
    close(fd);
    return 0;
}

static int test_get_optlen(void) {
    int fd = new_socket();
    CHECK(fd >= 0, "failed to create a socket");
    CHECK_SET(fd, SO_KEEPALIVE, 1);
    char buf[16];

    // A short buffer gets the first bytes of the value (the value is 1, little endian), and the
    // length that it got
    for (socklen_t len = 1; len < sizeof(int); len++) {
        memset(buf, 0x55, sizeof(buf));
        socklen_t l = len;
        CHECK(getsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, buf, &l) == 0 && l == len &&
              buf[0] == 1 && buf[len] == 0x55, "SO_KEEPALIVE in %u bytes is wrong",
              (unsigned)len);
    }
    // A long buffer gets the int
    memset(buf, 0x55, sizeof(buf));
    socklen_t l = sizeof(buf);
    CHECK(getsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, buf, &l) == 0 && l == sizeof(int) &&
          buf[0] == 1 && buf[sizeof(int)] == 0x55, "SO_KEEPALIVE in 16 bytes is wrong");
    // No bytes: the buffer is not written, and may be NULL
    memset(buf, 0x55, sizeof(buf));
    l = 0;
    CHECK(getsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, buf, &l) == 0 && l == 0 && buf[0] == 0x55,
          "SO_KEEPALIVE in no bytes is wrong");
    l = 0;
    CHECK(getsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, NULL, &l) == 0 && l == 0,
          "SO_KEEPALIVE in no bytes at NULL is wrong");
    l = 0;
    CHECK(getsockopt(fd, SOL_SOCKET, SO_TYPE, NULL, &l) == 0 && l == 0,
          "SO_TYPE in no bytes at NULL is wrong");
    // The pointers
    l = sizeof(int);
    CHECK(getsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, NULL, &l) == -1 && errno == EFAULT,
          "NULL buffer");
    CHECK(getsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, buf, NULL) == -1 && errno == EFAULT,
          "NULL length");
    l = sizeof(int);
    volatile uintptr_t bad_ptr = 1;
    CHECK(getsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, (void *)bad_ptr, &l) == -1 &&
          errno == EFAULT,
          "bad buffer");
    close(fd);
    return 0;
}

// Every option has its own value
static int test_options_are_independent(void) {
    int a, b;
    int sv[2];
    CHECK(socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0, "failed to create sockets");
    a = sv[0];
    b = sv[1];
    CHECK_SET(a, SO_KEEPALIVE, 1);
    CHECK_SET(a, SO_PRIORITY, 3);
    CHECK_SET(a, SO_RCVLOWAT, 9);
    CHECK_SET(a, SO_SNDBUF, 65536);
    CHECK_SET(a, SO_RCVBUF, 20000);
    CHECK_SET(a, OPT_INCOMING_CPU, 4);
    CHECK_SET(a, OPT_PEEK_OFF, 6);
    CHECK_SET(a, OPT_BUSY_POLL, 8);
    CHECK_SET(a, OPT_PASSSEC, 1);
    CHECK(set_linger(a, 1, 11) == 0, "setting SO_LINGER failed");
    CHECK(set_timeout(a, SO_RCVTIMEO, 12, 20000) == 0, "setting SO_RCVTIMEO failed");
    CHECK(set_timeout(a, SO_SNDTIMEO, 13, 40000) == 0, "setting SO_SNDTIMEO failed");
    CHECK_GET(a, SO_KEEPALIVE, 1);
    CHECK_GET(a, SO_REUSEADDR, 0);
    CHECK_GET(a, SO_PASSCRED, 0);
    CHECK_GET(a, OPT_PASSSEC, 1);
    CHECK_GET(a, SO_PRIORITY, 3);
    CHECK_GET(a, SO_RCVLOWAT, 9);
    CHECK_GET(a, SO_SNDBUF, 131072);
    CHECK_GET(a, SO_RCVBUF, 40000);
    CHECK_GET(a, OPT_INCOMING_CPU, 4);
    CHECK_GET(a, OPT_PEEK_OFF, 6);
    CHECK_GET(a, OPT_BUSY_POLL, 8);
    CHECK_LINGER(a, 1, 11);
    CHECK_TIMEOUT(a, SO_RCVTIMEO, 12, 20000);
    CHECK_TIMEOUT(a, SO_SNDTIMEO, 13, 40000);
    // The other end has its own
    if (check_default_options(b) < 0) {
        THROW_ERROR("the other end of a socket pair has other options than the default");
    }
    CHECK_SET(b, SO_REUSEADDR, 1);
    CHECK_GET(a, SO_REUSEADDR, 0);
    CHECK_GET(b, SO_REUSEADDR, 1);
    CHECK_GET(b, SO_KEEPALIVE, 0);
    close_files(2, a, b);
    return 0;
}

// The file descriptors of a dup() share the options, as they share the socket
static int test_options_of_dup(void) {
    int sv[2];
    CHECK(socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0, "failed to create sockets");
    int d = dup(sv[0]);
    int e = fcntl(sv[0], F_DUPFD_CLOEXEC, 100);
    CHECK(d >= 0 && e >= 0, "dup failed");
    CHECK_SET(d, SO_KEEPALIVE, 1);
    CHECK_SET(e, SO_SNDBUF, 65536);
    CHECK(set_timeout(sv[0], SO_RCVTIMEO, 2, 20000) == 0, "setting SO_RCVTIMEO failed");
    CHECK_GET(sv[0], SO_KEEPALIVE, 1);
    CHECK_GET(sv[0], SO_SNDBUF, 131072);
    CHECK_GET(d, SO_SNDBUF, 131072);
    CHECK_GET(e, SO_KEEPALIVE, 1);
    CHECK_TIMEOUT(d, SO_RCVTIMEO, 2, 20000);
    CHECK_TIMEOUT(e, SO_RCVTIMEO, 2, 20000);
    // They stay if a file descriptor is closed
    close(sv[0]);
    CHECK_GET(d, SO_KEEPALIVE, 1);
    CHECK_TIMEOUT(e, SO_RCVTIMEO, 2, 20000);
    // The other end is not shared
    CHECK_GET(sv[1], SO_KEEPALIVE, 0);
    close_files(3, d, e, sv[1]);
    return 0;
}

// The options stay when the socket binds, listens, connects, and when the peer is gone
static int test_options_survive_states(void) {
    struct sockaddr_un addr;
    socklen_t addr_len = make_addr(&addr);
    int listen_fd = new_socket();
    CHECK(listen_fd >= 0, "failed to create a socket");
    CHECK_SET(listen_fd, SO_REUSEADDR, 1);
    CHECK_SET(listen_fd, SO_SNDBUF, 65536);
    CHECK(set_timeout(listen_fd, SO_RCVTIMEO, 3, 20000) == 0, "setting SO_RCVTIMEO failed");
    CHECK(bind(listen_fd, (struct sockaddr *)&addr, addr_len) == 0, "bind failed");
    CHECK_GET(listen_fd, SO_REUSEADDR, 1);
    CHECK(listen(listen_fd, 4) == 0, "listen failed");
    CHECK_GET(listen_fd, SO_REUSEADDR, 1);
    CHECK_GET(listen_fd, SO_SNDBUF, 131072);
    CHECK_TIMEOUT(listen_fd, SO_RCVTIMEO, 3, 20000);
    // It can be set while listening
    CHECK_SET(listen_fd, SO_KEEPALIVE, 1);
    CHECK_GET(listen_fd, SO_KEEPALIVE, 1);
    CHECK_GET(listen_fd, SO_ACCEPTCONN, 1);

    int client_fd = new_socket();
    CHECK(client_fd >= 0, "failed to create a socket");
    CHECK_SET(client_fd, SO_OOBINLINE, 1);
    CHECK_SET(client_fd, SO_PRIORITY, 2);
    CHECK(set_linger(client_fd, 1, 7) == 0, "setting SO_LINGER failed");
    CHECK(connect(client_fd, (struct sockaddr *)&addr, addr_len) == 0, "connect failed");
    int accepted_fd = accept(listen_fd, NULL, NULL);
    CHECK(accepted_fd >= 0, "accept failed");
    CHECK_GET(client_fd, SO_OOBINLINE, 1);
    CHECK_GET(client_fd, SO_PRIORITY, 2);
    CHECK_LINGER(client_fd, 1, 7);
    // It can be set while connected, also on the end that was accepted
    CHECK_SET(client_fd, SO_BROADCAST, 1);
    CHECK_SET(accepted_fd, SO_KEEPALIVE, 1);
    CHECK_GET(client_fd, SO_BROADCAST, 1);
    CHECK_GET(accepted_fd, SO_KEEPALIVE, 1);
    CHECK_GET(accepted_fd, SO_BROADCAST, 0);

    // Data and a shutdown do not change them
    CHECK(write(client_fd, "x", 1) == 1, "write failed");
    char c;
    CHECK(read(accepted_fd, &c, 1) == 1, "read failed");
    CHECK(shutdown(client_fd, SHUT_RDWR) == 0, "shutdown failed");
    CHECK_GET(client_fd, SO_OOBINLINE, 1);
    CHECK_LINGER(client_fd, 1, 7);
    close(client_fd);
    CHECK_GET(accepted_fd, SO_KEEPALIVE, 1);
    CHECK_GET(accepted_fd, SO_ERROR, 0);
    CHECK_SET(accepted_fd, SO_REUSEADDR, 1);
    CHECK_GET(accepted_fd, SO_REUSEADDR, 1);
    close_files(2, accepted_fd, listen_fd);
    return 0;
}

// The socket that a listening socket accepts takes over SO_PASSCRED and SO_PASSSEC, and no other
// option (also not the sizes of the buffers and the timeouts)
static int test_accepted_socket_options(void) {
    struct sockaddr_un addr;
    socklen_t addr_len;
    int listen_fd = new_listener(&addr, &addr_len);
    CHECK(listen_fd >= 0, "failed to listen");
    CHECK_SET(listen_fd, SO_KEEPALIVE, 1);
    CHECK_SET(listen_fd, SO_REUSEADDR, 1);
    CHECK_SET(listen_fd, SO_PASSCRED, 1);
    CHECK_SET(listen_fd, OPT_PASSSEC, 1);
    CHECK_SET(listen_fd, SO_OOBINLINE, 1);
    CHECK_SET(listen_fd, SO_BROADCAST, 1);
    CHECK_SET(listen_fd, SO_PRIORITY, 3);
    CHECK_SET(listen_fd, SO_RCVLOWAT, 7);
    CHECK_SET(listen_fd, SO_SNDBUF, 65536);
    CHECK_SET(listen_fd, SO_RCVBUF, 65536);
    CHECK_SET(listen_fd, OPT_PEEK_OFF, 5);
    CHECK_SET(listen_fd, OPT_INCOMING_CPU, 2);
    CHECK(set_linger(listen_fd, 1, 4) == 0, "setting SO_LINGER failed");
    CHECK(set_timeout(listen_fd, SO_RCVTIMEO, 1, 0) == 0, "setting SO_RCVTIMEO failed");
    CHECK(set_timeout(listen_fd, SO_SNDTIMEO, 1, 0) == 0, "setting SO_SNDTIMEO failed");

    int client_fd, accepted_fd, client2_fd, accepted2_fd;
    CHECK(connect_and_accept(listen_fd, &addr, addr_len, &client_fd, &accepted_fd) == 0,
          "failed to connect");
    CHECK_GET(accepted_fd, SO_PASSCRED, 1);
    CHECK_GET(accepted_fd, OPT_PASSSEC, 1);
    CHECK_GET(accepted_fd, SO_KEEPALIVE, 0);
    CHECK_GET(accepted_fd, SO_REUSEADDR, 0);
    CHECK_GET(accepted_fd, SO_OOBINLINE, 0);
    CHECK_GET(accepted_fd, SO_BROADCAST, 0);
    CHECK_GET(accepted_fd, SO_PRIORITY, 0);
    CHECK_GET(accepted_fd, SO_RCVLOWAT, 1);
    CHECK_GET(accepted_fd, SO_SNDBUF, default_sndbuf());
    CHECK_GET(accepted_fd, SO_RCVBUF, default_rcvbuf());
    CHECK_GET(accepted_fd, OPT_PEEK_OFF, -1);
    CHECK_GET(accepted_fd, OPT_INCOMING_CPU, -1);
    CHECK_LINGER(accepted_fd, 0, 0);
    CHECK_TIMEOUT(accepted_fd, SO_RCVTIMEO, 0, 0);
    CHECK_TIMEOUT(accepted_fd, SO_SNDTIMEO, 0, 0);
    // The socket that connected is not the one that listens
    CHECK_GET(client_fd, SO_PASSCRED, 0);
    CHECK_GET(client_fd, OPT_PASSSEC, 0);
    CHECK_GET(client_fd, SO_KEEPALIVE, 0);
    CHECK_GET(client_fd, SO_SNDBUF, default_sndbuf());

    // The sockets have their own options from then on
    CHECK_SET(accepted_fd, SO_KEEPALIVE, 1);
    CHECK_SET(accepted_fd, SO_PASSCRED, 0);
    CHECK_GET(listen_fd, SO_PASSCRED, 1);
    CHECK_SET(listen_fd, SO_KEEPALIVE, 0);
    CHECK_GET(accepted_fd, SO_KEEPALIVE, 1);
    CHECK(connect_and_accept(listen_fd, &addr, addr_len, &client2_fd, &accepted2_fd) == 0,
          "failed to connect");
    CHECK_GET(accepted2_fd, SO_KEEPALIVE, 0);
    CHECK_GET(accepted2_fd, SO_PASSCRED, 1);
    // Without the options of the listening socket, nothing is taken over
    CHECK_SET(listen_fd, SO_PASSCRED, 0);
    CHECK_SET(listen_fd, OPT_PASSSEC, 0);
    int client3_fd, accepted3_fd;
    CHECK(connect_and_accept(listen_fd, &addr, addr_len, &client3_fd, &accepted3_fd) == 0,
          "failed to connect");
    CHECK_GET(accepted3_fd, SO_PASSCRED, 0);
    CHECK_GET(accepted3_fd, OPT_PASSSEC, 0);
    close_files(7, listen_fd, client_fd, accepted_fd, client2_fd, accepted2_fd, client3_fd,
                accepted3_fd);
    return 0;
}

// The credentials of the peer: of the process that created a socket pair for both ends, of the one
// that connected for the accepted socket, and of the one that listened for the one that connected.
static int check_peercred(int fd, pid_t pid, const char *what) {
    struct ucred cred;
    memset(&cred, 0x7f, sizeof(cred));
    socklen_t len = sizeof(cred);
    if (getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &cred, &len) < 0) {
        THROW_ERROR("getsockopt(SO_PEERCRED) of %s failed", what);
    }
    if (len != sizeof(cred) || cred.pid != pid || cred.uid != getuid() ||
            cred.gid != getgid()) {
        THROW_ERROR("SO_PEERCRED of %s is pid %d, uid %u, gid %u (length %u)", what, cred.pid,
                    cred.uid, cred.gid, (unsigned)len);
    }
    return 0;
}

static int test_peercred_socketpair(void) {
    int sv[2];
    CHECK(socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0, "failed to create sockets");
    CHECK(check_peercred(sv[0], getpid(), "the first end") == 0, "first end");
    CHECK(check_peercred(sv[1], getpid(), "the second end") == 0, "second end");
    // The same for the ones with flags, and the descriptors of a dup
    int sv2[2];
    CHECK(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sv2) == 0,
          "failed to create sockets");
    CHECK(check_peercred(sv2[0], getpid(), "the first end with flags") == 0, "first");
    CHECK(check_peercred(sv2[1], getpid(), "the second end with flags") == 0, "second");
    int d = dup(sv[1]);
    CHECK(check_peercred(d, getpid(), "a dup") == 0, "dup");
    // The credentials stay when the peer is gone
    close(sv[0]);
    CHECK(check_peercred(sv[1], getpid(), "the end of a closed peer") == 0, "closed peer");
    close_files(4, sv[1], sv2[0], sv2[1], d);
    return 0;
}

static int test_peercred_connect(void) {
    struct sockaddr_un addr;
    socklen_t addr_len;
    int listen_fd = new_listener(&addr, &addr_len);
    CHECK(listen_fd >= 0, "failed to listen");
    int client_fd, accepted_fd;
    CHECK(connect_and_accept(listen_fd, &addr, addr_len, &client_fd, &accepted_fd) == 0,
          "failed to connect");
    CHECK(check_peercred(accepted_fd, getpid(), "the accepted socket") == 0, "accepted");
    CHECK(check_peercred(client_fd, getpid(), "the client") == 0, "client");
    close(client_fd);
    close(accepted_fd);

    // A client that sent a request and is gone, whose credentials the server wants to know
    int c, s;
    CHECK(connect_and_accept(listen_fd, &addr, addr_len, &c, &s) == 0, "failed to connect");
    CHECK(write(c, "req", 3) == 3, "write failed");
    close(c);
    char buf[8];
    CHECK(read(s, buf, sizeof(buf)) == 3, "read failed");
    CHECK(check_peercred(s, getpid(), "the accepted socket of a closed client") == 0,
          "closed client");
    close(s);
    close(listen_fd);
    return 0;
}

static int test_peercred_idle(void) {
    // Without a peer, pid 0 and the uid and gid -1, as Linux does
    int fd = new_socket();
    CHECK(fd >= 0, "failed to create a socket");
    struct ucred cred;
    memset(&cred, 0x7f, sizeof(cred));
    socklen_t len = sizeof(cred);
    CHECK(getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &cred, &len) == 0, "SO_PEERCRED failed");
    CHECK(len == sizeof(cred) && cred.pid == 0 && cred.uid == (uid_t) -1 &&
          cred.gid == (gid_t) -1,
          "SO_PEERCRED of an idle socket is pid %d, uid %u, gid %u (length %u)", cred.pid,
          cred.uid, cred.gid, (unsigned)len);
    // A short buffer gets the first bytes
    len = 4;
    memset(&cred, 0x7f, sizeof(cred));
    CHECK(getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &cred, &len) == 0 && len == 4 &&
          cred.pid == 0 &&
          cred.uid == 0x7f7f7f7f, "SO_PEERCRED in 4 bytes is wrong");
    close(fd);
    return 0;
}

static int compare_gids(const void *a, const void *b) {
    gid_t x = *(const gid_t *)a, y = *(const gid_t *)b;
    return x < y ? -1 : x > y;
}

static int test_peergroups(void) {
    int sv[2];
    CHECK(socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0, "failed to create sockets");
    gid_t mine[1024], peers[1024];
    int n = getgroups(ARRAY_SIZE(mine), mine);
    CHECK(n >= 0, "getgroups failed");
    socklen_t len = sizeof(peers);
    CHECK(getsockopt(sv[0], SOL_SOCKET, OPT_PEERGROUPS, peers, &len) == 0,
          "SO_PEERGROUPS failed");
    CHECK(len == n * sizeof(gid_t), "SO_PEERGROUPS has %u bytes, the process has %d groups",
          (unsigned)len, n);
    qsort(mine, n, sizeof(gid_t), compare_gids);
    qsort(peers, n, sizeof(gid_t), compare_gids);
    CHECK(memcmp(mine, peers, n * sizeof(gid_t)) == 0, "the groups of the peer are wrong");

    // A buffer that is too short: ERANGE and the length that is needed
    if (n > 0) {
        len = n * sizeof(gid_t) - 1;
        CHECK(getsockopt(sv[1], SOL_SOCKET, OPT_PEERGROUPS, peers, &len) == -1 &&
              errno == ERANGE &&
              len == n * sizeof(gid_t), "a short buffer for SO_PEERGROUPS");
        len = 0;
        CHECK(getsockopt(sv[1], SOL_SOCKET, OPT_PEERGROUPS, peers, &len) == -1 &&
              errno == ERANGE &&
              len == n * sizeof(gid_t), "no buffer for SO_PEERGROUPS");
    }

    // Without a peer, there are no groups
    int fd = new_socket();
    CHECK(fd >= 0, "failed to create a socket");
    len = sizeof(peers);
    CHECK(getsockopt(fd, SOL_SOCKET, OPT_PEERGROUPS, peers, &len) == -1 && errno == ENODATA,
          "SO_PEERGROUPS of an idle socket");
    close_files(3, sv[0], sv[1], fd);
    return 0;
}

// The security context of the peer and a file descriptor of the peer: Linux has them if it has a
// security module or is recent enough, and fails with ENOPROTOOPT or ENODATA if it has not
static int test_peer_sec_and_pidfd(void) {
    int sv[2];
    CHECK(socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0, "failed to create sockets");
    char sec[256];
    socklen_t len = sizeof(sec);
    if (getsockopt(sv[0], SOL_SOCKET, OPT_PEERSEC, sec, &len) < 0) {
        CHECK(errno == ENOPROTOOPT, "SO_PEERSEC failed with %d", errno);
    }
    int fd = -1;
    len = sizeof(fd);
    if (getsockopt(sv[0], SOL_SOCKET, OPT_PEERPIDFD, &fd, &len) == 0) {
        CHECK(fd >= 0 && len == sizeof(fd), "SO_PEERPIDFD returned %d", fd);
        close(fd);
    } else {
        CHECK(errno == ENOPROTOOPT, "SO_PEERPIDFD failed with %d", errno);
    }
    // Without a peer, there is nothing
    int idle = new_socket();
    CHECK(idle >= 0, "failed to create a socket");
    len = sizeof(fd);
    CHECK(getsockopt(idle, SOL_SOCKET, OPT_PEERPIDFD, &fd, &len) == -1 &&
          (errno == ENODATA || errno == ENOPROTOOPT), "SO_PEERPIDFD of an idle socket: %d", errno);
    // The options cannot be set
    CHECK_SET_ERR(sv[0], OPT_PEERPIDFD, 1, ENOPROTOOPT);
    close_files(3, sv[0], sv[1], idle);
    return 0;
}

// The sockets give back what they store: the heap of the LibOS does not grow in cycles that create
// sockets, set their options and close them. Linux has no such heap, and the test skips itself.
#define LEAK_CYCLES         1000
#define LEAK_WARMUP_CYCLES  10
#define LEAK_LIMIT_KB       4

static int create_set_close(int listen_fd, const struct sockaddr_un *addr,
                            socklen_t addr_len,
                            int count) {
    for (int i = 0; i < count; i++) {
        int sv[2], client_fd, accepted_fd;
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0 ||
                connect_and_accept(listen_fd, addr, addr_len, &client_fd, &accepted_fd) < 0) {
            THROW_ERROR("failed to create sockets");
        }
        int socks[] = { sv[0], sv[1], client_fd, accepted_fd };
        for (size_t k = 0; k < ARRAY_SIZE(socks); k++) {
            int v;
            if (set_int(socks[k], SO_KEEPALIVE, 1) != 0 || set_int(socks[k], SO_SNDBUF, 65536) != 0 ||
                    set_timeout(socks[k], SO_RCVTIMEO, 1, 20000) != 0 ||
                    set_linger(socks[k], 1, 2) != 0 ||
                    get_int(socks[k], SOL_SOCKET, SO_SNDBUF, &v) != 0 || v != 131072) {
                THROW_ERROR("the options of a socket failed");
            }
        }
        close_files(4, sv[0], sv[1], client_fd, accepted_fd);
    }
    return 0;
}

static int test_options_leak_nothing(void) {
    struct sockaddr_un addr;
    socklen_t addr_len;
    int listen_fd = new_listener(&addr, &addr_len);
    CHECK(listen_fd >= 0, "failed to listen");
    if (create_set_close(listen_fd, &addr, addr_len, LEAK_WARMUP_CYCLES) < 0) {
        return -1;
    }
    long before = kernel_heap_in_use();
    if (before < 0) {
        printf("\t\tnote: no kernel heap monitor, skipped\n");
        close(listen_fd);
        return 0;
    }
    if (create_set_close(listen_fd, &addr, addr_len, LEAK_CYCLES) < 0) {
        return -1;
    }
    long after = kernel_heap_in_use();
    if (after - before > LEAK_LIMIT_KB) {
        THROW_ERROR("the kernel heap grew by %ld kB in %d cycles (limit %d kB)", after - before,
                    LEAK_CYCLES, LEAK_LIMIT_KB);
    }
    close(listen_fd);
    return 0;
}

static test_case_t test_cases[] = {
    TEST_CASE(test_default_options),
    TEST_CASE(test_flag_options),
    TEST_CASE(test_flag_options_with_exceptions),
    TEST_CASE(test_int_options),
    TEST_CASE(test_timestamp_options),
    TEST_CASE(test_linger),
    TEST_CASE(test_buffer_sizes),
    TEST_CASE(test_forced_buffer_sizes),
    TEST_CASE(test_timeouts),
    TEST_CASE(test_timeout_errors),
    TEST_CASE(test_invalid_level),
    TEST_CASE(test_invalid_option),
    TEST_CASE(test_read_only_options),
    TEST_CASE(test_set_optlen),
    TEST_CASE(test_get_optlen),
    TEST_CASE(test_options_are_independent),
    TEST_CASE(test_options_of_dup),
    TEST_CASE(test_options_survive_states),
    TEST_CASE(test_accepted_socket_options),
    TEST_CASE(test_peercred_socketpair),
    TEST_CASE(test_peercred_connect),
    TEST_CASE(test_peercred_idle),
    TEST_CASE(test_peergroups),
    TEST_CASE(test_peer_sec_and_pidfd),
    TEST_CASE(test_options_leak_nothing),
};

int main(int argc, const char *argv[]) {
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
    return test_suite_run(cases, num_cases);
}
