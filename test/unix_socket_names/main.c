// Names and addresses of unix stream sockets, and the errors of the calls that take them.
// The expectations are the behavior of Linux (checked on Linux 7.0): the test passes there, too
// (gcc -pthread -I../include main.c), apart from the cases that say that the LibOS may differ.
//
// The cases are about
//   - the names that are bytes: abstract names that are not UTF-8, have NULs or a single byte,
//     and the names that the kernel chooses for a socket that is bound without a name (autobind)
//   - the length of an address, which may or may not count the NUL of a path
//   - connecting to a path that does not exist, is renamed, or has another spelling, and the
//     names that the two ends of a connection show afterwards
//   - the small errors of the address and socket calls
//   - the descriptors that a peer passes, which the receiver finds gone
//
// Usage: unix_socket_names [test case name | --list]
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <unistd.h>

#include "test.h"

#define SUN_PATH_OFFSET offsetof(struct sockaddr_un, sun_path)
#define MAX_PATH_LEN    sizeof(((struct sockaddr_un *)0)->sun_path)
#define WAIT_MS         2000

#define EXPECT_OK(call) do { \
        if ((call) < 0) { \
            THROW_ERROR(#call " failed"); \
        } \
    } while (0)

#define EXPECT_ERR(call, expected_errno) do { \
        errno = 0; \
        long r_ = (long)(call); \
        if (r_ != -1 || errno != (expected_errno)) { \
            THROW_ERROR(#call " returned %ld, instead of failing with " #expected_errno, r_); \
        } \
    } while (0)

// ---------------------------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------------------------

typedef struct {
    struct sockaddr_un sa;
    socklen_t len;
} unix_addr_t;

static int g_seq;
static char *g_files[128];
static int g_nfiles;

static void remove_files(void) {
    for (int i = 0; i < g_nfiles; i++) {
        unlink(g_files[i]);
        free(g_files[i]);
    }
    g_nfiles = 0;
}

static void remember_file(const char *path) {
    if (g_nfiles < (int)(sizeof(g_files) / sizeof(g_files[0]))) {
        g_files[g_nfiles++] = strdup(path);
    }
}

// A path in /tmp of the given number of characters, if it is not zero
static void make_path(char *out, size_t len, const char *tag) {
    char path[256];
    snprintf(path, sizeof(path), "/tmp/unix_socket_names.%d.%d.%s", getpid(), ++g_seq, tag);
    size_t n = strlen(path);
    if (len > 0) {
        while (n < len) {
            path[n++] = 'p';
        }
        path[len] = '\0';
    }
    strcpy(out, path);
    remember_file(path);
}

// The address of the path, with a length that counts the NUL if `with_nul`, or that is
// SUN_LEN(), the length of the family and the path
static unix_addr_t path_addr(const char *path, int with_nul) {
    unix_addr_t a;
    memset(&a, 0, sizeof(a));
    a.sa.sun_family = AF_UNIX;
    memcpy(a.sa.sun_path, path, MIN(strlen(path), MAX_PATH_LEN));
    a.len = SUN_PATH_OFFSET + strlen(path) + (with_nul ? 1 : 0);
    return a;
}

// The abstract address of the bytes, which are preceded by the NUL that makes the name
// abstract. The name starts with the characters of a group, which the process shares with no
// other, and are those of one call of abstract_addr().
static unix_addr_t abstract_addr_in(int group, const char *bytes, size_t n) {
    unix_addr_t a;
    memset(&a, 0, sizeof(a));
    a.sa.sun_family = AF_UNIX;
    int k = snprintf(a.sa.sun_path + 1, 60, "unix_socket_names.%d.%d.", getpid(), group);
    memcpy(a.sa.sun_path + 1 + k, bytes, n);
    a.len = SUN_PATH_OFFSET + 1 + k + n;
    return a;
}

static unix_addr_t abstract_addr(const char *bytes, size_t n) {
    return abstract_addr_in(++g_seq, bytes, n);
}

static int new_socket(void) {
    return socket(AF_UNIX, SOCK_STREAM, 0);
}

static int bind_to(int fd, const unix_addr_t *a) {
    return bind(fd, (const struct sockaddr *)&a->sa, a->len);
}

static int connect_to(int fd, const unix_addr_t *a) {
    return connect(fd, (const struct sockaddr *)&a->sa, a->len);
}

// A socket that listens on the address, or -1
static int listener_at(const unix_addr_t *a) {
    int fd = new_socket();
    if (fd < 0) {
        return -1;
    }
    if (bind_to(fd, a) < 0 || listen(fd, 4) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

// A socket that is connected to the address, or -1
static int client_to(const unix_addr_t *a) {
    int fd = new_socket();
    if (fd < 0) {
        return -1;
    }
    if (connect_to(fd, a) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

// Waits a limited time for the connection, instead of blocking if there is none
static int accept_one(int listen_fd) {
    struct pollfd pfd = { .fd = listen_fd, .events = POLLIN };
    if (poll(&pfd, 1, WAIT_MS) != 1) {
        errno = ETIMEDOUT;
        return -1;
    }
    return accept(listen_fd, NULL, NULL);
}

// Checks the name that getsockname() (or getpeername()) returns: its length and bytes
static int check_name(int fd, int peer, const void *expected, socklen_t expected_len,
                      const char *what) {
    struct sockaddr_storage st;
    socklen_t len = sizeof(st);
    memset(&st, 0x55, sizeof(st));
    int ret = peer ? getpeername(fd, (struct sockaddr *)&st, &len) :
              getsockname(fd, (struct sockaddr *)&st, &len);
    if (ret < 0) {
        THROW_ERROR("%s: failed to get the name", what);
    }
    if (len != expected_len) {
        THROW_ERROR("%s: the length of the name is %u, but %u is expected", what,
                    (unsigned)len, (unsigned)expected_len);
    }
    if (((struct sockaddr_un *)&st)->sun_family != AF_UNIX) {
        THROW_ERROR("%s: the family is not AF_UNIX", what);
    }
    if (len > SUN_PATH_OFFSET &&
            memcmp(((struct sockaddr_un *)&st)->sun_path, expected,
                   len - SUN_PATH_OFFSET) != 0) {
        THROW_ERROR("%s: the bytes of the name differ", what);
    }
    return 0;
}

static int check_addr(int fd, int peer, const unix_addr_t *expected, const char *what) {
    return check_name(fd, peer, expected->sa.sun_path, expected->len, what);
}

static int check_unnamed(int fd, int peer, const char *what) {
    return check_name(fd, peer, NULL, SUN_PATH_OFFSET, what);
}

// Receives the byte that is sent
static int expect_byte(int to, char byte) {
    char buf = 0;
    struct pollfd pfd = { .fd = to, .events = POLLIN };
    if (poll(&pfd, 1, WAIT_MS) != 1 || read(to, &buf, 1) != 1 || buf != byte) {
        THROW_ERROR("the byte did not arrive");
    }
    return 0;
}

// Sends a byte through one socket and receives it from the other
static int check_transfer(int from, int to, char byte) {
    if (write(from, &byte, 1) != 1) {
        THROW_ERROR("failed to write");
    }
    return expect_byte(to, byte);
}

// ---------------------------------------------------------------------------------------------
// Names that are bytes
// ---------------------------------------------------------------------------------------------

// The names that are not UTF-8 are as good as any other: bind, connect, and the names that the
// sockets show, with all their bytes. The names that differ in a byte, which is not a
// character, are different names, even if both are not characters.
static int test_abstract_names_that_are_not_utf8(void) {
    static const struct {
        const char *bytes;
        size_t len;
    } names[] = {
        { "\xff", 1 }, { "\xfe", 1 }, { "\xc0\x80", 2 }, { "\xed\xa0\x80", 3 },
        { "\x80\x81", 2 }, { "a\xffz", 3 }, { "\xf8\x88\x80\x80\x80", 5 },
    };
    const size_t count = sizeof(names) / sizeof(names[0]);
    unix_addr_t addrs[sizeof(names) / sizeof(names[0])];
    int listeners[sizeof(names) / sizeof(names[0])];

    for (size_t i = 0; i < count; i++) {
        addrs[i] = abstract_addr(names[i].bytes, names[i].len);
        listeners[i] = new_socket();
        if (listeners[i] < 0 || bind_to(listeners[i], &addrs[i]) < 0) {
            THROW_ERROR("failed to bind the name %zu", i);
        }
        if (check_addr(listeners[i], 0, &addrs[i], "getsockname of a bound socket") < 0) {
            return -1;
        }
        EXPECT_OK(listen(listeners[i], 4));
        int dup_fd = new_socket();
        EXPECT_ERR(bind_to(dup_fd, &addrs[i]), EADDRINUSE);
        close(dup_fd);
    }

    for (size_t i = 0; i < count; i++) {
        // The connection goes to the listener of this name, and no other
        int client = client_to(&addrs[i]);
        if (client < 0) {
            THROW_ERROR("failed to connect to the name %zu", i);
        }
        if (check_addr(client, 1, &addrs[i], "getpeername of the client") < 0) {
            return -1;
        }
        for (size_t j = 0; j < count; j++) {
            struct pollfd pfd = { .fd = listeners[j], .events = POLLIN };
            int has_connection = poll(&pfd, 1, i == j ? WAIT_MS : 0) == 1;
            if (has_connection != (i == j)) {
                THROW_ERROR("the connection to the name %zu arrived at the listener %zu", i,
                            j);
            }
        }
        int accepted = accept_one(listeners[i]);
        if (accepted < 0) {
            THROW_ERROR("failed to accept the connection to the name %zu", i);
        }
        if (check_addr(accepted, 0, &addrs[i], "getsockname of the accepted socket") < 0 ||
                check_transfer(client, accepted, 'a' + i) < 0) {
            return -1;
        }
        close(accepted);
        close(client);
    }

    // Closing a socket frees its name
    close(listeners[0]);
    int again = new_socket();
    EXPECT_OK(bind_to(again, &addrs[0]));
    close(again);
    for (size_t i = 1; i < count; i++) {
        close(listeners[i]);
    }
    return 0;
}

// An address that is given with a name that is not UTF-8 is not a reason to fail, but to
// refuse, or send, wherever it is passed: sendto() and sendmsg() take it, too. The LibOS
// ignores the address of a connected stream socket, Linux says that it is connected.
static int test_non_utf8_name_in_send(void) {
    unix_addr_t a = abstract_addr("\xff\xfe", 2);
    int l = listener_at(&a);
    int c = client_to(&a);
    if (l < 0 || c < 0) {
        THROW_ERROR("failed to connect");
    }
    int s = accept_one(l);
    if (s < 0) {
        THROW_ERROR("failed to accept");
    }

    errno = 0;
    long ret = sendto(c, "x", 1, MSG_NOSIGNAL, (struct sockaddr *)&a.sa, a.len);
    if (ret == 1) {
        if (expect_byte(s, 'x') < 0) {
            return -1;
        }
    } else if (ret != -1 || errno != EISCONN) {
        THROW_ERROR("sendto() with the name did not send the byte, or fail with EISCONN");
    }
    struct iovec iov = { .iov_base = "y", .iov_len = 1 };
    struct msghdr msg = {
        .msg_name = &a.sa, .msg_namelen = a.len, .msg_iov = &iov, .msg_iovlen = 1,
    };
    errno = 0;
    long ret2 = sendmsg(c, &msg, MSG_NOSIGNAL);
    if (ret2 == 1) {
        if (expect_byte(s, 'y') < 0) {
            return -1;
        }
    } else if (ret2 != -1 || errno != EISCONN) {
        THROW_ERROR("sendmsg() with the name did not send the byte, or fail with EISCONN");
    }
    // The connection is not harmed
    if (check_transfer(c, s, 'z') < 0) {
        return -1;
    }
    close(s);
    close(c);
    close(l);
    return 0;
}

// The NULs in an abstract name count, in the middle and at the end of it, as does its length
static int test_abstract_names_with_nuls(void) {
    static const struct {
        const char *bytes;
        size_t len;
    } tails[] = {
        { "a\0b", 3 }, { "a\0c", 3 }, { "ab", 2 }, { "ab\0", 3 }, { "ab\0\0", 4 }, { "\0", 1 },
    };
    const size_t count = sizeof(tails) / sizeof(tails[0]);
    unix_addr_t names[sizeof(tails) / sizeof(tails[0])];
    int fds[sizeof(tails) / sizeof(tails[0])];
    int group = ++g_seq;

    for (size_t i = 0; i < count; i++) {
        names[i] = abstract_addr_in(group, tails[i].bytes, tails[i].len);
        fds[i] = listener_at(&names[i]);
        if (fds[i] < 0) {
            THROW_ERROR("failed to bind the name %zu", i);
        }
        if (check_addr(fds[i], 0, &names[i], "getsockname") < 0) {
            return -1;
        }
    }
    // Each name reaches its own listener
    for (size_t i = 0; i < count; i++) {
        int c = client_to(&names[i]);
        if (c < 0) {
            THROW_ERROR("failed to connect to the name %zu", i);
        }
        int s = accept_one(fds[i]);
        if (s < 0 || check_transfer(c, s, 'n') < 0) {
            THROW_ERROR("the connection to the name %zu did not reach its listener", i);
        }
        close(s);
        close(c);
    }
    for (size_t i = 0; i < count; i++) {
        close(fds[i]);
    }
    return 0;
}

// A name of a single byte (the NUL) is a name
static int test_abstract_name_of_one_byte(void) {
    unix_addr_t a;
    memset(&a, 0, sizeof(a));
    a.sa.sun_family = AF_UNIX;
    a.len = SUN_PATH_OFFSET + 1;

    int s = new_socket();
    int other = new_socket();
    EXPECT_OK(bind_to(s, &a));
    EXPECT_ERR(bind_to(other, &a), EADDRINUSE);
    if (check_addr(s, 0, &a, "getsockname") < 0) {
        return -1;
    }
    EXPECT_OK(listen(s, 1));
    int c = client_to(&a);
    if (c < 0) {
        THROW_ERROR("failed to connect to the name");
    }
    int accepted = accept_one(s);
    if (accepted < 0 || check_addr(c, 1, &a, "getpeername") < 0) {
        THROW_ERROR("the connection to the name failed");
    }
    // A name with two bytes is another name
    a.len++;
    int third = new_socket();
    EXPECT_OK(bind_to(third, &a));
    close(third);
    close(accepted);
    close(c);
    close(other);
    close(s);
    return 0;
}

// A socket that is bound without a name gets one: the NUL and five hexadecimal digits, and
// no other socket has it
static int check_autobound_name(int fd, struct sockaddr_un *name) {
    socklen_t len = sizeof(*name);
    memset(name, 0, sizeof(*name));
    EXPECT_OK(getsockname(fd, (struct sockaddr *)name, &len));
    if (len != SUN_PATH_OFFSET + 6 || name->sun_path[0] != '\0') {
        THROW_ERROR("the name of the autobound socket has the length %u", (unsigned)len);
    }
    for (int i = 1; i < 6; i++) {
        char c = name->sun_path[i];
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
            THROW_ERROR("the name of the autobound socket has the character 0x%x", c);
        }
    }
    return 0;
}

static int test_autobind(void) {
    struct sockaddr_un family_only;
    memset(&family_only, 0, sizeof(family_only));
    family_only.sun_family = AF_UNIX;

    int a = new_socket();
    int b = new_socket();
    EXPECT_OK(bind(a, (struct sockaddr *)&family_only, sizeof(sa_family_t)));
    EXPECT_OK(bind(b, (struct sockaddr *)&family_only, sizeof(sa_family_t)));
    struct sockaddr_un name_a, name_b;
    if (check_autobound_name(a, &name_a) < 0 || check_autobound_name(b, &name_b) < 0) {
        return -1;
    }
    if (memcmp(name_a.sun_path, name_b.sun_path, 6) == 0) {
        THROW_ERROR("two sockets have the same autobound name");
    }
    // It is bound: another bind() does nothing
    EXPECT_OK(bind(a, (struct sockaddr *)&family_only, sizeof(sa_family_t)));
    struct sockaddr_un name_again;
    if (check_autobound_name(a, &name_again) < 0 ||
            memcmp(name_a.sun_path, name_again.sun_path, 6) != 0) {
        THROW_ERROR("the name changed");
    }

    // The name works like any other
    unix_addr_t addr_a;
    memset(&addr_a, 0, sizeof(addr_a));
    addr_a.sa = name_a;
    addr_a.len = SUN_PATH_OFFSET + 6;
    EXPECT_OK(listen(a, 2));
    int c = client_to(&addr_a);
    if (c < 0) {
        THROW_ERROR("failed to connect to the autobound name");
    }
    int accepted = accept_one(a);
    if (accepted < 0 || check_addr(c, 1, &addr_a, "getpeername of the client") < 0 ||
            check_addr(accepted, 0, &addr_a, "getsockname of the accepted socket") < 0 ||
            check_transfer(c, accepted, 'q') < 0) {
        THROW_ERROR("the connection to the autobound name failed");
    }
    close(accepted);
    close(c);

    // The name is free when the socket is closed
    close(a);
    int again = new_socket();
    EXPECT_OK(bind_to(again, &addr_a));
    close(again);
    close(b);

    // It is not a name to connect to
    int d = new_socket();
    EXPECT_ERR(connect(d, (struct sockaddr *)&family_only, sizeof(sa_family_t)), EINVAL);
    close(d);
    return 0;
}

// ---------------------------------------------------------------------------------------------
// The length of an address
// ---------------------------------------------------------------------------------------------

// Whether the length counts the NUL of a path does not matter: SUN_LEN() and the textbook
// strlen() + sizeof(family) do not, and the kernel adds it, so that the name is one byte longer
// than the address.
static int check_path_lengths(const char *path) {
    unix_addr_t without_nul = path_addr(path, 0);
    unix_addr_t with_nul = path_addr(path, 1);
    int l = new_socket();
    EXPECT_OK(bind_to(l, &without_nul));
    if (check_addr(l, 0, &with_nul, "getsockname after a bind without NUL") < 0) {
        return -1;
    }
    EXPECT_OK(listen(l, 2));
    // Both spellings connect, and show the name with the NUL
    for (int i = 0; i < 2; i++) {
        int c = client_to(i == 0 ? &without_nul : &with_nul);
        if (c < 0) {
            THROW_ERROR("failed to connect to the path %d", i);
        }
        int accepted = accept_one(l);
        if (accepted < 0 || check_addr(c, 1, &with_nul, "getpeername of the client") < 0 ||
                check_addr(accepted, 0, &with_nul, "getsockname of the accepted socket") < 0) {
            THROW_ERROR("the connection to the path failed");
        }
        close(accepted);
        close(c);
    }
    close(l);
    return 0;
}

static int test_path_address_without_nul(void) {
    char path[128];
    make_path(path, 0, "nonul");
    return check_path_lengths(path);
}

static int test_path_address_of_107_characters(void) {
    char path[128];
    make_path(path, 107, "p107");
    return check_path_lengths(path);
}

// A path of 108 characters fills sun_path, which has no room for the NUL: the length of the
// address is that of the structure. The kernel counts the NUL in the name, so that the length
// that getsockname() returns is longer than the structure.
static int test_path_address_of_108_characters(void) {
    char path[128];
    make_path(path, 108, "p108");
    unix_addr_t a;
    memset(&a, 0, sizeof(a));
    a.sa.sun_family = AF_UNIX;
    memcpy(a.sa.sun_path, path, MAX_PATH_LEN);
    a.len = sizeof(a.sa);

    int l = new_socket();
    EXPECT_OK(bind_to(l, &a));
    struct sockaddr_storage st;
    socklen_t len = sizeof(st);
    memset(&st, 0x55, sizeof(st));
    EXPECT_OK(getsockname(l, (struct sockaddr *)&st, &len));
    if (len != sizeof(a.sa) + 1 || memcmp(((struct sockaddr_un *)&st)->sun_path, path,
                                          MAX_PATH_LEN) != 0 ||
            ((char *)&st)[sizeof(a.sa)] != '\0') {
        THROW_ERROR("getsockname() returned the length %u for a path of 108 characters",
                    (unsigned)len);
    }
    EXPECT_OK(listen(l, 1));
    int c = client_to(&a);
    if (c < 0) {
        THROW_ERROR("failed to connect to the path of 108 characters");
    }
    int accepted = accept_one(l);
    if (accepted < 0) {
        THROW_ERROR("failed to accept");
    }
    close(accepted);
    close(c);
    close(l);

    // Any longer is no address
    unix_addr_t too_long = a;
    too_long.len = sizeof(a.sa) + 1;
    int fd = new_socket();
    EXPECT_ERR(bind_to(fd, &too_long), EINVAL);
    EXPECT_ERR(connect_to(fd, &too_long), EINVAL);
    close(fd);
    return 0;
}

// The path ends at its first NUL, whatever follows within the length of the address
static int test_path_ends_at_nul(void) {
    char path[128];
    make_path(path, 0, "ends");
    unix_addr_t a = path_addr(path, 1);
    unix_addr_t padded = a;
    // Whatever the program left in the rest of sun_path
    memset(padded.sa.sun_path + strlen(path) + 1, 'z', MAX_PATH_LEN - strlen(path) - 1);
    padded.len = sizeof(padded.sa);

    int l = new_socket();
    EXPECT_OK(bind_to(l, &padded));
    if (check_addr(l, 0, &a, "getsockname") < 0) {
        return -1;
    }
    EXPECT_OK(listen(l, 1));
    int c = client_to(&padded);
    if (c < 0) {
        THROW_ERROR("failed to connect");
    }
    int accepted = accept_one(l);
    if (accepted < 0 || check_addr(c, 1, &a, "getpeername") < 0) {
        THROW_ERROR("failed to accept");
    }
    close(accepted);
    close(c);
    close(l);
    return 0;
}

// ---------------------------------------------------------------------------------------------
// Connecting to a path
// ---------------------------------------------------------------------------------------------

static int test_connect_to_path_that_does_not_exist(void) {
    char path[128], dir[128], file[128], child[160];
    make_path(path, 0, "missing");
    unix_addr_t a = path_addr(path, 1);
    int fd = new_socket();
    EXPECT_ERR(connect_to(fd, &a), ENOENT);

    // The directory is missing
    make_path(dir, 0, "nodir");
    snprintf(child, sizeof(child), "%s/sock", dir);
    a = path_addr(child, 1);
    EXPECT_ERR(connect_to(fd, &a), ENOENT);

    // The directory is a file
    make_path(file, 0, "file");
    int f = open(file, O_CREAT | O_RDWR, 0644);
    if (f < 0) {
        THROW_ERROR("failed to create a file");
    }
    close(f);
    snprintf(child, sizeof(child), "%s/sock", file);
    a = path_addr(child, 1);
    EXPECT_ERR(connect_to(fd, &a), ENOTDIR);

    // The name that is not that of a socket, or of a socket without a listener, is refused
    a = path_addr(file, 1);
    EXPECT_ERR(connect_to(fd, &a), ECONNREFUSED);
    char stale[128];
    make_path(stale, 0, "stale");
    a = path_addr(stale, 1);
    int l = listener_at(&a);
    close(l);
    EXPECT_ERR(connect_to(fd, &a), ECONNREFUSED);
    // as is a name that is bound, but is not listening
    char bound[128];
    make_path(bound, 0, "bound");
    a = path_addr(bound, 1);
    int b = new_socket();
    EXPECT_OK(bind_to(b, &a));
    EXPECT_ERR(connect_to(fd, &a), ECONNREFUSED);
    close(b);

    // An abstract name has no file: no one listens
    unix_addr_t abstr = abstract_addr("missing", 7);
    EXPECT_ERR(connect_to(fd, &abstr), ECONNREFUSED);
    // The socket can connect after the failures
    unix_addr_t working = abstract_addr("working", 7);
    int w = listener_at(&working);
    EXPECT_OK(connect_to(fd, &working));
    close(w);
    close(fd);
    return 0;
}

// The socket file is the name: the socket is reached with a name that is its file, and with
// nothing else
static int test_connect_after_unlink_and_rename(void) {
    char path[128], renamed[128];
    make_path(path, 0, "old");
    make_path(renamed, 0, "new");
    unix_addr_t a = path_addr(path, 1);
    unix_addr_t b = path_addr(renamed, 1);

    int l = listener_at(&a);
    if (l < 0) {
        THROW_ERROR("failed to listen");
    }
    EXPECT_OK(rename(path, renamed));
    int fd = new_socket();
    EXPECT_ERR(connect_to(fd, &a), ENOENT);
    EXPECT_OK(connect_to(fd, &b));
    int accepted = accept_one(l);
    if (accepted < 0) {
        THROW_ERROR("failed to accept");
    }
    close(accepted);
    close(fd);

    EXPECT_OK(unlink(renamed));
    fd = new_socket();
    EXPECT_ERR(connect_to(fd, &b), ENOENT);
    close(fd);
    close(l);
    return 0;
}

// A relative path is a name for the directory of the process: that changes, and the file is not
// in the new one
static int test_connect_to_relative_path(void) {
    char path[128];
    make_path(path, 0, "rel");
    const char *base = strrchr(path, '/') + 1;
    char cwd[256];
    if (getcwd(cwd, sizeof(cwd)) == NULL) {
        THROW_ERROR("failed to get the working directory");
    }
    EXPECT_OK(chdir("/tmp"));
    unix_addr_t rel = path_addr(base, 1);
    unix_addr_t abs = path_addr(path, 1);

    int l = new_socket();
    EXPECT_OK(bind_to(l, &rel));
    EXPECT_OK(listen(l, 2));
    int c1 = client_to(&rel);
    int c2 = client_to(&abs);
    if (c1 < 0 || c2 < 0) {
        THROW_ERROR("failed to connect with a relative or absolute path");
    }
    // The name is that of the bind, in the way that it was given
    int a1 = accept_one(l);
    int a2 = accept_one(l);
    if (a1 < 0 || a2 < 0 || check_addr(c1, 1, &rel, "getpeername") < 0 ||
            check_addr(c2, 1, &rel, "getpeername with the absolute path") < 0 ||
            check_addr(a1, 0, &rel, "getsockname") < 0) {
        return -1;
    }
    close(a1);
    close(a2);
    close(c1);
    close(c2);

    EXPECT_OK(chdir("/"));
    int fd = new_socket();
    EXPECT_ERR(connect_to(fd, &rel), ENOENT);
    close(fd);
    close(l);
    EXPECT_OK(chdir(cwd));
    return 0;
}

// ---------------------------------------------------------------------------------------------
// The names that the ends of a connection show
// ---------------------------------------------------------------------------------------------

// Both ends show the name that the listener bound, whatever the path that the client used to
// find it: a path through a symbolic link, with a dot, or after the file is renamed. The
// accepted socket has that name as its own.
static int test_names_of_the_connection_through_other_paths(void) {
    char path[128], link[128], renamed[128], dotted[200];
    make_path(path, 0, "listener");
    make_path(link, 0, "link");
    make_path(renamed, 0, "renamed");
    unix_addr_t a = path_addr(path, 1);

    int l = listener_at(&a);
    if (l < 0) {
        THROW_ERROR("failed to listen");
    }
    EXPECT_OK(symlink(path, link));
    snprintf(dotted, sizeof(dotted), "/tmp/./%s", strrchr(path, '/') + 1);
    unix_addr_t spellings[] = { path_addr(link, 1), path_addr(dotted, 1), path_addr(path, 0) };
    for (size_t i = 0; i < sizeof(spellings) / sizeof(spellings[0]); i++) {
        int c = client_to(&spellings[i]);
        if (c < 0) {
            THROW_ERROR("failed to connect with the spelling %zu", i);
        }
        int accepted = accept_one(l);
        if (accepted < 0) {
            THROW_ERROR("failed to accept");
        }
        if (check_addr(c, 1, &a, "getpeername of the client") < 0 ||
                check_addr(accepted, 0, &a, "getsockname of the accepted socket") < 0 ||
                check_unnamed(accepted, 1, "getpeername of the accepted socket") < 0 ||
                check_unnamed(c, 0, "getsockname of the client") < 0) {
            return -1;
        }
        close(accepted);
        close(c);
    }

    // The file is renamed, and the client finds it with the new name
    EXPECT_OK(rename(path, renamed));
    unix_addr_t b = path_addr(renamed, 1);
    int c = client_to(&b);
    int accepted = accept_one(l);
    if (c < 0 || accepted < 0) {
        THROW_ERROR("failed to connect to the renamed file");
    }
    if (check_addr(c, 1, &a, "getpeername of the client after the rename") < 0 ||
            check_addr(accepted, 0, &a, "getsockname of the accepted socket after the rename") <
            0 ||
            check_addr(l, 0, &a, "getsockname of the listener after the rename") < 0) {
        return -1;
    }
    close(accepted);
    close(c);
    close(l);
    return 0;
}

// What a socket knows of its peer stays after the peer is closed
static int test_peer_name_after_the_peer_is_closed(void) {
    unix_addr_t listen_addr = abstract_addr("listener", 8);
    unix_addr_t client_addr = abstract_addr("client", 6);
    int l = listener_at(&listen_addr);
    int c = new_socket();
    EXPECT_OK(bind_to(c, &client_addr));
    EXPECT_OK(connect_to(c, &listen_addr));
    int accepted = accept_one(l);
    if (accepted < 0) {
        THROW_ERROR("failed to accept");
    }
    close(c);
    if (check_addr(accepted, 1, &client_addr, "getpeername after the client is closed") < 0 ||
            check_addr(accepted, 0, &listen_addr, "getsockname after the client is closed") <
            0) {
        return -1;
    }
    close(accepted);

    // A client that has no name, the listener that closes
    int c2 = client_to(&listen_addr);
    int accepted2 = accept_one(l);
    if (c2 < 0 || accepted2 < 0) {
        THROW_ERROR("failed to connect");
    }
    close(accepted2);
    close(l);
    if (check_addr(c2, 1, &listen_addr, "getpeername after the accepted socket is closed") <
            0 || check_unnamed(c2, 0, "getsockname") < 0) {
        return -1;
    }
    close(c2);

    // A socket that binds after it has connected shows its name to the other end
    int l2 = listener_at(&listen_addr);
    int c3 = client_to(&listen_addr);
    int accepted3 = accept_one(l2);
    if (c3 < 0 || accepted3 < 0) {
        THROW_ERROR("failed to connect");
    }
    if (check_unnamed(accepted3, 1, "getpeername before the client binds") < 0) {
        return -1;
    }
    EXPECT_OK(bind_to(c3, &client_addr));
    if (check_addr(accepted3, 1, &client_addr, "getpeername after the client binds") < 0) {
        return -1;
    }
    close(c3);
    if (check_addr(accepted3, 1, &client_addr,
                   "getpeername after the client is closed") < 0) {
        return -1;
    }
    close(accepted3);
    close(l2);
    return 0;
}

// ---------------------------------------------------------------------------------------------
// The small errors
// ---------------------------------------------------------------------------------------------

static int test_bind_twice_and_wrong_family(void) {
    char path[128], other[128];
    make_path(path, 0, "twice");
    make_path(other, 0, "other");
    unix_addr_t a = path_addr(path, 1);
    unix_addr_t o = path_addr(other, 1);
    int s = new_socket();
    EXPECT_OK(bind_to(s, &a));
    // The path is in use, by the socket itself: Linux makes the file before it looks at
    // the socket
    EXPECT_ERR(bind_to(s, &a), EADDRINUSE);
    // Another path that is free: the socket is bound
    EXPECT_ERR(bind_to(s, &o), EINVAL);
    unlink(other);
    int t = new_socket();
    EXPECT_ERR(bind_to(t, &a), EADDRINUSE);
    // A name that is not a path is not in use by a file
    unix_addr_t abstr = abstract_addr("twice", 5);
    int u = new_socket();
    EXPECT_OK(bind_to(u, &abstr));
    EXPECT_ERR(bind_to(u, &abstr), EINVAL);
    close(u);
    close(t);
    close(s);

    // The family of the address is that of the socket
    struct sockaddr_in in;
    memset(&in, 0, sizeof(in));
    in.sin_family = AF_INET;
    in.sin_port = htons(1);
    struct sockaddr_un un = a.sa;
    un.sun_family = AF_UNSPEC;
    int fd = new_socket();
    EXPECT_ERR(bind(fd, (struct sockaddr *)&in, sizeof(in)), EINVAL);
    EXPECT_ERR(bind(fd, (struct sockaddr *)&un, a.len), EINVAL);
    EXPECT_ERR(connect(fd, (struct sockaddr *)&in, sizeof(in)), EINVAL);
    EXPECT_ERR(connect(fd, (struct sockaddr *)&un, a.len), EINVAL);
    close(fd);
    return 0;
}

static int test_address_pointer_and_length(void) {
    unix_addr_t a = abstract_addr("pointer", 7);
    int l = listener_at(&a);
    int fd = new_socket();
    // No address, and no length
    EXPECT_ERR(bind(fd, NULL, sizeof(struct sockaddr_un)), EFAULT);
    EXPECT_ERR(connect(fd, NULL, sizeof(struct sockaddr_un)), EFAULT);
    EXPECT_ERR(bind(fd, NULL, 0), EINVAL);
    EXPECT_ERR(connect(fd, NULL, 0), EINVAL);
    // Lengths that are not an address
    EXPECT_ERR(connect(fd, (struct sockaddr *)&a.sa, 0), EINVAL);
    EXPECT_ERR(connect(fd, (struct sockaddr *)&a.sa, 1), EINVAL);
    EXPECT_ERR(connect(fd, (struct sockaddr *)&a.sa, sizeof(sa_family_t)), EINVAL);
    EXPECT_ERR(connect(fd, (struct sockaddr *)&a.sa, sizeof(struct sockaddr_un) + 1), EINVAL);
    EXPECT_ERR(bind(fd, (struct sockaddr *)&a.sa, 1), EINVAL);
    EXPECT_ERR(bind(fd, (struct sockaddr *)&a.sa, sizeof(struct sockaddr_un) + 1), EINVAL);
    // The socket is not harmed by the failures
    EXPECT_OK(connect_to(fd, &a));
    // The address is validated before the state of the socket is
    EXPECT_ERR(connect(fd, (struct sockaddr *)&a.sa, sizeof(sa_family_t)), EINVAL);
    int accepted = accept_one(l);
    if (accepted < 0) {
        THROW_ERROR("failed to accept");
    }

    // The names are wanted by getsockname() and getpeername(), which have no use for nothing
    struct sockaddr_un name;
    socklen_t len = sizeof(name);
    EXPECT_ERR(getsockname(fd, NULL, NULL), EFAULT);
    EXPECT_ERR(getsockname(fd, (struct sockaddr *)&name, NULL), EFAULT);
    EXPECT_ERR(getpeername(fd, NULL, NULL), EFAULT);
    EXPECT_ERR(getpeername(fd, (struct sockaddr *)&name, NULL), EFAULT);
    EXPECT_OK(getsockname(fd, (struct sockaddr *)&name, &len));
    len = sizeof(name);
    EXPECT_OK(getpeername(fd, (struct sockaddr *)&name, &len));
    close(accepted);

    // accept(), which wants no name, has no use for the length of one
    int c2 = client_to(&a);
    len = sizeof(name);
    int accepted2 = accept(l, NULL, &len);
    if (c2 < 0 || accepted2 < 0) {
        THROW_ERROR("failed to accept without an address and with its length");
    }
    close(accepted2);
    close(c2);
    close(fd);
    close(l);
    return 0;
}

static int test_connect_to_oneself_and_listen_on_accepted(void) {
    // A socket that is bound, not listening, refuses to connect to its own name
    unix_addr_t a = abstract_addr("self", 4);
    int s = new_socket();
    EXPECT_OK(bind_to(s, &a));
    EXPECT_ERR(connect_to(s, &a), ECONNREFUSED);
    close(s);

    // listen() of the socket that accept() returned does nothing
    unix_addr_t b = abstract_addr("accepted", 8);
    int l = listener_at(&b);
    int c = client_to(&b);
    int accepted = accept_one(l);
    if (l < 0 || c < 0 || accepted < 0) {
        THROW_ERROR("failed to connect");
    }
    EXPECT_OK(listen(accepted, 4));
    if (check_transfer(c, accepted, 'u') < 0 || check_transfer(accepted, c, 'v') < 0) {
        return -1;
    }
    int is_listening = -1;
    socklen_t len = sizeof(is_listening);
    EXPECT_OK(getsockopt(accepted, SOL_SOCKET, SO_ACCEPTCONN, &is_listening, &len));
    if (is_listening != 0) {
        THROW_ERROR("the accepted socket is listening");
    }
    // It is the client that listen() refuses
    EXPECT_ERR(listen(c, 4), EINVAL);
    close(accepted);
    close(c);
    close(l);
    return 0;
}

typedef struct {
    int fd;
    atomic_int ready;
    atomic_int go;
} race_t;

static void *close_when_told(void *arg) {
    race_t *race = arg;
    atomic_store(&race->ready, 1);
    while (!atomic_load(&race->go)) {
    }
    close(race->fd);
    return NULL;
}

// A client that connects while the listener is closed gets a connection, or is refused: it
// finds the listener, and the listener is gone when the connection is queued
static int test_connect_while_the_listener_closes(void) {
    for (int i = 0; i < 400; i++) {
        unix_addr_t a = abstract_addr("closing", 7);
        race_t race;
        memset(&race, 0, sizeof(race));
        race.fd = listener_at(&a);
        if (race.fd < 0) {
            THROW_ERROR("failed to listen");
        }
        pthread_t thread;
        if (pthread_create(&thread, NULL, close_when_told, &race) != 0) {
            THROW_ERROR("failed to start the thread that closes the listener");
        }
        while (!atomic_load(&race.ready)) {
        }
        int c = new_socket();
        atomic_store(&race.go, 1);
        int ret = connect_to(c, &a);
        int connect_errno = errno;
        pthread_join(thread, NULL);
        close(c);
        if (ret < 0 && connect_errno != ECONNREFUSED) {
            errno = connect_errno;
            THROW_ERROR("connect() to the listener that closes failed in the round %d", i);
        }
    }
    return 0;
}

static int test_not_a_socket(void) {
    int p[2];
    EXPECT_OK(pipe(p));
    char buf[4];
    EXPECT_ERR(shutdown(p[0], SHUT_RDWR), ENOTSOCK);
    EXPECT_ERR(send(p[1], "x", 1, MSG_NOSIGNAL), ENOTSOCK);
    EXPECT_ERR(sendto(p[1], "x", 1, MSG_NOSIGNAL, NULL, 0), ENOTSOCK);
    EXPECT_ERR(recv(p[0], buf, sizeof(buf), MSG_DONTWAIT), ENOTSOCK);
    EXPECT_ERR(sendmsg(p[1], &(struct msghdr) { 0 }, MSG_NOSIGNAL), ENOTSOCK);
    EXPECT_ERR(recvmsg(p[0], &(struct msghdr) { 0 }, MSG_DONTWAIT), ENOTSOCK);
    EXPECT_ERR(listen(p[0], 1), ENOTSOCK);
    EXPECT_ERR(accept(p[0], NULL, NULL), ENOTSOCK);
    unix_addr_t a = abstract_addr("notasocket", 10);
    EXPECT_ERR(bind_to(p[0], &a), ENOTSOCK);
    EXPECT_ERR(connect_to(p[0], &a), ENOTSOCK);
    close(p[0]);
    close(p[1]);

    // A file is not a socket, either
    int f = open("/dev/null", O_RDWR);
    EXPECT_ERR(shutdown(f, SHUT_RDWR), ENOTSOCK);
    EXPECT_ERR(send(f, "x", 1, MSG_NOSIGNAL), ENOTSOCK);
    close(f);
    return 0;
}

// A socket has no position
static int test_no_position_of_a_socket(void) {
    int sv[2];
    EXPECT_OK(socketpair(AF_UNIX, SOCK_STREAM, 0, sv));
    char buf[4] = { 0 };
    struct iovec iov = { .iov_base = buf, .iov_len = 1 };
    EXPECT_ERR(lseek(sv[0], 0, SEEK_CUR), ESPIPE);
    EXPECT_ERR(lseek(sv[0], 0, SEEK_SET), ESPIPE);
    EXPECT_ERR(pread(sv[0], buf, 1, 0), ESPIPE);
    EXPECT_ERR(pwrite(sv[0], buf, 1, 0), ESPIPE);
    EXPECT_ERR(pread(sv[0], buf, 1, 5), ESPIPE);
    EXPECT_ERR(preadv(sv[0], &iov, 1, 0), ESPIPE);
    EXPECT_ERR(pwritev(sv[0], &iov, 1, 0), ESPIPE);
    // The socket still works: the failures did not take data
    if (check_transfer(sv[0], sv[1], 'p') < 0) {
        return -1;
    }
    close(sv[0]);
    close(sv[1]);

    int fd = new_socket();
    EXPECT_ERR(lseek(fd, 0, SEEK_CUR), ESPIPE);
    EXPECT_ERR(pread(fd, buf, 1, 0), ESPIPE);
    close(fd);
    return 0;
}

// An empty buffer needs no address
static int test_empty_buffer_at_a_null_address(void) {
    int sv[2], p[2];
    EXPECT_OK(socketpair(AF_UNIX, SOCK_STREAM, 0, sv));
    EXPECT_OK(pipe(p));
    if (write(sv[0], NULL, 0) != 0 || write(p[1], NULL, 0) != 0) {
        THROW_ERROR("write() of nothing at the null address did not return 0");
    }
    if (read(sv[1], NULL, 0) != 0 || read(p[0], NULL, 0) != 0) {
        THROW_ERROR("read() of nothing at the null address did not return 0");
    }
    // A buffer that is not empty needs a valid address
    const void *volatile null_buf = NULL;
    EXPECT_ERR(write(sv[0], null_buf, 1), EFAULT);
    if (check_transfer(sv[0], sv[1], 'e') < 0) {
        return -1;
    }
    close(sv[0]);
    close(sv[1]);
    close(p[0]);
    close(p[1]);
    return 0;
}

// A socket shows itself with a type, a mode, an inode and a device, which another socket does
// not share, and with the name of its descriptor in /proc
static int test_fstat_of_a_socket(void) {
    int a = new_socket();
    int b = new_socket();
    int sv[2];
    EXPECT_OK(socketpair(AF_UNIX, SOCK_STREAM, 0, sv));
    int d = dup(a);
    struct stat st_a, st_b, st_d, st_sv0, st_sv1;
    EXPECT_OK(fstat(a, &st_a));
    EXPECT_OK(fstat(b, &st_b));
    EXPECT_OK(fstat(d, &st_d));
    EXPECT_OK(fstat(sv[0], &st_sv0));
    EXPECT_OK(fstat(sv[1], &st_sv1));
    struct stat *all[] = { &st_a, &st_b, &st_sv0, &st_sv1 };
    for (size_t i = 0; i < 4; i++) {
        if (!S_ISSOCK(all[i]->st_mode) || (all[i]->st_mode & 07777) != 0777) {
            THROW_ERROR("the mode of the socket %zu is 0%o", i, (unsigned)all[i]->st_mode);
        }
        if (all[i]->st_ino == 0 || all[i]->st_dev == 0 || all[i]->st_nlink != 1) {
            THROW_ERROR("the inode, device or links of the socket %zu are 0", i);
        }
        for (size_t j = 0; j < i; j++) {
            if (all[i]->st_ino == all[j]->st_ino) {
                THROW_ERROR("the sockets %zu and %zu have the inode %lu", j, i,
                            (unsigned long)all[i]->st_ino);
            }
        }
    }
    if (st_d.st_ino != st_a.st_ino || st_d.st_dev != st_a.st_dev) {
        THROW_ERROR("a duplicate of the socket has another inode");
    }
    // The link of the descriptor in /proc names the socket by its inode
    char link[64], target[64], expected[64];
    snprintf(link, sizeof(link), "/proc/self/fd/%d", a);
    ssize_t len = readlink(link, target, sizeof(target) - 1);
    if (len < 0) {
        THROW_ERROR("failed to read the link %s", link);
    }
    target[len] = '\0';
    snprintf(expected, sizeof(expected), "socket:[%lu]", (unsigned long)st_a.st_ino);
    if (strcmp(target, expected) != 0) {
        THROW_ERROR("the link %s is %s instead of %s", link, target, expected);
    }
    close(d);
    close(sv[0]);
    close(sv[1]);
    close(b);
    close(a);
    return 0;
}

static int test_socket_and_socketpair_types(void) {
    // A type that no protocol has is not supported, one that is not a type is invalid
    EXPECT_ERR(socket(AF_UNIX, 0, 0), ESOCKTNOSUPPORT);
    EXPECT_ERR(socket(AF_UNIX, 77, 0), EINVAL);
    int sv[2];
    // There are no pairs of IP sockets
    EXPECT_ERR(socketpair(AF_INET, SOCK_STREAM, 0, sv), EOPNOTSUPP);
    return 0;
}

// ---------------------------------------------------------------------------------------------
// Descriptors that a peer passes
// ---------------------------------------------------------------------------------------------

// Sends a byte, with the descriptors, which are passed with the byte
static int send_fds(int sock, const int *fds, int count) {
    char data = 'x';
    struct iovec iov = { .iov_base = &data, .iov_len = 1 };
    union {
        char buf[CMSG_SPACE(2 * sizeof(int))];
        struct cmsghdr align;
    } control;
    memset(&control, 0, sizeof(control));
    struct msghdr msg = {
        .msg_iov = &iov, .msg_iovlen = 1, .msg_control = control.buf,
        .msg_controllen = CMSG_SPACE(count * sizeof(int)),
    };
    struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
    cmsg->cmsg_level = SOL_SOCKET;
    cmsg->cmsg_type = SCM_RIGHTS;
    cmsg->cmsg_len = CMSG_LEN(count * sizeof(int));
    memcpy(CMSG_DATA(cmsg), fds, count * sizeof(int));
    return sendmsg(sock, &msg, MSG_NOSIGNAL);
}

static int send_fd(int sock, int fd) {
    return send_fds(sock, &fd, 1);
}

// Receives what send_fds() sends, with the number of descriptors that it did. Returns the number
// of descriptors that arrived. The LibOS cannot pass the descriptors of a process that is gone, or
// of one that has closed them: it drops the control message, which it says with MSG_CTRUNC, and
// none arrives. It must not stop, as it did.
static int receive_fds(int sock, int *fds, int count) {
    char data = 0;
    struct iovec iov = { .iov_base = &data, .iov_len = 1 };
    union {
        char buf[CMSG_SPACE(2 * sizeof(int))];
        struct cmsghdr align;
    } control;
    memset(&control, 0, sizeof(control));
    struct msghdr msg = {
        .msg_iov = &iov, .msg_iovlen = 1, .msg_control = control.buf,
        .msg_controllen = CMSG_SPACE(count * sizeof(int)),
    };
    struct pollfd pfd = { .fd = sock, .events = POLLIN };
    if (poll(&pfd, 1, WAIT_MS) != 1) {
        THROW_ERROR("nothing was sent");
    }
    ssize_t n = recvmsg(sock, &msg, 0);
    if (n != 1 || data != 'x') {
        THROW_ERROR("recvmsg() returned %zd, data 0x%x", n, data);
    }
    struct cmsghdr *cmsg = msg.msg_controllen >= CMSG_LEN(sizeof(int)) ?
                           CMSG_FIRSTHDR(&msg) : NULL;
    if (cmsg == NULL) {
        if (!(msg.msg_flags & MSG_CTRUNC)) {
            THROW_ERROR("the descriptors were neither received, nor reported as truncated");
        }
        return 0;
    }
    if (cmsg->cmsg_level != SOL_SOCKET || cmsg->cmsg_type != SCM_RIGHTS) {
        THROW_ERROR("the control message is not that of the descriptors");
    }
    int received = (cmsg->cmsg_len - CMSG_LEN(0)) / sizeof(int);
    if (received != count) {
        THROW_ERROR("%d descriptors arrived instead of %d", received, count);
    }
    memcpy(fds, CMSG_DATA(cmsg), received * sizeof(int));
    return received;
}

// The descriptor is the read end of the pipe, of which we have the write end
static int check_pipe_fd(int fd, int pipe_write) {
    char buf = 0;
    if (write(pipe_write, "k", 1) != 1 || read(fd, &buf, 1) != 1 || buf != 'k') {
        THROW_ERROR("the descriptor that was received does not work");
    }
    return 0;
}

// Receives the descriptor that send_fd() sends, and checks it if it arrives
static int receive_fd(int sock, int pipe_write) {
    int fd;
    int received = receive_fds(sock, &fd, 1);
    if (received < 0 || (received == 1 && check_pipe_fd(fd, pipe_write) < 0)) {
        return -1;
    }
    if (received == 1) {
        close(fd);
    }
    return 0;
}

static int lowest_free_fd(void) {
    int fd = open("/dev/null", O_RDONLY);
    close(fd);
    return fd;
}

// The sender and the receiver are the same process, the pair has no connect or accept
static int test_descriptor_over_socketpair(void) {
    int sv[2], p[2];
    EXPECT_OK(socketpair(AF_UNIX, SOCK_STREAM, 0, sv));
    EXPECT_OK(pipe(p));
    if (send_fd(sv[0], p[0]) != 1) {
        THROW_ERROR("failed to send the descriptor");
    }
    if (receive_fd(sv[1], p[1]) < 0) {
        return -1;
    }
    // The pair works afterwards
    if (check_transfer(sv[0], sv[1], 'a') < 0 || check_transfer(sv[1], sv[0], 'b') < 0) {
        return -1;
    }
    close(sv[0]);
    close(sv[1]);
    close(p[0]);
    close(p[1]);
    return 0;
}

// The sender closes the descriptor after it has sent it
static int test_descriptor_closed_by_the_sender(void) {
    unix_addr_t a = abstract_addr("closed", 6);
    int l = listener_at(&a);
    int c = client_to(&a);
    int s = accept_one(l);
    int p[2];
    EXPECT_OK(pipe(p));
    if (l < 0 || c < 0 || s < 0) {
        THROW_ERROR("failed to connect");
    }
    if (send_fd(c, p[0]) != 1) {
        THROW_ERROR("failed to send the descriptor");
    }
    close(p[0]);
    if (receive_fd(s, p[1]) < 0) {
        return -1;
    }
    if (check_transfer(c, s, 'a') < 0 || check_transfer(s, c, 'b') < 0) {
        return -1;
    }
    close(p[1]);
    close(s);
    close(c);
    close(l);
    return 0;
}

// A control message with two descriptors, of which the sender closed the second: the receiver does
// not keep the first if it cannot have the second. The sender has the file table of the receiver
// here: the receiver is given the first descriptor in the free place that is the lowest, which must
// not be that of the descriptor that is closed, or the second descriptor is found again.
static int test_descriptors_of_which_one_is_closed(void) {
    unix_addr_t a = abstract_addr("two", 3);
    int l = listener_at(&a);
    int c = client_to(&a);
    int s = accept_one(l);
    int hole = open("/dev/null", O_RDONLY);
    int p[2], q[2];
    EXPECT_OK(pipe(p));
    EXPECT_OK(pipe(q));
    if (l < 0 || c < 0 || s < 0 || hole < 0) {
        THROW_ERROR("failed to connect");
    }
    int sent[2] = { p[0], q[0] };
    if (send_fds(c, sent, 2) != 1) {
        THROW_ERROR("failed to send the descriptors");
    }
    close(q[0]);
    close(hole);

    int lowest = lowest_free_fd();
    int fds[2];
    int received = receive_fds(s, fds, 2);
    if (received < 0) {
        return -1;
    }
    if (received == 2) {
        if (check_pipe_fd(fds[0], p[1]) < 0 || check_pipe_fd(fds[1], q[1]) < 0) {
            return -1;
        }
        close(fds[0]);
        close(fds[1]);
    } else if (lowest_free_fd() != lowest) {
        THROW_ERROR("a descriptor of the dropped control message is open");
    }
    if (check_transfer(c, s, 'a') < 0) {
        return -1;
    }
    close(p[0]);
    close(p[1]);
    close(q[1]);
    close(s);
    close(c);
    close(l);
    return 0;
}

typedef struct {
    unix_addr_t addr;
    int pipe_read;
    int client;
    int ok;
} sender_t;

static void *sender_thread(void *arg) {
    sender_t *sender = arg;
    sender->client = client_to(&sender->addr);
    sender->ok = sender->client >= 0 && send_fd(sender->client, sender->pipe_read) == 1;
    return NULL;
}

// The thread that connected and sent the descriptor has exited
static int test_descriptor_of_a_thread_that_exited(void) {
    unix_addr_t a = abstract_addr("exited", 6);
    int l = listener_at(&a);
    int p[2];
    EXPECT_OK(pipe(p));
    sender_t sender = { .addr = a, .pipe_read = p[0], .client = -1, .ok = 0 };
    pthread_t thread;
    if (l < 0 || pthread_create(&thread, NULL, sender_thread, &sender) != 0) {
        THROW_ERROR("failed to start the sender");
    }
    pthread_join(thread, NULL);
    if (!sender.ok) {
        THROW_ERROR("the sender failed to send the descriptor");
    }
    int s = accept_one(l);
    if (s < 0) {
        THROW_ERROR("failed to accept");
    }
    if (receive_fd(s, p[1]) < 0) {
        return -1;
    }
    if (check_transfer(sender.client, s, 'a') < 0) {
        return -1;
    }
    close(p[0]);
    close(p[1]);
    close(s);
    close(sender.client);
    close(l);
    return 0;
}

// ---------------------------------------------------------------------------------------------

static test_case_t test_cases[] = {
    TEST_CASE(test_abstract_names_that_are_not_utf8),
    TEST_CASE(test_non_utf8_name_in_send),
    TEST_CASE(test_abstract_names_with_nuls),
    TEST_CASE(test_abstract_name_of_one_byte),
    TEST_CASE(test_autobind),
    TEST_CASE(test_path_address_without_nul),
    TEST_CASE(test_path_address_of_107_characters),
    TEST_CASE(test_path_address_of_108_characters),
    TEST_CASE(test_path_ends_at_nul),
    TEST_CASE(test_connect_to_path_that_does_not_exist),
    TEST_CASE(test_connect_after_unlink_and_rename),
    TEST_CASE(test_connect_to_relative_path),
    TEST_CASE(test_names_of_the_connection_through_other_paths),
    TEST_CASE(test_peer_name_after_the_peer_is_closed),
    TEST_CASE(test_bind_twice_and_wrong_family),
    TEST_CASE(test_address_pointer_and_length),
    TEST_CASE(test_connect_to_oneself_and_listen_on_accepted),
    TEST_CASE(test_connect_while_the_listener_closes),
    TEST_CASE(test_not_a_socket),
    TEST_CASE(test_no_position_of_a_socket),
    TEST_CASE(test_empty_buffer_at_a_null_address),
    TEST_CASE(test_fstat_of_a_socket),
    TEST_CASE(test_socket_and_socketpair_types),
    TEST_CASE(test_descriptor_over_socketpair),
    TEST_CASE(test_descriptor_closed_by_the_sender),
    TEST_CASE(test_descriptors_of_which_one_is_closed),
    TEST_CASE(test_descriptor_of_a_thread_that_exited),
};

int main(int argc, const char *argv[]) {
    signal(SIGPIPE, SIG_IGN);
    atexit(remove_files);

    test_case_t selected[sizeof(test_cases) / sizeof(test_cases[0])];
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
