// uds_conf.c - conformance probe for AF_UNIX SOCK_STREAM sockets.
//
// A plain C program that prints one line per check ("ID: result"), so that the
// output of a run on native Linux can be diffed with the output of a run in
// Occlum (musl or glibc). The expected values are never hard-coded: Linux is
// the authority.
//
// usage: uds_conf [-d dir] [-t secs] [-o prefix[,prefix..]] [-s group[,group..]] [-l] [-q]
//   -d dir    directory for the socket files (default /tmp)
//   -t secs   a check group that runs longer than this is interrupted and
//             reported as a hang (default 60)
//   -o p,..   run only the groups whose name starts with one of the prefixes
//   -s g,..   skip these groups (the driver adds the groups that crashed)
//   -l        list the groups
//   -q        do not print the "#> group" markers
//   -e file   compare every result with the expected one (the output of a run on Linux, lines
//             "ID: result"); the exit status is 1 if a result differs and the ID is not listed with -k
//   -k file   IDs (or prefixes, ending in '*') whose result may differ from the expected one: the
//             known differences. A known difference that disappeared is reported, but is no failure
//
// A group that blocks forever is interrupted by a watchdog thread with SIGUSR1
// (the handler longjmps out of the blocking call) and reported as
// "<group>: HANG ...". If even that does not help, the process exits with 99.
#define _GNU_SOURCE
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/epoll.h>
#include <sys/select.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/time.h>
#include <sys/uio.h>
#include <sys/syscall.h>
#include <sys/sendfile.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <poll.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <setjmp.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <time.h>
#include <spawn.h>
#include <limits.h>
#include <stdatomic.h>

#ifndef SO_PEERPIDFD
#define SO_PEERPIDFD 77
#endif
#ifndef SO_PEERGROUPS
#define SO_PEERGROUPS 59
#endif
#ifndef SO_PEERSEC
#define SO_PEERSEC 31
#endif
#ifndef POLLRDHUP
#define POLLRDHUP 0x2000
#endif
#ifndef SIOCOUTQ
#define SIOCOUTQ 0x5411
#endif
#ifndef SIOCINQ
#define SIOCINQ FIONREAD
#endif
#ifndef SIOCATMARK
#define SIOCATMARK 0x8905
#endif
#ifndef SO_PEEK_OFF
#define SO_PEEK_OFF 42
#endif
#ifndef SO_PASSSEC
#define SO_PASSSEC 34
#endif
#ifndef SO_NO_CHECK
#define SO_NO_CHECK 11
#endif
#ifndef SO_BUSY_POLL
#define SO_BUSY_POLL 46
#endif
#ifndef SO_INCOMING_CPU
#define SO_INCOMING_CPU 49
#endif
#ifndef SO_RXQ_OVFL
#define SO_RXQ_OVFL 40
#endif
#ifndef SO_LOCK_FILTER
#define SO_LOCK_FILTER 44
#endif
#ifndef SO_MARK
#define SO_MARK 36
#endif
#ifndef SO_DOMAIN
#define SO_DOMAIN 39
#endif
#ifndef SO_PROTOCOL
#define SO_PROTOCOL 38
#endif
#ifndef SO_REUSEPORT
#define SO_REUSEPORT 15
#endif
#ifndef MSG_CMSG_CLOEXEC
#define MSG_CMSG_CLOEXEC 0x40000000
#endif

#define ARRAY_LEN(a) (sizeof(a) / sizeof((a)[0]))

/* ------------------------------------------------------------------ */
/* output                                                              */
/* ------------------------------------------------------------------ */

static int g_quiet;
static char g_id[160];

static void write_all(const char *buf, size_t len) {
    while (len > 0) {
        ssize_t n = write(1, buf, len);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return;
        }
        buf += n;
        len -= (size_t)n;
    }
}

/* the expected results (-e) and the known differences (-k) */
typedef struct {
    char *id;
    char *val;
} expect_t;
static expect_t *g_exp;
static int g_nexp;
static char **g_known;
static int g_nknown;
static int g_ncompared, g_nmismatch, g_nknown_diff, g_nfixed;

static int is_known(const char *id) {
    for (int i = 0; i < g_nknown; i++) {
        size_t l = strlen(g_known[i]);
        if (l > 0 && g_known[i][l - 1] == '*') {
            if (strncmp(id, g_known[i], l - 1) == 0) {
                return 1;
            }
        } else if (strcmp(id, g_known[i]) == 0) {
            return 1;
        }
    }
    return 0;
}

static int cmp_expect(const void *a, const void *b) {
    return strcmp(((const expect_t *)a)->id, ((const expect_t *)b)->id);
}

static void load_expected(const char *path) {
    FILE *f = fopen(path, "r");
    if (!f) {
        fprintf(stderr, "cannot read the expected results %s\n", path);
        exit(2);
    }
    char line[1100];
    int cap = 0;
    while (fgets(line, sizeof(line), f)) {
        size_t l = strlen(line);
        while (l > 0 && (line[l - 1] == '\n' || line[l - 1] == '\r')) {
            line[--l] = 0;
        }
        char *sep = strstr(line, ": ");
        if (!sep || line[0] == '#' || strncmp(line, "info.", 5) == 0) {
            continue;
        }
        *sep = 0;
        if (g_nexp == cap) {
            cap = cap ? cap * 2 : 2048;
            g_exp = realloc(g_exp, sizeof(expect_t) * (size_t)cap);
        }
        g_exp[g_nexp].id = strdup(line);
        g_exp[g_nexp].val = strdup(sep + 2);
        g_nexp++;
    }
    fclose(f);
    qsort(g_exp, (size_t)g_nexp, sizeof(expect_t), cmp_expect);
}

static void load_known(const char *path) {
    FILE *f = fopen(path, "r");
    if (!f) {
        fprintf(stderr, "cannot read the known differences %s\n", path);
        exit(2);
    }
    char line[300];
    while (fgets(line, sizeof(line), f)) {
        char *hash = strchr(line, '#');
        if (hash) {
            *hash = 0;
        }
        size_t l = strlen(line);
        while (l > 0 && (line[l - 1] == '\n' || line[l - 1] == ' ' || line[l - 1] == '\t')) {
            line[--l] = 0;
        }
        char *s = line;
        while (*s == ' ' || *s == '\t') {
            s++;
        }
        if (*s) {
            g_known = realloc(g_known, sizeof(char *) * (size_t)(g_nknown + 1));
            g_known[g_nknown++] = strdup(s);
        }
    }
    fclose(f);
}

static void compare_with_expected(const char *id, const char *val) {
    if (!g_nexp || strncmp(id, "info.", 5) == 0) {
        return;
    }
    expect_t key = {(char *)id, NULL};
    expect_t *e = bsearch(&key, g_exp, (size_t)g_nexp, sizeof(expect_t), cmp_expect);
    if (!e) {
        return;
    }
    g_ncompared++;
    int same = strcmp(e->val, val) == 0;
    int known = is_known(id);
    if (same && known) {
        g_nfixed++;
        char m[300];
        int n = snprintf(m, sizeof(m),
                         "#! now matches Linux, remove from the known differences: %s\n", id);
        write_all(m, (size_t)n);
    } else if (!same && known) {
        g_nknown_diff++;
    } else if (!same) {
        g_nmismatch++;
        char m[1400];
        int n = snprintf(m, sizeof(m), "#! MISMATCH %s: expected [%s] got [%s]\n", id, e->val,
                         val);
        write_all(m, (size_t)n);
    }
}

static void emit(const char *id, const char *fmt, ...) {
    char line[1024];
    int n = snprintf(line, sizeof(line), "%s: ", id);
    int start = n;
    va_list ap;
    va_start(ap, fmt);
    n += vsnprintf(line + n, sizeof(line) - (size_t)n - 2, fmt, ap);
    va_end(ap);
    if (n > (int)sizeof(line) - 2) {
        n = (int)sizeof(line) - 2;
    }
    line[n++] = '\n';
    write_all(line, (size_t)n);
    line[n - 1] = 0;
    compare_with_expected(id, line + start);
}

/* builds an id in a static buffer; use only for the emit() in the same statement */
#define ID(...) (snprintf(g_id, sizeof(g_id), __VA_ARGS__), g_id)

static const char *enm(int e) {
    static __thread char b[8][16];
    static __thread int i;
    switch (e) {
#define X(n) case n: return #n;
            X(EAGAIN) X(EINTR) X(EPIPE) X(ENOTCONN) X(ECONNREFUSED) X(ECONNRESET) X(EINVAL)
            X(EADDRINUSE) X(ENOENT) X(ENOTDIR) X(EISCONN) X(EBADF) X(ENOTSOCK) X(EOPNOTSUPP)
            X(EAFNOSUPPORT) X(EPROTONOSUPPORT) X(ESOCKTNOSUPPORT) X(ENOPROTOOPT) X(ENOTTY)
            X(EFAULT) X(EMSGSIZE) X(EACCES) X(ENAMETOOLONG) X(ESPIPE) X(EDESTADDRREQ)
            X(EALREADY) X(EINPROGRESS) X(ENOMEM) X(ENOBUFS) X(EMFILE) X(EEXIST) X(EISDIR)
            X(EPERM) X(ENXIO) X(EDOM) X(ERANGE) X(EPROTOTYPE) X(ENODEV) X(ENOSYS)
            X(ETIMEDOUT) X(EADDRNOTAVAIL) X(ENFILE) X(EPROTO) X(ECONNABORTED) X(ESRCH)
            X(ELOOP) X(EROFS) X(EBUSY) X(ENOSPC) X(EIO)
#undef X
        default: {
            char *p = b[i++ & 7];
            snprintf(p, 16, "E%d", e);
            return p;
        }
    }
}

/* a result: n, or -ERRNO */
static const char *rs(long r) {
    static __thread char b[16][32];
    static __thread int i;
    char *p = b[i++ & 15];
    if (r >= 0) {
        snprintf(p, 32, "%ld", r);
    } else if (r == -9999) {
        snprintf(p, 32, "BLOCKED");
    } else {
        snprintf(p, 32, "-%s", enm((int) - r));
    }
    return p;
}
#define SYS(e) ({ long _r = (long)(e); if (_r < 0) { _r = -(long)errno; } _r; })

static const char *evs(int ev) {
    static __thread char b[8][96];
    static __thread int i;
    char *p = b[i++ & 7];
    p[0] = 0;
    static const struct { int bit; const char *name; } t[] = {
        {POLLIN, "IN"}, {POLLPRI, "PRI"}, {POLLOUT, "OUT"}, {POLLERR, "ERR"},
        {POLLHUP, "HUP"}, {POLLNVAL, "NVAL"}, {POLLRDHUP, "RDHUP"},
    };
    for (size_t k = 0; k < ARRAY_LEN(t); k++) {
        if (ev & t[k].bit) {
            if (p[0]) {
                strcat(p, "|");
            }
            strcat(p, t[k].name);
        }
    }
    if (!p[0]) {
        strcpy(p, "0");
    }
    return p;
}

static long long now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000LL + ts.tv_nsec / 1000000;
}

static void msleep(int ms) {
    struct timespec ts = {ms / 1000, (ms % 1000) * 1000000L};
    while (nanosleep(&ts, &ts) < 0 && errno == EINTR) {
    }
}

/* ------------------------------------------------------------------ */
/* names                                                               */
/* ------------------------------------------------------------------ */

static int g_pid;
static const char *g_dir = "/tmp";
static int g_seq;
static char *g_files[4096];
static int g_nfiles;

static const char *newpath(const char *tag) {
    static __thread char b[8][108];
    static __thread int i;
    char *p = b[i++ & 7];
    snprintf(p, 108, "%s/uds_conf.%d.%d.%s", g_dir, g_pid, (1000 + ++g_seq), tag);
    if (g_nfiles < (int)ARRAY_LEN(g_files)) {
        g_files[g_nfiles++] = strdup(p);
    }
    return p;
}

static void cleanup_files(void) {
    for (int i = 0; i < g_nfiles; i++) {
        unlink(g_files[i]);
        free(g_files[i]);
    }
    g_nfiles = 0;
}

typedef struct {
    struct sockaddr_un sa;
    socklen_t len;
} uaddr_t;

static uaddr_t upath(const char *path) {
    uaddr_t a;
    memset(&a, 0, sizeof(a));
    a.sa.sun_family = AF_UNIX;
    strncpy(a.sa.sun_path, path, sizeof(a.sa.sun_path) - 1);
    a.len = (socklen_t)(offsetof(struct sockaddr_un, sun_path) + strlen(a.sa.sun_path) + 1);
    return a;
}

/* a pathname address with a fresh name */
static uaddr_t npath(const char *tag) {
    return upath(newpath(tag));
}

/* an abstract address with a fresh name */
static uaddr_t nabs(const char *tag) {
    uaddr_t a;
    memset(&a, 0, sizeof(a));
    a.sa.sun_family = AF_UNIX;
    int n = snprintf(a.sa.sun_path + 1, sizeof(a.sa.sun_path) - 1, "uds_conf.%d.%d.%s", g_pid,
                     (1000 + ++g_seq), tag);
    a.len = (socklen_t)(offsetof(struct sockaddr_un, sun_path) + 1 + n);
    return a;
}

static int usock(void) {
    return socket(AF_UNIX, SOCK_STREAM, 0);
}

static void closefd(int fd) {
    if (fd >= 0) {
        close(fd);
    }
}

#define BIND(fd, a) bind((fd), (struct sockaddr *)&(a).sa, (a).len)
#define CONNECT(fd, a) connect((fd), (struct sockaddr *)&(a).sa, (a).len)

/* a listening socket bound to the address; -1 on failure */
static int listener_at(uaddr_t *a, int backlog) {
    int fd = usock();
    if (fd < 0) {
        return -1;
    }
    if (BIND(fd, *a) < 0 || listen(fd, backlog) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static int listener(int abstract, int backlog, uaddr_t *out) {
    uaddr_t a = abstract ? nabs("l") : npath("l");
    if (out) {
        *out = a;
    }
    return listener_at(&a, backlog);
}

/* connects a new socket to the address; -1 on failure */
static int client_to(uaddr_t *a) {
    int fd = usock();
    if (fd < 0) {
        return -1;
    }
    if (CONNECT(fd, *a) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

/* a connected pair made with listen/connect/accept: c is the client, s the accepted socket */
static int conn_pair(int abstract, int *c, int *s) {
    uaddr_t a;
    int l = listener(abstract, 8, &a);
    if (l < 0) {
        return -1;
    }
    *c = client_to(&a);
    if (*c < 0) {
        close(l);
        return -1;
    }
    *s = accept(l, NULL, NULL);
    close(l);
    if (*s < 0) {
        close(*c);
        return -1;
    }
    return 0;
}

static int sockpair(int *a, int *b) {
    int sv[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) {
        return -1;
    }
    *a = sv[0];
    *b = sv[1];
    return 0;
}

/* ------------------------------------------------------------------ */
/* non-blocking operations that do not rely on MSG_DONTWAIT            */
/* ------------------------------------------------------------------ */

static int get_nb(int fd) {
    int fl = fcntl(fd, F_GETFL);
    return fl < 0 ? 0 : !!(fl & O_NONBLOCK);
}

static void set_nb(int fd, int on) {
    int fl = fcntl(fd, F_GETFL);
    if (fl < 0) {
        return;
    }
    fcntl(fd, F_SETFL, on ? (fl | O_NONBLOCK) : (fl & ~O_NONBLOCK));
}

static long nb_read(int fd, void *buf, size_t n) {
    int was = get_nb(fd);
    set_nb(fd, 1);
    long r = SYS(read(fd, buf, n));
    set_nb(fd, was);
    return r;
}

static long nb_write(int fd, const void *buf, size_t n) {
    int was = get_nb(fd);
    set_nb(fd, 1);
    long r = SYS(write(fd, buf, n));
    set_nb(fd, was);
    return r;
}

/* a non-blocking read that reports the data as well */
static const char *nb_read_s(int fd, size_t n) {
    static __thread char b[8][80];
    static __thread int i;
    char *p = b[i++ & 7];
    char buf[64];
    if (n > sizeof(buf)) {
        n = sizeof(buf);
    }
    long r = nb_read(fd, buf, n);
    if (r > 0) {
        int k = snprintf(p, 80, "%ld:", r);
        for (long j = 0; j < r && j < 8 && k < 70; j++) {
            k += snprintf(p + k, 80 - k, "%c", (buf[j] >= 32 && buf[j] < 127) ? buf[j] : '.');
        }
    } else {
        snprintf(p, 80, "%s", rs(r));
    }
    return p;
}

static const char *nb_write_s(int fd, const char *s) {
    return rs(nb_write(fd, s, strlen(s)));
}

/* fills the send buffer of fd (non-blocking), returns the byte count */
static long fill_buf(int fd) {
    char chunk[4096];
    memset(chunk, 'f', sizeof(chunk));
    long total = 0;
    int was = get_nb(fd);
    set_nb(fd, 1);
    for (;;) {
        ssize_t n = write(fd, chunk, sizeof(chunk));
        if (n < 0) {
            break;
        }
        total += n;
        if (total > 64 * 1024 * 1024) {
            break;
        }
    }
    set_nb(fd, was);
    return total;
}

static long drain_all(int fd) {
    char chunk[8192];
    long total = 0;
    int was = get_nb(fd);
    set_nb(fd, 1);
    for (;;) {
        ssize_t n = read(fd, chunk, sizeof(chunk));
        if (n <= 0) {
            break;
        }
        total += n;
    }
    set_nb(fd, was);
    return total;
}

static const char *pev(int fd, int want, int tmo) {
    struct pollfd p = {fd, (short)want, 0};
    int r = poll(&p, 1, tmo);
    if (r < 0) {
        return rs(-errno);
    }
    return evs(p.revents);
}
#define PALL (POLLIN | POLLOUT | POLLRDHUP | POLLPRI)
#define PEV(fd) pev((fd), PALL, 0)

static const char *epev(int fd, uint32_t want, int tmo) {
    static __thread char b[8][96];
    static __thread int i;
    char *p = b[i++ & 7];
    int ep = epoll_create1(0);
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.events = want;
    ev.data.fd = fd;
    if (epoll_ctl(ep, EPOLL_CTL_ADD, fd, &ev) < 0) {
        snprintf(p, 96, "ctl:%s", rs(-errno));
        close(ep);
        return p;
    }
    struct epoll_event out[4];
    int n = epoll_wait(ep, out, 4, tmo);
    close(ep);
    if (n < 0) {
        snprintf(p, 96, "%s", rs(-errno));
    } else if (n == 0) {
        snprintf(p, 96, "none");
    } else {
        snprintf(p, 96, "%s", evs((int)out[0].events));
    }
    return p;
}
#define EALL (EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLPRI)

static long get_int_opt(int fd, int level, int opt, int *val, socklen_t *len_out) {
    socklen_t len = sizeof(int);
    *val = -12345;
    long r = SYS(getsockopt(fd, level, opt, val, &len));
    if (len_out) {
        *len_out = len;
    }
    return r;
}

static const char *sockopt_s(int fd, int level, int opt) {
    static __thread char b[8][64];
    static __thread int i;
    char *p = b[i++ & 7];
    int v;
    socklen_t len;
    long r = get_int_opt(fd, level, opt, &v, &len);
    if (r < 0) {
        snprintf(p, 64, "%s", rs(r));
    } else {
        snprintf(p, 64, "%d(len=%u)", v, (unsigned)len);
    }
    return p;
}

/* ------------------------------------------------------------------ */
/* helper threads                                                      */
/* ------------------------------------------------------------------ */

typedef struct {
    int fd;
    int delay_ms;
    int arg;
    long result;
    int err;
    atomic_int done;
    uaddr_t addr;
    char data[64];
} tctx_t;

typedef void *(*tfn_t)(void *);

static pthread_t start_thread(tfn_t fn, tctx_t *c) {
    pthread_t t;
    atomic_store(&c->done, 0);
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setstacksize(&attr, 256 * 1024);
    pthread_create(&t, &attr, fn, c);
    pthread_attr_destroy(&attr);
    return t;
}

/* waits for the helper thread for at most ms; returns 1 if it finished (and joins it) */
static int join_thread(pthread_t t, tctx_t *c, int ms) {
    long long end = now_ms() + ms;
    while (!atomic_load(&c->done)) {
        if (now_ms() > end) {
            pthread_detach(t);
            return 0;
        }
        msleep(5);
    }
    pthread_join(t, NULL);
    return 1;
}

/* closes fd (or shuts it down, how >= 0) after delay_ms */
static void *th_close(void *p) {
    tctx_t *c = p;
    msleep(c->delay_ms);
    if (c->arg >= 0) {
        c->result = SYS(shutdown(c->fd, c->arg));
    } else {
        c->result = SYS(close(c->fd));
    }
    atomic_store(&c->done, 1);
    return NULL;
}

/* writes c->data (arg bytes) to fd after delay_ms */
static void *th_write(void *p) {
    tctx_t *c = p;
    msleep(c->delay_ms);
    c->result = SYS(write(c->fd, c->data, c->arg));
    atomic_store(&c->done, 1);
    return NULL;
}

/* reads and discards everything from fd until EOF, after delay_ms; result = bytes */
static void *th_drain(void *p) {
    tctx_t *c = p;
    msleep(c->delay_ms);
    char buf[8192];
    long total = 0;
    for (;;) {
        ssize_t n = read(c->fd, buf, sizeof(buf));
        if (n <= 0) {
            c->err = n < 0 ? errno : 0;
            break;
        }
        total += n;
        if (c->arg > 0 && total >= c->arg) {
            break;
        }
    }
    c->result = total;
    atomic_store(&c->done, 1);
    return NULL;
}

/* accepts one connection after delay_ms; result = fd or -errno */
static void *th_accept(void *p) {
    tctx_t *c = p;
    msleep(c->delay_ms);
    c->result = SYS(accept(c->fd, NULL, NULL));
    atomic_store(&c->done, 1);
    return NULL;
}

/* connects to c->addr after delay_ms; result = rc of connect */
static void *th_connect(void *p) {
    tctx_t *c = p;
    msleep(c->delay_ms);
    int fd = usock();
    c->result = SYS(connect(fd, (struct sockaddr *)&c->addr.sa, c->addr.len));
    c->fd = fd;
    atomic_store(&c->done, 1);
    return NULL;
}

/* blocking read of up to 64 bytes; result = bytes or -errno */
static void *th_read(void *p) {
    tctx_t *c = p;
    msleep(c->delay_ms);
    char buf[64];
    c->result = SYS(read(c->fd, buf, sizeof(buf)));
    atomic_store(&c->done, 1);
    return NULL;
}

/* ------------------------------------------------------------------ */
/* harness                                                             */
/* ------------------------------------------------------------------ */

typedef void (*check_fn)(void);
typedef struct {
    const char *name;
    check_fn fn;
} check_t;

#define MAX_CHECKS 128
static check_t g_checks[MAX_CHECKS];
static int g_nchecks;

#define CHECK(NAME)                                                                 \
    static void NAME(void);                                                         \
    __attribute__((constructor)) static void reg_##NAME(void) {                     \
        g_checks[g_nchecks].name = #NAME;                                           \
        g_checks[g_nchecks++].fn = NAME;                                            \
    }                                                                               \
    static void NAME(void)

static sigjmp_buf g_jmp;
static volatile int g_in_check;
static volatile long long g_start_ms;
static volatile long long g_sig_ms;
static volatile int g_hang_signalled;
static long g_timeout_ms = 60000;
static pthread_t g_main_thread;
static const char *g_cur = "";
static atomic_int g_sigpipes;

static void on_usr1(int s) {
    (void)s;
    if (g_in_check) {
        siglongjmp(g_jmp, 1);
    }
}

static void on_pipe(int s) {
    (void)s;
    atomic_fetch_add(&g_sigpipes, 1);
}

static void *watchdog(void *a) {
    (void)a;
    for (;;) {
        msleep(100);
        if (!g_in_check) {
            continue;
        }
        long long now = now_ms();
        if (!g_hang_signalled && now - g_start_ms > g_timeout_ms) {
            g_hang_signalled = 1;
            g_sig_ms = now;
            pthread_kill(g_main_thread, SIGUSR1);
        } else if (g_hang_signalled && now - g_sig_ms > 4000) {
            emit(g_cur, "HANG(uninterruptible)");
            _exit(99);
        }
    }
    return NULL;
}

static int in_list(const char *list, const char *name, int prefix) {
    if (!list) {
        return 0;
    }
    char *copy = strdup(list);
    int found = 0;
    for (char *tok = strtok(copy, ","); tok; tok = strtok(NULL, ",")) {
        if (prefix ? strncmp(name, tok, strlen(tok)) == 0 : strcmp(name, tok) == 0) {
            found = 1;
            break;
        }
    }
    free(copy);
    return found;
}

static int cmp_checks(const void *a, const void *b) {
    return strcmp(((const check_t *)a)->name, ((const check_t *)b)->name);
}

/* ------------------------------------------------------------------ */
/* c01: creation, descriptors, non-socket descriptors                  */
/* ------------------------------------------------------------------ */

static const char *ftype(mode_t m) {
    switch (m & S_IFMT) {
        case S_IFSOCK:
            return "SOCK";
        case S_IFREG:
            return "REG";
        case S_IFDIR:
            return "DIR";
        case S_IFIFO:
            return "FIFO";
        case S_IFLNK:
            return "LNK";
        case S_IFCHR:
            return "CHR";
        default:
            return "other";
    }
}

static const char *stat_s(const char *path, int follow) {
    static __thread char b[8][96];
    static __thread int i;
    char *p = b[i++ & 7];
    struct stat st;
    int r = follow ? stat(path, &st) : lstat(path, &st);
    if (r < 0) {
        snprintf(p, 96, "%s", rs(-errno));
    } else {
        snprintf(p, 96, "%s mode=0%o nlink=%d size=%ld", ftype(st.st_mode),
                 (unsigned)(st.st_mode & 07777), (int)st.st_nlink, (long)st.st_size);
    }
    return p;
}

static const char *fstat_s(int fd) {
    static __thread char b[8][96];
    static __thread int i;
    char *p = b[i++ & 7];
    struct stat st;
    if (fstat(fd, &st) < 0) {
        snprintf(p, 96, "%s", rs(-errno));
    } else {
        snprintf(p, 96, "%s mode=0%o nlink=%d size=%ld", ftype(st.st_mode),
                 (unsigned)(st.st_mode & 07777), (int)st.st_nlink, (long)st.st_size);
    }
    return p;
}

CHECK(c01_create) {
    int fd;
    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    emit("create.stream", "%s", rs(fd < 0 ? -errno : 0));
    if (fd >= 0) {
        emit("create.stream.getfl", "nonblock=%d acc=%d", get_nb(fd),
             fcntl(fd, F_GETFL) & O_ACCMODE);
        emit("create.stream.getfd", "cloexec=%d", !!(fcntl(fd, F_GETFD) & FD_CLOEXEC));
        emit("create.stream.fstat", "%s", fstat_s(fd));
        emit("create.stream.lseek", "%s", rs(SYS(lseek(fd, 0, SEEK_CUR))));
        char b[4];
        emit("create.stream.pread", "%s", rs(SYS(pread(fd, b, 1, 0))));
        emit("create.stream.pwrite", "%s", rs(SYS(pwrite(fd, b, 1, 0))));
        emit("create.stream.fionbio", "%s", rs(SYS(ioctl(fd, FIONBIO, &(int) {1}))));
        emit("create.stream.fionbio.state", "nonblock=%d", get_nb(fd));
        emit("create.stream.fioclex", "%s", rs(SYS(ioctl(fd, FIOCLEX, 0))));
        emit("create.stream.fioclex.state", "cloexec=%d", !!(fcntl(fd, F_GETFD) & FD_CLOEXEC));
        emit("create.stream.fionclex", "%s", rs(SYS(ioctl(fd, FIONCLEX, 0))));
        emit("create.stream.fionclex.state", "cloexec=%d", !!(fcntl(fd, F_GETFD) & FD_CLOEXEC));
        emit("create.stream.tcgets", "%s", rs(SYS(ioctl(fd, 0x5401, b))));
        emit("create.stream.isatty", "%d", isatty(fd));
        {
            struct stat st;
            fstat(fd, &st);
            emit("create.stream.fstat.ino_nonzero", "%d dev_nonzero=%d", st.st_ino != 0,
                 st.st_dev != 0);
            char lp[64], link[128];
            snprintf(lp, sizeof(lp), "/proc/self/fd/%d", fd);
            ssize_t ln = readlink(lp, link, sizeof(link) - 1);
            if (ln < 0) {
                emit("create.stream.proc_fd_link", "%s", rs(-errno));
            } else {
                link[ln] = 0;
                emit("create.stream.proc_fd_link", "prefix_socket=%d", strncmp(link, "socket:[", 8) == 0);
            }
        }
        close(fd);
    }
    fd = socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    emit("create.flags", "%s nonblock=%d cloexec=%d", rs(fd < 0 ? -errno : 0), get_nb(fd),
         !!(fcntl(fd, F_GETFD) & FD_CLOEXEC));
    if (fd >= 0) {
        set_nb(fd, 0);
        emit("create.flags.clear_nonblock", "nonblock=%d", get_nb(fd));
        int fl = fcntl(fd, F_GETFL);
        emit("create.flags.setfl_append", "%s", rs(SYS(fcntl(fd, F_SETFL, fl | O_APPEND))));
        emit("create.flags.setfl_async", "%s", rs(SYS(fcntl(fd, F_SETFL, fl | O_ASYNC))));
        emit("create.flags.getfl_after", "nonblock=%d", get_nb(fd));
        close(fd);
    }
    struct {
        const char *name;
        int type;
        int proto;
    } types[] = {
        {"dgram", SOCK_DGRAM, 0},
        {"seqpacket", SOCK_SEQPACKET, 0},
        {"raw", SOCK_RAW, 0},
        {"stream_proto1", SOCK_STREAM, 1},
        {"stream_proto_unix", SOCK_STREAM, 0},
        {"type_0", 0, 0},
        {"type_bad", 77, 0},
    };
    for (size_t i = 0; i < ARRAY_LEN(types); i++) {
        fd = socket(AF_UNIX, types[i].type, types[i].proto);
        emit(ID("create.%s", types[i].name), "%s", rs(fd < 0 ? -errno : 0));
        closefd(fd);
    }
    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    emit("create.domain_local", "%s", rs(fd < 0 ? -errno : 0));
    closefd(fd);

    int sv[2];
    emit("create.socketpair", "%s", rs(SYS(socketpair(AF_UNIX, SOCK_STREAM, 0, sv))));
    emit("create.socketpair.fstat", "%s", fstat_s(sv[0]));
    close(sv[0]);
    close(sv[1]);
    emit("create.socketpair.flags", "%s",
         rs(SYS(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sv))));
    emit("create.socketpair.flags.state", "nonblock=%d,%d cloexec=%d,%d", get_nb(sv[0]),
         get_nb(sv[1]), !!(fcntl(sv[0], F_GETFD) & FD_CLOEXEC),
         !!(fcntl(sv[1], F_GETFD) & FD_CLOEXEC));
    close(sv[0]);
    close(sv[1]);
    emit("create.socketpair.dgram", "%s", rs(SYS(socketpair(AF_UNIX, SOCK_DGRAM, 0, sv))));
    closefd(sv[0]);
    closefd(sv[1]);
    emit("create.socketpair.seqpacket", "%s", rs(SYS(socketpair(AF_UNIX, SOCK_SEQPACKET, 0,
            sv))));
    closefd(sv[0]);
    closefd(sv[1]);
    emit("create.socketpair.inet", "%s", rs(SYS(socketpair(AF_INET, SOCK_STREAM, 0, sv))));
    emit("create.socketpair.proto1", "%s", rs(SYS(socketpair(AF_UNIX, SOCK_STREAM, 1, sv))));
    emit("create.socketpair.efault", "%s", rs(SYS(socketpair(AF_UNIX, SOCK_STREAM, 0,
            NULL))));
}

CHECK(c01_nonsocket) {
    int p[2];
    pipe(p);
    char buf[8];
    struct sockaddr_un sa;
    socklen_t sl = sizeof(sa);
    uaddr_t a = npath("ns");
    emit("nonsock.bind", "%s", rs(SYS(BIND(p[0], a))));
    emit("nonsock.listen", "%s", rs(SYS(listen(p[0], 1))));
    emit("nonsock.accept", "%s", rs(SYS(accept(p[0], NULL, NULL))));
    emit("nonsock.connect", "%s", rs(SYS(CONNECT(p[0], a))));
    emit("nonsock.shutdown", "%s", rs(SYS(shutdown(p[0], SHUT_RDWR))));
    emit("nonsock.getsockname", "%s", rs(SYS(getsockname(p[0], (struct sockaddr *)&sa,
                                         &sl))));
    emit("nonsock.getpeername", "%s", rs(SYS(getpeername(p[0], (struct sockaddr *)&sa,
                                         &sl))));
    emit("nonsock.getsockopt", "%s", sockopt_s(p[0], SOL_SOCKET, SO_TYPE));
    emit("nonsock.setsockopt", "%s",
    rs(SYS(setsockopt(p[0], SOL_SOCKET, SO_KEEPALIVE, &(int) {1}, sizeof(int)))));
    emit("nonsock.send", "%s", rs(SYS(send(p[1], buf, 1, MSG_NOSIGNAL))));
    emit("nonsock.recv", "%s", rs(SYS(recv(p[0], buf, 1, MSG_DONTWAIT))));
    emit("nonsock.sendmsg", "%s", rs(SYS(sendmsg(p[1], &(struct msghdr) {0}, MSG_NOSIGNAL))));
    emit("nonsock.recvmsg", "%s", rs(SYS(recvmsg(p[0], &(struct msghdr) {0}, MSG_DONTWAIT))));
    close(p[0]);
    close(p[1]);
    emit("badfd.bind", "%s", rs(SYS(BIND(999, a))));
    emit("badfd.listen", "%s", rs(SYS(listen(999, 1))));
    emit("badfd.accept", "%s", rs(SYS(accept(999, NULL, NULL))));
    emit("badfd.connect", "%s", rs(SYS(CONNECT(999, a))));
    emit("badfd.shutdown", "%s", rs(SYS(shutdown(999, SHUT_RDWR))));
    emit("badfd.getsockname", "%s", rs(SYS(getsockname(999, (struct sockaddr *)&sa, &sl))));
    emit("badfd.getsockopt", "%s", sockopt_s(999, SOL_SOCKET, SO_TYPE));
    emit("badfd.send", "%s", rs(SYS(send(999, buf, 1, 0))));
    emit("badfd.recv", "%s", rs(SYS(recv(999, buf, 1, 0))));
}

CHECK(c01_dup) {
    int a, b;
    sockpair(&a, &b);
    int d = dup(a);
    set_nb(d, 1);
    emit("dup.nonblock_shared", "orig=%d dup=%d", get_nb(a), get_nb(d));
    set_nb(d, 0);
    emit("dup.write_via_dup", "%s", rs(SYS(write(d, "hi", 2))));
    emit("dup.read_peer", "%s", nb_read_s(b, 8));
    close(a);
    emit("dup.close_orig_dup_alive", "%s", rs(SYS(write(d, "yo", 2))));
    emit("dup.close_orig_peer_ev", "%s", PEV(b));
    emit("dup.close_orig_peer_read", "%s", nb_read_s(b, 8));
    emit("dup.close_orig_peer_read2", "%s", nb_read_s(b, 8));
    shutdown(d, SHUT_WR);
    emit("dup.shutdown_via_dup_peer_read", "%s", nb_read_s(b, 8));
    close(d);
    emit("dup.close_all_peer_read", "%s", nb_read_s(b, 8));
    close(b);

    sockpair(&a, &b);
    int c = fcntl(a, F_DUPFD_CLOEXEC, 100);
    emit("dup.dupfd_cloexec", "ge100=%d cloexec=%d", c >= 100, !!(fcntl(c,
            F_GETFD) & FD_CLOEXEC));
    int e = dup3(a, 150, O_CLOEXEC);
    emit("dup.dup3", "%d cloexec=%d", e, !!(fcntl(e, F_GETFD) & FD_CLOEXEC));
    shutdown(a, SHUT_RDWR);
    emit("dup.shutdown_visible_via_dup", "write=%s", nb_write_s(c, "x"));
    close(c);
    close(e);
    close(a);
    close(b);
}

/* ------------------------------------------------------------------ */
/* c02: bind() address handling                                        */
/* ------------------------------------------------------------------ */

/* binds a new socket to the raw address; returns the result (the socket is closed) */
static long bind_raw(const void *addr, socklen_t len) {
    int fd = usock();
    long r = SYS(bind(fd, (const struct sockaddr *)addr, len));
    close(fd);
    return r;
}

/* a path of exactly n characters under g_dir (the tail is padded) */
static void long_path(char *out, size_t n, const char *tag) {
    int k = snprintf(out, n + 1, "%s/uds_conf.%d.%d.%s.", g_dir, g_pid, (1000 + ++g_seq),
                     tag);
    while ((size_t)k < n) {
        out[k++] = 'p';
    }
    out[n] = 0;
    if (g_nfiles < (int)ARRAY_LEN(g_files)) {
        g_files[g_nfiles++] = strdup(out);
    }
}

CHECK(c02_bind_addr) {
    uaddr_t a;
    struct sockaddr_storage st;

    a = npath("full");
    emit("bind.path.len_sockaddr_un", "%s", rs(bind_raw(&a.sa, sizeof(a.sa))));
    a = npath("exact");
    emit("bind.path.len_exact_nul", "%s", rs(bind_raw(&a.sa, a.len)));
    a = npath("sunlen");
    emit("bind.path.len_sun_len_no_nul", "%s", rs(bind_raw(&a.sa, a.len - 1)));
    a = npath("sunlen2");
    emit("bind.path.len_one_past_nul", "%s", rs(bind_raw(&a.sa, a.len + 1)));
    {
        a = npath("sunlen3");
        emit("bind.path.len_sun_len_no_nul.getsockname", "%s",
        ({
            int fd = usock();
            static __thread char b[8][96];
            static __thread int i;
            char *p = b[i++ & 7];
            long r = SYS(bind(fd, (struct sockaddr *)&a.sa, a.len - 1));
            struct sockaddr_un o;
            socklen_t ol = sizeof(o);
            memset(&o, 0, sizeof(o));
            if (r == 0) {
                getsockname(fd, (struct sockaddr *)&o, &ol);
                snprintf(p, 96, "bind=0 len=%u(exact+1=%d) same=%d", (unsigned)ol,
                         ol == a.len, strcmp(o.sun_path, a.sa.sun_path) == 0);
            } else {
                snprintf(p, 96, "bind=%s", rs(r));
            }
            close(fd);
            p;
        }));
    }
    {
        char p107[200], p108[200];
        long_path(p107, 107, "p107");
        a = upath(p107);
        emit("bind.path.107_chars.len_full", "%s", rs(bind_raw(&a.sa, sizeof(a.sa))));
        long_path(p107, 107, "p107b");
        a = upath(p107);
        emit("bind.path.107_chars.len_sun_len", "%s", rs(bind_raw(&a.sa, a.len - 1)));
        long_path(p108, 108, "p108");
        memset(&a, 0, sizeof(a));
        a.sa.sun_family = AF_UNIX;
        memcpy(a.sa.sun_path, p108, 108);
        a.len = sizeof(a.sa);
        emit("bind.path.108_chars_no_nul", "%s", rs(bind_raw(&a.sa, a.len)));
        {
            int fd = usock();
            long long_path_r;
            long_path(p108, 108, "p108g");
            memcpy(a.sa.sun_path, p108, 108);
            long_path_r = SYS(bind(fd, (struct sockaddr *)&a.sa, a.len));
            struct sockaddr_un o;
            socklen_t ol = sizeof(o);
            memset(&o, 0x7f, sizeof(o));
            int gr = long_path_r == 0 ? getsockname(fd, (struct sockaddr *)&o, &ol) : -1;
            emit("bind.path.108_chars.getsockname", "bind=%s gsn=%d len=%u", rs(long_path_r), gr,
                 gr == 0 ? (unsigned)ol : 0);
            close(fd);
        }
    }
    memset(&st, 0, sizeof(st));
    ((struct sockaddr_un *)&st)->sun_family = AF_UNIX;
    memset(((struct sockaddr_un *)&st)->sun_path, 'q', 120);
    emit("bind.len_gt_sockaddr_un", "%s", rs(bind_raw(&st, sizeof(struct sockaddr_un) + 1)));
    emit("bind.len_gt_sockaddr_un_64", "%s", rs(bind_raw(&st,
            sizeof(struct sockaddr_un) + 64)));
    a = npath("len0");
    emit("bind.len_0", "%s", rs(bind_raw(&a.sa, 0)));
    emit("bind.len_1", "%s", rs(bind_raw(&a.sa, 1)));
    emit("bind.len_2.autobind", "%s", rs(bind_raw(&a.sa, 2)));
    emit("bind.len_3.path_first_byte", "%s", rs(bind_raw(&a.sa, 3)));
    emit("bind.null_addr", "%s", rs(bind_raw(NULL, 0)));
    emit("bind.null_addr_len", "%s", rs(bind_raw(NULL, sizeof(struct sockaddr_un))));
    {
        struct sockaddr_in in;
        memset(&in, 0, sizeof(in));
        in.sin_family = AF_INET;
        in.sin_port = htons(1);
        emit("bind.family_inet", "%s", rs(bind_raw(&in, sizeof(in))));
    }
    a = npath("unspec");
    a.sa.sun_family = AF_UNSPEC;
    emit("bind.family_unspec", "%s", rs(bind_raw(&a.sa, a.len)));
    a = npath("fam");
    a.sa.sun_family = AF_LOCAL;
    emit("bind.family_local", "%s", rs(bind_raw(&a.sa, a.len)));

    /* abstract names */
    uaddr_t ab;
    memset(&ab, 0, sizeof(ab));
    ab.sa.sun_family = AF_UNIX;
    ab.sa.sun_path[0] = 0;
    ab.len = offsetof(struct sockaddr_un, sun_path) + 1;
    {
        int f1 = usock();
        int f2 = usock();
        emit("bind.abstract.single_nul", "%s", rs(SYS(BIND(f1, ab))));
        emit("bind.abstract.single_nul.again", "%s", rs(SYS(BIND(f2, ab))));
        close(f1);
        close(f2);
    }

    struct {
        const char *id;
        const char *bytes;
        int nbytes;
    } names[] = {
#define NM(id, s) {id, s, (int)sizeof(s) - 1}
        NM("a_b", "\0uds.a\0b"),
        NM("a_c", "\0uds.a\0c"),
        NM("ab", "\0uds.ab"),
        NM("abc", "\0uds.abc"),
        NM("ab_nul", "\0uds.ab\0"),
        NM("ab_nul_nul", "\0uds.ab\0\0"),
    };
    char uniq[16];
    snprintf(uniq, sizeof(uniq), "%d.", g_pid);
    int fds[ARRAY_LEN(names)];
    for (size_t i = 0; i < ARRAY_LEN(names); i++) {
        uaddr_t x;
        memset(&x, 0, sizeof(x));
        x.sa.sun_family = AF_UNIX;
        /* name = "\0" + uniq + bytes[1..] */
        int k = 1;
        memcpy(x.sa.sun_path + k, uniq, strlen(uniq));
        k += (int)strlen(uniq);
        memcpy(x.sa.sun_path + k, names[i].bytes + 1, names[i].nbytes - 1);
        k += names[i].nbytes - 1;
        x.len = (socklen_t)(offsetof(struct sockaddr_un, sun_path) + k);
        fds[i] = usock();
        emit(ID("bind.abstract.%s", names[i].id), "%s", rs(SYS(BIND(fds[i], x))));
        int dup_fd = usock();
        emit(ID("bind.abstract.%s.again", names[i].id), "%s", rs(SYS(BIND(dup_fd, x))));
        close(dup_fd);
    }
    for (size_t i = 0; i < ARRAY_LEN(names); i++) {
        close(fds[i]);
    }
    {
        /* longest abstract name: 107 bytes after the first NUL */
        uaddr_t x;
        memset(&x, 0, sizeof(x));
        x.sa.sun_family = AF_UNIX;
        snprintf(x.sa.sun_path + 1, 100, "uds.long.%d", g_pid);
        memset(x.sa.sun_path + 1 + strlen(x.sa.sun_path + 1), 'z',
               107 - strlen(x.sa.sun_path + 1));
        x.len = sizeof(x.sa);
        emit("bind.abstract.max_len", "%s", rs(bind_raw(&x.sa, x.len)));
    }
    {
        /* abstract: bind, close, bind again */
        uaddr_t x = nabs("rebind");
        int f1 = usock();
        emit("bind.abstract.first", "%s", rs(SYS(BIND(f1, x))));
        close(f1);
        emit("bind.abstract.after_close", "%s", rs(bind_raw(&x.sa, x.len)));
    }
}

CHECK(c02_bind_path) {
    uaddr_t a;
    int f1, f2;
    long r;

    /* a second socket, the same path */
    a = npath("twice");
    f1 = usock();
    f2 = usock();
    emit("bind.path.first", "%s", rs(SYS(BIND(f1, a))));
    emit("bind.path.second_socket", "%s", rs(SYS(BIND(f2, a))));
    emit("bind.path.twice_same_socket", "%s", rs(SYS(BIND(f1, a))));
    {
        uaddr_t b = npath("other");
        emit("bind.path.rebind_other_name_same_socket", "%s", rs(SYS(BIND(f1, b))));
    }
    emit("bind.path.file_exists", "%s", stat_s(a.sa.sun_path, 1));
    close(f1);
    emit("bind.path.after_close_stat", "%s", stat_s(a.sa.sun_path, 1));
    emit("bind.path.after_close_rebind_no_unlink", "%s", rs(SYS(BIND(f2, a))));
    emit("bind.path.after_close_unlink", "%s", rs(SYS(unlink(a.sa.sun_path))));
    emit("bind.path.after_unlink_rebind", "%s", rs(SYS(BIND(f2, a))));
    emit("bind.path.bound_stat", "%s", stat_s(a.sa.sun_path, 1));
    close(f2);
    unlink(a.sa.sun_path);

    /* listening socket closed: the file stays */
    a = npath("lclose");
    f1 = listener_at(&a, 4);
    emit("bind.path.listener", "%d", f1 >= 0);
    close(f1);
    emit("bind.path.listener_closed_stat", "%s", stat_s(a.sa.sun_path, 1));
    f2 = usock();
    emit("bind.path.listener_closed_rebind", "%s", rs(SYS(BIND(f2, a))));
    close(f2);
    f2 = usock();
    emit("bind.path.listener_closed_connect", "%s", rs(SYS(CONNECT(f2, a))));
    close(f2);

    /* existing file types */
    a = npath("reg");
    int rf = open(a.sa.sun_path, O_CREAT | O_RDWR, 0644);
    close(rf);
    f1 = usock();
    emit("bind.path.existing_regular_file", "%s", rs(SYS(BIND(f1, a))));
    emit("bind.path.existing_regular_file.still_regular", "%s", stat_s(a.sa.sun_path, 1));
    close(f1);
    f1 = usock();
    emit("connect.path.regular_file", "%s", rs(SYS(CONNECT(f1, a))));
    close(f1);

    a = npath("dir");
    mkdir(a.sa.sun_path, 0755);
    f1 = usock();
    emit("bind.path.existing_dir", "%s", rs(SYS(BIND(f1, a))));
    close(f1);
    f1 = usock();
    emit("connect.path.dir", "%s", rs(SYS(CONNECT(f1, a))));
    close(f1);
    {
        char sub[200];
        snprintf(sub, sizeof(sub), "%s/in.sock", a.sa.sun_path);
        uaddr_t b = upath(sub);
        f1 = usock();
        emit("bind.path.in_new_dir", "%s", rs(SYS(BIND(f1, b))));
        close(f1);
        unlink(sub);
        char dot[220];
        snprintf(dot, sizeof(dot), "%s/../%s", a.sa.sun_path, strrchr(a.sa.sun_path, '/') + 1);
        uaddr_t c = upath(dot);
        f1 = usock();
        emit("bind.path.dir_dotdot_dir_itself", "%s", rs(SYS(BIND(f1, c))));
        close(f1);
        rmdir(a.sa.sun_path);
    }

    a = npath("nodir/x");
    f1 = usock();
    emit("bind.path.missing_dir", "%s", rs(SYS(BIND(f1, a))));
    close(f1);
    f1 = usock();
    emit("connect.path.missing_dir", "%s", rs(SYS(CONNECT(f1, a))));
    close(f1);

    a = npath("fileparent");
    rf = open(a.sa.sun_path, O_CREAT | O_RDWR, 0644);
    close(rf);
    {
        char sub[200];
        snprintf(sub, sizeof(sub), "%s/child", a.sa.sun_path);
        uaddr_t b = upath(sub);
        f1 = usock();
        emit("bind.path.parent_is_file", "%s", rs(SYS(BIND(f1, b))));
        close(f1);
        f1 = usock();
        emit("connect.path.parent_is_file", "%s", rs(SYS(CONNECT(f1, b))));
        close(f1);
    }

    /* symlinks */
    a = npath("lnk_target_missing");
    {
        uaddr_t l = npath("lnk");
        symlink(a.sa.sun_path, l.sa.sun_path);
        f1 = usock();
        emit("bind.path.symlink_dangling", "%s", rs(SYS(BIND(f1, l))));
        close(f1);
        unlink(l.sa.sun_path);
        unlink(a.sa.sun_path);
    }

    /* same file, different spelling */
    a = npath("spell");
    {
        char dotted[220];
        const char *base = strrchr(a.sa.sun_path, '/') + 1;
        snprintf(dotted, sizeof(dotted), "%s/./%s", g_dir, base);
        uaddr_t d = upath(dotted);
        f1 = usock();
        emit("bind.path.spell.first", "%s", rs(SYS(BIND(f1, a))));
        f2 = usock();
        emit("bind.path.spell.dot", "%s", rs(SYS(BIND(f2, d))));
        close(f2);
        f2 = usock();
        emit("bind.path.spell.connect_dot_before_listen", "%s", rs(SYS(CONNECT(f2, d))));
        close(f2);
        listen(f1, 3);
        f2 = usock();
        emit("bind.path.spell.connect_dot", "%s", rs(SYS(CONNECT(f2, d))));
        close(f2);
        close(f1);
    }

    /* the mode of the created file, with umask 022 */
    {
        mode_t old = umask(022);
        a = npath("mode");
        f1 = usock();
        BIND(f1, a);
        emit("bind.path.mode_umask022", "%s", stat_s(a.sa.sun_path, 1));
        close(f1);
        unlink(a.sa.sun_path);
        umask(077);
        a = npath("mode2");
        f1 = usock();
        BIND(f1, a);
        emit("bind.path.mode_umask077", "%s", stat_s(a.sa.sun_path, 1));
        close(f1);
        unlink(a.sa.sun_path);
        umask(old);
    }

    /* bind on other kinds of sockets */
    {
        int x, y;
        sockpair(&x, &y);
        a = npath("onpair");
        emit("bind.on_socketpair_end", "%s", rs(SYS(BIND(x, a))));
        close(x);
        close(y);
        int c, s;
        conn_pair(0, &c, &s);
        a = npath("onconn");
        emit("bind.on_connected_client", "%s", rs(SYS(BIND(c, a))));
        emit("bind.on_accepted", "%s", rs(SYS(BIND(s, a))));
        close(c);
        close(s);
        a = npath("lis");
        f1 = listener_at(&a, 2);
        uaddr_t b = npath("lis2");
        emit("bind.on_listening", "%s", rs(SYS(BIND(f1, b))));
        close(f1);
    }

    /* relative paths */
    {
        char old[512];
        if (getcwd(old, sizeof(old))) {
            r = SYS(chdir(g_dir));
            emit("bind.rel.chdir", "%s", rs(r));
            char rel[64];
            snprintf(rel, sizeof(rel), "uds_conf.rel.%d.%d", g_pid, (1000 + ++g_seq));
            char absn[200];
            snprintf(absn, sizeof(absn), "%s/%s", g_dir, rel);
            if (g_nfiles < (int)ARRAY_LEN(g_files)) {
                g_files[g_nfiles++] = strdup(absn);
            }
            uaddr_t ra = upath(rel);
            f1 = usock();
            emit("bind.rel.bind", "%s", rs(SYS(BIND(f1, ra))));
            listen(f1, 2);
            uaddr_t aa = upath(absn);
            f2 = usock();
            emit("bind.rel.connect_abs", "%s", rs(SYS(CONNECT(f2, aa))));
            close(f2);
            f2 = usock();
            emit("bind.rel.connect_rel", "%s", rs(SYS(CONNECT(f2, ra))));
            {
                struct sockaddr_un o;
                socklen_t ol = sizeof(o);
                memset(&o, 0, sizeof(o));
                getsockname(f1, (struct sockaddr *)&o, &ol);
                emit("bind.rel.getsockname", "len_ok=%d is_rel=%d", ol == ra.len,
                     strcmp(o.sun_path, rel) == 0);
                int c = accept(f1, NULL, NULL);
                if (c >= 0) {
                    /* the peer address is that of the client, here unnamed */
                    memset(&o, 0, sizeof(o));
                    ol = sizeof(o);
                    getpeername(f2, (struct sockaddr *)&o, &ol);
                    emit("bind.rel.peername_of_client", "is_rel=%d", strcmp(o.sun_path, rel) == 0);
                    close(c);
                }
            }
            close(f2);
            /* move away; the relative name resolves in the new directory */
            chdir("/");
            f2 = usock();
            emit("bind.rel.connect_rel_other_cwd", "%s", rs(SYS(CONNECT(f2, ra))));
            close(f2);
            close(f1);
            chdir(old);
        }
    }
}

/* ------------------------------------------------------------------ */
/* c03: getsockname / getpeername / accept addresses                   */
/* ------------------------------------------------------------------ */

/* describes a name returned by getsockname & co: result, length, kind */
static const char *name_s(long r, const struct sockaddr_un *o, socklen_t ol,
                          const uaddr_t *expect) {
    static __thread char b[8][128];
    static __thread int i;
    char *p = b[i++ & 7];
    if (r < 0) {
        snprintf(p, 128, "%s", rs(r));
        return p;
    }
    const char *kind;
    size_t off = offsetof(struct sockaddr_un, sun_path);
    if (ol <= off) {
        kind = "unnamed";
    } else if (o->sun_path[0] == 0) {
        kind = "abstract";
    } else {
        kind = "path";
    }
    int same = -1;
    if (expect) {
        same = ol == expect->len &&
               memcmp(o->sun_path, expect->sa.sun_path, ol > off ? ol - off : 0) == 0;
    }
    snprintf(p, 128, "fam=%d len=%u kind=%s same_as_expected=%d", (int)o->sun_family,
             (unsigned)ol,
             kind, same);
    return p;
}

static const char *gsn(int fd, const uaddr_t *expect) {
    struct sockaddr_un o;
    socklen_t ol = sizeof(o);
    memset(&o, 0, sizeof(o));
    long r = SYS(getsockname(fd, (struct sockaddr *)&o, &ol));
    return name_s(r, &o, ol, expect);
}

static const char *gpn(int fd, const uaddr_t *expect) {
    struct sockaddr_un o;
    socklen_t ol = sizeof(o);
    memset(&o, 0, sizeof(o));
    long r = SYS(getpeername(fd, (struct sockaddr *)&o, &ol));
    return name_s(r, &o, ol, expect);
}

CHECK(c03_names_get) {
    int fd, c, s, l;
    uaddr_t a, b;

    fd = usock();
    emit("gsn.unbound", "%s", gsn(fd, NULL));
    emit("gpn.unbound", "%s", gpn(fd, NULL));
    a = npath("gsn");
    BIND(fd, a);
    emit("gsn.bound_path", "%s", gsn(fd, &a));
    emit("gpn.bound_path", "%s", gpn(fd, NULL));
    listen(fd, 4);
    emit("gsn.listening_path", "%s", gsn(fd, &a));
    emit("gpn.listening_path", "%s", gpn(fd, NULL));
    close(fd);

    fd = usock();
    a = nabs("gsn");
    BIND(fd, a);
    emit("gsn.bound_abstract", "%s", gsn(fd, &a));
    emit("gpn.bound_abstract", "%s", gpn(fd, NULL));
    {
        /* truncation */
        struct sockaddr_un o;
        socklen_t ol = 4;
        memset(&o, 0x55, sizeof(o));
        long r = SYS(getsockname(fd, (struct sockaddr *)&o, &ol));
        emit("gsn.truncated_buf4", "%s len=%u fam=%d next=0x%x", rs(r), (unsigned)ol,
             (int)o.sun_family, (unsigned char)o.sun_path[1]);
        ol = 0;
        r = SYS(getsockname(fd, (struct sockaddr *)&o, &ol));
        emit("gsn.buf0", "%s len=%u", rs(r), (unsigned)ol);
        ol = 1;
        r = SYS(getsockname(fd, (struct sockaddr *)&o, &ol));
        emit("gsn.buf1", "%s len=%u", rs(r), (unsigned)ol);
        r = SYS(getsockname(fd, NULL, NULL));
        emit("gsn.null_null", "%s", rs(r));
        r = SYS(getsockname(fd, (struct sockaddr *)&o, NULL));
        emit("gsn.addr_nulllen", "%s", rs(r));
    }
    close(fd);

    /* autobind */
    fd = usock();
    {
        struct sockaddr_un o;
        socklen_t ol = sizeof(o);
        memset(&o, 0, sizeof(o));
        o.sun_family = AF_UNIX;
        long r = SYS(bind(fd, (struct sockaddr *)&o, sizeof(sa_family_t)));
        emit("autobind.bind", "%s", rs(r));
        memset(&o, 0, sizeof(o));
        r = SYS(getsockname(fd, (struct sockaddr *)&o, &ol));
        int hex = 1;
        for (int i = 1; i < 6 && ol >= 8; i++) {
            char ch = o.sun_path[i];
            hex &= (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f');
        }
        emit("autobind.getsockname", "%s len=%u first_nul=%d hex5=%d", rs(r), (unsigned)ol,
             o.sun_path[0] == 0, hex);
        emit("autobind.listen", "%s", rs(SYS(listen(fd, 1))));
        close(fd);
    }

    /* connected pair: client side */
    l = listener(0, 4, &a);
    c = client_to(&a);
    emit("gsn.client_unbound", "%s", gsn(c, NULL));
    emit("gpn.client", "%s", gpn(c, &a));
    {
        struct sockaddr_un o;
        socklen_t ol = sizeof(o);
        memset(&o, 0, sizeof(o));
        s = accept(l, (struct sockaddr *)&o, &ol);
        emit("accept.addr_unbound_client", "%s", name_s(s >= 0 ? 0 : -errno, &o, ol, NULL));
        emit("gsn.accepted", "%s", gsn(s, &a));
        emit("gpn.accepted_unbound_client", "%s", gpn(s, NULL));
    }
    close(s);
    close(c);
    close(l);

    /* a bound client */
    l = listener(1, 4, &a);
    c = usock();
    b = nabs("cli");
    BIND(c, b);
    CONNECT(c, a);
    {
        struct sockaddr_un o;
        socklen_t ol = sizeof(o);
        memset(&o, 0, sizeof(o));
        s = accept(l, (struct sockaddr *)&o, &ol);
        emit("accept.addr_bound_client", "%s", name_s(s >= 0 ? 0 : -errno, &o, ol, &b));
        emit("gpn.accepted_bound_client", "%s", gpn(s, &b));
        emit("gsn.client_bound", "%s", gsn(c, &b));
        emit("gpn.client_bound", "%s", gpn(c, &a));
        /* accept with a small buffer */
        struct sockaddr_un o2;
        socklen_t ol2 = 4;
        int c2 = usock();
        uaddr_t b2 = nabs("cli2");
        BIND(c2, b2);
        CONNECT(c2, a);
        memset(&o2, 0x55, sizeof(o2));
        int s2 = accept(l, (struct sockaddr *)&o2, &ol2);
        emit("accept.addr_truncated", "%s len=%u", rs(s2 >= 0 ? 0 : -errno), (unsigned)ol2);
        closefd(s2);
        close(c2);
        /* accept with NULL address and non-NULL length */
        int c3 = client_to(&a);
        socklen_t ol3 = sizeof(o2);
        int s3 = accept(l, NULL, &ol3);
        emit("accept.null_addr_with_len", "%s", rs(s3 >= 0 ? 0 : -errno));
        closefd(s3);
        close(c3);
    }
    close(s);
    close(c);
    close(l);

    /* a pair */
    int x, y;
    sockpair(&x, &y);
    emit("gsn.socketpair", "%s", gsn(x, NULL));
    emit("gpn.socketpair", "%s", gpn(x, NULL));
    close(x);
    close(y);
}

/* ------------------------------------------------------------------ */
/* c04: connect() errors and corner cases                              */
/* ------------------------------------------------------------------ */

CHECK(c04_connect) {
    int fd, l;
    uaddr_t a, b;

    a = npath("none");
    fd = usock();
    emit("connect.path_missing", "%s", rs(SYS(CONNECT(fd, a))));
    close(fd);
    a = nabs("none");
    fd = usock();
    emit("connect.abstract_missing", "%s", rs(SYS(CONNECT(fd, a))));
    emit("connect.abstract_missing.state_after", "gpn=%s", gpn(fd, NULL));
    close(fd);

    /* bound but not listening */
    a = npath("nolisten");
    l = usock();
    BIND(l, a);
    fd = usock();
    emit("connect.path_bound_not_listening", "%s", rs(SYS(CONNECT(fd, a))));
    close(fd);
    close(l);
    a = nabs("nolisten");
    l = usock();
    BIND(l, a);
    fd = usock();
    emit("connect.abstract_bound_not_listening", "%s", rs(SYS(CONNECT(fd, a))));
    close(fd);
    close(l);

    /* stale socket file */
    a = npath("stale");
    l = listener_at(&a, 2);
    close(l);
    fd = usock();
    emit("connect.path_stale_file", "%s", rs(SYS(CONNECT(fd, a))));
    close(fd);

    /* normal, twice, on connected sockets */
    l = listener(0, 4, &a);
    fd = usock();
    emit("connect.ok", "%s", rs(SYS(CONNECT(fd, a))));
    emit("connect.twice_same_listener", "%s", rs(SYS(CONNECT(fd, a))));
    {
        uaddr_t o = nabs("other");
        int l2 = listener_at(&o, 2);
        emit("connect.twice_other_listener", "%s", rs(SYS(CONNECT(fd, o))));
        close(l2);
    }
    emit("connect.after_connect_getpeername", "%s", gpn(fd, &a));
    close(fd);
    {
        int x, y;
        sockpair(&x, &y);
        emit("connect.on_socketpair_end", "%s", rs(SYS(CONNECT(x, a))));
        close(x);
        close(y);
    }
    emit("connect.listener_to_itself", "%s", rs(SYS(CONNECT(l, a))));
    close(l);

    /* a socket that connects to a name it bound itself, listening */
    a = nabs("self");
    l = listener_at(&a, 2);
    emit("connect.listening_socket_to_itself", "%s", rs(SYS(CONNECT(l, a))));
    close(l);
    a = nabs("self2");
    fd = usock();
    BIND(fd, a);
    emit("connect.bound_socket_to_itself", "%s", rs(SYS(CONNECT(fd, a))));
    close(fd);

    /* address validation */
    l = listener(0, 4, &a);
    fd = usock();
    emit("connect.len_sun_len_no_nul", "%s", rs(SYS(connect(fd, (struct sockaddr *)&a.sa,
            a.len - 1))));
    close(fd);
    fd = usock();
    emit("connect.len_sockaddr_un", "%s", rs(SYS(connect(fd, (struct sockaddr *)&a.sa,
            sizeof(a.sa)))));
    close(fd);
    fd = usock();
    emit("connect.len_0", "%s", rs(SYS(connect(fd, (struct sockaddr *)&a.sa, 0))));
    emit("connect.len_1", "%s", rs(SYS(connect(fd, (struct sockaddr *)&a.sa, 1))));
    emit("connect.len_2", "%s", rs(SYS(connect(fd, (struct sockaddr *)&a.sa, 2))));
    emit("connect.len_gt_max", "%s",
         rs(SYS(connect(fd, (struct sockaddr *)&a.sa, sizeof(struct sockaddr_un) + 1))));
    emit("connect.null_addr", "%s", rs(SYS(connect(fd, NULL, 0))));
    emit("connect.null_addr_len", "%s", rs(SYS(connect(fd, NULL,
                                           sizeof(struct sockaddr_un)))));
    {
        struct sockaddr_in in;
        memset(&in, 0, sizeof(in));
        in.sin_family = AF_INET;
        emit("connect.family_inet", "%s", rs(SYS(connect(fd, (struct sockaddr *)&in,
                                             sizeof(in)))));
        struct sockaddr_un un = a.sa;
        un.sun_family = AF_UNSPEC;
        emit("connect.family_unspec", "%s", rs(SYS(connect(fd, (struct sockaddr *)&un, a.len))));
    }
    emit("connect.after_errors_ok", "%s", rs(SYS(CONNECT(fd, a))));
    close(fd);
    close(l);

    /* the listener is closed while the client is connected-not-accepted */
    l = listener(1, 4, &a);
    fd = client_to(&a);
    close(l);
    emit("connect.listener_closed_pending.ev", "%s", PEV(fd));
    emit("connect.listener_closed_pending.read", "%s", nb_read_s(fd, 8));
    emit("connect.listener_closed_pending.read2", "%s", nb_read_s(fd, 8));
    emit("connect.listener_closed_pending.write", "%s", nb_write_s(fd, "x"));
    emit("connect.listener_closed_pending.so_error", "%s", sockopt_s(fd, SOL_SOCKET,
            SO_ERROR));
    close(fd);
    fd = usock();
    emit("connect.after_listener_closed", "%s", rs(SYS(CONNECT(fd, a))));
    close(fd);
    /* the name is free again after the listener closed, for abstract names */
    l = listener_at(&a, 2);
    emit("connect.relisten_same_abstract_name", "%d", l >= 0);
    closefd(l);

    /* data written before accept */
    l = listener(0, 4, &a);
    fd = client_to(&a);
    write(fd, "early", 5);
    close(fd);
    {
        int s = accept(l, NULL, NULL);
        emit("connect.write_close_before_accept.accept", "%s", rs(s >= 0 ? 0 : -errno));
        emit("connect.write_close_before_accept.read1", "%s", nb_read_s(s, 16));
        emit("connect.write_close_before_accept.read2", "%s", nb_read_s(s, 16));
        emit("connect.write_close_before_accept.ev", "%s", PEV(s));
        emit("connect.write_close_before_accept.write", "%s", nb_write_s(s, "x"));
        closefd(s);
    }
    close(l);

    /* many connect/close cycles do not exhaust names */
    a = nabs("cycle");
    l = listener_at(&a, 64);
    int ok = 0;
    for (int i = 0; i < 40; i++) {
        int c = client_to(&a);
        if (c < 0) {
            break;
        }
        int s = accept(l, NULL, NULL);
        if (s < 0) {
            close(c);
            break;
        }
        ok++;
        close(c);
        close(s);
    }
    emit("connect.cycles40", "%d", ok);
    close(l);
}

/* ------------------------------------------------------------------ */
/* c05: listen / accept                                                */
/* ------------------------------------------------------------------ */

CHECK(c05_listen) {
    int fd, l;
    uaddr_t a;

    fd = usock();
    emit("listen.unbound", "%s", rs(SYS(listen(fd, 1))));
    emit("listen.unbound.accept", "%s", rs(SYS(accept(fd, NULL, NULL))));
    emit("listen.unbound.acceptconn", "%s", sockopt_s(fd, SOL_SOCKET, SO_ACCEPTCONN));
    close(fd);

    a = nabs("lis");
    fd = usock();
    BIND(fd, a);
    emit("listen.acceptconn_before", "%s", sockopt_s(fd, SOL_SOCKET, SO_ACCEPTCONN));
    emit("listen.accept_bound_not_listening", "%s", rs(SYS(accept(fd, NULL, NULL))));
    emit("listen.first", "%s", rs(SYS(listen(fd, 1))));
    emit("listen.acceptconn_after", "%s", sockopt_s(fd, SOL_SOCKET, SO_ACCEPTCONN));
    emit("listen.again", "%s", rs(SYS(listen(fd, 5))));
    emit("listen.again_backlog0", "%s", rs(SYS(listen(fd, 0))));
    emit("listen.again_negative", "%s", rs(SYS(listen(fd, -1))));
    emit("listen.again_huge", "%s", rs(SYS(listen(fd, INT_MAX))));
    close(fd);

    for (int bl = -2; bl <= 2; bl += 2) {
        a = nabs("lisbl");
        fd = usock();
        BIND(fd, a);
        emit(ID("listen.backlog_%d", bl), "%s", rs(SYS(listen(fd, bl))));
        close(fd);
    }
    {
        a = nabs("lisbl2");
        fd = usock();
        BIND(fd, a);
        emit("listen.backlog_int_max", "%s", rs(SYS(listen(fd, INT_MAX))));
        close(fd);
    }

    int c, s;
    conn_pair(0, &c, &s);
    emit("listen.on_client", "%s", rs(SYS(listen(c, 1))));
    emit("listen.on_accepted", "%s", rs(SYS(listen(s, 1))));
    close(c);
    close(s);
    sockpair(&c, &s);
    emit("listen.on_socketpair", "%s", rs(SYS(listen(c, 1))));
    emit("accept.on_socketpair", "%s", rs(SYS(accept(c, NULL, NULL))));
    close(c);
    close(s);

    /* accept */
    l = listener(0, 4, &a);
    emit("accept.empty_nonblocking", "%s", ({
        set_nb(l, 1);
        const char *r = rs(SYS(accept(l, NULL, NULL)));
        set_nb(l, 0);
        r;
    }));
    emit("accept.invalid_flags", "%s", rs(SYS(syscall(SYS_accept4, l, NULL, NULL,
                                          0x12340000))));
    {
        /* accept4 flags */
        int c1 = client_to(&a);
        int s1 = accept4(l, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC);
        emit("accept4.nonblock_cloexec", "%s nonblock=%d cloexec=%d", rs(s1 >= 0 ? 0 : -errno),
             get_nb(s1), s1 >= 0 ? !!(fcntl(s1, F_GETFD) & FD_CLOEXEC) : -1);
        emit("accept4.nonblock.read", "%s", nb_read_s(s1, 4));
        closefd(s1);
        int c2 = client_to(&a);
        int s2 = accept4(l, NULL, NULL, 0);
        emit("accept4.no_flags", "%s nonblock=%d cloexec=%d", rs(s2 >= 0 ? 0 : -errno),
             get_nb(s2), s2 >= 0 ? !!(fcntl(s2, F_GETFD) & FD_CLOEXEC) : -1);
        closefd(s2);
        /* the listener's O_NONBLOCK is not inherited */
        set_nb(l, 1);
        int c3 = client_to(&a);
        int s3 = accept(l, NULL, NULL);
        emit("accept.listener_nonblock_not_inherited", "accepted_nonblock=%d", get_nb(s3));
        closefd(s3);
        set_nb(l, 0);
        /* the client socket's nonblock flag is its own */
        emit("accept.client_nonblock", "%d", get_nb(c1));
        close(c1);
        close(c2);
        close(c3);
    }
    {
        /* FIFO order */
        int cs[3], k;
        for (k = 0; k < 3; k++) {
            cs[k] = client_to(&a);
            char ch = (char)('1' + k);
            write(cs[k], &ch, 1);
        }
        char seq[8] = {0};
        for (k = 0; k < 3; k++) {
            int s = accept(l, NULL, NULL);
            char ch = '?';
            if (s >= 0) {
                nb_read(s, &ch, 1);
            }
            seq[k] = ch;
            closefd(s);
        }
        emit("accept.fifo_order", "%s", seq);
        for (k = 0; k < 3; k++) {
            close(cs[k]);
        }
    }
    {
        /* accepted sockets are independent: one is shut down, the other works */
        int c1 = client_to(&a);
        int c2 = client_to(&a);
        int s1 = accept(l, NULL, NULL);
        int s2 = accept(l, NULL, NULL);
        write(c1, "one", 3);
        write(c2, "two", 3);
        emit("accept.independent.s1", "%s", nb_read_s(s1, 8));
        emit("accept.independent.s2", "%s", nb_read_s(s2, 8));
        close(s1);
        emit("accept.independent.after_close_s1.s2_write", "%s", nb_write_s(s2, "ok"));
        emit("accept.independent.after_close_s1.c2_read", "%s", nb_read_s(c2, 8));
        close(c1);
        close(c2);
        close(s2);
    }
    close(l);
}

/* ------------------------------------------------------------------ */
/* c03: the socket file: unlink, rename, links, reuse of the name      */
/* ------------------------------------------------------------------ */

CHECK(c03_names_files) {
    uaddr_t a, b;
    int l, c1, c2, s;

    /* unlink while listening */
    a = npath("unl");
    l = listener_at(&a, 4);
    c1 = client_to(&a);
    emit("files.unlink.ok", "%s", rs(SYS(unlink(a.sa.sun_path))));
    c2 = usock();
    emit("files.unlink.new_connect", "%s", rs(SYS(CONNECT(c2, a))));
    close(c2);
    s = accept(l, NULL, NULL);
    emit("files.unlink.pending_accepted", "%s", rs(s >= 0 ? 0 : -errno));
    emit("files.unlink.pending_works", "%s", rs(SYS(write(c1, "hi", 2))));
    emit("files.unlink.pending_read", "%s", nb_read_s(s, 8));
    emit("files.unlink.getsockname", "%s", gsn(l, &a));
    emit("files.unlink.getpeername", "%s", gpn(c1, &a));
    emit("files.unlink.rebind_other_socket", "%s", ({
        int f = usock();
        const char *r = rs(SYS(BIND(f, a)));
        if (f >= 0 && r[0] != '-') {
            listen(f, 4);
        }
        /* the new socket gets the connections, not the old listener */
        int c3 = usock();
        long cr = SYS(CONNECT(c3, a));
        set_nb(f, 1);
        set_nb(l, 1);
        int sf = accept(f, NULL, NULL);
        int sl = accept(l, NULL, NULL);
        static __thread char tmp[96];
        snprintf(tmp, sizeof(tmp), "bind=%s connect=%s accepted_by_new=%d accepted_by_old=%d", r, rs(cr),
                 sf >= 0, sl >= 0);
        closefd(sf);
        closefd(sl);
        close(c3);
        close(f);
        tmp;
    }));
    close(s);
    close(c1);
    close(l);

    /* rename */
    a = npath("ren");
    b = npath("ren2");
    l = listener_at(&a, 4);
    emit("files.rename.ok", "%s", rs(SYS(rename(a.sa.sun_path, b.sa.sun_path))));
    c1 = usock();
    emit("files.rename.connect_new_name", "%s", rs(SYS(CONNECT(c1, b))));
    c2 = usock();
    emit("files.rename.connect_old_name", "%s", rs(SYS(CONNECT(c2, a))));
    emit("files.rename.getsockname", "%s", gsn(l, &a));
    emit("files.rename.peername_of_client", "%s", gpn(c1, &b));
    s = accept(l, NULL, NULL);
    emit("files.rename.accepted", "%s", rs(s >= 0 ? 0 : -errno));
    closefd(s);
    close(c1);
    close(c2);
    close(l);

    /* symlink and hard link to the socket file */
    a = npath("tgt");
    l = listener_at(&a, 4);
    b = npath("sym");
    symlink(a.sa.sun_path, b.sa.sun_path);
    c1 = usock();
    emit("files.symlink.connect", "%s", rs(SYS(CONNECT(c1, b))));
    close(c1);
    emit("files.symlink.stat_follow", "%s", stat_s(b.sa.sun_path, 1));
    emit("files.symlink.lstat", "%s", stat_s(b.sa.sun_path, 0));
    uaddr_t h = npath("hard");
    emit("files.hardlink.link", "%s", rs(SYS(link(a.sa.sun_path, h.sa.sun_path))));
    c1 = usock();
    emit("files.hardlink.connect", "%s", rs(SYS(CONNECT(c1, h))));
    close(c1);
    emit("files.hardlink.stat", "%s", stat_s(h.sa.sun_path, 1));
    emit("files.open_socket_file", "%s", rs(SYS(open(a.sa.sun_path, O_RDONLY))));
    emit("files.open_socket_file_wronly", "%s", rs(SYS(open(a.sa.sun_path, O_WRONLY))));
    emit("files.truncate_socket_file", "%s", rs(SYS(truncate(a.sa.sun_path, 0))));
    emit("files.chmod_socket_file", "%s", rs(SYS(chmod(a.sa.sun_path, 0600))));
    emit("files.chmod_socket_file.stat", "%s", stat_s(a.sa.sun_path, 1));
    {
        /* after the listener closes the file stays: a name lookup finds a socket, nobody listens */
        close(l);
        c1 = usock();
        emit("files.after_close.connect_via_symlink", "%s", rs(SYS(CONNECT(c1, b))));
        close(c1);
        c1 = usock();
        emit("files.after_close.connect_via_hardlink", "%s", rs(SYS(CONNECT(c1, h))));
        close(c1);
    }

    /* unlink while connected: the connection goes on */
    {
        int x, y;
        a = npath("conn");
        l = listener_at(&a, 4);
        x = client_to(&a);
        y = accept(l, NULL, NULL);
        unlink(a.sa.sun_path);
        write(x, "ab", 2);
        emit("files.unlink_connected.read", "%s", nb_read_s(y, 8));
        close(l);
        write(y, "cd", 2);
        emit("files.unlink_connected.read_after_listener_close", "%s", nb_read_s(x, 8));
        emit("files.unlink_connected.ev", "%s", PEV(x));
        close(x);
        close(y);
    }

    /* the same name for a new socket after the old file was replaced (inode numbers) */
    {
        int cnt_ok = 0, cnt_total = 0;
        for (int i = 0; i < 20; i++) {
            a = npath("reuse");
            int f1 = listener_at(&a, 2);
            unlink(a.sa.sun_path);
            int f2 = listener_at(&a, 2);
            int c = usock();
            long r = SYS(CONNECT(c, a));
            int ok = 0;
            if (f2 >= 0 && r == 0) {
                set_nb(f2, 1);
                int s2 = accept(f2, NULL, NULL);
                ok = s2 >= 0;
                closefd(s2);
            }
            cnt_total++;
            cnt_ok += ok;
            close(c);
            closefd(f1);
            closefd(f2);
            unlink(a.sa.sun_path);
        }
        emit("files.name_reuse_after_unlink_reaches_new_listener", "%d/%d", cnt_ok, cnt_total);
    }
}

/* ------------------------------------------------------------------ */
/* timed operations: run a possibly blocking call in a helper thread   */
/* ------------------------------------------------------------------ */

#define BLOCKED (-9999L)

enum {
    OP_READ, OP_RECV, OP_WRITE, OP_SEND, OP_ACCEPT, OP_CONNECT, OP_RECVMSG_CTL, OP_LISTEN,
    OP_POLLIN, OP_WRITEV_BIG, OP_RECVMSG, OP_EPOLLWAIT
};

typedef struct {
    int kind;
    int fd;
    size_t len;
    int flags;
    int arg;
    uaddr_t addr;
    long res;
    int extra;
    char buf[1 << 16];
    atomic_int done;
    char *big;
} op_t;

static void *op_thread(void *p) {
    op_t *o = p;
    switch (o->kind) {
        case OP_READ:
            o->res = SYS(read(o->fd, o->buf, o->len));
            break;
        case OP_RECV:
            o->res = SYS(recv(o->fd, o->buf, o->len, o->flags));
            break;
        case OP_WRITE:
            o->res = SYS(write(o->fd, o->buf, o->len));
            break;
        case OP_SEND:
            o->res = SYS(send(o->fd, o->buf, o->len, o->flags));
            break;
        case OP_ACCEPT:
            o->res = SYS(accept(o->fd, NULL, NULL));
            break;
        case OP_CONNECT:
            o->res = SYS(connect(o->fd, (struct sockaddr *)&o->addr.sa, o->addr.len));
            break;
        case OP_LISTEN:
            o->res = SYS(listen(o->fd, o->arg));
            break;
        case OP_POLLIN: {
            struct pollfd pf = {o->fd, (short)o->arg, 0};
            o->res = SYS(poll(&pf, 1, o->flags));
            o->extra = pf.revents;
            break;
        }
        case OP_EPOLLWAIT: {
            struct epoll_event ee[4];
            o->res = SYS(epoll_wait(o->fd, ee, 4, o->flags));
            o->extra = o->res > 0 ? (int)ee[0].events : 0;
            break;
        }
        case OP_WRITEV_BIG:
            o->res = SYS(write(o->fd, o->big, o->len));
            break;
        case OP_RECVMSG_CTL: {
            struct iovec iov = {o->buf, o->len};
            char ctl[64];
            struct msghdr m;
            memset(&m, 0, sizeof(m));
            m.msg_iov = &iov;
            m.msg_iovlen = 1;
            m.msg_control = ctl;
            m.msg_controllen = sizeof(ctl);
            o->res = SYS(recvmsg(o->fd, &m, o->flags));
            o->extra = (int)m.msg_controllen;
            break;
        }
        default:
            o->res = -ENOSYS;
    }
    atomic_store(&o->done, 1);
    return NULL;
}

/* returns the result, or BLOCKED if the call did not return within tmo ms */
static long timed_op(op_t *o, int tmo, long long *elapsed) {
    pthread_t t;
    pthread_attr_t attr;
    atomic_store(&o->done, 0);
    pthread_attr_init(&attr);
    pthread_attr_setstacksize(&attr, 256 * 1024);
    long long t0 = now_ms();
    if (pthread_create(&t, &attr, op_thread, o) != 0) {
        return -ENOMEM;
    }
    pthread_attr_destroy(&attr);
    while (!atomic_load(&o->done)) {
        if (now_ms() - t0 > tmo) {
            pthread_detach(t);
            if (elapsed) {
                *elapsed = now_ms() - t0;
            }
            return BLOCKED;
        }
        usleep(2000);
    }
    pthread_join(t, NULL);
    if (elapsed) {
        *elapsed = now_ms() - t0;
    }
    return o->res;
}

static op_t *new_op(int kind, int fd) {
    op_t *o = calloc(1, sizeof(op_t));
    o->kind = kind;
    o->fd = fd;
    o->len = 16;
    return o;
}

/* classifies the elapsed time against an expected timeout */
static const char *tclass(long long el, int want) {
    static __thread char b[8][32];
    static __thread int i;
    char *p = b[i++ & 7];
    if (el < want - 30) {
        snprintf(p, 32, "early");
    } else if (el <= want + 400) {
        snprintf(p, 32, "on_time");
    } else {
        snprintf(p, 32, "late");
    }
    return p;
}

/* ------------------------------------------------------------------ */
/* c06: backlog                                                        */
/* ------------------------------------------------------------------ */

/* non-blocking connects until one fails or max succeed; fills fds */
static int connect_until_full(uaddr_t *a, int max, int *fds, int *err) {
    int n = 0;
    *err = 0;
    while (n < max) {
        int c = usock();
        set_nb(c, 1);
        if (CONNECT(c, *a) < 0) {
            *err = errno;
            close(c);
            break;
        }
        fds[n++] = c;
    }
    return n;
}

CHECK(c06_backlog) {
    int fds[64];
    int bls[] = {1, 2, 5, -1};
    for (size_t i = 0; i < ARRAY_LEN(bls); i++) {
        uaddr_t a;
        int l = listener(1, bls[i], &a);
        int err;
        int n = connect_until_full(&a, 24, fds, &err);
        emit(ID("backlog.count.bl%d", bls[i]), "n=%d%s err=%s", n < 24 ? n : 24,
             n >= 24 ? "(cap)" : "", n >= 24 ? "none" : enm(err));
        /* accept them all */
        int acc = 0;
        set_nb(l, 1);
        for (;;) {
            int s = accept(l, NULL, NULL);
            if (s < 0) {
                break;
            }
            acc++;
            close(s);
        }
        emit(ID("backlog.accepted.bl%d", bls[i]), "%d", acc);
        for (int k = 0; k < n; k++) {
            close(fds[k]);
        }
        close(l);
    }

    {
        /* backlog 0 */
        uaddr_t a;
        int l = listener(1, 0, &a);
        int err;
        int n = connect_until_full(&a, 8, fds, &err);
        emit("backlog.count.bl0", "n=%d err=%s", n, n >= 8 ? "none" : enm(err));
        for (int k = 0; k < n; k++) {
            close(fds[k]);
        }
        close(l);
    }

    {
        /* after the queue is full one accept makes room */
        uaddr_t a;
        int l = listener(1, 2, &a);
        int err;
        int n = connect_until_full(&a, 10, fds, &err);
        int s = accept(l, NULL, NULL);
        int c = usock();
        set_nb(c, 1);
        emit("backlog.room_after_accept", "full_at=%d accept=%s connect=%s", n,
             rs(s >= 0 ? 0 : -errno),
             rs(SYS(CONNECT(c, a))));
        closefd(s);
        close(c);
        for (int k = 0; k < n; k++) {
            close(fds[k]);
        }
        close(l);
    }

    {
        /* blocking connect with a full queue: Linux waits for an accept */
        uaddr_t a;
        int l = listener(1, 1, &a);
        int err;
        int n = connect_until_full(&a, 10, fds, &err);
        op_t *o = new_op(OP_CONNECT, usock());
        o->addr = a;
        pthread_t acc;
        tctx_t tc;
        memset(&tc, 0, sizeof(tc));
        tc.fd = l;
        tc.delay_ms = 400;
        acc = start_thread(th_accept, &tc);
        long long el;
        long r = timed_op(o, 3000, &el);
        emit("backlog.blocking_connect_full", "%s elapsed=%s", r == BLOCKED ? "BLOCKED" : rs(r),
             r == BLOCKED ? "-" : (el >= 300 ? "waited_for_accept" : "immediate"));
        join_thread(acc, &tc, 2000);
        if (tc.result >= 0) {
            close((int)tc.result);
        }
        for (int k = 0; k < n; k++) {
            close(fds[k]);
        }
        closefd(o->fd);
        close(l);
    }

    {
        /* shrinking the backlog with connections waiting */
        uaddr_t a;
        int l = listener(1, 8, &a);
        int cs[8];
        for (int k = 0; k < 4; k++) {
            cs[k] = client_to(&a);
            write(cs[k], "m", 1);
        }
        emit("backlog.shrink.listen", "%s", rs(SYS(listen(l, 1))));
        int acc = 0;
        int held[8];
        set_nb(l, 1);
        for (;;) {
            int s = accept(l, NULL, NULL);
            if (s < 0) {
                break;
            }
            held[acc++] = s;
        }
        emit("backlog.shrink.accepted_of_4_waiting", "%d", acc);
        const char *rd[4];
        for (int k = 0; k < 4; k++) {
            rd[k] = strdup(nb_read_s(cs[k], 4));
        }
        emit("backlog.shrink.waiting_clients_read", "%s %s %s %s", rd[0], rd[1], rd[2], rd[3]);
        for (int k = 0; k < acc; k++) {
            close(held[k]);
        }
        for (int k = 0; k < 4; k++) {
            free((void *)rd[k]);
            close(cs[k]);
        }
        close(l);
    }

    {
        /* growing it */
        uaddr_t a;
        int l = listener(1, 1, &a);
        emit("backlog.grow.listen", "%s", rs(SYS(listen(l, 10))));
        int err;
        int n = connect_until_full(&a, 24, fds, &err);
        emit("backlog.grow.count", "n=%d err=%s", n < 24 ? n : 24, n >= 24 ? "cap" : enm(err));
        for (int k = 0; k < n; k++) {
            close(fds[k]);
        }
        close(l);
    }

}

/* ------------------------------------------------------------------ */
/* c20: closing and shutting down listeners, races with connect        */
/* ------------------------------------------------------------------ */

typedef struct {
    int fd;
    atomic_int ready;
    atomic_int go;
} race_t;

static void *th_race_close(void *p) {
    race_t *r = p;
    atomic_store(&r->ready, 1);
    while (!atomic_load(&r->go)) {
    }
    close(r->fd);
    return NULL;
}

CHECK(c20_listener_close) {
    uaddr_t a;
    int l = listener(1, 8, &a);
    int c1 = client_to(&a);
    int c2 = client_to(&a);
    write(c1, "d", 1);
    int s = accept(l, NULL, NULL);
    /* c1 is accepted, c2 is pending; close the listener */
    close(l);
    emit("lclose.accepted_still_works.write", "%s", nb_write_s(c1, "z"));
    emit("lclose.accepted_still_works.read", "%s", nb_read_s(s, 8));
    emit("lclose.accepted_ev", "%s", PEV(s));
    emit("lclose.pending_client.ev", "%s", PEV(c2));
    emit("lclose.pending_client.read", "%s", nb_read_s(c2, 8));
    emit("lclose.pending_client.read2", "%s", nb_read_s(c2, 8));
    emit("lclose.pending_client.write", "%s", nb_write_s(c2, "x"));
    emit("lclose.new_connect", "%s", ({
        int c3 = usock();
        const char *r = rs(SYS(CONNECT(c3, a)));
        close(c3);
        r;
    }));
    close(s);
    close(c1);
    close(c2);

    /* the race of connect with the close of the listener */
    int seen_ok = 0, seen_refused = 0, seen_other = 0;
    char other[160] = "";
    int resets_ok = 1;
    for (int i = 0; i < 80; i++) {
        uaddr_t b;
        int lis = listener(1, 4, &b);
        tctx_t tc;
        memset(&tc, 0, sizeof(tc));
        tc.fd = lis;
        tc.delay_ms = 0;
        tc.arg = -1;
        pthread_t t = start_thread(th_close, &tc);
        usleep((i % 8) * 30);
        int c = usock();
        long r = SYS(CONNECT(c, b));
        join_thread(t, &tc, 2000);
        if (r == 0) {
            seen_ok++;
            /* the connection was queued and then dropped: the client must not hang */
            char buf[4];
            long rr = nb_read(c, buf, 4);
            if (rr != 0 && rr != -ECONNRESET && rr != -EAGAIN) {
                resets_ok = 0;
            }
        } else if (r == -ECONNREFUSED) {
            seen_refused++;
        } else {
            seen_other++;
            if (strlen(other) < 100 && !strstr(other, enm((int) - r))) {
                strcat(other, enm((int) - r));
                strcat(other, " ");
            }
        }
        close(c);
    }
    /* a tighter race: both threads wait for the same flag, then close() and connect() at once */
    {
        int other2 = 0, ok2 = 0, refused2 = 0;
        char names[160] = "";
        for (int i = 0; i < 400; i++) {
            uaddr_t b;
            int lis = listener(1, 4, &b);
            race_t rc;
            memset(&rc, 0, sizeof(rc));
            rc.fd = lis;
            pthread_t t;
            pthread_create(&t, NULL, th_race_close, &rc);
            while (!atomic_load(&rc.ready)) {
            }
            int c = usock();
            atomic_store(&rc.go, 1);
            long r = SYS(CONNECT(c, b));
            pthread_join(t, NULL);
            if (r == 0) {
                ok2++;
            } else if (r == -ECONNREFUSED) {
                refused2++;
            } else {
                other2++;
                if (!strstr(names, enm((int) - r)) && strlen(names) < 100) {
                    strcat(names, enm((int) - r));
                    strcat(names, " ");
                }
            }
            close(c);
        }
        emit("lclose.race_tight.only_ok_or_refused", "%d other=[%s]", other2 == 0, names);
    }
    emit("lclose.race.connect_results", "only_ok_or_refused=%d other=[%s]", seen_other == 0,
         other);
    emit("lclose.race.some_refused_or_ok", "%d", seen_ok + seen_refused == 80 - seen_other);
    emit("lclose.race.queued_then_dropped_client_read_sane", "%d", resets_ok);
}

/* ------------------------------------------------------------------ */
/* c13: blocking calls woken up by the other side or by shutdown       */
/* ------------------------------------------------------------------ */

static const char *blk_res(long r, long long el, int min_delay) {
    static __thread char b[8][64];
    static __thread int i;
    char *p = b[i++ & 7];
    if (r == BLOCKED) {
        snprintf(p, 64, "BLOCKED");
    } else {
        snprintf(p, 64, "%s %s", rs(r), el >= min_delay ? "woken" : "immediate");
    }
    return p;
}

CHECK(c13_blocking) {
    int a, b;
    long long el;
    long r;
    op_t *o;
    tctx_t tc;
    pthread_t t;

    /* read woken by data */
    sockpair(&a, &b);
    o = new_op(OP_READ, a);
    memset(&tc, 0, sizeof(tc));
    tc.fd = b;
    tc.delay_ms = 200;
    tc.arg = 3;
    memcpy(tc.data, "abc", 3);
    t = start_thread(th_write, &tc);
    r = timed_op(o, 3000, &el);
    emit("blk.read.woken_by_write", "%s", blk_res(r, el, 150));
    join_thread(t, &tc, 1000);
    close(a);
    close(b);

    struct {
        const char *name;
        int how; /* -1 = close the peer, else shutdown(peer, how) */
        int on_peer;
    } rd_cases[] = {
        {"peer_close", -1, 1},
        {"peer_shut_wr", SHUT_WR, 1},
        {"peer_shut_rdwr", SHUT_RDWR, 1},
        {"peer_shut_rd", SHUT_RD, 1},
        {"self_shut_rd", SHUT_RD, 0},
        {"self_shut_rdwr", SHUT_RDWR, 0},
        {"self_shut_wr", SHUT_WR, 0},
    };
    for (size_t i = 0; i < ARRAY_LEN(rd_cases); i++) {
        sockpair(&a, &b);
        o = new_op(OP_READ, a);
        memset(&tc, 0, sizeof(tc));
        tc.fd = rd_cases[i].on_peer ? b : a;
        tc.delay_ms = 200;
        tc.arg = rd_cases[i].how;
        t = start_thread(th_close, &tc);
        r = timed_op(o, 1500, &el);
        emit(ID("blk.read.%s", rd_cases[i].name), "%s", blk_res(r, el, 150));
        join_thread(t, &tc, 1000);
        close(a);
        close(b);
    }

    /* write on a full buffer */
    struct {
        const char *name;
        int how;
        int on_peer;
    } wr_cases[] = {
        {"peer_close", -1, 1},
        {"peer_shut_rd", SHUT_RD, 1},
        {"peer_shut_rdwr", SHUT_RDWR, 1},
        {"peer_shut_wr", SHUT_WR, 1},
        {"self_shut_wr", SHUT_WR, 0},
        {"self_shut_rdwr", SHUT_RDWR, 0},
        {"self_shut_rd", SHUT_RD, 0},
    };
    for (size_t i = 0; i < ARRAY_LEN(wr_cases); i++) {
        sockpair(&a, &b);
        fill_buf(a);
        o = new_op(OP_WRITE, a);
        o->len = 1;
        o->buf[0] = 'w';
        memset(&tc, 0, sizeof(tc));
        tc.fd = wr_cases[i].on_peer ? b : a;
        tc.delay_ms = 200;
        tc.arg = wr_cases[i].how;
        t = start_thread(th_close, &tc);
        r = timed_op(o, 1500, &el);
        emit(ID("blk.write_full.%s", wr_cases[i].name), "%s", blk_res(r, el, 150));
        join_thread(t, &tc, 1000);
        close(a);
        close(b);
    }

    /* write on a full buffer woken by a read of the peer */
    sockpair(&a, &b);
    fill_buf(a);
    o = new_op(OP_WRITE, a);
    o->len = 1;
    memset(&tc, 0, sizeof(tc));
    tc.fd = b;
    tc.delay_ms = 200;
    tc.arg = 100000;
    t = start_thread(th_drain, &tc);
    r = timed_op(o, 3000, &el);
    emit("blk.write_full.woken_by_peer_read", "%s", blk_res(r, el, 150));
    close(a);
    join_thread(t, &tc, 1000);
    close(b);

    /* accept */
    {
        uaddr_t ad;
        int l = listener(1, 4, &ad);
        o = new_op(OP_ACCEPT, l);
        memset(&tc, 0, sizeof(tc));
        tc.addr = ad;
        tc.delay_ms = 200;
        t = start_thread(th_connect, &tc);
        r = timed_op(o, 3000, &el);
        emit("blk.accept.woken_by_connect", "%s", r >= 0 ? blk_res(0, el, 150) : blk_res(r, el,
                150));
        closefd((int)r);
        join_thread(t, &tc, 1000);
        close(tc.fd);
        close(l);
    }
    for (int how = 0; how < 3; how++) {
        uaddr_t ad;
        int l = listener(1, 4, &ad);
        o = new_op(OP_ACCEPT, l);
        memset(&tc, 0, sizeof(tc));
        tc.fd = l;
        tc.delay_ms = 200;
        tc.arg = how;
        t = start_thread(th_close, &tc);
        r = timed_op(o, 1500, &el);
        static const char *hn[] = {"shut_rd", "shut_wr", "shut_rdwr"};
        emit(ID("blk.accept.%s", hn[how]), "shutdown=%s accept=%s", rs(tc.result), blk_res(r, el,
                150));
        join_thread(t, &tc, 1000);
        /* afterwards: does the listener still accept? */
        int c = usock();
        set_nb(c, 1);
        long cr = SYS(CONNECT(c, ad));
        int s = -1;
        if (r == BLOCKED) {
            msleep(100);
        } else {
            set_nb(l, 1);
            s = accept(l, NULL, NULL);
        }
        emit(ID("blk.accept.%s.after", hn[how]), "connect=%s accept=%s", rs(cr),
             r == BLOCKED ? (atomic_load(&o->done) ? "blocked_one_woke_on_connect" : "still_blocked")
             : rs(s >= 0 ? 0 : -errno));
        closefd(s);
        close(c);
        close(l);
    }

    /* poll() woken */
    sockpair(&a, &b);
    o = new_op(OP_POLLIN, a);
    o->arg = POLLIN | POLLRDHUP;
    o->flags = 2000;
    memset(&tc, 0, sizeof(tc));
    tc.fd = b;
    tc.delay_ms = 200;
    tc.arg = 1;
    memcpy(tc.data, "x", 1);
    t = start_thread(th_write, &tc);
    r = timed_op(o, 3500, &el);
    emit("blk.poll.woken_by_write", "%s ev=%s", blk_res(r, el, 150),
         r == BLOCKED ? "-" : evs(o->extra));
    join_thread(t, &tc, 1000);
    close(a);
    close(b);
    int how_list[] = {-1, SHUT_WR, SHUT_RDWR};
    const char *how_names[] = {"peer_close", "peer_shut_wr", "peer_shut_rdwr"};
    for (int i = 0; i < 3; i++) {
        sockpair(&a, &b);
        o = new_op(OP_POLLIN, a);
        o->arg = POLLIN | POLLRDHUP;
        o->flags = 2000;
        memset(&tc, 0, sizeof(tc));
        tc.fd = b;
        tc.delay_ms = 200;
        tc.arg = how_list[i];
        t = start_thread(th_close, &tc);
        r = timed_op(o, 3500, &el);
        emit(ID("blk.poll.%s", how_names[i]), "%s ev=%s", blk_res(r, el, 150),
             r == BLOCKED ? "-" : evs(o->extra));
        join_thread(t, &tc, 1000);
        close(a);
        close(b);
    }
    int self_how[] = {SHUT_RD, SHUT_WR, SHUT_RDWR};
    const char *self_names[] = {"self_shut_rd", "self_shut_wr", "self_shut_rdwr"};
    for (int i = 0; i < 3; i++) {
        sockpair(&a, &b);
        o = new_op(OP_POLLIN, a);
        o->arg = POLLIN | POLLRDHUP;
        o->flags = 1500;
        memset(&tc, 0, sizeof(tc));
        tc.fd = a;
        tc.delay_ms = 200;
        tc.arg = self_how[i];
        t = start_thread(th_close, &tc);
        r = timed_op(o, 3500, &el);
        emit(ID("blk.poll.%s", self_names[i]), "%s ev=%s", blk_res(r, el, 150),
             r == BLOCKED ? "-" : evs(o->extra));
        join_thread(t, &tc, 1000);
        close(a);
        close(b);
    }
}

/* ------------------------------------------------------------------ */
/* c14: SO_RCVTIMEO / SO_SNDTIMEO                                      */
/* ------------------------------------------------------------------ */

static long set_tmo(int fd, int opt, int ms) {
    struct timeval tv = {ms / 1000, (ms % 1000) * 1000};
    return SYS(setsockopt(fd, SOL_SOCKET, opt, &tv, sizeof(tv)));
}

CHECK(c14_timeouts) {
    int a, b;
    long long el;
    long r;
    op_t *o;
    const int T = 300;

    sockpair(&a, &b);
    emit("tmo.set_rcvtimeo", "%s", rs(set_tmo(a, SO_RCVTIMEO, T)));
    {
        struct timeval tv = {99, 99};
        socklen_t l = sizeof(tv);
        long gr = SYS(getsockopt(a, SOL_SOCKET, SO_RCVTIMEO, &tv, &l));
        emit("tmo.get_rcvtimeo", "%s sec=%ld usec_ms=%ld len=%u", rs(gr), (long)tv.tv_sec,
             (long)tv.tv_usec / 1000, (unsigned)l);
    }
    emit("tmo.set_sndtimeo", "%s", rs(set_tmo(a, SO_SNDTIMEO, T)));
    {
        struct timeval tv = {99, 99};
        socklen_t l = sizeof(tv);
        long gr = SYS(getsockopt(a, SOL_SOCKET, SO_SNDTIMEO, &tv, &l));
        emit("tmo.get_sndtimeo", "%s sec=%ld usec_ms=%ld len=%u", rs(gr), (long)tv.tv_sec,
             (long)tv.tv_usec / 1000, (unsigned)l);
    }
    {
        struct timeval bad = {0, 1000000};
        emit("tmo.set_usec_too_big", "%s", rs(SYS(setsockopt(a, SOL_SOCKET, SO_RCVTIMEO, &bad,
                                              sizeof(bad)))));
        struct timeval neg = {-1, 0};
        emit("tmo.set_negative", "%s", rs(SYS(setsockopt(a, SOL_SOCKET, SO_RCVTIMEO, &neg,
                                              sizeof(neg)))));
        emit("tmo.set_short_optlen", "%s", rs(SYS(setsockopt(a, SOL_SOCKET, SO_RCVTIMEO, &bad,
                                              4))));
    }
    set_tmo(a, SO_RCVTIMEO, T);

    o = new_op(OP_READ, a);
    r = timed_op(o, 2500, &el);
    emit("tmo.read", "%s %s", r == BLOCKED ? "BLOCKED" : rs(r),
         r == BLOCKED ? "-" : tclass(el, T));
    o = new_op(OP_RECV, a);
    r = timed_op(o, 2500, &el);
    emit("tmo.recv", "%s %s", r == BLOCKED ? "BLOCKED" : rs(r),
         r == BLOCKED ? "-" : tclass(el, T));
    o = new_op(OP_RECVMSG, a);
    {
        /* recvmsg: reuse the control variant without a control buffer is not possible here */
        o->kind = OP_READ;
    }
    o = new_op(OP_RECV, a);
    o->flags = MSG_WAITALL;
    o->len = 8;
    r = timed_op(o, 2500, &el);
    emit("tmo.recv_waitall", "%s %s", r == BLOCKED ? "BLOCKED" : rs(r),
         r == BLOCKED ? "-" : tclass(el, T));
    {
        /* data arrives before the timeout (fresh pair: a read that is still blocked above would take the data) */
        int a2, b2;
        sockpair(&a2, &b2);
        set_tmo(a2, SO_RCVTIMEO, T);
        tctx_t tc;
        memset(&tc, 0, sizeof(tc));
        tc.fd = b2;
        tc.delay_ms = 100;
        tc.arg = 2;
        memcpy(tc.data, "ok", 2);
        pthread_t t = start_thread(th_write, &tc);
        o = new_op(OP_READ, a2);
        r = timed_op(o, 2500, &el);
        emit("tmo.read_data_before_timeout", "%s", r == BLOCKED ? "BLOCKED" : rs(r));
        join_thread(t, &tc, 1000);
        close(a2);
        close(b2);
    }
    {
        /* partial data at timeout with WAITALL */
        int a2, b2;
        sockpair(&a2, &b2);
        set_tmo(a2, SO_RCVTIMEO, T);
        write(b2, "abc", 3);
        o = new_op(OP_RECV, a2);
        o->flags = MSG_WAITALL;
        o->len = 8;
        r = timed_op(o, 2500, &el);
        emit("tmo.recv_waitall_partial", "%s %s", r == BLOCKED ? "BLOCKED" : rs(r),
             r == BLOCKED ? "-" : tclass(el, T));
        close(a2);
        close(b2);
    }
    {
        /* a timeout of 0 means wait forever: with a closed peer it returns EOF at once */
        int a2, b2;
        sockpair(&a2, &b2);
        set_tmo(a2, SO_RCVTIMEO, 0);
        close(b2);
        o = new_op(OP_READ, a2);
        r = timed_op(o, 1000, &el);
        emit("tmo.zero_then_eof", "%s", r == BLOCKED ? "BLOCKED" : rs(r));
        close(a2);
    }
    close(a);

    /* write on a full buffer with SO_SNDTIMEO */
    sockpair(&a, &b);
    fill_buf(a);
    set_tmo(a, SO_SNDTIMEO, T);
    o = new_op(OP_WRITE, a);
    o->len = 1;
    r = timed_op(o, 2500, &el);
    emit("tmo.write_full", "%s %s", r == BLOCKED ? "BLOCKED" : rs(r),
         r == BLOCKED ? "-" : tclass(el, T));
    o = new_op(OP_SEND, a);
    o->len = 1;
    r = timed_op(o, 2500, &el);
    emit("tmo.send_full", "%s %s", r == BLOCKED ? "BLOCKED" : rs(r),
         r == BLOCKED ? "-" : tclass(el, T));
    close(a);
    close(b);

    /* accept with SO_RCVTIMEO on the listener */
    {
        uaddr_t ad;
        int l = listener(1, 2, &ad);
        emit("tmo.listener_set_rcvtimeo", "%s", rs(set_tmo(l, SO_RCVTIMEO, T)));
        o = new_op(OP_ACCEPT, l);
        r = timed_op(o, 2500, &el);
        emit("tmo.accept", "%s %s", r == BLOCKED ? "BLOCKED" : rs(r),
             r == BLOCKED ? "-" : tclass(el, T));
        close(l);
    }
    /* connect on a full backlog with SO_SNDTIMEO */
    {
        uaddr_t ad;
        int l = listener(1, 1, &ad);
        int fds[16], err;
        int n = connect_until_full(&ad, 8, fds, &err);
        int c = usock();
        set_tmo(c, SO_SNDTIMEO, T);
        o = new_op(OP_CONNECT, c);
        o->addr = ad;
        r = timed_op(o, 2500, &el);
        emit("tmo.connect_full_backlog", "%s %s", r == BLOCKED ? "BLOCKED" : rs(r),
             r == BLOCKED ? "-" : tclass(el, T));
        close(c);
        for (int k = 0; k < n; k++) {
            close(fds[k]);
        }
        close(l);
    }
    /* options of the listener and accepted socket */
    {
        uaddr_t ad;
        int l = listener(1, 2, &ad);
        set_tmo(l, SO_RCVTIMEO, T);
        int c = client_to(&ad);
        int s = accept(l, NULL, NULL);
        o = new_op(OP_READ, s);
        r = timed_op(o, 1200, &el);
        emit("tmo.accepted_inherits_rcvtimeo", "%s",
             r == BLOCKED ? "BLOCKED(not inherited)" : rs(r));
        close(c);
        close(s);
        close(l);
    }
    /* O_NONBLOCK wins over a timeout */
    sockpair(&a, &b);
    set_tmo(a, SO_RCVTIMEO, T);
    emit("tmo.nonblock_read", "%s", nb_read_s(a, 4));
    close(a);
    close(b);
}

/* ------------------------------------------------------------------ */
/* c13: signals, close from another thread, several accepting threads  */
/* ------------------------------------------------------------------ */

static void on_usr2(int s) {
    (void)s;
}

typedef struct {
    int kind; /* 0 read, 1 accept, 2 write on a full buffer, 3 connect to a full backlog */
    int fd;
    uaddr_t addr;
    long res;
    atomic_int started;
    atomic_int done;
} sigop_t;

static void *th_sigop(void *p) {
    sigop_t *s = p;
    char buf[8];
    atomic_store(&s->started, 1);
    switch (s->kind) {
        case 0:
            s->res = SYS(read(s->fd, buf, sizeof(buf)));
            break;
        case 1:
            s->res = SYS(accept(s->fd, NULL, NULL));
            break;
        case 2:
            s->res = SYS(write(s->fd, "w", 1));
            break;
        case 3:
            s->res = SYS(connect(s->fd, (struct sockaddr *)&s->addr.sa, s->addr.len));
            break;
    }
    atomic_store(&s->done, 1);
    return NULL;
}

CHECK(c13_signals) {
    for (int restart = 0; restart < 2; restart++) {
        struct sigaction sa, old;
        memset(&sa, 0, sizeof(sa));
        sa.sa_handler = on_usr2;
        sigemptyset(&sa.sa_mask);
        sa.sa_flags = restart ? SA_RESTART : 0;
        sigaction(SIGUSR2, &sa, &old);
        const char *rn = restart ? "sa_restart" : "no_restart";
        for (int kind = 0; kind < 4; kind++) {
            static const char *kn[] = {"read", "accept", "write_full", "connect_full_backlog"};
            int a = -1, b = -1, l = -1;
            uaddr_t ad;
            sigop_t st;
            memset(&st, 0, sizeof(st));
            st.kind = kind;
            int fds[16], nf = 0, err;
            if (kind == 0 || kind == 2) {
                sockpair(&a, &b);
                st.fd = a;
                if (kind == 2) {
                    fill_buf(a);
                }
            } else if (kind == 1) {
                l = listener(1, 4, &ad);
                st.fd = l;
            } else {
                l = listener(1, 1, &ad);
                nf = connect_until_full(&ad, 8, fds, &err);
                st.fd = usock();
                st.addr = ad;
            }
            pthread_t t;
            pthread_create(&t, NULL, th_sigop, &st);
            while (!atomic_load(&st.started)) {
                msleep(2);
            }
            msleep(150);
            pthread_kill(t, SIGUSR2);
            msleep(200);
            int done_after_signal = atomic_load(&st.done);
            long res_after_signal = st.res;
            /* let the call finish if it was restarted */
            if (!done_after_signal) {
                if (kind == 0) {
                    write(b, "d", 1);
                } else if (kind == 1) {
                    int c = client_to(&ad);
                    (void)c;
                } else if (kind == 2) {
                    drain_all(b);
                } else {
                    int s = accept(l, NULL, NULL);
                    closefd(s);
                }
            }
            long long t0 = now_ms();
            while (!atomic_load(&st.done) && now_ms() - t0 < 1500) {
                msleep(5);
            }
            if (atomic_load(&st.done)) {
                pthread_join(t, NULL);
            } else {
                pthread_detach(t);
            }
            emit(ID("sig.%s.%s", kn[kind], rn), "%s",
                 done_after_signal ? rs(res_after_signal) : (atomic_load(&st.done) ?
                         "restarted_and_completed" : "STILL_BLOCKED"));
            for (int k = 0; k < nf; k++) {
                close(fds[k]);
            }
            closefd(a);
            closefd(b);
            closefd(l);
        }
        sigaction(SIGUSR2, &old, NULL);
    }
}

CHECK(c13_threads) {
    /* two threads accept, one connection arrives: exactly one accept returns */
    {
        uaddr_t ad;
        int l = listener(1, 4, &ad);
        op_t *o1 = new_op(OP_ACCEPT, l);
        op_t *o2 = new_op(OP_ACCEPT, l);
        pthread_t t1, t2;
        atomic_store(&o1->done, 0);
        atomic_store(&o2->done, 0);
        pthread_create(&t1, NULL, op_thread, o1);
        pthread_create(&t2, NULL, op_thread, o2);
        pthread_detach(t1);
        pthread_detach(t2);
        msleep(200);
        int c1 = client_to(&ad);
        msleep(300);
        emit("thr.two_acceptors_one_connect", "finished=%d",
             atomic_load(&o1->done) + atomic_load(&o2->done));
        int c2 = client_to(&ad);
        msleep(300);
        emit("thr.two_acceptors_two_connects", "finished=%d",
             atomic_load(&o1->done) + atomic_load(&o2->done));
        close(c1);
        close(c2);
        close(l);
    }
    /* the fd is closed while another thread blocks in read(): the read goes on */
    {
        int a, b;
        sockpair(&a, &b);
        op_t *o = new_op(OP_READ, a);
        pthread_t t;
        atomic_store(&o->done, 0);
        pthread_create(&t, NULL, op_thread, o);
        pthread_detach(t);
        msleep(200);
        close(a);
        msleep(100);
        emit("thr.close_fd_while_reading.still_blocked", "%d", !atomic_load(&o->done));
        write(b, "x", 1);
        msleep(300);
        emit("thr.close_fd_while_reading.data_wakes", "done=%d res=%s", atomic_load(&o->done),
             atomic_load(&o->done) ? rs(o->res) : "-");
        close(b);
    }
    /* the listening fd is closed while another thread blocks in accept() */
    {
        uaddr_t ad;
        int l = listener(1, 4, &ad);
        op_t *o = new_op(OP_ACCEPT, l);
        pthread_t t;
        atomic_store(&o->done, 0);
        pthread_create(&t, NULL, op_thread, o);
        pthread_detach(t);
        msleep(200);
        close(l);
        msleep(100);
        int c = usock();
        long r = SYS(CONNECT(c, ad));
        msleep(300);
        emit("thr.close_listener_while_accepting", "connect=%s accept_done=%d", rs(r),
             atomic_load(&o->done));
        close(c);
    }
    /* a peer closing wakes a reader that blocks in a thread of another process-like situation: many readers */
    {
        int a, b;
        sockpair(&a, &b);
        enum { N = 6 };
        op_t *ops[N];
        for (int i = 0; i < N; i++) {
            ops[i] = new_op(OP_READ, a);
            pthread_t t;
            atomic_store(&ops[i]->done, 0);
            pthread_create(&t, NULL, op_thread, ops[i]);
            pthread_detach(t);
        }
        msleep(200);
        write(b, "ab", 2);
        msleep(300);
        int fin = 0;
        for (int i = 0; i < N; i++) {
            fin += atomic_load(&ops[i]->done);
        }
        emit("thr.six_readers_two_bytes", "finished=%d", fin);
        close(b);
        msleep(300);
        fin = 0;
        for (int i = 0; i < N; i++) {
            fin += atomic_load(&ops[i]->done);
        }
        emit("thr.six_readers_after_peer_close", "finished=%d", fin);
        close(a);
    }
}

/* ------------------------------------------------------------------ */
/* c07: basic I/O                                                      */
/* ------------------------------------------------------------------ */

static unsigned long long fnv(const unsigned char *p, size_t n, unsigned long long h) {
    for (size_t i = 0; i < n; i++) {
        h ^= p[i];
        h *= 1099511628211ULL;
    }
    return h;
}

typedef struct {
    int fd;
    size_t total;
    size_t chunk;
    long result;
    int err;
    atomic_int done;
} bulk_t;

static void *th_bulk_write(void *p) {
    bulk_t *b = p;
    unsigned char *buf = malloc(b->chunk);
    size_t pos = 0;
    while (pos < b->total) {
        size_t n = b->total - pos < b->chunk ? b->total - pos : b->chunk;
        for (size_t i = 0; i < n; i++) {
            buf[i] = (unsigned char)(pos + i);
        }
        size_t off = 0;
        while (off < n) {
            ssize_t w = write(b->fd, buf + off, n - off);
            if (w < 0) {
                b->err = errno;
                goto out;
            }
            if ((size_t)w < n - off) {
                /* a short write of a blocking socket */
                b->result++;
            }
            off += (size_t)w;
        }
        pos += n;
    }
out:
    free(buf);
    atomic_store(&b->done, 1);
    return NULL;
}

static const char *fionread_s(int fd, int req) {
    static __thread char b[8][48];
    static __thread int i;
    char *p = b[i++ & 7];
    int n = -1;
    long r = SYS(ioctl(fd, req, &n));
    snprintf(p, 48, "%s n=%d", rs(r), n);
    return p;
}

CHECK(c07_io) {
    int a, b;
    char buf[256];

    sockpair(&a, &b);
    emit("io.write_read", "%s", rs(SYS(write(a, "hello", 5))));
    emit("io.read_partial", "%s", nb_read_s(b, 2));
    emit("io.read_rest", "%s", nb_read_s(b, 100));
    emit("io.read_empty_nb", "%s", nb_read_s(b, 100));
    emit("io.read_zero_len_empty", "%s", rs(nb_read(b, buf, 0)));
    write(a, "xyz", 3);
    emit("io.read_zero_len_data", "%s", rs(nb_read(b, buf, 0)));
    emit("io.write_zero_len", "%s", rs(SYS(write(a, buf, 0))));
    emit("io.write_zero_len_send", "%s", rs(SYS(send(a, buf, 0, 0))));
    emit("io.write_null_zero", "%s", rs(SYS(write(a, NULL, 0))));
    emit("io.write_efault", "%s", rs(SYS(write(a, (void *)1, 10))));
    emit("io.read_efault", "%s", rs(nb_read(b, (void *)1, 10)));
    emit("io.read_after_efault", "%s", nb_read_s(b, 100));

    /* the bytes of a failed write must not arrive */
    emit("io.send_efault", "%s", rs(SYS(send(a, (void *)1, 10, 0))));
    emit("io.read_after_send_efault", "%s", nb_read_s(b, 100));

    /* vectored */
    {
        struct iovec iov[3] = {{"ab", 2}, {"", 0}, {"cdef", 4}};
        emit("io.writev", "%s", rs(SYS(writev(a, iov, 3))));
        char r1[3], r2[10];
        struct iovec rv[2] = {{r1, 3}, {r2, 10}};
        emit("io.readv", "%s", rs(SYS(readv(b, rv, 2))));
        emit("io.readv.data", "%.3s|%.3s", r1, r2);
        emit("io.writev_zero_iov", "%s", rs(SYS(writev(a, iov, 0))));
        struct iovec empty[2] = {{"", 0}, {"", 0}};
        emit("io.writev_all_empty", "%s", rs(SYS(writev(a, empty, 2))));
        emit("io.readv_zero_iov", "%s", rs(nb_read(b, buf, 0)));
    }

    /* FIONREAD */
    {
        int n = -1;
        write(a, "1234567", 7);
        emit("io.fionread.after_write", "%s", fionread_s(b, FIONREAD));
        nb_read(b, buf, 3);
        emit("io.fionread.after_read3", "%s", fionread_s(b, FIONREAD));
        emit("io.fionread.sender_side", "%s", fionread_s(a, FIONREAD));
        n = -1;
        emit("io.siocoutq.sender", "%s", rs(SYS(ioctl(a, SIOCOUTQ, &n))));
        drain_all(b);
        emit("io.fionread.empty", "%s", fionread_s(b, FIONREAD));
        int fd = usock();
        n = -1;
        emit("io.fionread.idle", "%s", fionread_s(fd, FIONREAD));
        uaddr_t ad = nabs("fionread");
        BIND(fd, ad);
        listen(fd, 1);
        n = -1;
        emit("io.fionread.listening", "%s", fionread_s(fd, FIONREAD));
        close(fd);
        shutdown(b, SHUT_RD);
        write(a, "zz", 2);
        n = -1;
        emit("io.fionread.after_self_shut_rd", "%s", fionread_s(b, FIONREAD));
    }
    close(a);
    close(b);

    /* blocking write of a large buffer with a reader */
    {
        size_t big = 1024 * 1024;
        char *data = malloc(big);
        memset(data, 'B', big);
        sockpair(&a, &b);
        tctx_t tc;
        memset(&tc, 0, sizeof(tc));
        tc.fd = b;
        tc.delay_ms = 100;
        tc.arg = (int)big;
        pthread_t t = start_thread(th_drain, &tc);
        op_t *o = new_op(OP_WRITEV_BIG, a);
        o->big = data;
        o->len = big;
        long long el;
        long r = timed_op(o, 8000, &el);
        emit("io.blocking_write_1MB_with_reader", "%s",
             r == BLOCKED ? "BLOCKED" : (r == (long)big ? "full" : (r > 0 ? "PARTIAL" : rs(r))));
        emit("info.io.blocking_write_1MB_with_reader.bytes", "%s", rs(r));
        join_thread(t, &tc, 3000);
        close(a);
        close(b);
        free(data);
    }
    /* non-blocking write of a large buffer */
    {
        size_t big = 1024 * 1024;
        char *data = malloc(big);
        memset(data, 'N', big);
        sockpair(&a, &b);
        set_nb(a, 1);
        long r = SYS(write(a, data, big));
        emit("io.nonblocking_write_1MB.partial", "%s", r > 0 &&
             r < (long)big ? "partial" : rs(r));
        emit("info.io.nonblocking_write_1MB.bytes", "%ld", r);
        long r2 = SYS(write(a, data, 10));
        emit("io.nonblocking_write_then_more", "%s", rs(r2));
        long drained = drain_all(b);
        emit("io.nonblocking_write_1MB.drained_equals_written", "%d", drained == r);
        emit("io.nonblocking_write_after_drain", "%s", rs(SYS(write(a, data, 10))));
        close(a);
        close(b);
        free(data);
    }
    /* bulk transfer integrity */
    {
        sockpair(&a, &b);
        bulk_t bk;
        memset(&bk, 0, sizeof(bk));
        bk.fd = a;
        bk.total = 8 * 1024 * 1024;
        bk.chunk = 100000;
        atomic_store(&bk.done, 0);
        pthread_t t;
        pthread_create(&t, NULL, th_bulk_write, &bk);
        unsigned char *rb = malloc(65536);
        size_t got = 0;
        unsigned char ctr = 0;
        int bad = 0;
        long long t0 = now_ms();
        while (got < bk.total && now_ms() - t0 < 20000) {
            struct pollfd pf = {b, POLLIN, 0};
            if (poll(&pf, 1, 2000) <= 0) {
                break;
            }
            ssize_t n = read(b, rb, 65536);
            if (n <= 0) {
                break;
            }
            for (ssize_t i = 0; i < n; i++) {
                if (rb[i] != ctr++) {
                    bad++;
                }
            }
            got += (size_t)n;
        }
        long long el = now_ms() - t0;
        emit("io.bulk_8MB.received", "%s bad=%d", got == bk.total ? "all" : "SHORT", bad);
        emit("info.io.bulk_8MB.ms", "%lld shortwrites=%ld", el, bk.result);
        close(b);
        pthread_join(t, NULL);
        close(a);
        free(rb);
    }
    /* many small messages are not merged or split wrongly */
    {
        sockpair(&a, &b);
        for (int i = 0; i < 100; i++) {
            char m[8];
            int n = snprintf(m, sizeof(m), "%03d,", i);
            write(a, m, n);
        }
        char all[512];
        int total = 0;
        for (;;) {
            long r = nb_read(b, all + total, sizeof(all) - total);
            if (r <= 0) {
                break;
            }
            total += (int)r;
        }
        emit("io.small_messages_stream", "total=%d first=%.4s last=%.4s", total, all,
             all + total - 4);
        close(a);
        close(b);
    }
    /* sendfile from a file to a socket, a file to a socket via pwrite-less copy */
    {
        const char *path = newpath("sendfile");
        int ff = open(path, O_CREAT | O_RDWR | O_TRUNC, 0644);
        char data[100];
        for (int i = 0; i < 100; i++) {
            data[i] = (char)('A' + i % 26);
        }
        write(ff, data, 100);
        sockpair(&a, &b);
        off_t off = 10;
        emit("io.sendfile_to_socket", "%s off=%ld", rs(SYS(sendfile(a, ff, &off, 50))),
             (long)off);
        emit("io.sendfile_to_socket.read", "%s", nb_read_s(b, 64));
        close(ff);
        close(a);
        close(b);
    }
    /* read/write on idle sockets */
    {
        int fd = usock();
        emit("io.idle.read", "%s", nb_read_s(fd, 4));
        emit("io.idle.write", "%s", nb_write_s(fd, "x"));
        emit("io.idle.recv", "%s", rs(SYS(recv(fd, buf, 4, 0))));
        emit("io.idle.send", "%s", rs(SYS(send(fd, "x", 1, MSG_NOSIGNAL))));
        emit("io.idle.read_zero", "%s", rs(nb_read(fd, buf, 0)));
        emit("io.idle.write_zero", "%s", rs(SYS(write(fd, buf, 0))));
        close(fd);
    }
}

/* ------------------------------------------------------------------ */
/* c08: send/recv flags, addresses in send/recvmsg                     */
/* ------------------------------------------------------------------ */

static const char *trecv_s(int fd, size_t n, int flags, int tmo) {
    static __thread char b[8][96];
    static __thread int i;
    char *p = b[i++ & 7];
    op_t *o = new_op(OP_RECV, fd);
    o->len = n < sizeof(o->buf) ? n : sizeof(o->buf);
    o->flags = flags;
    long long el;
    long r = timed_op(o, tmo, &el);
    if (r == BLOCKED) {
        snprintf(p, 96, "BLOCKED");
    } else if (r > 0) {
        int k = snprintf(p, 96, "%ld:", r);
        for (long j = 0; j < r && j < 8 && k < 90; j++) {
            k += snprintf(p + k, 96 - k, "%c", (o->buf[j] >= 32 &&
                                                o->buf[j] < 127) ? o->buf[j] : '.');
        }
    } else {
        snprintf(p, 96, "%s", rs(r));
    }
    if (r != BLOCKED) {
        free(o);
    }
    return p;
}

CHECK(c08_msgflags) {
    int a, b;
    long long el;
    long r;
    op_t *o;
    char buf[64];

    /* MSG_DONTWAIT on a blocking socket */
    sockpair(&a, &b);
    o = new_op(OP_RECV, a);
    o->flags = MSG_DONTWAIT;
    r = timed_op(o, 800, &el);
    emit("flags.recv_dontwait_empty_blocking_socket", "%s", r == BLOCKED ? "BLOCKED" : rs(r));
    fill_buf(a);
    o = new_op(OP_SEND, a);
    o->flags = MSG_DONTWAIT;
    o->len = 1;
    r = timed_op(o, 800, &el);
    emit("flags.send_dontwait_full_blocking_socket", "%s", r == BLOCKED ? "BLOCKED" : rs(r));
    close(a);
    close(b);

    /* MSG_PEEK: the calls that block when the flags are ignored run in helper threads */
    sockpair(&a, &b);
    write(a, "abcdef", 6);
    emit("flags.recv_peek", "%s", trecv_s(b, 3, MSG_PEEK, 700));
    emit("flags.recv_after_peek", "%s", trecv_s(b, 10, MSG_DONTWAIT, 700));
    close(a);
    close(b);
    sockpair(&a, &b);
    emit("flags.recv_peek_empty", "%s", trecv_s(b, 10, MSG_PEEK | MSG_DONTWAIT, 700));
    close(a);
    close(b);
    sockpair(&a, &b);
    write(a, "abcdef", 6);
    {
        trecv_s(b, 4, MSG_PEEK, 700);
        emit("flags.peek_does_not_consume_fionread", "%s", fionread_s(b, FIONREAD));
        emit("flags.recv_peek_larger_than_data", "%s", trecv_s(b, 64, MSG_PEEK, 700));
    }
    close(a);
    close(b);

    /* MSG_WAITALL */
    sockpair(&a, &b);
    write(a, "12345", 5);
    {
        tctx_t tc;
        memset(&tc, 0, sizeof(tc));
        tc.fd = a;
        tc.delay_ms = 200;
        tc.arg = 5;
        memcpy(tc.data, "67890", 5);
        pthread_t t = start_thread(th_write, &tc);
        o = new_op(OP_RECV, b);
        o->flags = MSG_WAITALL;
        o->len = 10;
        r = timed_op(o, 3000, &el);
        emit("flags.recv_waitall", "%s %s", r == BLOCKED ? "BLOCKED" : rs(r),
             r == BLOCKED ? "-" : (el >= 150 ? "waited" : "immediate"));
        join_thread(t, &tc, 1000);
    }
    close(a);
    close(b);
    /* MSG_WAITALL with EOF in the middle */
    sockpair(&a, &b);
    write(a, "xy", 2);
    shutdown(a, SHUT_WR);
    o = new_op(OP_RECV, b);
    o->flags = MSG_WAITALL;
    o->len = 10;
    r = timed_op(o, 2000, &el);
    emit("flags.recv_waitall_eof_in_middle", "%s", r == BLOCKED ? "BLOCKED" : rs(r));
    close(a);
    close(b);

    /* MSG_OOB */
    sockpair(&a, &b);
    emit("flags.send_oob", "%s", rs(SYS(send(a, "o", 1, MSG_OOB))));
    emit("flags.recv_oob", "%s", rs(SYS(recv(b, buf, 1, MSG_OOB | MSG_DONTWAIT))));
    emit("flags.recv_normal_after_oob", "%s", nb_read_s(b, 4));
    emit("flags.sockatmark", "%s", ({
        int mark = -1;
        long rr = SYS(ioctl(b, SIOCATMARK, &mark));
        static __thread char tmp[32];
        snprintf(tmp, sizeof(tmp), "%s mark=%d", rs(rr), mark);
        tmp;
    }));
    close(a);
    close(b);

    /* unknown / ignored flags */
    sockpair(&a, &b);
    write(a, "qq", 2);
    emit("flags.recv_flags_msg_trunc", "%s", rs(SYS(recv(b, buf, 1,
            MSG_TRUNC | MSG_DONTWAIT))));
    emit("flags.recv_flags_cmsg_cloexec", "%s", rs(SYS(recv(b, buf, 1,
            MSG_CMSG_CLOEXEC | MSG_DONTWAIT))));
    drain_all(b);
    emit("flags.recv_flags_errqueue", "%s", trecv_s(b, 1, MSG_ERRQUEUE | MSG_DONTWAIT, 700));
    emit("flags.send_flags_more", "%s", rs(SYS(send(a, "m", 1, MSG_MORE))));
    emit("flags.send_flags_confirm", "%s", rs(SYS(send(a, "m", 1, MSG_CONFIRM))));
    emit("flags.send_flags_bogus_bit", "%s", rs(SYS(send(a, "m", 1, 0x10000000))));
    close(a);
    close(b);

    /* addresses in sendto / recvfrom / sendmsg / recvmsg of a connected socket */
    {
        uaddr_t ad;
        int l = listener(1, 4, &ad);
        uaddr_t ca = nabs("named_client");
        int c = usock();
        BIND(c, ca);
        CONNECT(c, ad);
        int s = accept(l, NULL, NULL);
        emit("addr.sendto_with_addr_on_connected", "%s", rs(SYS(sendto(c, "x", 1, MSG_NOSIGNAL,
                (struct sockaddr *)&ad.sa, ad.len))));
        emit("addr.sendto_with_addr_on_connected.same_addr_as_peer", "%s", rs(SYS(sendto(s, "x",
                1, MSG_NOSIGNAL, (struct sockaddr *)&ca.sa, ca.len))));
        {
            struct sockaddr_un from;
            socklen_t fl = sizeof(from);
            memset(&from, 0, sizeof(from));
            drain_all(s);
            write(c, "r", 1);
            long rr = SYS(recvfrom(s, buf, 8, 0, (struct sockaddr *)&from, &fl));
            emit("addr.recvfrom_named_peer", "%s addrlen=%u", rs(rr), (unsigned)fl);
        }
        {
            write(s, "r", 1);
            struct sockaddr_un from;
            socklen_t fl = sizeof(from);
            long rr = SYS(recvfrom(c, buf, 8, 0, (struct sockaddr *)&from, &fl));
            emit("addr.recvfrom_unnamed_peer_listener", "%s addrlen=%u", rs(rr), (unsigned)fl);
        }
        {
            /* recvmsg: msg_namelen after the call */
            write(c, "m", 1);
            struct sockaddr_un from;
            memset(&from, 0, sizeof(from));
            struct iovec iov = {buf, 8};
            struct msghdr m;
            memset(&m, 0, sizeof(m));
            m.msg_name = &from;
            m.msg_namelen = sizeof(from);
            m.msg_iov = &iov;
            m.msg_iovlen = 1;
            long rr = SYS(recvmsg(s, &m, 0));
            emit("addr.recvmsg_named_peer", "%s namelen=%u flags=0x%x", rs(rr),
                 (unsigned)m.msg_namelen, m.msg_flags);
        }
        {
            struct sockaddr_un to = ad.sa;
            struct iovec iov = {"s", 1};
            struct msghdr m;
            memset(&m, 0, sizeof(m));
            m.msg_name = &to;
            m.msg_namelen = ad.len;
            m.msg_iov = &iov;
            m.msg_iovlen = 1;
            emit("addr.sendmsg_with_name_on_connected", "%s", rs(SYS(sendmsg(c, &m, MSG_NOSIGNAL))));
        }
        close(c);
        close(s);
        /* sendto with an address on an unconnected socket */
        c = usock();
        emit("addr.sendto_unconnected_with_addr", "%s", rs(SYS(sendto(c, "x", 1, MSG_NOSIGNAL,
                (struct sockaddr *)&ad.sa, ad.len))));
        emit("addr.sendto_unconnected_no_addr", "%s", rs(SYS(sendto(c, "x", 1, MSG_NOSIGNAL, NULL,
                0))));
        emit("addr.sendto_addr_len_mismatch", "%s", rs(SYS(sendto(c, "x", 1, MSG_NOSIGNAL,
                (struct sockaddr *)&ad.sa, 0))));
        close(c);
        close(l);
    }

    /* sendmsg / recvmsg without control data */
    sockpair(&a, &b);
    {
        struct iovec iov[2] = {{"he", 2}, {"llo", 3}};
        struct msghdr m;
        memset(&m, 0, sizeof(m));
        m.msg_iov = iov;
        m.msg_iovlen = 2;
        emit("msg.sendmsg_2iov", "%s", rs(SYS(sendmsg(a, &m, 0))));
        char r1[3], r2[16];
        struct iovec riov[2] = {{r1, 3}, {r2, 16}};
        struct msghdr rm;
        memset(&rm, 0, sizeof(rm));
        rm.msg_iov = riov;
        rm.msg_iovlen = 2;
        rm.msg_flags = 0x1234;
        emit("msg.recvmsg_2iov", "%s flags=0x%x controllen=%zu", rs(SYS(recvmsg(b, &rm, 0))),
             rm.msg_flags, rm.msg_controllen);
        set_nb(b, 1);
        emit("msg.recvmsg_empty_nb", "%s", rs(SYS(recvmsg(b, &rm, MSG_DONTWAIT))));
        set_nb(b, 0);
        write(a, "abcdef", 6);
        struct iovec small = {r1, 3};
        rm.msg_iov = &small;
        rm.msg_iovlen = 1;
        emit("msg.recvmsg_buffer_smaller_than_data", "%s flags=0x%x", rs(SYS(recvmsg(b, &rm, 0))),
             rm.msg_flags);
        emit("msg.recvmsg_rest", "%s", nb_read_s(b, 8));
        struct msghdr zero;
        memset(&zero, 0, sizeof(zero));
        emit("msg.sendmsg_no_iov", "%s", rs(SYS(sendmsg(a, &zero, 0))));
        {
            struct msghdr bad;
            memset(&bad, 0, sizeof(bad));
            bad.msg_iov = (struct iovec *)1;
            bad.msg_iovlen = 1;
            emit("msg.sendmsg_efault_iov", "%s", rs(SYS(sendmsg(a, &bad, MSG_NOSIGNAL))));
        }
        set_nb(b, 1);
        {
            struct msghdr bad;
            memset(&bad, 0, sizeof(bad));
            bad.msg_iov = (struct iovec *)1;
            bad.msg_iovlen = 1;
            emit("msg.recvmsg_efault_iov", "%s", rs(SYS(recvmsg(b, &bad, MSG_DONTWAIT))));
        }
        set_nb(b, 0);
    }
    {
        struct mmsghdr mm[1];
        memset(mm, 0, sizeof(mm));
        struct iovec iov = {"mm", 2};
        mm[0].msg_hdr.msg_iov = &iov;
        mm[0].msg_hdr.msg_iovlen = 1;
        emit("msg.sendmmsg", "%s", rs(SYS(sendmmsg(a, mm, 1, 0))));
        char rb[8];
        struct iovec riov = {rb, 8};
        memset(mm, 0, sizeof(mm));
        mm[0].msg_hdr.msg_iov = &riov;
        mm[0].msg_hdr.msg_iovlen = 1;
        emit("msg.recvmmsg", "%s", rs(SYS(recvmmsg(b, mm, 1, MSG_DONTWAIT, NULL))));
    }
    close(a);
    close(b);
}

/* ------------------------------------------------------------------ */
/* c16: EPIPE and SIGPIPE                                              */
/* ------------------------------------------------------------------ */

static const char *pipe_res(long r) {
    static __thread char b[8][64];
    static __thread int i;
    char *p = b[i++ & 7];
    msleep(30);
    snprintf(p, 64, "%s sigpipe=%d", rs(r), atomic_exchange(&g_sigpipes, 0));
    return p;
}

CHECK(c16_sigpipe) {
    int a, b;
    atomic_store(&g_sigpipes, 0);

    sockpair(&a, &b);
    close(b);
    emit("pipe.write_after_peer_close", "%s", pipe_res(SYS(write(a, "x", 1))));
    emit("pipe.write_again", "%s", pipe_res(SYS(write(a, "x", 1))));
    emit("pipe.send_nosignal", "%s", pipe_res(SYS(send(a, "x", 1, MSG_NOSIGNAL))));
    emit("pipe.send_no_flags", "%s", pipe_res(SYS(send(a, "x", 1, 0))));
    emit("pipe.writev", "%s", pipe_res(SYS(writev(a, &(struct iovec) {"x", 1}, 1))));
    {
        struct iovec iov = {"x", 1};
        struct msghdr m;
        memset(&m, 0, sizeof(m));
        m.msg_iov = &iov;
        m.msg_iovlen = 1;
        emit("pipe.sendmsg", "%s", pipe_res(SYS(sendmsg(a, &m, 0))));
        emit("pipe.sendmsg_nosignal", "%s", pipe_res(SYS(sendmsg(a, &m, MSG_NOSIGNAL))));
    }
    emit("pipe.write_zero_after_peer_close", "%s", pipe_res(SYS(write(a, "x", 0))));
    close(a);

    sockpair(&a, &b);
    shutdown(a, SHUT_WR);
    emit("pipe.write_after_self_shut_wr", "%s", pipe_res(SYS(write(a, "x", 1))));
    close(a);
    close(b);

    sockpair(&a, &b);
    shutdown(b, SHUT_RD);
    emit("pipe.write_after_peer_shut_rd", "%s", pipe_res(SYS(write(a, "x", 1))));
    close(a);
    close(b);

    sockpair(&a, &b);
    shutdown(a, SHUT_RD);
    emit("pipe.write_after_self_shut_rd", "%s", pipe_res(SYS(write(a, "x", 1))));
    close(a);
    close(b);

    sockpair(&a, &b);
    shutdown(b, SHUT_WR);
    emit("pipe.write_after_peer_shut_wr", "%s", pipe_res(SYS(write(a, "x", 1))));
    close(a);
    close(b);

    /* unconnected */
    a = usock();
    emit("pipe.write_unconnected", "%s", pipe_res(SYS(write(a, "x", 1))));
    close(a);

    /* a blocked writer when the peer closes */
    sockpair(&a, &b);
    fill_buf(a);
    {
        op_t *o = new_op(OP_WRITE, a);
        o->len = 1;
        tctx_t tc;
        memset(&tc, 0, sizeof(tc));
        tc.fd = b;
        tc.delay_ms = 150;
        tc.arg = -1;
        pthread_t t = start_thread(th_close, &tc);
        long long el;
        long r = timed_op(o, 1500, &el);
        emit("pipe.blocked_write_peer_closes", "%s", pipe_res(r));
        join_thread(t, &tc, 1000);
    }
    close(a);

    /* with SIGPIPE ignored and blocked nothing changes for the result */
    sockpair(&a, &b);
    close(b);
    sigset_t set, old;
    sigemptyset(&set);
    sigaddset(&set, SIGPIPE);
    pthread_sigmask(SIG_BLOCK, &set, &old);
    emit("pipe.write_sigpipe_blocked", "%s", pipe_res(SYS(write(a, "x", 1))));
    {
        /* a blocked SIGPIPE stays pending after the failed write: discard it */
        struct timespec zero = {0, 0};
        int got = sigtimedwait(&set, NULL, &zero);
        emit("pipe.sigpipe_pending_after_blocked_write", "%d", got == SIGPIPE);
    }
    pthread_sigmask(SIG_SETMASK, &old, NULL);
    close(a);
}

/* ------------------------------------------------------------------ */
/* c09: shutdown() over states x modes x operations                    */
/* ------------------------------------------------------------------ */

enum {
    ST_IDLE, ST_BOUND, ST_LISTENING, ST_SP, ST_CLIENT, ST_ACCEPTED, ST_PEER_CLOSED,
    ST_PEER_SHUT_WR, ST_PEER_SHUT_RD, ST_SELF_SHUT_WR, ST_SELF_SHUT_RD, ST_PENDING,
    ST_DATA_AT_A, ST_DATA_AT_B, ST_A_FULL, ST_B_FULL, ST_PEER_CLOSED_DATA, ST_COUNT
};

static const char *st_names[ST_COUNT] = {
    "idle", "bound", "listening", "sp", "client", "accepted", "peer_closed", "peer_shut_wr",
    "peer_shut_rd", "self_shut_wr", "self_shut_rd", "pending", "data_at_a", "data_at_b",
    "a_full", "b_full", "peer_closed_data",
};

static uaddr_t g_laddr;

/* builds a socket A in the state; B is the peer if any, else -1 */
static void mk_state(int st, int *a, int *b) {
    *b = -1;
    switch (st) {
        case ST_IDLE:
            *a = usock();
            break;
        case ST_BOUND:
            *a = usock();
            g_laddr = nabs("bound");
            BIND(*a, g_laddr);
            break;
        case ST_LISTENING:
            *a = listener(1, 4, &g_laddr);
            break;
        case ST_CLIENT:
            conn_pair(1, a, b);
            break;
        case ST_ACCEPTED:
            conn_pair(1, b, a);
            break;
        case ST_PENDING: {
            int l = listener(1, 4, &g_laddr);
            (void)l; /* stays open until the global cleanup */
            *a = client_to(&g_laddr);
            break;
        }
        default:
            sockpair(a, b);
            break;
    }
    switch (st) {
        case ST_PEER_CLOSED:
            close(*b);
            *b = -1;
            break;
        case ST_PEER_CLOSED_DATA:
            write(*b, "d", 1);
            close(*b);
            *b = -1;
            break;
        case ST_PEER_SHUT_WR:
            shutdown(*b, SHUT_WR);
            break;
        case ST_PEER_SHUT_RD:
            shutdown(*b, SHUT_RD);
            break;
        case ST_SELF_SHUT_WR:
            shutdown(*a, SHUT_WR);
            break;
        case ST_SELF_SHUT_RD:
            shutdown(*a, SHUT_RD);
            break;
        case ST_DATA_AT_A:
            write(*b, "x", 1);
            break;
        case ST_DATA_AT_B:
            write(*a, "x", 1);
            break;
        case ST_A_FULL:
            fill_buf(*a);
            break;
        case ST_B_FULL:
            fill_buf(*b);
            break;
        default:
            break;
    }
}

static const char *how_name(int how) {
    return how == SHUT_RD ? "rd" : how == SHUT_WR ? "wr" : how == SHUT_RDWR ? "rdwr" : "none";
}

CHECK(c09_shutdown_matrix) {
    for (int st = 0; st < ST_COUNT; st++) {
        int a, b;
        mk_state(st, &a, &b);
        emit(ID("shut.%s.pre.a_ev", st_names[st]), "%s", PEV(a));
        if (b >= 0) {
            emit(ID("shut.%s.pre.b_ev", st_names[st]), "%s", PEV(b));
        }
        close(a);
        closefd(b);
    }
    for (int st = 0; st < ST_COUNT; st++) {
        for (int how = 0; how < 3; how++) {
            int a, b;
            mk_state(st, &a, &b);
            const char *sn = st_names[st];
            const char *hn = how_name(how);
            emit(ID("shut.%s.%s.rc", sn, hn), "%s", rs(SYS(shutdown(a, how))));
            emit(ID("shut.%s.%s.a_ev", sn, hn), "%s", PEV(a));
            if (b >= 0) {
                emit(ID("shut.%s.%s.b_ev", sn, hn), "%s", PEV(b));
            }
            emit(ID("shut.%s.%s.a_read", sn, hn), "%s", nb_read_s(a, 4));
            emit(ID("shut.%s.%s.a_write", sn, hn), "%s", nb_write_s(a, "w"));
            if (b >= 0) {
                emit(ID("shut.%s.%s.b_read", sn, hn), "%s", nb_read_s(b, 4));
                emit(ID("shut.%s.%s.b_write", sn, hn), "%s", nb_write_s(b, "v"));
                emit(ID("shut.%s.%s.a_read2", sn, hn), "%s", nb_read_s(a, 4));
            }
            if (st == ST_LISTENING) {
                int c = usock();
                set_nb(c, 1);
                emit(ID("shut.%s.%s.new_connect", sn, hn), "%s", rs(SYS(CONNECT(c, g_laddr))));
                set_nb(a, 1);
                emit(ID("shut.%s.%s.accept", sn, hn), "%s", ({
                    int s = accept(a, NULL, NULL);
                    const char *r = rs(s >= 0 ? 0 : -errno);
                    closefd(s);
                    r;
                }));
                close(c);
            }
            emit(ID("shut.%s.%s.again", sn, hn), "%s", rs(SYS(shutdown(a, how))));
            emit(ID("shut.%s.%s.other_how", sn, hn), "%s", rs(SYS(shutdown(a, (how + 1) % 3))));
            close(a);
            closefd(b);
        }
    }
    {
        int a, b;
        sockpair(&a, &b);
        emit("shut.invalid_how_3", "%s", rs(SYS(shutdown(a, 3))));
        emit("shut.invalid_how_neg", "%s", rs(SYS(shutdown(a, -1))));
        emit("shut.invalid_how_on_idle", "%s", ({
            int fd = usock();
            const char *r = rs(SYS(shutdown(fd, 7)));
            close(fd);
            r;
        }));
        close(a);
        close(b);
    }
    /* the data of the peer arrives although this end shut down its reading */
    {
        int a, b;
        sockpair(&a, &b);
        write(b, "before", 6);
        shutdown(a, SHUT_RD);
        emit("shut.rd.data_before_is_readable", "%s", nb_read_s(a, 16));
        emit("shut.rd.write_by_peer_after", "%s", rs(SYS(write(b, "after", 5))));
        emit("shut.rd.read_after", "%s", nb_read_s(a, 16));
        close(a);
        close(b);
    }
    /* shutdown(SHUT_WR): the peer reads the buffered data, then EOF */
    {
        int a, b;
        sockpair(&a, &b);
        write(a, "tail", 4);
        shutdown(a, SHUT_WR);
        emit("shut.wr.peer_read1", "%s", nb_read_s(b, 16));
        emit("shut.wr.peer_read2", "%s", nb_read_s(b, 16));
        emit("shut.wr.peer_read3", "%s", nb_read_s(b, 16));
        emit("shut.wr.peer_ev", "%s", PEV(b));
        emit("shut.wr.peer_can_still_write", "%s", nb_write_s(b, "back"));
        emit("shut.wr.self_read_back", "%s", nb_read_s(a, 16));
        close(a);
        close(b);
    }
    /* a thread polling during shutdown(SHUT_RDWR): no half-updated flags visible */
    {
        int a, b;
        int bad = 0;
        for (int i = 0; i < 40; i++) {
            sockpair(&a, &b);
            tctx_t tc;
            memset(&tc, 0, sizeof(tc));
            tc.fd = a;
            tc.delay_ms = 0;
            tc.arg = SHUT_RDWR;
            pthread_t t = start_thread(th_close, &tc);
            for (int k = 0; k < 200; k++) {
                struct pollfd pf = {a, POLLIN | POLLOUT | POLLRDHUP, 0};
                poll(&pf, 1, 0);
                int ev = pf.revents;
                /* after shutdown(RDWR) both IN (EOF) and HUP must come together */
                if ((ev & POLLHUP) && !(ev & POLLIN)) {
                    bad++;
                }
                if ((ev & POLLRDHUP) && !(ev & POLLHUP)) {
                    bad++;
                }
            }
            join_thread(t, &tc, 1000);
            close(a);
            close(b);
        }
        emit("shut.rdwr.poll_never_sees_half_state", "%s", bad == 0 ? "ok" : "SEEN_HALF_STATE");
    }
}

/* ------------------------------------------------------------------ */
/* c10: what the peer sees when a socket is closed                     */
/* ------------------------------------------------------------------ */

enum { CL_PLAIN, CL_UNREAD_AT_A, CL_UNREAD_AT_B, CL_A_FULL, CL_B_SHUT_WR, CL_B_SHUT_RD, CL_A_SHUT_WR,
       CL_A_SHUT_RD, CL_BOTH_DATA, CL_A_SHUT_RDWR, CL_COUNT
     };

static const char *cl_names[CL_COUNT] = {
    "plain", "unread_at_a", "unread_at_b", "a_full", "b_shut_wr", "b_shut_rd", "a_shut_wr",
    "a_shut_rd", "both_data", "a_shut_rdwr",
};

CHECK(c10_close_matrix) {
    for (int cl = 0; cl < CL_COUNT; cl++) {
        for (int kind = 0; kind < 3; kind++) {
            /* kind 0: socketpair, 1: client closes (server b), 2: accepted closes (client b) */
            int a, b;
            if (kind == 0) {
                sockpair(&a, &b);
            } else if (kind == 1) {
                conn_pair(1, &a, &b);
            } else {
                conn_pair(1, &b, &a);
            }
            switch (cl) {
                case CL_UNREAD_AT_A:
                    write(b, "u", 1);
                    break;
                case CL_UNREAD_AT_B:
                    write(a, "u", 1);
                    break;
                case CL_A_FULL:
                    fill_buf(a);
                    break;
                case CL_B_SHUT_WR:
                    shutdown(b, SHUT_WR);
                    break;
                case CL_B_SHUT_RD:
                    shutdown(b, SHUT_RD);
                    break;
                case CL_A_SHUT_WR:
                    shutdown(a, SHUT_WR);
                    break;
                case CL_A_SHUT_RD:
                    shutdown(a, SHUT_RD);
                    break;
                case CL_BOTH_DATA:
                    write(a, "1", 1);
                    write(b, "2", 1);
                    break;
                case CL_A_SHUT_RDWR:
                    shutdown(a, SHUT_RDWR);
                    break;
                default:
                    break;
            }
            static const char *kn[] = {"sp", "client", "accepted"};
            char tag[64];
            snprintf(tag, sizeof(tag), "close.%s.%s", kn[kind], cl_names[cl]);
            close(a);
            emit(ID("%s.b_ev", tag), "%s", PEV(b));
            /* the reset (SO_ERROR) of Linux is consumed by the first read or by getsockopt */
            if (cl == CL_UNREAD_AT_A || cl == CL_BOTH_DATA) {
                emit(ID("%s.b_read1_first", tag), "%s", nb_read_s(b, 4));
                emit(ID("%s.b_so_error_after_read", tag), "%s", sockopt_s(b, SOL_SOCKET, SO_ERROR));
            } else {
                emit(ID("%s.b_so_error", tag), "%s", sockopt_s(b, SOL_SOCKET, SO_ERROR));
            }
            emit(ID("%s.b_read1", tag), "%s", nb_read_s(b, 4));
            emit(ID("%s.b_read2", tag), "%s", nb_read_s(b, 4));
            emit(ID("%s.b_ev_after", tag), "%s", PEV(b));
            emit(ID("%s.b_write1", tag), "%s", nb_write_s(b, "w"));
            emit(ID("%s.b_write2", tag), "%s", nb_write_s(b, "w"));
            emit(ID("%s.b_shutdown", tag), "%s", rs(SYS(shutdown(b, SHUT_RDWR))));
            emit(ID("%s.b_getpeername", tag), "%s", gpn(b, NULL));
            close(b);
        }
    }
    /* closing with data in flight both ways, peer reads in order */
    {
        int a, b;
        sockpair(&a, &b);
        write(a, "last words", 10);
        close(a);
        emit("close.data_then_eof.read1", "%s", nb_read_s(b, 32));
        emit("close.data_then_eof.read2", "%s", nb_read_s(b, 32));
        close(b);
    }
    /* a dup keeps the connection open */
    {
        int a, b;
        sockpair(&a, &b);
        int d = dup(a);
        close(a);
        emit("close.dup_alive.b_ev", "%s", PEV(b));
        emit("close.dup_alive.b_read", "%s", nb_read_s(b, 4));
        close(d);
        emit("close.dup_closed.b_ev", "%s", PEV(b));
        close(b);
    }
    /* a listener with pending connections is closed: clients see a reset */
    for (int k = 0; k < 4; k++) {
        uaddr_t ad;
        int l = listener(k & 1, 4, &ad);
        int c = client_to(&ad);
        if (k & 2) {
            write(c, "d", 1);
        }
        close(l);
        char tag[64];
        snprintf(tag, sizeof(tag), "close.listener_pending.%s.%s",
                 (k & 2) ? "client_wrote" : "client_idle",
                 (k & 1) ? "abstract" : "pathname");
        emit(ID("%s.ev", tag), "%s", PEV(c));
        emit(ID("%s.read1_first", tag), "%s", nb_read_s(c, 4));
        emit(ID("%s.so_error_after_read", tag), "%s", sockopt_s(c, SOL_SOCKET, SO_ERROR));
        emit(ID("%s.read2", tag), "%s", nb_read_s(c, 4));
        emit(ID("%s.write", tag), "%s", nb_write_s(c, "x"));
        close(c);
        l = listener(k & 1, 4, &ad);
        c = client_to(&ad);
        if (k & 2) {
            write(c, "d", 1);
        }
        close(l);
        emit(ID("%s.so_error_first", tag), "%s", sockopt_s(c, SOL_SOCKET, SO_ERROR));
        emit(ID("%s.read_after_so_error", tag), "%s", nb_read_s(c, 4));
        close(c);
    }
    /* SO_ERROR is cleared by reading it: the closing socket has unread data */
    {
        int a, b;
        sockpair(&a, &b);
        write(b, "u", 1);
        close(a);
        emit("close.so_error.first", "%s", sockopt_s(b, SOL_SOCKET, SO_ERROR));
        emit("close.so_error.second", "%s", sockopt_s(b, SOL_SOCKET, SO_ERROR));
        emit("close.so_error.then_read", "%s", nb_read_s(b, 4));
        close(b);
    }
}

/* ------------------------------------------------------------------ */
/* c11: poll / select / epoll events                                   */
/* ------------------------------------------------------------------ */

static const char *sel_s(int fd) {
    static __thread char b[8][48];
    static __thread int i;
    char *p = b[i++ & 7];
    fd_set r, w, e;
    FD_ZERO(&r);
    FD_ZERO(&w);
    FD_ZERO(&e);
    FD_SET(fd, &r);
    FD_SET(fd, &w);
    FD_SET(fd, &e);
    struct timeval tv = {0, 0};
    int n = select(fd + 1, &r, &w, &e, &tv);
    if (n < 0) {
        snprintf(p, 48, "%s", rs(-errno));
    } else {
        snprintf(p, 48, "r=%d w=%d e=%d", FD_ISSET(fd, &r), FD_ISSET(fd, &w), FD_ISSET(fd, &e));
    }
    return p;
}

CHECK(c11_events) {
    static const int sts[] = {ST_IDLE, ST_BOUND, ST_LISTENING, ST_SP, ST_CLIENT, ST_ACCEPTED,
                              ST_PEER_CLOSED, ST_PEER_SHUT_WR, ST_PEER_SHUT_RD, ST_SELF_SHUT_WR,
                              ST_SELF_SHUT_RD, ST_PENDING, ST_DATA_AT_A, ST_A_FULL, ST_B_FULL,
                              ST_PEER_CLOSED_DATA
                             };
    for (size_t i = 0; i < ARRAY_LEN(sts); i++) {
        int a, b;
        mk_state(sts[i], &a, &b);
        const char *sn = st_names[sts[i]];
        emit(ID("ev.%s.poll_all", sn), "%s", PEV(a));
        emit(ID("ev.%s.poll_in_only", sn), "%s", pev(a, POLLIN, 0));
        emit(ID("ev.%s.poll_out_only", sn), "%s", pev(a, POLLOUT, 0));
        emit(ID("ev.%s.poll_none_requested", sn), "%s", pev(a, 0, 0));
        emit(ID("ev.%s.epoll_all", sn), "%s", epev(a, EALL, 0));
        emit(ID("ev.%s.epoll_in_only", sn), "%s", epev(a, EPOLLIN, 0));
        emit(ID("ev.%s.epoll_out_only", sn), "%s", epev(a, EPOLLOUT, 0));
        emit(ID("ev.%s.epoll_none_requested", sn), "%s", epev(a, 0, 0));
        emit(ID("ev.%s.select", sn), "%s", sel_s(a));
        close(a);
        closefd(b);
    }

    /* listener events */
    {
        uaddr_t ad;
        int l = listener(1, 4, &ad);
        emit("ev.listener.empty", "%s", PEV(l));
        int c1 = client_to(&ad);
        emit("ev.listener.one_pending", "%s", PEV(l));
        int c2 = client_to(&ad);
        emit("ev.listener.two_pending", "%s", PEV(l));
        int s1 = accept(l, NULL, NULL);
        emit("ev.listener.one_accepted", "%s", PEV(l));
        int s2 = accept(l, NULL, NULL);
        emit("ev.listener.both_accepted", "%s", PEV(l));
        emit("ev.listener.epoll_both_accepted", "%s", epev(l, EALL, 0));
        emit("ev.listener.select_both_accepted", "%s", sel_s(l));
        closefd(c1);
        closefd(c2);
        closefd(s1);
        closefd(s2);
        close(l);
    }

    /* POLLOUT threshold: after the buffer was full, how much must the peer read? */
    {
        int a, b;
        sockpair(&a, &b);
        long filled = fill_buf(a);
        emit("ev.out_threshold.full", "%s", PEV(a));
        size_t steps[] = {1, 99, 3996, 12288, 49152, 65536, 40000};
        long total = 0;
        char *tmp = malloc(65536);
        char line[256] = "";
        for (size_t i = 0; i < ARRAY_LEN(steps); i++) {
            long left = steps[i];
            while (left > 0) {
                long r = nb_read(b, tmp, left > 65536 ? 65536 : (size_t)left);
                if (r <= 0) {
                    break;
                }
                left -= r;
                total += r;
            }
            char one[32];
            snprintf(one, sizeof(one), "%ldk:%s ", total / 1024, (pev(a, POLLOUT,
                     0)[0] == 'O') ? "OUT" : "-");
            strcat(line, one);
        }
        free(tmp);
        if (line[0] != 0) {
            line[strlen(line) - 1] = 0;   // the blank after the last step
        }
        emit("ev.out_threshold.after_reading", "%s", line);
        (void)filled;
        close(a);
        close(b);
    }

    /* POLLNVAL on a closed fd, a plain file */
    {
        int fd = usock();
        close(fd);
        emit("ev.closed_fd", "%s", pev(fd, POLLIN, 0));
    }

    /* poll woken by connect on the listener */
    {
        uaddr_t ad;
        int l = listener(1, 4, &ad);
        op_t *o = new_op(OP_POLLIN, l);
        o->arg = POLLIN;
        o->flags = 2000;
        tctx_t tc;
        memset(&tc, 0, sizeof(tc));
        tc.addr = ad;
        tc.delay_ms = 150;
        pthread_t t = start_thread(th_connect, &tc);
        long long el;
        long r = timed_op(o, 3500, &el);
        emit("ev.poll_woken_by_connect", "%s ev=%s", r == BLOCKED ? "BLOCKED" : rs(r),
             r == BLOCKED ? "-" : evs(o->extra));
        join_thread(t, &tc, 1000);
        close(tc.fd);
        close(l);
    }
}

/* ------------------------------------------------------------------ */
/* c12: epoll semantics                                                */
/* ------------------------------------------------------------------ */

static int ep_add(int ep, int fd, uint32_t ev) {
    struct epoll_event e;
    memset(&e, 0, sizeof(e));
    e.events = ev;
    e.data.fd = fd;
    return epoll_ctl(ep, EPOLL_CTL_ADD, fd, &e);
}

static const char *ep_wait(int ep, int tmo) {
    static __thread char b[8][96];
    static __thread int i;
    char *p = b[i++ & 7];
    struct epoll_event out[8];
    int n = epoll_wait(ep, out, 8, tmo);
    if (n < 0) {
        snprintf(p, 96, "%s", rs(-errno));
    } else if (n == 0) {
        snprintf(p, 96, "none");
    } else {
        snprintf(p, 96, "n=%d %s", n, evs((int)out[0].events));
    }
    return p;
}

CHECK(c12_epoll) {
    int a, b, ep;
    char buf[64];

    /* edge triggered read */
    sockpair(&a, &b);
    ep = epoll_create1(0);
    ep_add(ep, a, EPOLLIN | EPOLLET);
    emit("epoll.et.initial", "%s", ep_wait(ep, 0));
    write(b, "1", 1);
    emit("epoll.et.after_write", "%s", ep_wait(ep, 0));
    emit("epoll.et.no_new_data", "%s", ep_wait(ep, 0));
    write(b, "2", 1);
    emit("epoll.et.second_write_unread_first", "%s", ep_wait(ep, 0));
    nb_read(a, buf, 1);
    emit("epoll.et.after_partial_read", "%s", ep_wait(ep, 0));
    nb_read(a, buf, 8);
    write(b, "3", 1);
    emit("epoll.et.after_drain_and_write", "%s", ep_wait(ep, 0));
    close(ep);
    close(a);
    close(b);

    /* data pending before EPOLL_CTL_ADD */
    sockpair(&a, &b);
    write(b, "x", 1);
    ep = epoll_create1(0);
    ep_add(ep, a, EPOLLIN | EPOLLET);
    emit("epoll.et.data_before_add", "%s", ep_wait(ep, 0));
    close(ep);
    ep = epoll_create1(0);
    ep_add(ep, a, EPOLLIN);
    emit("epoll.lt.data_before_add", "%s", ep_wait(ep, 0));
    emit("epoll.lt.repeats", "%s", ep_wait(ep, 0));
    close(ep);
    close(a);
    close(b);

    /* edge triggered OUT */
    sockpair(&a, &b);
    ep = epoll_create1(0);
    ep_add(ep, a, EPOLLOUT | EPOLLET);
    emit("epoll.et.out.initial", "%s", ep_wait(ep, 0));
    emit("epoll.et.out.again", "%s", ep_wait(ep, 0));
    fill_buf(a);
    emit("epoll.et.out.after_fill", "%s", ep_wait(ep, 0));
    drain_all(b);
    emit("epoll.et.out.after_drain", "%s", ep_wait(ep, 0));
    close(ep);
    close(a);
    close(b);

    /* a registered socket reads data in two steps: level triggered stays ready */
    sockpair(&a, &b);
    ep = epoll_create1(0);
    ep_add(ep, a, EPOLLIN);
    write(b, "abcd", 4);
    nb_read(a, buf, 2);
    emit("epoll.lt.partial_read_still_ready", "%s", ep_wait(ep, 0));
    nb_read(a, buf, 8);
    emit("epoll.lt.drained", "%s", ep_wait(ep, 0));
    close(ep);
    close(a);
    close(b);

    /* peer close / shutdown events */
    {
        const char *names[] = {"peer_close", "peer_shut_wr", "peer_shut_rd", "peer_shut_rdwr"};
        for (int k = 0; k < 4; k++) {
            sockpair(&a, &b);
            ep = epoll_create1(0);
            ep_add(ep, a, EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET);
            ep_wait(ep, 0);
            if (k == 0) {
                close(b);
            } else {
                shutdown(b, k == 1 ? SHUT_WR : k == 2 ? SHUT_RD : SHUT_RDWR);
            }
            emit(ID("epoll.et.%s", names[k]), "%s", ep_wait(ep, 0));
            emit(ID("epoll.et.%s.again", names[k]), "%s", ep_wait(ep, 0));
            close(ep);
            close(a);
            if (k) {
                close(b);
            }
        }
        const char *snames[] = {"self_shut_rd", "self_shut_wr", "self_shut_rdwr"};
        int shows[] = {SHUT_RD, SHUT_WR, SHUT_RDWR};
        for (int k = 0; k < 3; k++) {
            sockpair(&a, &b);
            ep = epoll_create1(0);
            ep_add(ep, a, EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET);
            ep_wait(ep, 0);
            shutdown(a, shows[k]);
            emit(ID("epoll.et.%s", snames[k]), "%s", ep_wait(ep, 0));
            emit(ID("epoll.et.%s.again", snames[k]), "%s", ep_wait(ep, 0));
            close(ep);
            close(a);
            close(b);
        }
    }

    /* listener with edge trigger */
    {
        uaddr_t ad;
        int l = listener(1, 8, &ad);
        ep = epoll_create1(0);
        ep_add(ep, l, EPOLLIN | EPOLLET);
        emit("epoll.listener.et.empty", "%s", ep_wait(ep, 0));
        int c1 = client_to(&ad);
        emit("epoll.listener.et.first", "%s", ep_wait(ep, 0));
        int c2 = client_to(&ad);
        emit("epoll.listener.et.second", "%s", ep_wait(ep, 0));
        emit("epoll.listener.et.no_new", "%s", ep_wait(ep, 0));
        int s1 = accept(l, NULL, NULL);
        emit("epoll.listener.et.after_one_accept_one_left", "%s", ep_wait(ep, 0));
        int s2 = accept(l, NULL, NULL);
        closefd(s1);
        closefd(s2);
        closefd(c1);
        closefd(c2);
        close(ep);
        close(l);
    }
    /* connect before the listener is registered */
    {
        uaddr_t ad;
        int l = listener(1, 8, &ad);
        int c1 = client_to(&ad);
        ep = epoll_create1(0);
        ep_add(ep, l, EPOLLIN);
        emit("epoll.listener.lt.connect_before_add", "%s", ep_wait(ep, 0));
        int s = accept(l, NULL, NULL);
        emit("epoll.listener.lt.after_accept", "%s", ep_wait(ep, 0));
        close(s);
        close(c1);
        close(ep);
        close(l);
    }
    /* a closed fd is removed from the set */
    sockpair(&a, &b);
    ep = epoll_create1(0);
    ep_add(ep, a, EPOLLIN);
    write(b, "x", 1);
    close(a);
    emit("epoll.closed_fd_removed", "%s", ep_wait(ep, 0));
    close(ep);
    close(b);
    /* a fd that was dup'ed stays registered after the original is closed */
    sockpair(&a, &b);
    int d = dup(a);
    ep = epoll_create1(0);
    ep_add(ep, a, EPOLLIN);
    close(a);
    write(b, "x", 1);
    emit("epoll.dup_stays_registered", "%s", ep_wait(ep, 0));
    close(ep);
    close(d);
    close(b);

    /* wake-ups of a blocked epoll_wait */
    {
        struct {
            const char *name;
            int on_peer;
            int how; /* -1 close, -2 write, else shutdown */
        } cases[] = {
            {"peer_write", 1, -2}, {"peer_close", 1, -1}, {"peer_shut_wr", 1, SHUT_WR},
            {"self_shut_rd", 0, SHUT_RD}, {"self_shut_rdwr", 0, SHUT_RDWR},
        };
        for (size_t i = 0; i < ARRAY_LEN(cases); i++) {
            sockpair(&a, &b);
            ep = epoll_create1(0);
            ep_add(ep, a, EPOLLIN | EPOLLRDHUP);
            op_t *o = new_op(OP_EPOLLWAIT, ep);
            o->flags = 1500;
            tctx_t tc;
            memset(&tc, 0, sizeof(tc));
            tc.fd = cases[i].on_peer ? b : a;
            tc.delay_ms = 200;
            tc.arg = cases[i].how;
            pthread_t t;
            if (cases[i].how == -2) {
                memcpy(tc.data, "x", 1);
                tc.arg = 1;
                t = start_thread(th_write, &tc);
            } else {
                t = start_thread(th_close, &tc);
            }
            long long el;
            long r = timed_op(o, 3500, &el);
            emit(ID("epoll.blocked_wait.%s", cases[i].name), "%s ev=%s", blk_res(r, el, 150),
                 r == BLOCKED ? "-" : evs(o->extra));
            join_thread(t, &tc, 1000);
            close(ep);
            close(a);
            close(b);
        }
    }
    /* the pipe-like write-wakeup: a blocked writer is woken by epoll-driven reader */
    {
        sockpair(&a, &b);
        fill_buf(a);
        ep = epoll_create1(0);
        ep_add(ep, a, EPOLLOUT);
        emit("epoll.out.full", "%s", ep_wait(ep, 0));
        nb_read(b, buf, 1);
        emit("epoll.out.after_1_byte_read", "%s", ep_wait(ep, 0));
        drain_all(b);
        emit("epoll.out.after_drain", "%s", ep_wait(ep, 0));
        close(ep);
        close(a);
        close(b);
    }
    /* adding errors */
    {
        sockpair(&a, &b);
        ep = epoll_create1(0);
        emit("epoll.add", "%s", rs(SYS(ep_add(ep, a, EPOLLIN))));
        emit("epoll.add_again", "%s", rs(SYS(ep_add(ep, a, EPOLLIN))));
        emit("epoll.add_oneshot", "%s", rs(SYS(ep_add(ep, b, EPOLLIN | EPOLLONESHOT))));
        write(a, "x", 1);
        emit("epoll.oneshot.first", "%s", ep_wait(ep, 0));
        emit("epoll.oneshot.second", "%s", ep_wait(ep, 0));
        close(ep);
        close(a);
        close(b);
    }
}

/* ------------------------------------------------------------------ */
/* c15: socket options                                                 */
/* ------------------------------------------------------------------ */

static const char *cred_s(int fd) {
    static __thread char b[8][128];
    static __thread int i;
    char *p = b[i++ & 7];
    struct ucred uc;
    socklen_t l = sizeof(uc);
    memset(&uc, 0x55, sizeof(uc));
    long r = SYS(getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &uc, &l));
    if (r < 0) {
        snprintf(p, 128, "%s", rs(r));
    } else {
        snprintf(p, 128, "len=%u pid_is_self=%d pid_zero=%d uid_is_self=%d uid=%s gid_is_self=%d",
                 (unsigned)l, uc.pid == getpid(), uc.pid == 0, uc.uid == getuid(),
                 uc.uid == (uid_t) -1 ? "-1" : (uc.uid == 0 ? "0" : "other"), uc.gid == getgid());
    }
    return p;
}

CHECK(c15_options) {
    int fd = usock();
    static const struct {
        const char *name;
        int opt;
    } ints[] = {
        {"SO_TYPE", SO_TYPE}, {"SO_DOMAIN", SO_DOMAIN}, {"SO_PROTOCOL", SO_PROTOCOL},
        {"SO_ERROR", SO_ERROR}, {"SO_ACCEPTCONN", SO_ACCEPTCONN}, {"SO_SNDBUF", SO_SNDBUF},
        {"SO_RCVBUF", SO_RCVBUF}, {"SO_KEEPALIVE", SO_KEEPALIVE}, {"SO_REUSEADDR", SO_REUSEADDR},
        {"SO_REUSEPORT", SO_REUSEPORT}, {"SO_PASSCRED", SO_PASSCRED}, {"SO_OOBINLINE", SO_OOBINLINE},
        {"SO_BROADCAST", SO_BROADCAST}, {"SO_PRIORITY", SO_PRIORITY}, {"SO_RCVLOWAT", SO_RCVLOWAT},
        {"SO_SNDLOWAT", SO_SNDLOWAT}, {"SO_TIMESTAMP", SO_TIMESTAMP}, {"SO_DONTROUTE", SO_DONTROUTE},
        {"SO_DEBUG", SO_DEBUG}, {"SO_MARK", SO_MARK}, {"SO_PEEK_OFF", SO_PEEK_OFF},
        {"SO_PASSSEC", SO_PASSSEC}, {"SO_NO_CHECK", SO_NO_CHECK}, {"SO_BUSY_POLL", SO_BUSY_POLL},
        {"SO_INCOMING_CPU", SO_INCOMING_CPU}, {"SO_RXQ_OVFL", SO_RXQ_OVFL},
        {"SO_LOCK_FILTER", SO_LOCK_FILTER}, {"SO_PEERPIDFD", SO_PEERPIDFD},
    };
    for (size_t i = 0; i < ARRAY_LEN(ints); i++) {
        emit(ID("opt.get.%s.idle", ints[i].name), "%s", sockopt_s(fd, SOL_SOCKET, ints[i].opt));
    }
    close(fd);

    /* values on connected sockets */
    int a, b, c, s;
    sockpair(&a, &b);
    conn_pair(1, &c, &s);
    emit("opt.get.SO_TYPE.sp", "%s", sockopt_s(a, SOL_SOCKET, SO_TYPE));
    emit("opt.get.SO_ACCEPTCONN.sp", "%s", sockopt_s(a, SOL_SOCKET, SO_ACCEPTCONN));
    emit("opt.get.SO_PEERCRED.sp", "%s", cred_s(a));
    emit("opt.get.SO_PEERCRED.client", "%s", cred_s(c));
    emit("opt.get.SO_PEERCRED.accepted", "%s", cred_s(s));
    fd = usock();
    emit("opt.get.SO_PEERCRED.idle", "%s", cred_s(fd));
    close(fd);
    emit("opt.get.SO_PEERPIDFD.sp", "%s", ({
        int v = -1;
        socklen_t l = sizeof(v);
        long r = SYS(getsockopt(a, SOL_SOCKET, SO_PEERPIDFD, &v, &l));
        static __thread char tmp[48];
        snprintf(tmp, sizeof(tmp), "%s fd_ge3=%d", rs(r), v >= 3);
        tmp;
    }));
    emit("opt.get.SO_PEERSEC.sp", "%s", ({
        char sec[64];
        socklen_t l = sizeof(sec);
        rs(SYS(getsockopt(a, SOL_SOCKET, SO_PEERSEC, sec, &l)));
    }));
    emit("opt.get.SO_PEERGROUPS.sp", "%s", ({
        gid_t g[8];
        socklen_t l = sizeof(g);
        long r = SYS(getsockopt(a, SOL_SOCKET, SO_PEERGROUPS, g, &l));
        static __thread char tmp[48];
        snprintf(tmp, sizeof(tmp), "%s len=%u", rs(r), (unsigned)l);
        tmp;
    }));
    emit("opt.get.SO_BINDTODEVICE.sp", "%s", ({
        char dev[32];
        socklen_t l = sizeof(dev);
        long r = SYS(getsockopt(a, SOL_SOCKET, SO_BINDTODEVICE, dev, &l));
        static __thread char tmp[48];
        snprintf(tmp, sizeof(tmp), "%s len=%u", rs(r), (unsigned)l);
        tmp;
    }));
    emit("opt.get.SO_LINGER.sp", "%s", ({
        struct linger lg = {7, 7};
        socklen_t l = sizeof(lg);
        long r = SYS(getsockopt(a, SOL_SOCKET, SO_LINGER, &lg, &l));
        static __thread char tmp[64];
        snprintf(tmp, sizeof(tmp), "%s on=%d secs=%d len=%u", rs(r), lg.l_onoff, lg.l_linger, (unsigned)l);
        tmp;
    }));

    /* set / get round trips */
    static const struct {
        const char *name;
        int opt;
        int val;
    } sets[] = {
        {"SO_KEEPALIVE", SO_KEEPALIVE, 1}, {"SO_REUSEADDR", SO_REUSEADDR, 1},
        {"SO_REUSEPORT", SO_REUSEPORT, 1}, {"SO_PASSCRED", SO_PASSCRED, 1},
        {"SO_OOBINLINE", SO_OOBINLINE, 1}, {"SO_BROADCAST", SO_BROADCAST, 1},
        {"SO_PRIORITY", SO_PRIORITY, 3}, {"SO_RCVLOWAT", SO_RCVLOWAT, 5}, {"SO_SNDLOWAT", SO_SNDLOWAT, 5},
        {"SO_TIMESTAMP", SO_TIMESTAMP, 1}, {"SO_DONTROUTE", SO_DONTROUTE, 1}, {"SO_DEBUG", SO_DEBUG, 1},
        {"SO_MARK", SO_MARK, 7}, {"SO_PASSSEC", SO_PASSSEC, 1}, {"SO_BUSY_POLL", SO_BUSY_POLL, 0},
        {"SO_SNDBUF", SO_SNDBUF, 65536}, {"SO_RCVBUF", SO_RCVBUF, 65536},
        {"SO_ERROR", SO_ERROR, 1}, {"SO_TYPE", SO_TYPE, 1}, {"SO_ACCEPTCONN", SO_ACCEPTCONN, 1},
        {"SO_DOMAIN", SO_DOMAIN, 1},
    };
    for (size_t i = 0; i < ARRAY_LEN(sets); i++) {
        int v = sets[i].val;
        long sr = SYS(setsockopt(a, SOL_SOCKET, sets[i].opt, &v, sizeof(v)));
        emit(ID("opt.set.%s", sets[i].name), "%s -> get %s", rs(sr), sockopt_s(a, SOL_SOCKET,
                sets[i].opt));
    }
    {
        int v = 1;
        long sr;
        sr = SYS(setsockopt(b, SOL_SOCKET, SO_SNDBUF, &(int) {1}, sizeof(v)));
        emit("opt.set.SO_SNDBUF.tiny", "%s -> get %s", rs(sr), sockopt_s(b, SOL_SOCKET,
                SO_SNDBUF));
        sr = SYS(setsockopt(b, SOL_SOCKET, SO_RCVBUF, &(int) {1}, sizeof(v)));
        emit("opt.set.SO_RCVBUF.tiny", "%s -> get %s", rs(sr), sockopt_s(b, SOL_SOCKET,
                SO_RCVBUF));
        /* The size is limited to net.core.wmem_max, which is not the same on every host (a host may
         * raise it to MBytes), so the result says whether it is twice that limit, like Linux. The
         * LibOS has no such file, and its limit is the default of Linux. */
        sr = SYS(setsockopt(b, SOL_SOCKET, SO_SNDBUF, &(int) {1 << 30}, sizeof(v)));
        {
            long limit = 212992;
            FILE *f = fopen("/proc/sys/net/core/wmem_max", "r");
            if (f) {
                long n;
                if (fscanf(f, "%ld", &n) == 1 && n > 0) {
                    limit = n;
                }
                fclose(f);
            }
            int got = 0;
            socklen_t got_len = sizeof(got);
            if (getsockopt(b, SOL_SOCKET, SO_SNDBUF, &got, &got_len) == 0 && got == 2 * limit) {
                emit("opt.set.SO_SNDBUF.huge", "%s -> get twice net.core.wmem_max", rs(sr));
            } else {
                emit("opt.set.SO_SNDBUF.huge", "%s -> get %s", rs(sr), sockopt_s(b, SOL_SOCKET,
                        SO_SNDBUF));
            }
        }
        emit("opt.set.SO_SNDBUF.negative", "%s", rs(SYS(setsockopt(b, SOL_SOCKET, SO_SNDBUF,
        &(int) {-5}, sizeof(v)))));
    }
    {
        struct linger lg = {1, 5};
        emit("opt.set.SO_LINGER", "%s", rs(SYS(setsockopt(a, SOL_SOCKET, SO_LINGER, &lg,
                                               sizeof(lg)))));
        struct linger g = {0, 0};
        socklen_t l = sizeof(g);
        getsockopt(a, SOL_SOCKET, SO_LINGER, &g, &l);
        emit("opt.set.SO_LINGER.get", "on=%d secs=%d", g.l_onoff, g.l_linger);
    }
    /* SO_SNDBUF has an effect on how much can be written */
    {
        int p, q;
        sockpair(&p, &q);
        int small = 4096;
        setsockopt(p, SOL_SOCKET, SO_SNDBUF, &small, sizeof(small));
        long n = fill_buf(p);
        emit("opt.sndbuf_effect.fill_after_set_4096", "%s",
             n < 100 * 1024 ? "small" : "large(setting ignored)");
        emit("info.opt.sndbuf_effect.fill_bytes", "%ld", n);
        close(p);
        close(q);
    }
    /* other levels */
    {
        int v = 1;
        emit("opt.set.TCP_NODELAY", "%s", rs(SYS(setsockopt(a, IPPROTO_TCP, 1, &v, sizeof(v)))));
        emit("opt.get.TCP_NODELAY", "%s", sockopt_s(a, IPPROTO_TCP, 1));
        emit("opt.set.IP_TTL", "%s", rs(SYS(setsockopt(a, IPPROTO_IP, 2, &v, sizeof(v)))));
        emit("opt.get.IP_TTL", "%s", sockopt_s(a, IPPROTO_IP, 2));
        emit("opt.set.IPV6_V6ONLY", "%s", rs(SYS(setsockopt(a, IPPROTO_IPV6, 26, &v,
                                             sizeof(v)))));
        emit("opt.set.SOL_UNIX_level_0", "%s", rs(SYS(setsockopt(a, 0, 1, &v, sizeof(v)))));
        emit("opt.set.level_bogus", "%s", rs(SYS(setsockopt(a, 9999, 1, &v, sizeof(v)))));
        emit("opt.get.level_bogus", "%s", sockopt_s(a, 9999, 1));
        emit("opt.set.optname_bogus", "%s", rs(SYS(setsockopt(a, SOL_SOCKET, 9999, &v,
                                               sizeof(v)))));
        emit("opt.get.optname_bogus", "%s", sockopt_s(a, SOL_SOCKET, 9999));
        emit("opt.set.optlen_short", "%s", rs(SYS(setsockopt(a, SOL_SOCKET, SO_KEEPALIVE, &v,
                                              2))));
        emit("opt.set.optlen_zero", "%s", rs(SYS(setsockopt(a, SOL_SOCKET, SO_KEEPALIVE, &v,
                                             0))));
        emit("opt.set.optval_null", "%s", rs(SYS(setsockopt(a, SOL_SOCKET, SO_KEEPALIVE, NULL,
                                             4))));
        emit("opt.set.optval_bad_ptr", "%s", rs(SYS(setsockopt(a, SOL_SOCKET, SO_KEEPALIVE,
                                                (void *)1, 4))));
    }
    /* getsockopt buffer sizes */
    {
        char big[16];
        socklen_t l = 1;
        long r = SYS(getsockopt(a, SOL_SOCKET, SO_TYPE, big, &l));
        emit("opt.get.optlen_1", "%s len=%u", rs(r), (unsigned)l);
        l = 8;
        r = SYS(getsockopt(a, SOL_SOCKET, SO_TYPE, big, &l));
        emit("opt.get.optlen_8", "%s len=%u", rs(r), (unsigned)l);
        l = 0;
        r = SYS(getsockopt(a, SOL_SOCKET, SO_TYPE, big, &l));
        emit("opt.get.optlen_0", "%s len=%u", rs(r), (unsigned)l);
        r = SYS(getsockopt(a, SOL_SOCKET, SO_TYPE, NULL, &l));
        emit("opt.get.optval_null_len0", "%s", rs(r));
        l = 4;
        r = SYS(getsockopt(a, SOL_SOCKET, SO_TYPE, NULL, &l));
        emit("opt.get.optval_null_len4", "%s", rs(r));
        l = 4;
        r = SYS(getsockopt(a, SOL_SOCKET, SO_TYPE, big, NULL));
        emit("opt.get.optlen_null", "%s", rs(r));
    }
    /* SO_ERROR and the state of an in-use socket */
    emit("opt.get.SO_ERROR.client", "%s", sockopt_s(c, SOL_SOCKET, SO_ERROR));
    emit("opt.get.SO_ACCEPTCONN.accepted", "%s", sockopt_s(s, SOL_SOCKET, SO_ACCEPTCONN));
    emit("opt.get.SO_SNDBUF.accepted", "%s", sockopt_s(s, SOL_SOCKET, SO_SNDBUF));
    close(a);
    close(b);
    close(c);
    close(s);
}

/* ------------------------------------------------------------------ */
/* c17: ancillary data                                                 */
/* ------------------------------------------------------------------ */

static const char *cmsg_s(struct msghdr *m) {
    static __thread char b[8][160];
    static __thread int i;
    char *p = b[i++ & 7];
    p[0] = 0;
    int k = 0;
    if (m->msg_controllen == 0 || m->msg_control == NULL) {
        return "none";
    }
    for (struct cmsghdr *c = CMSG_FIRSTHDR(m); c && k < 140; c = CMSG_NXTHDR(m, c)) {
        if (c->cmsg_len < sizeof(struct cmsghdr) || c->cmsg_len > m->msg_controllen) {
            k += snprintf(p + k, 160 - k, "[garbage header] ");
            break;
        }
        k += snprintf(p + k, 160 - k, "[lvl=%d type=%d len=%zu] ", c->cmsg_level, c->cmsg_type,
                      (size_t)c->cmsg_len);
    }
    if (k > 0 && p[k - 1] == ' ') {
        p[k - 1] = 0;   // no blank at the end
    }
    return p;
}

static long send_with_fd(int sock, const char *payload, int fd) {
    char ctl[CMSG_SPACE(sizeof(int))];
    memset(ctl, 0, sizeof(ctl));
    struct iovec iov = {(void *)payload, strlen(payload)};
    struct msghdr m;
    memset(&m, 0, sizeof(m));
    m.msg_iov = &iov;
    m.msg_iovlen = 1;
    m.msg_control = ctl;
    m.msg_controllen = sizeof(ctl);
    struct cmsghdr *c = CMSG_FIRSTHDR(&m);
    c->cmsg_level = SOL_SOCKET;
    c->cmsg_type = SCM_RIGHTS;
    c->cmsg_len = CMSG_LEN(sizeof(int));
    memcpy(CMSG_DATA(c), &fd, sizeof(int));
    return SYS(sendmsg(sock, &m, MSG_NOSIGNAL));
}

/* receives up to n data bytes with a control buffer; describes the result; the received fd (if any) in *fdout */
static const char *recv_ctl(int sock, size_t n, size_t ctllen, int flags, int *fdout,
                            char *data) {
    static __thread char b[8][256];
    static __thread int i;
    char *p = b[i++ & 7];
    char ctl[256];
    char local[64];
    if (!data) {
        data = local;
    }
    memset(ctl, 0, sizeof(ctl));
    struct iovec iov = {data, n};
    struct msghdr m;
    memset(&m, 0, sizeof(m));
    m.msg_iov = &iov;
    m.msg_iovlen = 1;
    if (ctllen) {
        m.msg_control = ctl;
        m.msg_controllen = ctllen;
    }
    long r = SYS(recvmsg(sock, &m, flags));
    if (r < 0) {
        snprintf(p, 256, "%s", rs(r));
        return p;
    }
    int nfd = -1;
    for (struct cmsghdr *c = m.msg_controllen ? CMSG_FIRSTHDR(&m) : NULL; c;
            c = CMSG_NXTHDR(&m, c)) {
        if (c->cmsg_level == SOL_SOCKET && c->cmsg_type == SCM_RIGHTS) {
            nfd = (int)((c->cmsg_len - CMSG_LEN(0)) / sizeof(int));
            if (nfd > 0 && fdout) {
                memcpy(fdout, CMSG_DATA(c), sizeof(int));
            }
        }
    }
    snprintf(p, 256, "%ld controllen=%zu flags=0x%x nfd=%d cmsgs=%s", r,
             (size_t)m.msg_controllen,
             m.msg_flags, nfd, cmsg_s(&m));
    return p;
}

CHECK(c17_ancillary) {
    int c, s;
    char buf[64];

    /* a received file descriptor works */
    conn_pair(1, &c, &s);
    {
        int pp[2];
        pipe(pp);
        emit("anc.rights.send", "%s", rs(send_with_fd(c, "x", pp[0])));
        int got = -1;
        char data[8] = {0};
        emit("anc.rights.recv", "%s", recv_ctl(s, 8, CMSG_SPACE(sizeof(int)), 0, &got, data));
        emit("anc.rights.fd_is_new", "%d", got >= 0 && got != pp[0]);
        if (got >= 0) {
            write(pp[1], "pipe!", 5);
            char r[8] = {0};
            long rr = SYS(read(got, r, 8));
            emit("anc.rights.fd_works", "%s %.5s", rs(rr), r);
            close(got);
        }
        close(pp[0]);
        close(pp[1]);
    }
    close(c);
    close(s);

    /* the same with a receive buffer of exactly the length of the data */
    conn_pair(1, &c, &s);
    {
        int pp[2];
        pipe(pp);
        send_with_fd(c, "x", pp[0]);
        int got = -1;
        char data[8] = {0};
        emit("anc.rights.exact_len.recv", "%s", recv_ctl(s, 1, CMSG_SPACE(sizeof(int)), 0, &got,
                data));
        if (got >= 0) {
            write(pp[1], "pipe!", 5);
            char r[8] = {0};
            long rr = SYS(read(got, r, 8));
            emit("anc.rights.exact_len.fd_works", "%s %.5s", rs(rr), r);
            close(got);
        }
        close(pp[0]);
        close(pp[1]);
    }
    close(c);
    close(s);

    /* the data of a message with a descriptor read by a plain read() */
    conn_pair(1, &c, &s);
    {
        int pp[2];
        pipe(pp);
        send_with_fd(c, "x", pp[0]);
        emit("anc.rights.plain_read_gets", "%s", ({
            long r = nb_read(s, buf, sizeof(buf));
            static __thread char tmp[48];
            snprintf(tmp, sizeof(tmp), "%s", rs(r));
            tmp;
        }));
        emit("anc.rights.plain_read_then_more", "%s", nb_read_s(s, 64));
        close(pp[0]);
        close(pp[1]);
    }
    close(c);
    close(s);

    /* descriptor message followed by plain data */
    conn_pair(1, &c, &s);
    {
        int pp[2];
        pipe(pp);
        send_with_fd(c, "x", pp[0]);
        write(c, "yz", 2);
        int got = -1;
        emit("anc.rights.then_plain.recv1", "%s", recv_ctl(s, 16, CMSG_SPACE(sizeof(int)), 0,
                &got, NULL));
        emit("anc.rights.then_plain.recv2", "%s", nb_read_s(s, 16));
        closefd(got);
        close(pp[0]);
        close(pp[1]);
    }
    close(c);
    close(s);

    /* no control buffer: the descriptor is dropped */
    conn_pair(1, &c, &s);
    {
        int pp[2];
        pipe(pp);
        send_with_fd(c, "x", pp[0]);
        emit("anc.rights.recv_without_ctl_buffer", "%s", recv_ctl(s, 8, 0, 0, NULL, NULL));
        emit("anc.rights.recv_without_ctl_buffer.then_read", "%s", nb_read_s(s, 64));
        close(pp[0]);
        close(pp[1]);
    }
    close(c);
    close(s);

    /* control buffer too small */
    conn_pair(1, &c, &s);
    {
        int pp[2];
        pipe(pp);
        send_with_fd(c, "x", pp[0]);
        emit("anc.rights.ctl_buffer_too_small", "%s", recv_ctl(s, 8, CMSG_LEN(0), 0, NULL, NULL));
        close(pp[0]);
        close(pp[1]);
    }
    close(c);
    close(s);

    /* recvmsg with a control buffer, but the sender sent no control data */
    conn_pair(1, &c, &s);
    write(c, "hello", 5);
    {
        op_t *o = new_op(OP_RECVMSG_CTL, s);
        o->len = 5;
        long long el;
        long r = timed_op(o, 1500, &el);
        emit("anc.recvmsg_ctl_blocking_data_only", "%s controllen=%d",
             r == BLOCKED ? "BLOCKED" : rs(r),
             r == BLOCKED ? -1 : o->extra);
    }
    close(c);
    close(s);
    conn_pair(1, &c, &s);
    write(c, "hello", 5);
    set_nb(s, 1);
    emit("anc.recvmsg_ctl_nonblocking_data_only", "%s", recv_ctl(s, 5, 64, 0, NULL, NULL));
    emit("anc.recvmsg_ctl_nonblocking_data_only.then_read", "%s", nb_read_s(s, 64));
    close(c);
    close(s);
    conn_pair(1, &c, &s);
    write(c, "hello" "world!", 11);
    set_nb(s, 1);
    emit("anc.recvmsg_ctl_following_data", "%s", recv_ctl(s, 5, 64, 0, NULL, NULL));
    emit("anc.recvmsg_ctl_following_data.then_read", "%s", nb_read_s(s, 64));
    close(c);
    close(s);

    /* SO_PASSCRED */
    conn_pair(1, &c, &s);
    {
        int one = 1;
        emit("anc.passcred.set", "%s", rs(SYS(setsockopt(s, SOL_SOCKET, SO_PASSCRED, &one,
                                              sizeof(one)))));
        write(c, "p", 1);
        set_nb(s, 1);
        char data[8];
        const char *r = recv_ctl(s, 8, 128, 0, NULL, data);
        emit("anc.passcred.recv", "%s", r);
        emit("anc.passcred.then_read", "%s", nb_read_s(s, 16));
        /* explicit credentials */
        struct ucred uc = {getpid(), getuid(), getgid()};
        char ctl[CMSG_SPACE(sizeof(uc))];
        memset(ctl, 0, sizeof(ctl));
        struct iovec iov = {"q", 1};
        struct msghdr m;
        memset(&m, 0, sizeof(m));
        m.msg_iov = &iov;
        m.msg_iovlen = 1;
        m.msg_control = ctl;
        m.msg_controllen = sizeof(ctl);
        struct cmsghdr *ch = CMSG_FIRSTHDR(&m);
        ch->cmsg_level = SOL_SOCKET;
        ch->cmsg_type = SCM_CREDENTIALS;
        ch->cmsg_len = CMSG_LEN(sizeof(uc));
        memcpy(CMSG_DATA(ch), &uc, sizeof(uc));
        emit("anc.credentials.send", "%s", rs(SYS(sendmsg(c, &m, MSG_NOSIGNAL))));
        emit("anc.credentials.recv", "%s", recv_ctl(s, 8, 128, 0, NULL, data));
        emit("anc.credentials.then_read", "%s", nb_read_s(s, 16));
    }
    close(c);
    close(s);

    /* invalid control messages */
    conn_pair(1, &c, &s);
    {
        char ctl[CMSG_SPACE(4)];
        memset(ctl, 0, sizeof(ctl));
        struct iovec iov = {"b", 1};
        struct msghdr m;
        memset(&m, 0, sizeof(m));
        m.msg_iov = &iov;
        m.msg_iovlen = 1;
        m.msg_control = ctl;
        m.msg_controllen = sizeof(ctl);
        struct cmsghdr *ch = CMSG_FIRSTHDR(&m);
        ch->cmsg_level = SOL_SOCKET;
        ch->cmsg_type = 9999;
        ch->cmsg_len = CMSG_LEN(4);
        emit("anc.bogus_cmsg.send", "%s", rs(SYS(sendmsg(c, &m, MSG_NOSIGNAL))));
        emit("anc.bogus_cmsg.peer_read", "%s", nb_read_s(s, 64));
        ch->cmsg_level = 12345;
        ch->cmsg_type = 1;
        emit("anc.bogus_level.send", "%s", rs(SYS(sendmsg(c, &m, MSG_NOSIGNAL))));
        emit("anc.bogus_level.peer_read", "%s", nb_read_s(s, 64));
        m.msg_controllen = 3;
        emit("anc.short_controllen.send", "%s", rs(SYS(sendmsg(c, &m, MSG_NOSIGNAL))));
        emit("anc.short_controllen.peer_read", "%s", nb_read_s(s, 64));
    }
    close(c);
    close(s);
}

/* ------------------------------------------------------------------ */
/* c18: inheritance through spawn                                      */
/* ------------------------------------------------------------------ */

static char
g_self_path[PATH_MAX]; /* realpath() needs PATH_MAX: glibc's fortified version aborts with less */
extern char **environ;

static int run_child(const char *role, int fd, int *status) {
    char fdstr[16];
    snprintf(fdstr, sizeof(fdstr), "%d", fd);
    char *argv[] = {g_self_path, "--child", (char *)role, fdstr, NULL};
    pid_t pid;
    int rc = posix_spawn(&pid, g_self_path, NULL, NULL, argv, environ);
    if (rc != 0) {
        *status = -1;
        return -rc;
    }
    int st = 0;
    long long t0 = now_ms();
    for (;;) {
        pid_t w = waitpid(pid, &st, WNOHANG);
        if (w == pid) {
            break;
        }
        if (now_ms() - t0 > 6000) {
            kill(pid, SIGKILL);
            waitpid(pid, &st, 0);
            *status = -2;
            return 0;
        }
        msleep(10);
    }
    *status = WIFEXITED(st) ? WEXITSTATUS(st) : 200 + WTERMSIG(st);
    return 0;
}

static int child_main(const char *role, int fd) {
    char buf[16];
    if (!strcmp(role, "echo")) {
        long n = SYS(read(fd, buf, 5));
        if (n != 5) {
            return 11;
        }
        for (int i = 0; i < 5; i++) {
            if (buf[i] >= 'a' && buf[i] <= 'z') {
                buf[i] -= 32;
            }
        }
        return write(fd, buf, 5) == 5 ? 0 : 12;
    }
    if (!strcmp(role, "cloexec")) {
        return fcntl(fd, F_GETFD) == -1 && errno == EBADF ? 0 : 10;
    }
    if (!strcmp(role, "nonblock")) {
        long n = SYS(read(fd, buf, 1));
        return n == -EAGAIN ? 0 : 10;
    }
    if (!strcmp(role, "accept")) {
        struct pollfd pf = {fd, POLLIN, 0};
        if (poll(&pf, 1, 3000) != 1) {
            return 13;
        }
        int s = accept(fd, NULL, NULL);
        if (s < 0) {
            return 14;
        }
        long n = SYS(read(s, buf, 3));
        return n == 3 ? 0 : 15;
    }
    if (!strcmp(role, "eof")) {
        /* the parent closes its end: the child sees EOF */
        long n = SYS(read(fd, buf, 4));
        return n == 0 ? 0 : 10;
    }
    return 99;
}

CHECK(c18_spawn) {
    int a, b, st;
    long rc;

    sockpair(&a, &b);
    write(a, "hello", 5);
    rc = run_child("echo", b, &st);
    emit("spawn.echo.child_status", "spawn=%s status=%d", rs(-rc), st);
    emit("spawn.echo.reply", "%s", nb_read_s(a, 8));
    close(a);
    close(b);

    sockpair(&a, &b);
    fcntl(b, F_SETFD, FD_CLOEXEC);
    rc = run_child("cloexec", b, &st);
    emit("spawn.cloexec_not_inherited", "spawn=%s status=%d", rs(-rc), st);
    close(a);
    close(b);

    sockpair(&a, &b);
    set_nb(b, 1);
    rc = run_child("nonblock", b, &st);
    emit("spawn.nonblock_flag_inherited", "spawn=%s status=%d", rs(-rc), st);
    close(a);
    close(b);

    {
        uaddr_t ad;
        int l = listener(1, 4, &ad);
        int c = client_to(&ad);
        write(c, "abc", 3);
        rc = run_child("accept", l, &st);
        emit("spawn.child_accepts_inherited_listener", "spawn=%s status=%d", rs(-rc), st);
        close(c);
        close(l);
    }
    {
        /* child exit closes its descriptors: the parent sees EOF */
        sockpair(&a, &b);
        write(a, "hello", 5);
        rc = run_child("echo", b, &st);
        close(b);
        emit("spawn.after_child_exit_and_parent_close_b.read", "%s", nb_read_s(a, 8));
        emit("spawn.after_child_exit_and_parent_close_b.read2", "%s", nb_read_s(a, 8));
        close(a);
    }
}

/* ------------------------------------------------------------------ */
/* c19: memory per object (Occlum: KernelHeapInUse of /proc/meminfo)   */
/* ------------------------------------------------------------------ */

static long kh_kb(void) {
    char buf[1024];
    int fd = open("/proc/meminfo", O_RDONLY);
    if (fd < 0) {
        return -1;
    }
    int len = read(fd, buf, sizeof(buf) - 1);
    close(fd);
    if (len <= 0) {
        return -1;
    }
    buf[len] = 0;
    char *line = strstr(buf, "KernelHeapInUse:");
    long kb = -1;
    if (!line || sscanf(line, "KernelHeapInUse: %ld", &kb) != 1) {
        return -1;
    }
    return kb;
}

static long khpeak_kb(void) {
    char buf[1024];
    int fd = open("/proc/meminfo", O_RDONLY);
    if (fd < 0) {
        return -1;
    }
    int len = read(fd, buf, sizeof(buf) - 1);
    close(fd);
    if (len <= 0) {
        return -1;
    }
    buf[len] = 0;
    char *line = strstr(buf, "KernelHeapPeakUsed:");
    long kb = -1;
    if (!line || sscanf(line, "KernelHeapPeakUsed: %ld", &kb) != 1) {
        return -1;
    }
    return kb;
}

static void mem_report(const char *what, long before, long after, int n) {
    char id[120];
    snprintf(id, sizeof(id), "info.mem.%s", what);
    if (before < 0 || after < 0) {
        emit(id, "n/a");
    } else {
        emit(id, "n=%d total_kb=%ld per_object_bytes=%ld", n, after - before,
             n ? (after - before) * 1024 / n : 0);
    }
}

CHECK(c19_memory) {
    enum { N = 20 };
    int fds[2 * N + 8];
    long k0, k1;

    /* idle sockets */
    k0 = kh_kb();
    for (int i = 0; i < N; i++) {
        fds[i] = usock();
    }
    k1 = kh_kb();
    mem_report("idle_socket", k0, k1, N);
    for (int i = 0; i < N; i++) {
        close(fds[i]);
    }
    mem_report("idle_socket.residual_after_close", k0, kh_kb(), 1);

    /* socketpairs */
    k0 = kh_kb();
    for (int i = 0; i < N; i++) {
        sockpair(&fds[2 * i], &fds[2 * i + 1]);
        if (i == N / 2 - 1) {
            /* 10 pairs are enough */
        }
    }
    k1 = kh_kb();
    mem_report("socketpair", k0, k1, N);
    for (int i = 0; i < N; i++) {
        write(fds[2 * i], "x", 1);
    }
    mem_report("socketpair.after_1_byte_each", k0, kh_kb(), N);
    for (int i = 0; i < N; i++) {
        shutdown(fds[2 * i], SHUT_RDWR);
    }
    mem_report("socketpair.after_shutdown_rdwr_of_one_end", k0, kh_kb(), N);
    for (int i = 0; i < 2 * N; i++) {
        close(fds[i]);
    }
    mem_report("socketpair.residual_after_close", k0, kh_kb(), 1);

    /* pipes (they use the same channel, with 1 MiB) */
    k0 = kh_kb();
    for (int i = 0; i < 10; i++) {
        int pp[2];
        if (pipe(pp) == 0) {
            fds[2 * i] = pp[0];
            fds[2 * i + 1] = pp[1];
        }
    }
    k1 = kh_kb();
    mem_report("pipe", k0, k1, 10);
    for (int i = 0; i < 20; i++) {
        close(fds[i]);
    }

    /* bound sockets */
    k0 = kh_kb();
    for (int i = 0; i < N; i++) {
        uaddr_t a = nabs("mem_bound");
        fds[i] = usock();
        BIND(fds[i], a);
    }
    k1 = kh_kb();
    mem_report("bound_abstract_socket", k0, k1, N);
    for (int i = 0; i < N; i++) {
        close(fds[i]);
    }

    /* listeners with different backlogs */
    int bls[] = {1, 128, 4096};
    for (size_t b = 0; b < ARRAY_LEN(bls); b++) {
        k0 = kh_kb();
        for (int i = 0; i < N; i++) {
            uaddr_t a = nabs("mem_lis");
            fds[i] = listener_at(&a, bls[b]);
        }
        k1 = kh_kb();
        {
            char nm[48];
            snprintf(nm, sizeof(nm), "listener_backlog_%d", bls[b]);
            mem_report(nm, k0, k1, N);
        }
        for (int i = 0; i < N; i++) {
            close(fds[i]);
        }
    }

    /* pending connections */
    {
        uaddr_t a;
        int l = listener(1, 64, &a);
        enum { P = 12 };
        k0 = kh_kb();
        int n = 0;
        for (int i = 0; i < P; i++) {
            fds[i] = client_to(&a);
            if (fds[i] >= 0) {
                n++;
            }
        }
        k1 = kh_kb();
        mem_report("pending_connection", k0, k1, n);
        for (int i = 0; i < n; i++) {
            write(fds[i], "x", 1);
        }
        mem_report("pending_connection.after_1_byte_written", k0, kh_kb(), n);
        long k2 = kh_kb();
        int acc[P];
        for (int i = 0; i < n; i++) {
            acc[i] = accept(l, NULL, NULL);
        }
        mem_report("pending_to_accepted.delta_of_accept", k2, kh_kb(), n);
        for (int i = 0; i < n; i++) {
            closefd(acc[i]);
        }
        for (int i = 0; i < n; i++) {
            close(fds[i]);
        }
        mem_report("pending_connection.residual_after_close", k0, kh_kb(), 1);
        close(l);
    }

    /* accepted pairs, with and without data */
    {
        uaddr_t a;
        int l = listener(1, 64, &a);
        enum { P = 10 };
        int cs[P], ss[P];
        k0 = kh_kb();
        for (int i = 0; i < P; i++) {
            cs[i] = client_to(&a);
            ss[i] = accept(l, NULL, NULL);
        }
        k1 = kh_kb();
        mem_report("accepted_pair", k0, k1, P);
        char *chunk = malloc(65536);
        memset(chunk, 'm', 65536);
        for (int i = 0; i < P; i++) {
            nb_write(cs[i], chunk, 65536);
            nb_write(ss[i], chunk, 65536);
        }
        mem_report("accepted_pair.after_64k_each_way", k0, kh_kb(), P);
        free(chunk);
        for (int i = 0; i < P; i++) {
            close(cs[i]);
            close(ss[i]);
        }
        mem_report("accepted_pair.residual_after_close", k0, kh_kb(), 1);
        close(l);
    }

    /* the number of pending connections that fit into the heap */
    {
        uaddr_t a;
        int l = listener(1, 4096, &a);
        int *cs = malloc(sizeof(int) * 400);
        int n = 0, err = 0;
        long start = kh_kb();
        while (n < 400) {
            long cur = kh_kb();
            if (cur >= 0 && cur > 28000) {
                err = -1;
                break;
            }
            int c = usock();
            set_nb(c, 1);
            if (CONNECT(c, a) < 0) {
                err = errno;
                close(c);
                break;
            }
            cs[n++] = c;
        }
        emit("info.mem.max_pending_connections", "n=%d%s stop=%s heap_kb=%ld start_kb=%ld", n,
             n >= 400 ? "(cap)" : "", err == -1 ? "heap_guard_28MB" : (err ? enm(err) : "cap"),
             kh_kb(), start);
        for (int i = 0; i < n; i++) {
            close(cs[i]);
        }
        free(cs);
        close(l);
    }
    emit("info.mem.peak_kb", "%ld", khpeak_kb());

    /* many descriptors */
    {
        int n = 0;
        int *f = malloc(sizeof(int) * 2000);
        int err = 0;
        for (; n < 1900; n++) {
            f[n] = usock();
            if (f[n] < 0) {
                err = errno;
                break;
            }
        }
        emit("mem.many_sockets_created", "%s", n >= 1900 ? "1900+" : ({
            static __thread char tmp[48];
            snprintf(tmp, sizeof(tmp), "stopped_at_%d_%s", n, enm(err));
            tmp;
        }));
        for (int i = 0; i < n; i++) {
            close(f[i]);
        }
        free(f);
    }
}

/* ------------------------------------------------------------------ */
/* c22: concurrency stress                                             */
/* ------------------------------------------------------------------ */

typedef struct {
    int fd;
    int echo;
    int rounds;
    int errors;
    int timeouts;
    atomic_int done;
} pp_t;

static int read_n(int fd, char *buf, int n, int tmo) {
    int got = 0;
    while (got < n) {
        struct pollfd pf = {fd, POLLIN, 0};
        int pr = poll(&pf, 1, tmo);
        if (pr == 0) {
            return -2;
        }
        if (pr < 0) {
            return -1;
        }
        ssize_t r = read(fd, buf + got, n - got);
        if (r <= 0) {
            return -1;
        }
        got += (int)r;
    }
    return got;
}

static void *th_pingpong(void *p) {
    pp_t *c = p;
    char *buf = malloc(8192), *rb = malloc(8192);
    for (int i = 0; i < c->rounds; i++) {
        int n = 1 + (i * 37) % 8000;
        if (c->echo) {
            int r = read_n(c->fd, buf, n, 5000);
            if (r == -2) {
                c->timeouts++;
                break;
            }
            if (r < 0) {
                c->errors++;
                break;
            }
            if (write(c->fd, buf, n) != n) {
                c->errors++;
                break;
            }
        } else {
            memset(buf, 'a' + i % 26, n);
            ssize_t w = write(c->fd, buf, n);
            if (w != n) {
                c->errors++;
                break;
            }
            int r = read_n(c->fd, rb, n, 5000);
            if (r == -2) {
                c->timeouts++;
                break;
            }
            if (r < 0 || memcmp(buf, rb, n) != 0) {
                c->errors++;
                break;
            }
        }
    }
    free(buf);
    free(rb);
    atomic_store(&c->done, 1);
    return NULL;
}

typedef struct {
    uaddr_t addr;
    int count;
    int ok;
    int failed;
    int last_errno;
    atomic_int done;
} storm_t;

static void *th_storm_client(void *p) {
    storm_t *s = p;
    for (int i = 0; i < s->count; i++) {
        int c = usock();
        if (CONNECT(c, s->addr) < 0) {
            s->failed++;
            s->last_errno = errno;
            close(c);
            continue;
        }
        char m[4] = "ping";
        if (write(c, m, 4) != 4) {
            s->failed++;
            s->last_errno = errno;
            close(c);
            continue;
        }
        char r[4];
        if (read_n(c, r, 2, 5000) == 2) {
            s->ok++;
        } else {
            s->failed++;
        }
        close(c);
    }
    atomic_store(&s->done, 1);
    return NULL;
}

typedef struct {
    int lfd;
    int expect;
    int served;
    atomic_int done;
} srv_t;

static void *th_storm_server(void *p) {
    srv_t *s = p;
    while (s->served < s->expect) {
        struct pollfd pf = {s->lfd, POLLIN, 0};
        if (poll(&pf, 1, 6000) <= 0) {
            break;
        }
        int c = accept(s->lfd, NULL, NULL);
        if (c < 0) {
            continue;
        }
        char b[4];
        if (read_n(c, b, 4, 3000) == 4) {
            write(c, "ok", 2);
        }
        close(c);
        s->served++;
    }
    atomic_store(&s->done, 1);
    return NULL;
}

CHECK(c22_stress) {
    {
        enum { P = 4 };
        int sv[P][2];
        pp_t ctx[2 * P];
        pthread_t th[2 * P];
        for (int i = 0; i < P; i++) {
            sockpair(&sv[i][0], &sv[i][1]);
            memset(&ctx[2 * i], 0, sizeof(pp_t));
            memset(&ctx[2 * i + 1], 0, sizeof(pp_t));
            ctx[2 * i].fd = sv[i][0];
            ctx[2 * i].rounds = 150;
            ctx[2 * i + 1].fd = sv[i][1];
            ctx[2 * i + 1].echo = 1;
            ctx[2 * i + 1].rounds = 150;
            pthread_create(&th[2 * i], NULL, th_pingpong, &ctx[2 * i]);
            pthread_create(&th[2 * i + 1], NULL, th_pingpong, &ctx[2 * i + 1]);
        }
        long long t0 = now_ms();
        int errors = 0, timeouts = 0, unfinished = 0;
        for (int i = 0; i < 2 * P; i++) {
            while (!atomic_load(&ctx[i].done) && now_ms() - t0 < 30000) {
                msleep(10);
            }
            if (!atomic_load(&ctx[i].done)) {
                unfinished++;
                pthread_detach(th[i]);
            } else {
                pthread_join(th[i], NULL);
            }
            errors += ctx[i].errors;
            timeouts += ctx[i].timeouts;
        }
        emit("stress.pingpong_4_pairs_150_rounds", "errors=%d timeouts=%d unfinished=%d", errors,
             timeouts, unfinished);
        emit("info.stress.pingpong.ms", "%lld", now_ms() - t0);
        for (int i = 0; i < P; i++) {
            close(sv[i][0]);
            close(sv[i][1]);
        }
    }
    {
        uaddr_t a;
        int l = listener(1, 128, &a);
        enum { C = 4, PER = 40 };
        storm_t st[C];
        pthread_t th[C];
        srv_t srv;
        memset(&srv, 0, sizeof(srv));
        srv.lfd = l;
        srv.expect = C * PER;
        pthread_t sth;
        pthread_create(&sth, NULL, th_storm_server, &srv);
        for (int i = 0; i < C; i++) {
            memset(&st[i], 0, sizeof(st[i]));
            st[i].addr = a;
            st[i].count = PER;
            pthread_create(&th[i], NULL, th_storm_client, &st[i]);
        }
        long long t0 = now_ms();
        int ok = 0, failed = 0, unfinished = 0, err = 0;
        for (int i = 0; i < C; i++) {
            while (!atomic_load(&st[i].done) && now_ms() - t0 < 40000) {
                msleep(10);
            }
            if (!atomic_load(&st[i].done)) {
                unfinished++;
                pthread_detach(th[i]);
            } else {
                pthread_join(th[i], NULL);
            }
            ok += st[i].ok;
            failed += st[i].failed;
            if (st[i].last_errno) {
                err = st[i].last_errno;
            }
        }
        emit("stress.connect_storm_4x40", "ok=%d failed=%d unfinished=%d last_errno=%s", ok,
             failed, unfinished, err ? enm(err) : "none");
        emit("info.stress.connect_storm.ms", "%lld served=%d", now_ms() - t0, srv.served);
        close(l);
        pthread_detach(sth);
    }
}

/* ------------------------------------------------------------------ */
/* groups that may crash the LibOS: each runs on its own at the end    */
/* ------------------------------------------------------------------ */

CHECK(c90_anc_rights_over_socketpair) {
    int a, b;
    sockpair(&a, &b);
    int pp[2];
    pipe(pp);
    emit("crash.rights_socketpair.send", "%s", rs(send_with_fd(a, "x", pp[0])));
    int got = -1;
    emit("crash.rights_socketpair.recv", "%s", recv_ctl(b, 1, CMSG_SPACE(sizeof(int)), 0,
            &got, NULL));
    emit("crash.rights_socketpair.fd_works", "%d", got >= 0 && write(pp[1], "k", 1) == 1 &&
    nb_read(got, (char[2]) {0}, 1) == 1);
}

CHECK(c90_anc_rights_sender_closed_fd) {
    int c, s;
    conn_pair(1, &c, &s);
    int pp[2];
    pipe(pp);
    emit("crash.rights_closed.send", "%s", rs(send_with_fd(c, "x", pp[0])));
    close(pp[0]);
    int got = -1;
    emit("crash.rights_closed.recv", "%s", recv_ctl(s, 1, CMSG_SPACE(sizeof(int)), 0, &got,
            NULL));
    emit("crash.rights_closed.fd_works", "%d", got >= 0 && write(pp[1], "k", 1) == 1 &&
    nb_read(got, (char[2]) {0}, 1) == 1);
}

typedef struct {
    uaddr_t addr;
    int pipe_rd;
    int client;
    atomic_int done;
} sender_t;

static void *th_sender(void *p) {
    sender_t *s = p;
    int c = usock();
    if (CONNECT(c, s->addr) == 0) {
        send_with_fd(c, "x", s->pipe_rd);
    }
    s->client = c;
    atomic_store(&s->done, 1);
    return NULL;
}

CHECK(c90_anc_rights_sender_thread_exited) {
    uaddr_t a;
    int l = listener(1, 4, &a);
    int pp[2];
    pipe(pp);
    sender_t st;
    memset(&st, 0, sizeof(st));
    st.addr = a;
    st.pipe_rd = pp[0];
    pthread_t t;
    pthread_create(&t, NULL, th_sender, &st);
    pthread_join(t, NULL);
    int s = accept(l, NULL, NULL);
    int got = -1;
    emit("crash.rights_thread_gone.recv", "%s", recv_ctl(s, 1, CMSG_SPACE(sizeof(int)), 0,
            &got, NULL));
    emit("crash.rights_thread_gone.fd_works", "%d", got >= 0 && write(pp[1], "k", 1) == 1 &&
    nb_read(got, (char[2]) {0}, 1) == 1);
}

CHECK(c91_abstract_non_utf8) {
    uaddr_t x;
    memset(&x, 0, sizeof(x));
    x.sa.sun_family = AF_UNIX;
    x.sa.sun_path[0] = 0;
    x.sa.sun_path[1] = (char)0xff;
    x.sa.sun_path[2] = (char)0xfe;
    snprintf(x.sa.sun_path + 3, 40, "uds.%d", g_pid);
    x.len = (socklen_t)(offsetof(struct sockaddr_un,
                                 sun_path) + 3 + strlen(x.sa.sun_path + 3));
    int fd = usock();
    emit("crash.abstract_non_utf8.bind", "%s", rs(SYS(BIND(fd, x))));
    int c = usock();
    listen(fd, 2);
    emit("crash.abstract_non_utf8.connect", "%s", rs(SYS(CONNECT(c, x))));
    emit("crash.abstract_non_utf8.getsockname", "%s", gsn(fd, &x));
}

typedef struct {
    int fd;
    int iters;
    atomic_int stop;
    atomic_int done;
    long counter;
} nbrace_t;

static void *th_toggle(void *p) {
    nbrace_t *r = p;
    for (int i = 0; i < r->iters && !atomic_load(&r->stop); i++) {
        set_nb(r->fd, i & 1);
    }
    atomic_store(&r->stop, 1);
    atomic_store(&r->done, 1);
    return NULL;
}

static void *th_getfl(void *p) {
    nbrace_t *r = p;
    while (!atomic_load(&r->stop)) {
        r->counter += fcntl(r->fd, F_GETFL) & O_NONBLOCK;
    }
    return NULL;
}

CHECK(c92_nonblock_race) {
    int a, b;
    sockpair(&a, &b);
    nbrace_t r;
    memset(&r, 0, sizeof(r));
    r.fd = a;
    r.iters = 30000;
    pthread_t t1, t2;
    pthread_create(&t1, NULL, th_toggle, &r);
    pthread_create(&t2, NULL, th_getfl, &r);
    pthread_join(t1, NULL);
    pthread_join(t2, NULL);
    emit("crash.nonblock_toggle_vs_getfl_race", "survived");
    close(a);
    close(b);
}

/* listen() again while another thread blocks in accept(): the resize waits for the
 * accepting thread, and then connect() hangs behind the pending writer */
CHECK(c93_relisten_while_accept_blocked) {
    {
        /* listen() while another thread blocks in accept() */
        uaddr_t a;
        int l = listener(1, 2, &a);
        op_t *o = new_op(OP_ACCEPT, l);
        pthread_t t;
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setstacksize(&attr, 256 * 1024);
        atomic_store(&o->done, 0);
        pthread_create(&t, &attr, op_thread, o);
        pthread_detach(t);
        msleep(150);
        op_t *o2 = new_op(OP_LISTEN, l);
        o2->arg = 5;
        long long el;
        long r = timed_op(o2, 1500, &el);
        emit("backlog.relisten_while_accept_blocked", "%s", r == BLOCKED ? "BLOCKED" : rs(r));
        /* a connect now: does it reach the blocked accept? */
        int c = usock();
        set_nb(c, 1);
        long cr = SYS(CONNECT(c, a));
        msleep(300);
        emit("backlog.relisten_while_accept_blocked.connect_then_accept_done",
             "connect=%s accept_done=%d",
             rs(cr), atomic_load(&o->done));
        close(c);
        close(l);
    }
}

/* ------------------------------------------------------------------ */
/* main                                                                */
/* ------------------------------------------------------------------ */

int main(int argc, char **argv) {
    if (argc >= 4 && !strcmp(argv[1], "--child")) {
        return child_main(argv[2], atoi(argv[3]));
    }
    const char *only = NULL, *skip = NULL, *expected_file = NULL, *known_file = NULL;
    int list = 0;
    g_self_path[0] = 0;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "-d") && i + 1 < argc) {
            g_dir = argv[++i];
        } else if (!strcmp(argv[i], "-t") && i + 1 < argc) {
            g_timeout_ms = atol(argv[++i]) * 1000;
        } else if (!strcmp(argv[i], "-o") && i + 1 < argc) {
            only = argv[++i];
        } else if (!strcmp(argv[i], "-s") && i + 1 < argc) {
            skip = argv[++i];
        } else if (!strcmp(argv[i], "-x") && i + 1 < argc) {
            snprintf(g_self_path, sizeof(g_self_path), "%s", argv[++i]);
        } else if (!strcmp(argv[i], "-e") && i + 1 < argc) {
            expected_file = argv[++i];
        } else if (!strcmp(argv[i], "-k") && i + 1 < argc) {
            known_file = argv[++i];
        } else if (!strcmp(argv[i], "-l")) {
            list = 1;
        } else if (!strcmp(argv[i], "-q")) {
            g_quiet = 1;
        }
    }
    qsort(g_checks, g_nchecks, sizeof(check_t), cmp_checks);
    if (list) {
        for (int i = 0; i < g_nchecks; i++) {
            printf("%s\n", g_checks[i].name);
        }
        return 0;
    }
    if (!g_self_path[0]) {
        if (argv[0][0] == '/') {
            snprintf(g_self_path, sizeof(g_self_path), "%s", argv[0]);
        } else if (!realpath(argv[0], g_self_path)) {
            snprintf(g_self_path, sizeof(g_self_path), "/bin/%s", argv[0]);
        }
    }
    if (expected_file) {
        load_expected(expected_file);
    }
    if (known_file) {
        load_known(known_file);
    }
    g_pid = 10000 + getpid() %
            90000; /* always five digits: the names have the same length everywhere */
    umask(022);

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_usr1;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGUSR1, &sa, NULL);
    sa.sa_handler = on_pipe;
    sigaction(SIGPIPE, &sa, NULL);

    g_main_thread = pthread_self();
    pthread_t wd;
    pthread_create(&wd, NULL, watchdog, NULL);
    pthread_detach(wd);

    for (int i = 0; i < g_nchecks; i++) {
        const char *name = g_checks[i].name;
        if (only && !in_list(only, name, 1)) {
            continue;
        }
        if (skip && in_list(skip, name, 0)) {
            continue;
        }
        if (!g_quiet) {
            char line[200];
            int n = snprintf(line, sizeof(line), "#> %s\n", name);
            write_all(line, (size_t)n);
        }
        g_cur = name;
        g_hang_signalled = 0;
        g_start_ms = now_ms();
        atomic_store(&g_sigpipes, 0);
        if (sigsetjmp(g_jmp, 1) == 0) {
            g_in_check = 1;
            g_checks[i].fn();
            g_in_check = 0;
        } else {
            g_in_check = 0;
            emit(name, "HANG(interrupted after %lld ms)", now_ms() - g_start_ms);
            if (g_nexp && !is_known(name)) {
                g_nmismatch++;
                char m[200];
                int n = snprintf(m, sizeof(m), "#! MISMATCH %s: the group hangs\n", name);
                write_all(m, (size_t)n);
            }
        }
        for (int fd = 3; fd < 1024; fd++) {
            close(fd);
        }
        cleanup_files();
    }
    if (!g_quiet) {
        write_all("#> done\n", 8);
    }
    if (g_nexp) {
        char m[300];
        int n = snprintf(m, sizeof(m),
                         "#! compared with the expected results: %d compared, %d unexpected differences, "
                         "%d known differences, %d known differences that now match\n",
                         g_ncompared, g_nmismatch, g_nknown_diff, g_nfixed);
        write_all(m, (size_t)n);
        return g_nmismatch ? 1 : 0;
    }
    return 0;
}
