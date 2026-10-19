// The files that bind() makes for unix stream sockets, and the sockets that the names of the
// files reach. The expectations are the behavior of Linux (checked on Linux 7.0): the test passes
// there, too (gcc -pthread -I../include main.c), apart from the cases that are only for Occlum.
//
// The cases are about
//   - the file that bind() creates: a socket file, with the mode that the umask leaves
//   - the paths that bind() refuses because they exist (EADDRINUSE), the sockets that own a file,
//     and the file that stays when its socket is closed: Occlum lets another socket take that,
//     where Linux asks to unlink the file first
//   - open() and truncate() of the file, which a socket file does not allow
//   - unlinking, renaming and linking the file, which does not change the socket that it names
//   - what other file systems of Occlum (RamFS, HostFS) do with the files, and the files of
//     programs that are killed
//
// Usage: unix_socket_inodes [test case name | --list]
#define _GNU_SOURCE
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <spawn.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

#include "test.h"

#if defined(SGX_MODE_HW) || defined(SGX_MODE_SIM) || defined(SGX_MODE_HYPER)
#define IN_OCCLUM 1
#else
#define IN_OCCLUM 0
#endif

#define WAIT_MS 2000

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

static char g_self[256];
static int g_seq;
static char g_paths[256][160];
static int g_npaths;

// Unlinks the files of the test, and removes its directories, the last made first
static void remove_files(void) {
    for (int i = g_npaths - 1; i >= 0; i--) {
        if (unlink(g_paths[i]) < 0) {
            rmdir(g_paths[i]);
        }
    }
    g_npaths = 0;
}

static void remember_path(const char *path) {
    if (g_npaths < (int)ARRAY_SIZE(g_paths)) {
        strcpy(g_paths[g_npaths++], path);
    }
}

static void make_path(char *out, const char *tag) {
    snprintf(out, 160, "/tmp/unix_socket_inodes.%d.%d.%s", getpid(), ++g_seq, tag);
    remember_path(out);
}

static int new_socket(void) {
    return socket(AF_UNIX, SOCK_STREAM, 0);
}

static struct sockaddr_un path_addr(const char *path) {
    struct sockaddr_un a;
    memset(&a, 0, sizeof(a));
    a.sun_family = AF_UNIX;
    strncpy(a.sun_path, path, sizeof(a.sun_path) - 1);
    return a;
}

static int bind_path(int fd, const char *path) {
    struct sockaddr_un a = path_addr(path);
    return bind(fd, (struct sockaddr *)&a, sizeof(a));
}

static int connect_path(int fd, const char *path) {
    struct sockaddr_un a = path_addr(path);
    return connect(fd, (struct sockaddr *)&a, sizeof(a));
}

// A socket that listens at the path, or -1
static int listener_at(const char *path) {
    int fd = new_socket();
    if (fd >= 0 && (bind_path(fd, path) < 0 || listen(fd, 4) < 0)) {
        close(fd);
        return -1;
    }
    return fd;
}

// A socket that is connected to the path, or -1
static int client_to(const char *path) {
    int fd = new_socket();
    if (fd >= 0 && connect_path(fd, path) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

// Whether the listener has a connection to accept, which it takes if it has
static int accepts(int listen_fd) {
    struct pollfd pfd = { .fd = listen_fd, .events = POLLIN };
    if (poll(&pfd, 1, WAIT_MS) != 1) {
        return 0;
    }
    int fd = accept(listen_fd, NULL, NULL);
    if (fd < 0) {
        return 0;
    }
    close(fd);
    return 1;
}

// Whether the listener has no connection to accept: it is checked for a short while
static int accepts_nothing(int listen_fd) {
    struct pollfd pfd = { .fd = listen_fd, .events = POLLIN };
    return poll(&pfd, 1, 100) == 0;
}

// Binds a new socket to the path, which fails with the errno
static int check_bind_fails(const char *path, int expected_errno) {
    int fd = new_socket();
    errno = 0;
    if (bind_path(fd, path) != -1 || errno != expected_errno) {
        THROW_ERROR("bind to %s should fail with errno %d", path, expected_errno);
    }
    close(fd);
    return 0;
}

// Connects a new socket to the path, which fails with the errno
static int check_connect_fails(const char *path, int expected_errno) {
    int fd = new_socket();
    errno = 0;
    if (connect_path(fd, path) != -1 || errno != expected_errno) {
        THROW_ERROR("connect to %s should fail with errno %d", path, expected_errno);
    }
    close(fd);
    return 0;
}

// Checks the type and, if it is not negative, the permission bits of the file, which is not
// followed if it is a symbolic link and `follow` is not set
static int check_file(const char *path, int follow, mode_t type, int perm) {
    struct stat st;
    if ((follow ? stat(path, &st) : lstat(path, &st)) < 0) {
        THROW_ERROR("failed to stat %s", path);
    }
    if ((st.st_mode & S_IFMT) != type) {
        THROW_ERROR("%s has the type %o instead of %o", path, (unsigned)(st.st_mode & S_IFMT),
                    (unsigned)type);
    }
    if (perm >= 0 && (int)(st.st_mode & 07777) != perm) {
        THROW_ERROR("%s has the mode %o instead of %o", path, (unsigned)(st.st_mode & 07777),
                    (unsigned)perm);
    }
    return 0;
}

static int check_no_file(const char *path) {
    struct stat st;
    errno = 0;
    if (lstat(path, &st) != -1 || errno != ENOENT) {
        THROW_ERROR("%s should not exist", path);
    }
    return 0;
}

static int make_regular_file(const char *path, const char *content) {
    int fd = open(path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0 || write(fd, content, strlen(content)) != (ssize_t)strlen(content)) {
        THROW_ERROR("failed to create %s", path);
    }
    close(fd);
    return 0;
}

static int check_content(const char *path, const char *content) {
    char buf[64] = {};
    int fd = open(path, O_RDONLY);
    if (fd < 0 || read(fd, buf, sizeof(buf) - 1) != (ssize_t)strlen(content) ||
            strcmp(buf, content) != 0) {
        THROW_ERROR("the content of %s changed", path);
    }
    close(fd);
    return 0;
}

// ---------------------------------------------------------------------------------------------
// The file that bind() makes
// ---------------------------------------------------------------------------------------------

// The file is a socket file, whose mode is that of a new file with all permissions, less the bits
// of the umask
static int check_new_file_with_umask(mode_t mask) {
    char path[160];
    make_path(path, "mode");
    umask(mask);
    int fd = new_socket();
    EXPECT_OK(bind_path(fd, path));
    if (check_file(path, 0, S_IFSOCK, 0777 & ~mask) < 0 ||
            check_file(path, 1, S_IFSOCK, 0777 & ~mask) < 0) {
        return -1;
    }
    struct stat st;
    EXPECT_OK(lstat(path, &st));
    if (st.st_nlink != 1 || st.st_size != 0) {
        THROW_ERROR("a new socket file has %d links and %ld bytes", (int)st.st_nlink,
                    (long)st.st_size);
    }
    close(fd);
    EXPECT_OK(unlink(path));
    return 0;
}

static int test_bind_makes_a_socket_file(void) {
    mode_t old = umask(0);
    mode_t masks[] = { 022, 077, 0, 0777, 0137 };
    for (size_t i = 0; i < ARRAY_SIZE(masks); i++) {
        if (check_new_file_with_umask(masks[i]) < 0) {
            umask(old);
            return -1;
        }
    }
    umask(old);
    return 0;
}

// A new name in a directory that does not exist, or is no directory, is an error, which does not
// make a file
static int test_bind_errors_of_the_path(void) {
    char dir[160], file[160], path[200];
    make_path(dir, "nodir");
    snprintf(path, sizeof(path), "%s/sock", dir);
    EXPECT_OK(check_bind_fails(path, ENOENT));
    EXPECT_OK(check_connect_fails(path, ENOENT));

    make_path(file, "file");
    EXPECT_OK(make_regular_file(file, "x"));
    snprintf(path, sizeof(path), "%s/sock", file);
    EXPECT_OK(check_bind_fails(path, ENOTDIR));
    EXPECT_OK(check_connect_fails(path, ENOTDIR));

    // A path with a slash at its end names a directory, which does not exist, unless it is a
    // file that exists, which is in use whatever it is
    make_path(path, "slash");
    strcat(path, "/");
    EXPECT_OK(check_bind_fails(path, ENOENT));
    strcpy(path, file);
    strcat(path, "/");
    EXPECT_OK(check_bind_fails(path, EADDRINUSE));
    char sock[160];
    make_path(sock, "slash_sock");
    int l = listener_at(sock);
    if (l < 0) {
        THROW_ERROR("failed to listen");
    }
    strcpy(path, sock);
    strcat(path, "/");
    EXPECT_OK(check_bind_fails(path, EADDRINUSE));
    close(l);
    return 0;
}

// ---------------------------------------------------------------------------------------------
// The paths that exist
// ---------------------------------------------------------------------------------------------

// A path that exists is in use, whatever it is: bind() fails and leaves it as it is, and nothing
// listens there
static int test_bind_to_files_that_exist(void) {
    char reg[160], dir[160], dangling[160], missing[160], to_reg[160], to_dir[160], path[240];

    make_path(reg, "reg");
    EXPECT_OK(make_regular_file(reg, "data"));
    EXPECT_OK(check_bind_fails(reg, EADDRINUSE));
    EXPECT_OK(check_file(reg, 0, S_IFREG, 0644));
    EXPECT_OK(check_content(reg, "data"));
    EXPECT_OK(check_connect_fails(reg, ECONNREFUSED));

    make_path(dir, "dir");
    EXPECT_OK(mkdir(dir, 0755));
    EXPECT_OK(check_bind_fails(dir, EADDRINUSE));
    EXPECT_OK(check_file(dir, 0, S_IFDIR, 0755));
    EXPECT_OK(check_connect_fails(dir, ECONNREFUSED));
    // The same directory with other spellings, or the root directory
    snprintf(path, sizeof(path), "%s/.", dir);
    EXPECT_OK(check_bind_fails(path, EADDRINUSE));
    snprintf(path, sizeof(path), "%s/../%s", dir, strrchr(dir, '/') + 1);
    EXPECT_OK(check_bind_fails(path, EADDRINUSE));
    EXPECT_OK(check_bind_fails("/", EADDRINUSE));

    // A symbolic link is a file that exists, which bind() does not follow: it does not create
    // the file that a dangling link names
    make_path(missing, "missing");
    make_path(dangling, "dangling");
    EXPECT_OK(symlink(missing, dangling));
    EXPECT_OK(check_bind_fails(dangling, EADDRINUSE));
    EXPECT_OK(check_file(dangling, 0, S_IFLNK, -1));
    EXPECT_OK(check_no_file(missing));

    make_path(to_reg, "to_reg");
    EXPECT_OK(symlink(reg, to_reg));
    EXPECT_OK(check_bind_fails(to_reg, EADDRINUSE));
    EXPECT_OK(check_file(to_reg, 0, S_IFLNK, -1));
    EXPECT_OK(check_content(reg, "data"));
    make_path(to_dir, "to_dir");
    EXPECT_OK(symlink(dir, to_dir));
    EXPECT_OK(check_bind_fails(to_dir, EADDRINUSE));
    EXPECT_OK(check_file(to_dir, 0, S_IFLNK, -1));
    return 0;
}

// A path that a socket owns is in use, whatever other path leads to the file, and as long as the
// file is the same, whatever its name
static int test_bind_to_the_file_of_a_socket(void) {
    char path[160], sym[160], hard[160], dotted[240], renamed[160];
    make_path(path, "owner");
    int l = listener_at(path);
    if (l < 0) {
        THROW_ERROR("failed to listen");
    }
    EXPECT_OK(check_bind_fails(path, EADDRINUSE));

    make_path(sym, "sym");
    EXPECT_OK(symlink(path, sym));
    EXPECT_OK(check_bind_fails(sym, EADDRINUSE));
    make_path(hard, "hard");
    EXPECT_OK(link(path, hard));
    EXPECT_OK(check_bind_fails(hard, EADDRINUSE));
    snprintf(dotted, sizeof(dotted), "/tmp/./%s", strrchr(path, '/') + 1);
    EXPECT_OK(check_bind_fails(dotted, EADDRINUSE));

    // A socket that is only bound owns its file, too
    char bound_path[160];
    make_path(bound_path, "bound");
    int b = new_socket();
    EXPECT_OK(bind_path(b, bound_path));
    EXPECT_OK(check_bind_fails(bound_path, EADDRINUSE));

    // The file of the socket with another name is still in use, and the old name is free
    make_path(renamed, "renamed");
    EXPECT_OK(rename(path, renamed));
    EXPECT_OK(check_bind_fails(renamed, EADDRINUSE));
    int other = listener_at(path);
    if (other < 0) {
        THROW_ERROR("failed to listen at the name that the rename has freed");
    }
    // Each name reaches its own socket
    int c1 = client_to(renamed);
    int c2 = client_to(path);
    if (c1 < 0 || c2 < 0) {
        THROW_ERROR("failed to connect");
    }
    if (!accepts(l) || !accepts(other) || !accepts_nothing(l) || !accepts_nothing(other)) {
        THROW_ERROR("a connection did not reach the socket of its name");
    }
    close(c1);
    close(c2);
    close(other);
    close(b);
    close(l);
    return 0;
}

// The file stays when the socket is closed, and nothing listens there. Linux refuses to bind to
// it until it is unlinked, but Occlum lets another socket take the name of a closed socket
static int test_the_file_of_a_closed_socket(void) {
    char path[160];
    make_path(path, "closed");
    int l = listener_at(path);
    if (l < 0) {
        THROW_ERROR("failed to listen");
    }
    close(l);
    EXPECT_OK(check_file(path, 0, S_IFSOCK, 0777 & ~022));
    EXPECT_OK(check_connect_fails(path, ECONNREFUSED));

    // A symbolic link to the file is a path that exists, whether the socket lives or not
    char sym[160];
    make_path(sym, "sym");
    EXPECT_OK(symlink(path, sym));
    EXPECT_OK(check_bind_fails(sym, EADDRINUSE));

    int fd = new_socket();
    if (IN_OCCLUM) {
        EXPECT_OK(bind_path(fd, path));
        EXPECT_OK(listen(fd, 4));
        int c = client_to(path);
        if (c < 0 || !accepts(fd)) {
            THROW_ERROR("the socket that took the name does not get connections");
        }
        close(c);
        EXPECT_OK(check_bind_fails(path, EADDRINUSE));
        close(fd);
    } else {
        EXPECT_ERR(bind_path(fd, path), EADDRINUSE);
        close(fd);
    }

    // After unlinking, any socket binds
    EXPECT_OK(unlink(path));
    EXPECT_OK(check_no_file(path));
    l = listener_at(path);
    if (l < 0) {
        THROW_ERROR("failed to listen after the unlink");
    }
    EXPECT_OK(check_file(path, 0, S_IFSOCK, -1));
    close(l);

    // A socket that was only bound leaves its file as well
    EXPECT_OK(unlink(path));
    fd = new_socket();
    EXPECT_OK(bind_path(fd, path));
    close(fd);
    EXPECT_OK(check_file(path, 0, S_IFSOCK, -1));
    EXPECT_OK(unlink(path));
    return 0;
}

// Two threads that bind to the same new path: one gets it, the other finds it in use, and no more
// than one file is made
static atomic_int g_go;
typedef struct {
    const char *path;
    int ret;
    int err;
    int fd;
} bind_job_t;

static void *bind_thread(void *arg) {
    bind_job_t *job = arg;
    if (job->fd < 0) {
        job->fd = new_socket();
    }
    while (!atomic_load(&g_go)) {
        sched_yield();
    }
    errno = 0;
    job->ret = bind_path(job->fd, job->path);
    job->err = errno;
    return NULL;
}

static int test_bind_races(void) {
    char path[160];
    make_path(path, "race");
    for (int round = 0; round < 100; round++) {
        bind_job_t jobs[2] = { { .path = path, .fd = -1 }, { .path = path, .fd = -1 } };
        pthread_t threads[2];
        atomic_store(&g_go, 0);
        for (int i = 0; i < 2; i++) {
            if (pthread_create(&threads[i], NULL, bind_thread, &jobs[i]) != 0) {
                THROW_ERROR("failed to create a thread");
            }
        }
        atomic_store(&g_go, 1);
        for (int i = 0; i < 2; i++) {
            pthread_join(threads[i], NULL);
        }
        int wins = (jobs[0].ret == 0) + (jobs[1].ret == 0);
        int in_use = (jobs[0].err == EADDRINUSE) + (jobs[1].err == EADDRINUSE);
        if (wins != 1 || in_use != 1) {
            THROW_ERROR("%d sockets got the path and %d found it in use instead of 1 and 1",
                        wins, in_use);
        }
        EXPECT_OK(check_file(path, 0, S_IFSOCK, -1));
        close(jobs[0].fd);
        close(jobs[1].fd);
        EXPECT_OK(unlink(path));
    }
    return 0;
}

// Two threads that bind one socket, each to a path of its own: one binds, the other finds the
// socket bound, and the file that it may have made is gone
static int test_bind_one_socket_twice_at_once(void) {
    char path1[160], path2[160];
    make_path(path1, "once1");
    make_path(path2, "once2");
    for (int round = 0; round < 100; round++) {
        int fd = new_socket();
        bind_job_t jobs[2] = { { .path = path1, .fd = fd }, { .path = path2, .fd = fd } };
        pthread_t threads[2];
        atomic_store(&g_go, 0);
        for (int i = 0; i < 2; i++) {
            if (pthread_create(&threads[i], NULL, bind_thread, &jobs[i]) != 0) {
                THROW_ERROR("failed to create a thread");
            }
        }
        atomic_store(&g_go, 1);
        for (int i = 0; i < 2; i++) {
            pthread_join(threads[i], NULL);
        }
        int wins = (jobs[0].ret == 0) + (jobs[1].ret == 0);
        int failures = (jobs[0].err == EINVAL) + (jobs[1].err == EINVAL);
        if (wins != 1 || failures != 1) {
            THROW_ERROR("%d binds worked and %d failed with EINVAL instead of 1 and 1", wins,
                        failures);
        }
        EXPECT_OK(check_file(jobs[0].ret == 0 ? path1 : path2, 0, S_IFSOCK, -1));
        EXPECT_OK(check_no_file(jobs[0].ret == 0 ? path2 : path1));
        close(fd);
        EXPECT_OK(unlink(jobs[0].ret == 0 ? path1 : path2));
    }
    return 0;
}

// ---------------------------------------------------------------------------------------------
// What can be done with the file
// ---------------------------------------------------------------------------------------------

// A file of a socket cannot be opened, nor truncated, but it has an owner and a mode like any
// file, and can be named (O_PATH)
static int test_open_and_truncate_of_a_socket_file(void) {
    char path[160];
    make_path(path, "node");
    int l = listener_at(path);
    if (l < 0) {
        THROW_ERROR("failed to listen");
    }
    EXPECT_ERR(open(path, O_RDONLY), ENXIO);
    EXPECT_ERR(open(path, O_WRONLY), ENXIO);
    EXPECT_ERR(open(path, O_RDWR), ENXIO);
    EXPECT_ERR(open(path, O_RDWR | O_CREAT, 0600), ENXIO);
    EXPECT_ERR(open(path, O_RDWR | O_CREAT | O_EXCL, 0600), EEXIST);
    EXPECT_ERR(truncate(path, 0), EINVAL);
    EXPECT_ERR(truncate(path, 100), EINVAL);
    EXPECT_OK(check_file(path, 0, S_IFSOCK, -1));

    int fd = open(path, O_PATH);
    if (fd < 0) {
        THROW_ERROR("failed to open the file with O_PATH");
    }
    struct stat st;
    if (fstat(fd, &st) < 0 || (st.st_mode & S_IFMT) != S_IFSOCK) {
        THROW_ERROR("the descriptor of O_PATH does not show a socket file");
    }
    close(fd);

    EXPECT_OK(chmod(path, 0600));
    EXPECT_OK(check_file(path, 0, S_IFSOCK, 0600));
    EXPECT_OK(access(path, F_OK));

    // The socket is not affected
    int c = client_to(path);
    if (c < 0 || !accepts(l)) {
        THROW_ERROR("the socket does not get connections through its file");
    }
    close(c);
    close(l);
    return 0;
}

// Unlinking the file takes the name away from the socket, which goes on with its connections. A
// new socket binds to the name, and has the connections to it.
static int test_unlink_and_rebind(void) {
    char path[160];
    make_path(path, "unlinked");
    int l = listener_at(path);
    if (l < 0) {
        THROW_ERROR("failed to listen");
    }
    int pending = client_to(path);
    if (pending < 0) {
        THROW_ERROR("failed to connect");
    }
    EXPECT_OK(unlink(path));
    EXPECT_OK(check_connect_fails(path, ENOENT));

    int l2 = listener_at(path);
    if (l2 < 0) {
        THROW_ERROR("a new socket cannot bind to the name of the unlinked file");
    }
    EXPECT_OK(check_file(path, 0, S_IFSOCK, -1));
    // The old socket has its connection from before
    int accepted = accept(l, NULL, NULL);
    if (accepted < 0) {
        THROW_ERROR("the old socket lost its pending connection");
    }
    int c = client_to(path);
    if (c < 0) {
        THROW_ERROR("failed to connect to the new socket");
    }
    if (!accepts(l2) || !accepts_nothing(l)) {
        THROW_ERROR("the connection did not reach the new socket");
    }
    if (write(pending, "x", 1) != 1) {
        THROW_ERROR("failed to write");
    }
    char byte = 0;
    if (read(accepted, &byte, 1) != 1 || byte != 'x') {
        THROW_ERROR("the connection of the old socket does not work");
    }
    close(accepted);
    close(pending);
    close(c);
    close(l2);
    close(l);
    return 0;
}

// The file that is renamed is the same file: the socket is reached with the new name, and the
// name of the socket is the one that it bound
static int test_rename_the_file(void) {
    char path[160], renamed[160];
    make_path(path, "old");
    make_path(renamed, "new");
    int l = listener_at(path);
    if (l < 0) {
        THROW_ERROR("failed to listen");
    }
    EXPECT_OK(rename(path, renamed));
    EXPECT_OK(check_file(renamed, 0, S_IFSOCK, -1));
    EXPECT_OK(check_no_file(path));
    EXPECT_OK(check_connect_fails(path, ENOENT));
    int c = client_to(renamed);
    if (c < 0 || !accepts(l)) {
        THROW_ERROR("the socket does not get connections through the new name");
    }
    close(c);

    // Over a file that exists: the name is the socket file
    char other[160];
    make_path(other, "over");
    EXPECT_OK(make_regular_file(other, "x"));
    EXPECT_OK(rename(renamed, other));
    EXPECT_OK(check_file(other, 0, S_IFSOCK, -1));
    c = client_to(other);
    if (c < 0 || !accepts(l)) {
        THROW_ERROR("the socket does not get connections through the name of the old file");
    }
    close(c);
    close(l);
    return 0;
}

// A symbolic link to the file reaches the socket, and so does another name of the file (a hard
// link), which is the same file: it has the same inode number and the same links
static int test_links_to_a_socket_file(void) {
    char path[160], sym[160], hard[160], rel[200];
    make_path(path, "target");
    int l = listener_at(path);
    if (l < 0) {
        THROW_ERROR("failed to listen");
    }

    make_path(sym, "sym");
    EXPECT_OK(symlink(path, sym));
    EXPECT_OK(check_file(sym, 0, S_IFLNK, -1));
    EXPECT_OK(check_file(sym, 1, S_IFSOCK, -1));
    int c = client_to(sym);
    if (c < 0 || !accepts(l)) {
        THROW_ERROR("the socket does not get connections through the symbolic link");
    }
    close(c);

    // A link with a relative target
    make_path(rel, "rel");
    EXPECT_OK(symlink(strrchr(path, '/') + 1, rel));
    c = client_to(rel);
    if (c < 0 || !accepts(l)) {
        THROW_ERROR("the socket does not get connections through the relative link");
    }
    close(c);

    make_path(hard, "hard");
    EXPECT_OK(link(path, hard));
    struct stat st_path, st_hard;
    EXPECT_OK(lstat(path, &st_path));
    EXPECT_OK(lstat(hard, &st_hard));
    if ((st_hard.st_mode & S_IFMT) != S_IFSOCK || st_hard.st_ino != st_path.st_ino ||
            st_hard.st_dev != st_path.st_dev || st_hard.st_nlink != 2 || st_path.st_nlink != 2) {
        THROW_ERROR("the hard link is not the same file: inode %ld and %ld, %d links",
                    (long)st_hard.st_ino, (long)st_path.st_ino, (int)st_hard.st_nlink);
    }
    c = client_to(hard);
    if (c < 0 || !accepts(l)) {
        THROW_ERROR("the socket does not get connections through the hard link");
    }
    close(c);

    // Another name remains when one is unlinked
    EXPECT_OK(unlink(path));
    c = client_to(hard);
    if (c < 0 || !accepts(l)) {
        THROW_ERROR("the socket does not get connections through the name that is left");
    }
    close(c);
    EXPECT_OK(check_connect_fails(path, ENOENT));

    // Nothing listens once the socket is closed
    close(l);
    EXPECT_OK(check_connect_fails(hard, ECONNREFUSED));
    EXPECT_OK(check_connect_fails(sym, ENOENT));
    return 0;
}

// The links of a regular file and of a directory entry are the file, too
static int test_hard_links_have_one_inode_number(void) {
    char dir[160], a[200], b[200];
    make_path(dir, "dir");
    EXPECT_OK(mkdir(dir, 0755));
    snprintf(a, sizeof(a), "%s/a", dir);
    snprintf(b, sizeof(b), "%s/b", dir);
    EXPECT_OK(make_regular_file(a, "linked"));
    remember_path(a);
    remember_path(b);
    EXPECT_OK(link(a, b));

    struct stat sa, sb;
    EXPECT_OK(stat(a, &sa));
    EXPECT_OK(stat(b, &sb));
    if (sa.st_ino != sb.st_ino || sa.st_nlink != 2 || sb.st_nlink != 2) {
        THROW_ERROR("the links have the inode numbers %ld and %ld", (long)sa.st_ino,
                    (long)sb.st_ino);
    }
    // The inode numbers of the directory are those of its files, too
    DIR *d = opendir(dir);
    if (d == NULL) {
        THROW_ERROR("failed to open the directory");
    }
    int seen = 0;
    struct dirent *e;
    while ((e = readdir(d)) != NULL) {
        if (strcmp(e->d_name, "a") == 0 || strcmp(e->d_name, "b") == 0) {
            seen++;
            if ((long)e->d_ino != (long)sa.st_ino) {
                closedir(d);
                THROW_ERROR("%s has the inode number %ld in the directory", e->d_name,
                            (long)e->d_ino);
            }
        }
    }
    closedir(d);
    if (seen != 2) {
        THROW_ERROR("the directory lists %d of the links", seen);
    }
    return 0;
}

// A directory entry of a socket file has the type of a socket
static int test_directory_entry_of_a_socket_file(void) {
    char dir[160], sock[200], reg[200], sub[200], sym[200];
    make_path(dir, "entries");
    EXPECT_OK(mkdir(dir, 0755));
    snprintf(sock, sizeof(sock), "%s/sock", dir);
    snprintf(reg, sizeof(reg), "%s/reg", dir);
    snprintf(sub, sizeof(sub), "%s/sub", dir);
    snprintf(sym, sizeof(sym), "%s/sym", dir);
    remember_path(sock);
    remember_path(reg);
    remember_path(sub);
    remember_path(sym);
    int fd = new_socket();
    EXPECT_OK(bind_path(fd, sock));
    EXPECT_OK(make_regular_file(reg, "x"));
    EXPECT_OK(mkdir(sub, 0755));
    EXPECT_OK(symlink(reg, sym));

    DIR *d = opendir(dir);
    if (d == NULL) {
        THROW_ERROR("failed to open the directory");
    }
    int seen = 0;
    struct dirent *e;
    while ((e = readdir(d)) != NULL) {
        int expected = -1;
        if (strcmp(e->d_name, "sock") == 0) {
            expected = DT_SOCK;
        } else if (strcmp(e->d_name, "reg") == 0) {
            expected = DT_REG;
        } else if (strcmp(e->d_name, "sub") == 0) {
            expected = DT_DIR;
        } else if (strcmp(e->d_name, "sym") == 0) {
            expected = DT_LNK;
        }
        if (expected >= 0) {
            seen++;
            if (e->d_type != expected) {
                closedir(d);
                THROW_ERROR("%s has the entry type %d instead of %d", e->d_name, e->d_type,
                            expected);
            }
        }
    }
    closedir(d);
    if (seen != 4) {
        THROW_ERROR("the directory lists %d of the 4 files", seen);
    }
    close(fd);
    return 0;
}

// ---------------------------------------------------------------------------------------------
// A program that is killed
// ---------------------------------------------------------------------------------------------

// The role of the child: listen at the path, until killed
static int child_main(const char *path) {
    int l = listener_at(path);
    if (l < 0) {
        return 1;
    }
    for (int i = 0; i < 300; i++) {
        usleep(100 * 1000);
    }
    return 2;
}

// A program that is killed leaves its file, but not its socket: the name is free to take, which is
// to unlink the file for Linux, and nothing listens
static int test_the_file_of_a_killed_program(void) {
    char path[160];
    make_path(path, "killed");
    char *argv[] = { g_self, "--child", path, NULL };
    pid_t pid;
    if (posix_spawn(&pid, g_self, NULL, NULL, argv, environ) != 0) {
        THROW_ERROR("failed to spawn the child");
    }
    // Wait until the child listens
    int c = -1;
    for (int i = 0; i < 100 && c < 0; i++) {
        c = client_to(path);
        if (c < 0) {
            usleep(50 * 1000);
        }
    }
    if (c < 0) {
        kill(pid, SIGKILL);
        waitpid(pid, NULL, 0);
        THROW_ERROR("the child does not listen");
    }
    EXPECT_OK(check_bind_fails(path, EADDRINUSE));

    EXPECT_OK(kill(pid, SIGKILL));
    int status = 0;
    if (waitpid(pid, &status, 0) != pid || !WIFSIGNALED(status)) {
        THROW_ERROR("the child was not killed");
    }
    close(c);
    EXPECT_OK(check_file(path, 0, S_IFSOCK, -1));
    EXPECT_OK(check_connect_fails(path, ECONNREFUSED));

    int fd = new_socket();
    if (IN_OCCLUM) {
        EXPECT_OK(bind_path(fd, path));
        close(fd);
    } else {
        EXPECT_ERR(bind_path(fd, path), EADDRINUSE);
        close(fd);
        EXPECT_OK(unlink(path));
        int l = listener_at(path);
        if (l < 0) {
            THROW_ERROR("failed to listen after the unlink");
        }
        close(l);
    }
    return 0;
}

// ---------------------------------------------------------------------------------------------
// Other file systems (they are for Occlum, which mounts them: the cases pass on Linux without
// doing anything)
// ---------------------------------------------------------------------------------------------

// The files of the root file system and of a RamFS: the inode numbers of the two are two counts,
// and a socket file of each has the number of the other. The files are different, and so are
// their sockets.
static int check_ramfs(void) {
    char root_path[160], dir[160], path[200], other[200];

    make_path(root_path, "root");
    int root_l = listener_at(root_path);
    if (root_l < 0) {
        THROW_ERROR("failed to listen");
    }
    struct stat st;
    EXPECT_OK(stat(root_path, &st));
    long target = (long)st.st_ino;

    make_path(dir, "ramfs");
    EXPECT_OK(mkdir(dir, 0755));
    EXPECT_OK(mount("ramfs", dir, "ramfs", 0, NULL));

    // A RamFS counts its files, which a root file system may have counted to a number, too: make
    // files up to the one before the number
    for (int i = 0; i < 100000; i++) {
        snprintf(other, sizeof(other), "%s/f%d", dir, i);
        int fd = open(other, O_CREAT | O_RDWR, 0644);
        if (fd < 0 || fstat(fd, &st) < 0) {
            THROW_ERROR("failed to create a file in the RamFS");
        }
        close(fd);
        if ((long)st.st_ino + 1 >= target) {
            break;
        }
    }
    snprintf(path, sizeof(path), "%s/sock", dir);
    int ram_l = listener_at(path);
    if (ram_l < 0) {
        THROW_ERROR("failed to listen in the RamFS");
    }
    EXPECT_OK(stat(path, &st));
    if ((long)st.st_ino != target) {
        // The files of the file systems are numbered differently, which is as good
        printf("\t\tnote: the numbers differ (%ld and %ld), the case does not collide\n", target,
               (long)st.st_ino);
    }
    EXPECT_OK(check_file(path, 0, S_IFSOCK, 0777 & ~022));
    EXPECT_ERR(truncate(path, 0), EINVAL);
    EXPECT_ERR(open(path, O_RDONLY), ENXIO);

    int c1 = client_to(root_path);
    int c2 = client_to(path);
    if (c1 < 0 || c2 < 0) {
        THROW_ERROR("failed to connect");
    }
    if (!accepts(root_l) || !accepts(ram_l) || !accepts_nothing(root_l) ||
            !accepts_nothing(ram_l)) {
        THROW_ERROR("a connection did not reach the socket of its file system");
    }
    close(c1);
    close(c2);
    EXPECT_OK(check_bind_fails(path, EADDRINUSE));
    // A file that is not a socket file is in use
    EXPECT_OK(check_bind_fails(other, EADDRINUSE));

    // The same rules for the socket file as in the root file system
    close(ram_l);
    EXPECT_OK(check_file(path, 0, S_IFSOCK, -1));
    EXPECT_OK(check_connect_fails(path, ECONNREFUSED));
    ram_l = listener_at(path);
    if (ram_l < 0) {
        THROW_ERROR("a closed socket in the RamFS does not give its name");
    }
    EXPECT_OK(unlink(path));
    close(ram_l);
    close(root_l);
    EXPECT_OK(umount(dir));
    return 0;
}

// A HostFS has no files of the type socket: the file of a socket is a regular file. It stays when
// the socket is closed, and is in use like any other file then. The sockets are reached with it,
// as they are in the other file systems.
static int check_hostfs(void) {
    char dir[160], path[200];
    make_path(dir, "hostfs");
    EXPECT_OK(mkdir(dir, 0755));
    EXPECT_OK(mount("hostfs", dir, "hostfs", 0, "dir=./unix_socket_inodes_host"));
    snprintf(path, sizeof(path), "%s/sock.%d", dir, getpid());
    remember_path(path);

    int l = listener_at(path);
    if (l < 0) {
        THROW_ERROR("failed to listen in the HostFS");
    }
    EXPECT_OK(check_file(path, 0, S_IFREG, -1));
    EXPECT_OK(check_bind_fails(path, EADDRINUSE));
    int c = client_to(path);
    if (c < 0 || !accepts(l)) {
        THROW_ERROR("the socket does not get connections in the HostFS");
    }
    close(c);

    // A name that a live socket has, as the socket has a file that is unlinked: the name is free
    EXPECT_OK(unlink(path));
    int l2 = listener_at(path);
    if (l2 < 0) {
        THROW_ERROR("a new socket cannot bind to the path of the unlinked file in the HostFS");
    }
    c = client_to(path);
    if (c < 0 || !accepts(l2) || !accepts_nothing(l)) {
        THROW_ERROR("the connection did not reach the new socket in the HostFS");
    }
    close(c);
    close(l);
    close(l2);
    EXPECT_OK(check_file(path, 0, S_IFREG, -1));
    EXPECT_OK(check_connect_fails(path, ECONNREFUSED));
    EXPECT_OK(unlink(path));

    // Files that are unlinked while their sockets live keep their numbers, which the host may give
    // to the next files, if it were free to: a socket does not find its file taken
    int sockets[8];
    char names[8][200];
    for (int i = 0; i < 8; i++) {
        snprintf(names[i], sizeof(names[i]), "%s/n%d.%d", dir, i, getpid());
        remember_path(names[i]);
        sockets[i] = listener_at(names[i]);
        if (sockets[i] < 0) {
            THROW_ERROR("failed to listen at %s", names[i]);
        }
        EXPECT_OK(unlink(names[i]));
    }
    for (int i = 0; i < 8; i++) {
        snprintf(names[i], sizeof(names[i]), "%s/m%d.%d", dir, i, getpid());
        remember_path(names[i]);
        int fd = listener_at(names[i]);
        if (fd < 0) {
            THROW_ERROR("failed to listen at a new name %s after the unlinks", names[i]);
        }
        close(fd);
        EXPECT_OK(unlink(names[i]));
    }
    for (int i = 0; i < 8; i++) {
        close(sockets[i]);
    }
    EXPECT_OK(umount(dir));
    return 0;
}

static int test_files_of_a_ramfs(void) {
    return IN_OCCLUM ? check_ramfs() : 0;
}

static int test_files_of_a_hostfs(void) {
    return IN_OCCLUM ? check_hostfs() : 0;
}

// ---------------------------------------------------------------------------------------------

static test_case_t test_cases[] = {
    TEST_CASE(test_bind_makes_a_socket_file),
    TEST_CASE(test_bind_errors_of_the_path),
    TEST_CASE(test_bind_to_files_that_exist),
    TEST_CASE(test_bind_to_the_file_of_a_socket),
    TEST_CASE(test_the_file_of_a_closed_socket),
    TEST_CASE(test_bind_races),
    TEST_CASE(test_bind_one_socket_twice_at_once),
    TEST_CASE(test_open_and_truncate_of_a_socket_file),
    TEST_CASE(test_unlink_and_rebind),
    TEST_CASE(test_rename_the_file),
    TEST_CASE(test_links_to_a_socket_file),
    TEST_CASE(test_hard_links_have_one_inode_number),
    TEST_CASE(test_directory_entry_of_a_socket_file),
    TEST_CASE(test_the_file_of_a_killed_program),
    TEST_CASE(test_files_of_a_ramfs),
    TEST_CASE(test_files_of_a_hostfs),
};

int main(int argc, const char *argv[]) {
    if (argc == 3 && strcmp(argv[1], "--child") == 0) {
        return child_main(argv[2]);
    }
    signal(SIGPIPE, SIG_IGN);
    // The program starts itself as the child of a test case
    ssize_t n = readlink("/proc/self/exe", g_self, sizeof(g_self) - 1);
    if (n <= 0) {
        strncpy(g_self, argv[0], sizeof(g_self) - 1);
    }
    atexit(remove_files);
    // The mode of the files below is that of the umask that Linux has in general
    umask(022);

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
