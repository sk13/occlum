#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <errno.h>
#include <fcntl.h>
#include <dirent.h>
#include <stdbool.h>
#include <poll.h>
#include <pthread.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <sys/stat.h>
#include "test_fs.h"

// ============================================================================
// Helper function
// ============================================================================

static int create_file(const char *file_path) {
    int fd;
    int flags = O_RDONLY | O_CREAT | O_TRUNC;
    int mode = 00666;
    fd = open(file_path, flags, mode);
    if (fd < 0) {
        THROW_ERROR("failed to create a file");
    }
    close(fd);
    return 0;
}

static int remove_file(const char *file_path) {
    int ret;
    ret = unlink(file_path);
    if (ret < 0) {
        THROW_ERROR("failed to unlink the created file");
    }
    return 0;
}

// ============================================================================
// Test cases for hostfs
// ============================================================================

static int __test_write_read(const char *file_path) {
    char *write_str = "Write to hostfs successfully!";
    int fd;

    fd = open(file_path, O_WRONLY);
    if (fd < 0) {
        THROW_ERROR("failed to open a file to write");
    }
    if (write(fd, write_str, strlen(write_str)) <= 0) {
        THROW_ERROR("failed to write to the file");
    }
    close(fd);

    if (fs_check_file_content(file_path, write_str) < 0) {
        THROW_ERROR("failed to check file content");
    }
    return 0;
}

static int __test_write_fdatasync_read(const char *file_path) {
    char *write_str = "Write to hostfs and fdatasync successfully!";
    int fd;

    fd = open(file_path, O_WRONLY);
    if (fd < 0) {
        THROW_ERROR("failed to open a file to write");
    }
    if (write(fd, write_str, strlen(write_str)) <= 0) {
        THROW_ERROR("failed to write to the file");
    }
    if (fdatasync(fd) < 0) {
        THROW_ERROR("failed to sync data into file");
    }
    close(fd);

    if (fs_check_file_content(file_path, write_str) < 0) {
        THROW_ERROR("failed to check file content");
    }
    return 0;
}

static int __test_write_fsync_read(const char *file_path) {
    char *write_str = "Write to hostfs and fsync successfully!";
    int fd;

    fd = open(file_path, O_WRONLY);
    if (fd < 0) {
        THROW_ERROR("failed to open a file to write");
    }
    if (write(fd, write_str, strlen(write_str)) <= 0) {
        THROW_ERROR("failed to write to the file");
    }
    if (fsync(fd) < 0) {
        THROW_ERROR("failed to sync all into file");
    }
    close(fd);

    if (fs_check_file_content(file_path, write_str) < 0) {
        THROW_ERROR("failed to check file content");
    }
    return 0;
}

static int __test_rename(const char *file_path) {
    char *rename_path = "/host/hostfs_rename.txt";
    struct stat stat_buf;
    int ret;

    ret = rename(file_path, rename_path);
    if (ret < 0) {
        THROW_ERROR("failed to rename");
    }
    ret = stat(file_path, &stat_buf);
    if (!(ret < 0 && errno == ENOENT)) {
        THROW_ERROR("stat should return ENOENT");
    }
    ret = stat(rename_path, &stat_buf);
    if (ret < 0) {
        THROW_ERROR("failed to stat the file");
    }
    if (rename(rename_path, file_path) < 0) {
        THROW_ERROR("failed to rename back");
    }
    return 0;
}

static int __test_readdir(const char *file_path) {
    struct dirent *dp;
    DIR *dirp;
    char base_buf[PATH_MAX] = { 0 };
    char *base_name;
    bool found = false;

    if (fs_split_path(file_path, NULL, NULL, base_buf, &base_name) < 0) {
        THROW_ERROR("failed to split path");
    }
    dirp = opendir("/host");
    if (dirp == NULL) {
        THROW_ERROR("failed to open host directory");
    }
    while (1) {
        errno = 0;
        dp = readdir(dirp);
        if (dp == NULL) {
            if (errno != 0) {
                THROW_ERROR("faild to call readdir");
            }
            break;
        }
        if (strncmp(base_name, dp->d_name, strlen(base_name)) == 0) {
            found = true;
        }
    }
    if (!found) {
        THROW_ERROR("faild to read file entry");
    }
    closedir(dirp);
    return 0;
}

static int __test_truncate(const char *file_path) {
    off_t len = 256;
    if (truncate(file_path, len) < 0) {
        THROW_ERROR("failed to call truncate");
    }
    struct stat stat_buf;
    if (stat(file_path, &stat_buf) < 0) {
        THROW_ERROR("failed to stat file");
    }
    if (stat_buf.st_size != len) {
        THROW_ERROR("failed to check the len after truncate");
    }
    return 0;
}

typedef int(*test_hostfs_func_t)(const char *);

static int test_hostfs_framework(test_hostfs_func_t fn) {
    const char *file_path = "/host/hostfs_test.txt";

    if (create_file(file_path) < 0) {
        return -1;
    }
    if (fn(file_path) < 0) {
        return -1;
    }
    if (remove_file(file_path) < 0) {
        return -1;
    }
    return 0;
}

static int test_write_read() {
    return test_hostfs_framework(__test_write_read);
}

static int test_write_fdatasync_read() {
    return test_hostfs_framework(__test_write_fdatasync_read);
}

static int test_write_fsync_read() {
    return test_hostfs_framework(__test_write_fsync_read);
}

static int test_rename() {
    return test_hostfs_framework(__test_rename);
}

static int test_readdir() {
    return test_hostfs_framework(__test_readdir);
}

static int test_truncate() {
    return test_hostfs_framework(__test_truncate);
}

static int test_mkdir_then_rmdir() {
    const char *dir_path = "/host/hostfs_dir/";
    struct stat stat_buf;
    int dir_fd;

    if (mkdir(dir_path, 00775) < 0) {
        THROW_ERROR("failed to create the dir");
    }
    if (stat(dir_path, &stat_buf) < 0) {
        THROW_ERROR("failed to stat dir");
    }
    if (!S_ISDIR(stat_buf.st_mode)) {
        THROW_ERROR("failed to check if it is dir");
    }

    dir_fd = open(dir_path, O_RDONLY | O_DIRECTORY);
    if (dir_fd < 0) {
        THROW_ERROR("failed to open dir");
    }

    if (fsync(dir_fd) < 0) {
        THROW_ERROR("failed to fsync dir");
    }
    close(dir_fd);

    if (rmdir(dir_path) < 0) {
        THROW_ERROR("failed to remove the created dir");
    }
    return 0;
}

// ============================================================================
// Test suite main
// ============================================================================

// ============================================================================
// Test cases for named pipes (FIFOs) of the host
// ============================================================================

#define FIFO_PATH "/host/hostfs_test.fifo"

static int test_mkfifo_stat_unlink() {
    unlink(FIFO_PATH);
    if (mkfifo(FIFO_PATH, 0600) < 0) {
        THROW_ERROR("mkfifo failed");
    }
    struct stat stat_buf;
    if (stat(FIFO_PATH, &stat_buf) < 0 || !S_ISFIFO(stat_buf.st_mode)) {
        THROW_ERROR("the file is not a named pipe");
    }
    if (mkfifo(FIFO_PATH, 0600) != -1 || errno != EEXIST) {
        THROW_ERROR("the existing named pipe is created again");
    }
    // The directory lists it as a named pipe
    DIR *dir = opendir("/host");
    if (dir == NULL) {
        THROW_ERROR("opendir failed");
    }
    int found = 0;
    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, "hostfs_test.fifo") == 0) {
            found = entry->d_type == DT_FIFO;
        }
    }
    closedir(dir);
    if (!found) {
        THROW_ERROR("the directory does not list the named pipe with its type");
    }
    if (unlink(FIFO_PATH) < 0) {
        THROW_ERROR("unlink failed");
    }
    if (stat(FIFO_PATH, &stat_buf) != -1 || errno != ENOENT) {
        THROW_ERROR("the named pipe is still there");
    }
    return 0;
}

// What the control FIFO of a server does: the server opens it for reading and writing without blocking, and
// reads it again and again
static int test_fifo_read_write() {
    unlink(FIFO_PATH);
    if (mkfifo(FIFO_PATH, 0600) < 0) {
        THROW_ERROR("mkfifo failed");
    }
    int ret = -1;
    int reader = open(FIFO_PATH, O_RDWR | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC);
    int writer = -1;
    if (reader < 0) {
        THROW_ERROR("open failed");
    }
    char buf[64];
    if (read(reader, buf, sizeof(buf)) != -1 || errno != EAGAIN) {
        printf("\t\tan empty named pipe does not give EAGAIN\n");
        goto out;
    }
    // Another process writes to it
    writer = open(FIFO_PATH, O_WRONLY | O_NONBLOCK);
    if (writer < 0) {
        printf("\t\tthe named pipe is not opened for writing\n");
        goto out;
    }
    const char *message = "status\n";
    if (write(writer, message, strlen(message)) != (ssize_t)strlen(message)) {
        printf("\t\twrite failed\n");
        goto out;
    }
    ssize_t len = read(reader, buf, sizeof(buf));
    if (len != (ssize_t)strlen(message) || memcmp(buf, message, len) != 0) {
        printf("\t\tthe bytes are not read as written\n");
        goto out;
    }
    if (read(reader, buf, sizeof(buf)) != -1 || errno != EAGAIN) {
        printf("\t\tthe named pipe is not empty again\n");
        goto out;
    }
    ret = 0;
out:
    close(reader);
    if (writer >= 0) {
        close(writer);
    }
    unlink(FIFO_PATH);
    return ret;
}

static void *delayed_writer(void *arg) {
    int fd = *(int *)arg;
    struct timespec ts = { 0, 300 * 1000 * 1000 };
    nanosleep(&ts, NULL);
    return (void *)(long)(write(fd, "late", 4) == 4 ? 0 : 1);
}

// A blocking read waits for the bytes that another thread writes
static int test_fifo_blocking_read() {
    unlink(FIFO_PATH);
    if (mkfifo(FIFO_PATH, 0600) < 0) {
        THROW_ERROR("mkfifo failed");
    }
    int fd = open(FIFO_PATH, O_RDWR);
    if (fd < 0) {
        THROW_ERROR("open failed");
    }
    pthread_t thread;
    if (pthread_create(&thread, NULL, delayed_writer, &fd) != 0) {
        THROW_ERROR("pthread_create failed");
    }
    char buf[16];
    time_t begin = time(NULL);
    ssize_t len = read(fd, buf, sizeof(buf));
    void *thread_ret;
    pthread_join(thread, &thread_ret);
    close(fd);
    unlink(FIFO_PATH);
    if (len != 4 || memcmp(buf, "late", 4) != 0 || thread_ret != NULL) {
        THROW_ERROR("the blocking read did not get the bytes (%ld)", (long)len);
    }
    if (time(NULL) - begin > 5) {
        THROW_ERROR("the blocking read took too long");
    }
    return 0;
}

static int test_fifo_poll() {
    unlink(FIFO_PATH);
    if (mkfifo(FIFO_PATH, 0600) < 0) {
        THROW_ERROR("mkfifo failed");
    }
    int fd = open(FIFO_PATH, O_RDWR | O_NONBLOCK);
    if (fd < 0) {
        THROW_ERROR("open failed");
    }
    int ret = -1;
    struct pollfd pfd = { .fd = fd, .events = POLLIN };
    if (poll(&pfd, 1, 0) != 0) {
        printf("\t\tan empty named pipe is readable\n");
        goto out;
    }
    if (write(fd, "x", 1) != 1) {
        printf("\t\twrite failed\n");
        goto out;
    }
    if (poll(&pfd, 1, 1000) != 1 || !(pfd.revents & POLLIN)) {
        printf("\t\tthe named pipe is not readable after a write\n");
        goto out;
    }
    char c;
    if (read(fd, &c, 1) != 1) {
        printf("\t\tread failed\n");
        goto out;
    }
    // epoll
    int epfd = epoll_create1(0);
    struct epoll_event ev = { .events = EPOLLIN, .data.fd = fd };
    if (epfd < 0 || epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev) < 0) {
        printf("\t\tepoll_ctl failed\n");
        goto out;
    }
    struct epoll_event out_ev;
    if (epoll_wait(epfd, &out_ev, 1, 0) != 0) {
        printf("\t\tepoll reports an empty named pipe as readable\n");
        close(epfd);
        goto out;
    }
    write(fd, "y", 1);
    if (epoll_wait(epfd, &out_ev, 1, 1000) != 1 || !(out_ev.events & EPOLLIN)) {
        printf("\t\tepoll does not report the byte\n");
        close(epfd);
        goto out;
    }
    close(epfd);
    ret = 0;
out:
    close(fd);
    unlink(FIFO_PATH);
    return ret;
}

// A file system of the enclave can not hold a named pipe, and a device can not be made
static int test_mknod_types() {
    if (mkfifo("/tmp/hostfs_test.fifo", 0600) != -1 || errno != EPERM) {
        THROW_ERROR("a named pipe in the file system of the enclave is not refused with EPERM");
    }
    if (mknod("/host/hostfs_test.dev", S_IFCHR | 0600, makedev(1, 3)) != -1 || errno != EPERM) {
        THROW_ERROR("a device is not refused with EPERM");
    }
    unlink("/host/hostfs_test.regular");
    if (mknod("/host/hostfs_test.regular", S_IFREG | 0644, 0) < 0) {
        THROW_ERROR("mknod of a regular file failed");
    }
    struct stat stat_buf;
    if (stat("/host/hostfs_test.regular", &stat_buf) < 0 || !S_ISREG(stat_buf.st_mode)) {
        THROW_ERROR("the file is not a regular file");
    }
    unlink("/host/hostfs_test.regular");
    return 0;
}

static test_case_t test_cases[] = {
    TEST_CASE(test_write_read),
    TEST_CASE(test_write_fdatasync_read),
    TEST_CASE(test_write_fsync_read),
    TEST_CASE(test_rename),
    TEST_CASE(test_readdir),
    TEST_CASE(test_truncate),
    TEST_CASE(test_mkdir_then_rmdir),
    TEST_CASE(test_mkfifo_stat_unlink),
    TEST_CASE(test_fifo_read_write),
    TEST_CASE(test_fifo_blocking_read),
    TEST_CASE(test_fifo_poll),
    TEST_CASE(test_mknod_types),
};

int main(int argc, const char *argv[]) {
    return test_suite_run(test_cases, ARRAY_SIZE(test_cases));
}
