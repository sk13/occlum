#ifndef __KERNEL_HEAP_H
#define __KERNEL_HEAP_H

#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

// Returns the kernel heap in use in kB, -1 if reading it failed, or -2 if the
// LibOS is built without the kernel_heap_monitor feature (the tests that need
// it skip themselves then). The resolution is 1 kB.
static inline long kernel_heap_in_use(void) {
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
    char *line = strstr(buf, "KernelHeapInUse:");
    long kb = -1;
    if (line == NULL) {
        return -1;
    }
    char *end = strchr(line, '\n');
    if (end != NULL) {
        *end = '\0';
    }
    if (strstr(line, "Feature not enabled") != NULL) {
        return -2;
    }
    if (sscanf(line, "KernelHeapInUse: %ld", &kb) != 1) {
        return -1;
    }
    return kb;
}

#endif /* __KERNEL_HEAP_H */
