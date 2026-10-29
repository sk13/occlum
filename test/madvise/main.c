#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>
#include "test.h"

// ============================================================================
// Helper definition
// ============================================================================

// madvise(MADV_DONTNEED) on a private anonymous mapping makes the range read as
// zeros at the next access. A runtime that gives memory back this way (the Go
// scavenger, jemalloc, ...) takes the pages again as zeroed ones, so the data
// of a range that stays as it was corrupts the heap of the program.
//
// Not covered: a MAP_SHARED | MAP_ANONYMOUS mapping keeps its data on Linux, but
// the LibOS does not record MAP_SHARED of an anonymous mapping, so it is
// dropped like a private one. Only shared file mappings are kept.

#define PAGE_SIZE 4096

static void fill(unsigned char *p, size_t len, unsigned char value) {
    memset(p, value, len);
}

static int all_equal(const unsigned char *p, size_t len, unsigned char value) {
    for (size_t i = 0; i < len; i++) {
        if (p[i] != value) {
            return 0;
        }
    }
    return 1;
}

static unsigned char *map_anon(size_t len, int prot, int flags) {
    void *p = mmap(NULL, len, prot, flags | MAP_ANONYMOUS, -1, 0);
    return p == MAP_FAILED ? NULL : p;
}

// ============================================================================
// Test cases
// ============================================================================

static int test_dontneed_zeroes_the_range() {
    size_t len = 8 * PAGE_SIZE;
    unsigned char *p = map_anon(len, PROT_READ | PROT_WRITE, MAP_PRIVATE);
    if (p == NULL) {
        THROW_ERROR("mmap failed");
    }
    fill(p, len, 0xAB);

    // The pages 2..5 are dropped, the others keep their data
    if (madvise(p + 2 * PAGE_SIZE, 4 * PAGE_SIZE, MADV_DONTNEED) < 0) {
        THROW_ERROR("madvise failed");
    }
    if (!all_equal(p, 2 * PAGE_SIZE, 0xAB) ||
            !all_equal(p + 6 * PAGE_SIZE, 2 * PAGE_SIZE, 0xAB)) {
        THROW_ERROR("the data outside of the range changed");
    }
    if (!all_equal(p + 2 * PAGE_SIZE, 4 * PAGE_SIZE, 0)) {
        THROW_ERROR("the range does not read as zeros");
    }

    // The range is usable again
    fill(p + 2 * PAGE_SIZE, 4 * PAGE_SIZE, 0xCD);
    if (!all_equal(p + 2 * PAGE_SIZE, 4 * PAGE_SIZE, 0xCD)) {
        THROW_ERROR("the range is not writable again");
    }
    munmap(p, len);
    return 0;
}

static int test_dontneed_length_is_rounded_up() {
    unsigned char *p = map_anon(2 * PAGE_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE);
    if (p == NULL) {
        THROW_ERROR("mmap failed");
    }
    fill(p, 2 * PAGE_SIZE, 0x5A);
    // One byte covers the whole first page
    if (madvise(p, 1, MADV_DONTNEED) < 0) {
        THROW_ERROR("madvise failed");
    }
    if (!all_equal(p, PAGE_SIZE, 0)) {
        THROW_ERROR("the page does not read as zeros");
    }
    if (!all_equal(p + PAGE_SIZE, PAGE_SIZE, 0x5A)) {
        THROW_ERROR("the next page changed");
    }
    munmap(p, 2 * PAGE_SIZE);
    return 0;
}

static int test_dontneed_large_range() {
    size_t len = 16 * 1024 * 1024;
    unsigned char *p = map_anon(len, PROT_READ | PROT_WRITE, MAP_PRIVATE);
    if (p == NULL) {
        THROW_ERROR("mmap failed");
    }
    fill(p, len, 0x11);
    if (madvise(p, len, MADV_DONTNEED) < 0) {
        THROW_ERROR("madvise failed");
    }
    if (!all_equal(p, len, 0)) {
        THROW_ERROR("the range does not read as zeros");
    }
    munmap(p, len);
    return 0;
}

static int test_dontneed_spans_several_mappings() {
    // Mappings with different protections are separate VMAs; the range of
    // one call crosses them
    size_t len = 6 * PAGE_SIZE;
    unsigned char *p = map_anon(len, PROT_READ | PROT_WRITE, MAP_PRIVATE);
    if (p == NULL) {
        THROW_ERROR("mmap failed");
    }
    fill(p, len, 0x77);
    if (mprotect(p + 2 * PAGE_SIZE, 2 * PAGE_SIZE, PROT_READ) < 0) {
        THROW_ERROR("mprotect failed");
    }
    if (madvise(p, len, MADV_DONTNEED) < 0) {
        THROW_ERROR("madvise failed");
    }
    if (!all_equal(p, 2 * PAGE_SIZE, 0) || !all_equal(p + 4 * PAGE_SIZE, 2 * PAGE_SIZE, 0)) {
        THROW_ERROR("the writable parts do not read as zeros");
    }
    // A read-only private mapping is dropped on Linux, too, but a program
    // can not have written to it; only check that the call works
    if (mprotect(p + 2 * PAGE_SIZE, 2 * PAGE_SIZE, PROT_READ | PROT_WRITE) < 0) {
        THROW_ERROR("mprotect failed");
    }
    munmap(p, len);
    return 0;
}

static int test_dontneed_keeps_a_shared_file_mapping() {
    const char *path = "/tmp/madvise_test_file";
    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0600);
    if (fd < 0) {
        THROW_ERROR("open failed");
    }
    if (ftruncate(fd, PAGE_SIZE) < 0) {
        THROW_ERROR("ftruncate failed");
    }
    unsigned char *p = mmap(NULL, PAGE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (p == MAP_FAILED) {
        THROW_ERROR("mmap failed");
    }
    fill(p, PAGE_SIZE, 0x42);
    if (madvise(p, PAGE_SIZE, MADV_DONTNEED) < 0) {
        THROW_ERROR("madvise failed");
    }
    if (!all_equal(p, PAGE_SIZE, 0x42)) {
        THROW_ERROR("the data of a shared file mapping changed");
    }
    munmap(p, PAGE_SIZE);
    close(fd);
    unlink(path);
    return 0;
}

static int test_dontneed_rejects_an_unaligned_address() {
    unsigned char *p = map_anon(2 * PAGE_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE);
    if (p == NULL) {
        THROW_ERROR("mmap failed");
    }
    fill(p, 2 * PAGE_SIZE, 0x99);
    if (madvise(p + 1, PAGE_SIZE, MADV_DONTNEED) != -1 || errno != EINVAL) {
        THROW_ERROR("an unaligned address is not rejected with EINVAL");
    }
    if (!all_equal(p, 2 * PAGE_SIZE, 0x99)) {
        THROW_ERROR("the data changed");
    }
    munmap(p, 2 * PAGE_SIZE);
    return 0;
}

static int test_dontneed_length_zero() {
    unsigned char *p = map_anon(PAGE_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE);
    if (p == NULL) {
        THROW_ERROR("mmap failed");
    }
    fill(p, PAGE_SIZE, 0x61);
    if (madvise(p, 0, MADV_DONTNEED) < 0) {
        THROW_ERROR("madvise with length 0 failed");
    }
    if (!all_equal(p, PAGE_SIZE, 0x61)) {
        THROW_ERROR("the data changed");
    }
    munmap(p, PAGE_SIZE);
    return 0;
}

// Threads that fill, drop and check their own range, as the allocator of a
// multi-threaded runtime does, while the other threads fault in pages

#define THREADS 4
#define ROUNDS 200
#define THREAD_PAGES 16

static void *thread_main(void *arg) {
    unsigned char *p = arg;
    size_t len = THREAD_PAGES * PAGE_SIZE;
    for (int i = 0; i < ROUNDS; i++) {
        fill(p, len, (unsigned char)(i + 1));
        if (madvise(p, len, MADV_DONTNEED) < 0) {
            return (void *)1;
        }
        if (!all_equal(p, len, 0)) {
            return (void *)2;
        }
    }
    return NULL;
}

static int test_dontneed_in_threads() {
    size_t len = THREADS * THREAD_PAGES * PAGE_SIZE;
    unsigned char *p = map_anon(len, PROT_READ | PROT_WRITE, MAP_PRIVATE);
    if (p == NULL) {
        THROW_ERROR("mmap failed");
    }
    pthread_t threads[THREADS];
    for (int i = 0; i < THREADS; i++) {
        if (pthread_create(&threads[i], NULL, thread_main,
                           p + (size_t)i * THREAD_PAGES * PAGE_SIZE) != 0) {
            THROW_ERROR("pthread_create failed");
        }
    }
    for (int i = 0; i < THREADS; i++) {
        void *ret;
        pthread_join(threads[i], &ret);
        if (ret != NULL) {
            THROW_ERROR("thread %d failed with %ld", i, (long)ret);
        }
    }
    munmap(p, len);
    return 0;
}

// ============================================================================
// Test suite main
// ============================================================================

static test_case_t test_cases[] = {
    TEST_CASE(test_dontneed_zeroes_the_range),
    TEST_CASE(test_dontneed_length_is_rounded_up),
    TEST_CASE(test_dontneed_large_range),
    TEST_CASE(test_dontneed_spans_several_mappings),
    TEST_CASE(test_dontneed_keeps_a_shared_file_mapping),
    TEST_CASE(test_dontneed_rejects_an_unaligned_address),
    TEST_CASE(test_dontneed_length_zero),
    TEST_CASE(test_dontneed_in_threads),
};

int main(int argc, const char *argv[]) {
    return test_suite_run(test_cases, ARRAY_SIZE(test_cases));
}
