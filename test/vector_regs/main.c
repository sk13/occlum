// Test that the LibOS preserves the vector state of the user: the SSE, AVX and AVX-512
// registers (xmm0-15, ymm0-15 and zmm0-31; the opmask registers k0-k7 are not tested).
// Every case runs for each of these levels, the levels that the CPU does not support are
// skipped.
//
// A case loads known values into all the vector registers of the level, does the operation
// under test and stores all the registers again, all in one block of assembly (so that the
// compiler does not touch the registers in between). Then the registers are compared with
// the values that were loaded.
//
// The cases cover what the LibOS guarantees:
//   1. The "syscall" instruction. An enclave cannot execute it, so the LibOS emulates it
//      when the CPU raises #UD, and the vector state comes from the exception area of the
//      SGX SDK. This is done with and without a signal that is delivered when the system
//      call returns (kill to itself) and whose handler overwrites all vector registers.
//   2. Exceptions that are handled by a signal handler (divide by zero; the handler skips
//      the instruction and overwrites all vector registers).
//   3. Asynchronous signals (interrupts) that arrive while the user runs a loop of vector
//      instructions, even if the handler overwrites all vector registers. A second thread
//      sends the signals. The loop ends when enough signals have interrupted it (or after
//      a timeout, which fails the case), so the case cannot pass without having been
//      interrupted.
//   4. vfork with the "syscall" instruction. The LibOS saves the context of the parent
//      when it creates the child, and restores it when the child exits or executes another
//      program. The child runs on the thread of the parent, overwrites the vector registers
//      and makes (emulated) system calls, and the parent must still get its own vector
//      state back.
//
// The cases that execute the "syscall" instruction (1 and 4) only run in the HW mode. In the
// other modes (e.g., the simulation mode, where there is no enclave) the CPU executes the
// instruction and the kernel of the host handles the system call, so nothing of the LibOS
// would be tested (and a kill would signal a process of the host). They are skipped there.
//
// With an argument, only the cases with this string in their name are run, e.g.,
// "occlum exec /bin/vector_regs test_vfork_exec_avx".
//
// Not tested on purpose: system calls through the Occlum entry (the address that libc
// jumps to, it is in the auxiliary vector entry AT_OCCLUM_ENTRY). From the point of view
// of libc, such a system call is a function call, and the LibOS is free to use and
// clobber the vector registers there (xmm0-xmm2 and more), as they are caller-saved in
// the calling convention of x86-64. So there is no guarantee to test.
#define _GNU_SOURCE
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <ucontext.h>
#include <unistd.h>
#include "test.h"

// ============================================================================
// The blocks of assembly
// ============================================================================

// The operations are functions in assembly. They take a pointer to this struct, which
// the assembly accesses by the offsets in the comments.
struct vr_op {
    long nr;                // 0: the number of the system call
    long arg1;              // 8: the first argument of the system call
    long arg2;              // 16: the second argument of the system call
    const void *in;         // 24: the values for the registers
    void *out;              // 32: where the values of the registers are stored
};

_Static_assert(offsetof(struct vr_op, in) == 24 && offsetof(struct vr_op, out) == 32,
               "the assembly uses the offsets of struct vr_op");

#define REGS_0_15   "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15"
#define REGS_0_31   REGS_0_15 ",16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31"

__asm__(
    ".pushsection .text\n"
    // VR_LOAD_<level> base: load all registers of the level from base[0...]
    // VR_STORE_<level> base: the same in the other direction
    // VR_CLOBBER_<level>: overwrite all registers of the level
    // VR_ADD_<level>: xmm/ymm/zmm0 += xmm/ymm/zmm1, as packed doubles (AVX has no
    //                 packed integer addition of 256 bits, that is AVX2)
    // VR_END_<level>: what to do before leaving the code that uses the level
    ".macro VR_LOAD_SSE base\n"
    "   .irp i," REGS_0_15 "\n"
    "       movdqu (\\i*16)(\\base), %xmm\\i\n"
    "   .endr\n"
    ".endm\n"
    ".macro VR_STORE_SSE base\n"
    "   .irp i," REGS_0_15 "\n"
    "       movdqu %xmm\\i, (\\i*16)(\\base)\n"
    "   .endr\n"
    ".endm\n"
    ".macro VR_CLOBBER_SSE\n"
    "   .irp i," REGS_0_15 "\n"
    "       pcmpeqd %xmm\\i, %xmm\\i\n"
    "   .endr\n"
    ".endm\n"
    ".macro VR_ADD_SSE\n"
    "   addpd %xmm1, %xmm0\n"
    ".endm\n"
    ".macro VR_END_SSE\n"
    ".endm\n"

    ".macro VR_LOAD_AVX base\n"
    "   .irp i," REGS_0_15 "\n"
    "       vmovdqu (\\i*32)(\\base), %ymm\\i\n"
    "   .endr\n"
    ".endm\n"
    ".macro VR_STORE_AVX base\n"
    "   .irp i," REGS_0_15 "\n"
    "       vmovdqu %ymm\\i, (\\i*32)(\\base)\n"
    "   .endr\n"
    ".endm\n"
    ".macro VR_CLOBBER_AVX\n"
    "   .irp i," REGS_0_15 "\n"
    "       vcmpps $15, %ymm\\i, %ymm\\i, %ymm\\i\n"    // true: all bits set
    "   .endr\n"
    ".endm\n"
    ".macro VR_ADD_AVX\n"
    "   vaddpd %ymm1, %ymm0, %ymm0\n"
    ".endm\n"
    ".macro VR_END_AVX\n"
    "   vzeroupper\n"
    ".endm\n"

    ".macro VR_LOAD_AVX512 base\n"
    "   .irp i," REGS_0_31 "\n"
    "       vmovdqu64 (\\i*64)(\\base), %zmm\\i\n"
    "   .endr\n"
    ".endm\n"
    ".macro VR_STORE_AVX512 base\n"
    "   .irp i," REGS_0_31 "\n"
    "       vmovdqu64 %zmm\\i, (\\i*64)(\\base)\n"
    "   .endr\n"
    ".endm\n"
    ".macro VR_CLOBBER_AVX512\n"
    "   .irp i," REGS_0_31 "\n"
    "       vpternlogd $0xff, %zmm\\i, %zmm\\i, %zmm\\i\n"  // all bits set
    "   .endr\n"
    ".endm\n"
    ".macro VR_ADD_AVX512\n"
    "   vaddpd %zmm1, %zmm0, %zmm0\n"
    ".endm\n"
    ".macro VR_END_AVX512\n"
    "   vzeroupper\n"
    ".endm\n"

    // VR_DEFINE <level>, <symbol suffix>: the operations for a level
    ".macro VR_DEFINE level, sym\n"

    // long vr_syscall_<sym>(struct vr_op *op)
    // Load the registers, execute "syscall" (nr, arg1, arg2), store the registers.
    // Returns the result of the system call.
    ".globl vr_syscall_\\sym\n"
    ".type vr_syscall_\\sym, @function\n"
    "vr_syscall_\\sym:\n"
    "   push %r12\n"
    "   push %r13\n"
    "   mov 24(%rdi), %r12\n"
    "   mov 32(%rdi), %r13\n"
    "   VR_LOAD_\\level %r12\n"
    "   mov 0(%rdi), %rax\n"
    "   mov 16(%rdi), %rsi\n"
    "   mov 8(%rdi), %rdi\n"
    "   syscall\n"
    "   VR_STORE_\\level %r13\n"
    "   VR_END_\\level\n"
    "   pop %r13\n"
    "   pop %r12\n"
    "   ret\n"
    ".size vr_syscall_\\sym, .-vr_syscall_\\sym\n"

    // long vr_div0_<sym>(struct vr_op *op)
    // Load the registers, divide by zero, store the registers. The signal handler has to
    // skip the instruction (2 bytes).
    ".globl vr_div0_\\sym\n"
    ".type vr_div0_\\sym, @function\n"
    "vr_div0_\\sym:\n"
    "   push %r12\n"
    "   push %r13\n"
    "   mov 24(%rdi), %r12\n"
    "   mov 32(%rdi), %r13\n"
    "   VR_LOAD_\\level %r12\n"
    "   xor %ecx, %ecx\n"
    "   mov $1, %eax\n"
    "   xor %edx, %edx\n"
    "   idiv %ecx\n"
    "   VR_STORE_\\level %r13\n"
    "   VR_END_\\level\n"
    "   xor %eax, %eax\n"
    "   pop %r13\n"
    "   pop %r12\n"
    "   ret\n"
    ".size vr_div0_\\sym, .-vr_div0_\\sym\n"

    // long vr_vfork_<sym>(struct vr_op *op)
    // Load the registers, execute vfork with the "syscall" instruction. The child
    // overwrites all registers and exits with the "syscall" instruction. The parent
    // stores the registers and returns the pid of the child (or a negative error).
    // The child shares the stack of the parent: it must not use it, not even a call.
    ".globl vr_vfork_\\sym\n"
    ".type vr_vfork_\\sym, @function\n"
    "vr_vfork_\\sym:\n"
    "   push %r12\n"
    "   push %r13\n"
    "   mov 24(%rdi), %r12\n"
    "   mov 32(%rdi), %r13\n"
    "   VR_LOAD_\\level %r12\n"
    "   mov $" STR(__NR_vfork) ", %eax\n"
    "   syscall\n"
    "   test %rax, %rax\n"
    "   jnz 1f\n"
    // The child
    "   VR_CLOBBER_\\level\n"
    "   mov $" STR(__NR_exit_group) ", %eax\n"
    "   xor %edi, %edi\n"
    "   syscall\n"
    "   ud2\n"
    // The parent
    "1:\n"
    "   VR_STORE_\\level %r13\n"
    "   VR_END_\\level\n"
    "   pop %r13\n"
    "   pop %r12\n"
    "   ret\n"
    ".size vr_vfork_\\sym, .-vr_vfork_\\sym\n"

    // long vr_vforkexec_<sym>(struct vr_op *op)
    // Like vr_vfork, but the child executes a program, op->arg1 is its path and op->arg2
    // its argv. It exits with 77 if it fails to.
    ".globl vr_vforkexec_\\sym\n"
    ".type vr_vforkexec_\\sym, @function\n"
    "vr_vforkexec_\\sym:\n"
    "   push %r12\n"
    "   push %r13\n"
    "   mov 24(%rdi), %r12\n"
    "   mov 32(%rdi), %r13\n"
    "   VR_LOAD_\\level %r12\n"
    "   mov $" STR(__NR_vfork) ", %eax\n"
    "   syscall\n"
    "   test %rax, %rax\n"
    "   jnz 1f\n"
    // The child
    "   VR_CLOBBER_\\level\n"
    "   mov $" STR(__NR_execve) ", %eax\n"
    "   mov 16(%rdi), %rsi\n"
    "   mov 8(%rdi), %rdi\n"
    "   xor %edx, %edx\n"
    "   syscall\n"
    "   mov $" STR(__NR_exit_group) ", %eax\n"
    "   mov $77, %edi\n"
    "   syscall\n"
    "   ud2\n"
    // The parent
    "1:\n"
    "   VR_STORE_\\level %r13\n"
    "   VR_END_\\level\n"
    "   pop %r13\n"
    "   pop %r12\n"
    "   ret\n"
    ".size vr_vforkexec_\\sym, .-vr_vforkexec_\\sym\n"

    // long vr_spin_<sym>(volatile int *stop, void *out, const void *in)
    // Load the registers from in. Add xmm1 to xmm0 in a loop, until *stop is not 0, which
    // is tested every 65536 iterations. Store the registers to out. Returns the number
    // of iterations. vr_spin_end_<sym> is the end of the function.
    ".globl vr_spin_\\sym\n"
    ".type vr_spin_\\sym, @function\n"
    "vr_spin_\\sym:\n"
    "   VR_LOAD_\\level %rdx\n"
    "   xor %eax, %eax\n"
    "1:\n"
    "   VR_ADD_\\level\n"
    "   add $1, %rax\n"
    "   test $0xffff, %eax\n"
    "   jnz 1b\n"
    "   cmpl $0, (%rdi)\n"
    "   je 1b\n"
    "   VR_STORE_\\level %rsi\n"
    "   VR_END_\\level\n"
    "   ret\n"
    ".globl vr_spin_end_\\sym\n"
    "vr_spin_end_\\sym:\n"
    ".size vr_spin_\\sym, .-vr_spin_\\sym\n"

    // void vr_clobber_<sym>(void)
    // Overwrite all registers of the level (they are not preserved by the C calling
    // convention). The registers are not cleaned up with vzeroupper on purpose.
    ".globl vr_clobber_\\sym\n"
    ".type vr_clobber_\\sym, @function\n"
    "vr_clobber_\\sym:\n"
    "   VR_CLOBBER_\\level\n"
    "   ret\n"
    ".size vr_clobber_\\sym, .-vr_clobber_\\sym\n"

    // long vr_check_clobber_<sym>(struct vr_op *op)
    // Load the registers, call vr_clobber_<sym>, store the registers: tests that the
    // clobbering that the signal handlers do changes all registers.
    ".globl vr_check_clobber_\\sym\n"
    ".type vr_check_clobber_\\sym, @function\n"
    "vr_check_clobber_\\sym:\n"
    "   push %r12\n"
    "   push %r13\n"
    "   mov 24(%rdi), %r12\n"
    "   mov 32(%rdi), %r13\n"
    "   VR_LOAD_\\level %r12\n"
    "   sub $8, %rsp\n"        // the stack is aligned for the call
    "   call vr_clobber_\\sym\n"
    "   add $8, %rsp\n"
    "   VR_STORE_\\level %r13\n"
    "   VR_END_\\level\n"
    "   xor %eax, %eax\n"
    "   pop %r13\n"
    "   pop %r12\n"
    "   ret\n"
    ".size vr_check_clobber_\\sym, .-vr_check_clobber_\\sym\n"
    ".endm\n"

    "VR_DEFINE SSE, sse\n"
    "VR_DEFINE AVX, avx\n"
    "VR_DEFINE AVX512, avx512\n"
    ".popsection\n"
);

typedef long (*op_func_t)(struct vr_op *);
typedef long (*spin_func_t)(volatile int *, void *, const void *);

#define DECLARE_LEVEL(sym) \
    extern long vr_syscall_##sym(struct vr_op *); \
    extern long vr_div0_##sym(struct vr_op *); \
    extern long vr_vfork_##sym(struct vr_op *); \
    extern long vr_vforkexec_##sym(struct vr_op *); \
    extern long vr_spin_##sym(volatile int *, void *, const void *); \
    extern char vr_spin_end_##sym[]; \
    extern void vr_clobber_##sym(void); \
    extern long vr_check_clobber_##sym(struct vr_op *);

DECLARE_LEVEL(sse)
DECLARE_LEVEL(avx)
DECLARE_LEVEL(avx512)

// ============================================================================
// The levels
// ============================================================================

enum {
    LEVEL_SSE,
    LEVEL_AVX,
    LEVEL_AVX512,
    LEVEL_COUNT
};

typedef struct {
    const char *name;
    char reg_prefix;    // the registers are named xmm0, ymm0 and zmm0
    int num_regs;
    int reg_size;       // in bytes
    op_func_t syscall_op;
    op_func_t div0_op;
    op_func_t vfork_op;
    op_func_t vforkexec_op;
    spin_func_t spin_op;
    const char *spin_end;
    void (*clobber)(void);
    op_func_t check_clobber_op;
} level_t;

#define LEVEL(NAME, prefix, nregs, size, sym) \
    { NAME, prefix, nregs, size, vr_syscall_##sym, vr_div0_##sym, vr_vfork_##sym, \
      vr_vforkexec_##sym, vr_spin_##sym, vr_spin_end_##sym, vr_clobber_##sym, \
      vr_check_clobber_##sym }

static const level_t LEVELS[LEVEL_COUNT] = {
    LEVEL("SSE", 'x', 16, 16, sse),
    LEVEL("AVX", 'y', 16, 32, avx),
    LEVEL("AVX-512", 'z', 32, 64, avx512),
};

#define MAX_STATE_SIZE  (32 * 64)

static int is_level_supported(int level) {
    __builtin_cpu_init();
    switch (level) {
        case LEVEL_SSE:
            return 1;
        case LEVEL_AVX:
            return __builtin_cpu_supports("avx");
        default:
            return __builtin_cpu_supports("avx512f");
    }
}

// Returns 1 and prints a note if the CPU does not support the level
static int skip_level(int level) {
    if (is_level_supported(level)) {
        return 0;
    }
    printf("\t\tskipped, the CPU does not support %s\n", LEVELS[level].name);
    return 1;
}

// Returns 1 and prints a note if the LibOS does not emulate the "syscall" instruction in the
// mode of the build. Only in the HW mode the enclave raises #UD, which the LibOS handles.
static int skip_no_syscall_emulation(void) {
#ifdef SGX_MODE_HW
    return 0;
#else
    printf("\t\tskipped, the \"syscall\" instruction is only emulated in the HW mode\n");
    return 1;
#endif
}

// ============================================================================
// Helper functions
// ============================================================================

// The values for the registers and the values that are stored after the operation
static uint8_t g_in[MAX_STATE_SIZE] __attribute__((aligned(64)));
static uint8_t g_out[MAX_STATE_SIZE] __attribute__((aligned(64)));

static uint64_t g_random_state = 0x9e3779b97f4a7c15ULL;

static uint64_t next_random(void) {
    g_random_state ^= g_random_state << 13;
    g_random_state ^= g_random_state >> 7;
    g_random_state ^= g_random_state << 17;
    return g_random_state;
}

static void fill_random(void *buf, size_t len) {
    uint64_t *p = buf;
    for (size_t i = 0; i < len / sizeof(*p); i++) {
        p[i] = next_random();
    }
}

// Compares the registers of a level in two buffers. Returns the number of different
// registers. Their names are written to the string "names".
static int diff_regs(int level, const void *want, const void *got, char *names,
                     size_t names_len) {
    const level_t *l = &LEVELS[level];
    int num_diff = 0;
    names[0] = '\0';
    for (int r = 0; r < l->num_regs; r++) {
        const uint8_t *w = (const uint8_t *)want + r * l->reg_size;
        const uint8_t *g = (const uint8_t *)got + r * l->reg_size;
        if (memcmp(w, g, l->reg_size) == 0) {
            continue;
        }
        num_diff++;
        size_t used = strlen(names);
        if (used + 8 < names_len) {
            snprintf(names + used, names_len - used, " %cmm%d", l->reg_prefix, r);
        }
    }
    return num_diff;
}

#define CHECK_REGS(level, want, got, ...) \
    do { \
        char names[512]; \
        int num_diff = diff_regs(level, want, got, names, sizeof(names)); \
        if (num_diff != 0) { \
            printf("\t\t%s: %d registers changed:%s\n", LEVELS[level].name, num_diff, \
                   names); \
            THROW_ERROR(__VA_ARGS__); \
        } \
    } while (0)

// The signal handlers overwrite all vector registers of this level
static volatile int g_handler_level;
static volatile int g_handler_runs;

// When a signal is delivered, the LibOS keeps the vector state of the user in a buffer in
// its heap, until the handler returns. The LibOS must not free the buffer after it has
// restored the vector state: an allocator that uses vector registers (e.g., the allocator of
// the SGX SDK when it is compiled with GCC 12 or later) would change the state that has
// just been restored. Whether it does depends on the memory next to the buffer (for example,
// it may be unused or not). So the handlers open and close a few files (objects in the heap
// of the LibOS) before they overwrite the vector registers, to vary the heap layout. The
// files are closed later.
#define MAX_HEAP_FDS    4
static int g_heap_fds[MAX_HEAP_FDS];
static int g_num_heap_fds;
static unsigned g_heap_rounds;

static void close_heap_fds(void) {
    while (g_num_heap_fds > 0) {
        close(g_heap_fds[--g_num_heap_fds]);
    }
}

static void use_heap(void) {
    int saved_errno = errno;
    close_heap_fds();
    unsigned num_fds = 1 + g_heap_rounds++ % MAX_HEAP_FDS;
    for (unsigned i = 0; i < num_fds; i++) {
        int fd = eventfd(0, 0);
        if (fd >= 0) {
            g_heap_fds[g_num_heap_fds++] = fd;
        }
    }
    errno = saved_errno;
}

static void overwrite_all_regs(void) {
    use_heap();
    LEVELS[g_handler_level].clobber();
}

static void signal_handler(int signum, siginfo_t *info, void *ucontext) {
    g_handler_runs++;
    overwrite_all_regs();
}

static void sigfpe_handler(int signum, siginfo_t *info, void *ucontext) {
    ucontext_t *uc = ucontext;
    // Skip the instruction that has raised the exception: idiv %ecx
    uc->uc_mcontext.gregs[REG_RIP] += 2;
    g_handler_runs++;
    overwrite_all_regs();
}

static int install_handler(int signum, void (*handler)(int, siginfo_t *, void *)) {
    struct sigaction action;
    memset(&action, 0, sizeof(action));
    action.sa_sigaction = handler;
    action.sa_flags = SA_SIGINFO;
    return sigaction(signum, &action, NULL);
}

// ============================================================================
// Test case for the helper that the signal handlers use
// ============================================================================

// All the registers of the level must change when the handlers overwrite them, or the
// other test cases would not test them
static int test_handlers_overwrite_all_regs(int level) {
    if (skip_level(level)) {
        return 0;
    }
    fill_random(g_in, sizeof(g_in));
    memset(g_out, 0, sizeof(g_out));
    struct vr_op op = { 0, 0, 0, g_in, g_out };
    LEVELS[level].check_clobber_op(&op);
    for (int r = 0; r < LEVELS[level].num_regs; r++) {
        size_t size = LEVELS[level].reg_size;
        if (memcmp(g_in + r * size, g_out + r * size, size) == 0) {
            THROW_ERROR("%s: register %d is not overwritten", LEVELS[level].name, r);
        }
    }
    return 0;
}

// ============================================================================
// Test cases for system calls and exceptions
// ============================================================================

#define SYSCALL_REPS    200
#define SIGNAL_REPS     200
#define EXCEPTION_REPS  200

// Does the operation many times with random values in the vector registers. The signal
// handler has to run "handler_runs" times in every operation.
static int run_op(int level, op_func_t op_func, long nr, long arg1, long arg2, int reps,
                  int handler_runs, long expected_ret) {
    g_handler_level = level;
    for (int i = 0; i < reps; i++) {
        fill_random(g_in, sizeof(g_in));
        memset(g_out, 0, sizeof(g_out));
        struct vr_op op = { nr, arg1, arg2, g_in, g_out };
        g_handler_runs = 0;

        long ret = op_func(&op);

        if (ret != expected_ret) {
            THROW_ERROR("the operation returned %ld, not %ld (run %d)", ret,
                        expected_ret, i);
        }
        if (g_handler_runs != handler_runs) {
            THROW_ERROR("the signal handler ran %d times, not %d (run %d)",
                        g_handler_runs, handler_runs, i);
        }
        close_heap_fds();
        CHECK_REGS(level, g_in, g_out, "vector registers changed (run %d)", i);
    }
    return 0;
}

// The emulated "syscall" instruction, without a signal
static int test_syscall_insn(int level) {
    if (skip_no_syscall_emulation() || skip_level(level)) {
        return 0;
    }
    return run_op(level, LEVELS[level].syscall_op, __NR_getpid, 0, 0, SYSCALL_REPS, 0,
                  getpid());
}

// The emulated "syscall" instruction, with a signal that is delivered when it returns
static int test_syscall_insn_with_signal(int level) {
    if (skip_no_syscall_emulation() || skip_level(level)) {
        return 0;
    }
    if (install_handler(SIGUSR1, signal_handler) < 0) {
        THROW_ERROR("failed to install the handler");
    }
    return run_op(level, LEVELS[level].syscall_op, __NR_kill, getpid(), SIGUSR1,
                  SIGNAL_REPS, 1, 0);
}

// An exception (divide by zero) that is handled by a signal handler
static int test_exception_with_signal(int level) {
    if (skip_level(level)) {
        return 0;
    }
    if (install_handler(SIGFPE, sigfpe_handler) < 0) {
        THROW_ERROR("failed to install the handler");
    }
    return run_op(level, LEVELS[level].div0_op, 0, 0, 0, EXCEPTION_REPS, 1, 0);
}

// ============================================================================
// Test case for asynchronous signals
// ============================================================================

// The loop ends when this number of signals have interrupted it, or after the timeout.
// It is an error if less than the minimum number of signals have interrupted it.
#define ASYNC_SIGNALS_WANTED    30
#define ASYNC_SIGNALS_MIN       10
#define ASYNC_TIMEOUT_SECS      5

static volatile int g_stop;
static pthread_t g_main_thread;
// The code of the loop: signals that interrupt it are counted
static volatile uintptr_t g_loop_begin;
static volatile uintptr_t g_loop_end;
static volatile int g_loop_interrupts;

static void async_signal_handler(int signum, siginfo_t *info, void *ucontext) {
    ucontext_t *uc = ucontext;
    uintptr_t rip = uc->uc_mcontext.gregs[REG_RIP];
    g_handler_runs++;
    overwrite_all_regs();
    if (rip >= g_loop_begin && rip < g_loop_end) {
        if (++g_loop_interrupts >= ASYNC_SIGNALS_WANTED) {
            g_stop = 1;
        }
    }
}

// Sends signals to the main thread until the loop has stopped
static void *signal_sender(void *arg) {
    struct timespec start, now;
    clock_gettime(CLOCK_MONOTONIC, &start);
    while (!g_stop) {
        pthread_kill(g_main_thread, SIGUSR2);
        usleep(100);
        clock_gettime(CLOCK_MONOTONIC, &now);
        if (now.tv_sec - start.tv_sec >= ASYNC_TIMEOUT_SECS) {
            g_stop = 1;
        }
    }
    return NULL;
}

// The accumulator xmm0 starts with 0 and is incremented by xmm1 = {1.0, 2.0, ...} in
// every iteration of the loop, so its values show how many iterations have run. All the
// other registers have random values that must not change.
static int test_async_signals(int level) {
    const level_t *l = &LEVELS[level];
    if (skip_level(level)) {
        return 0;
    }
    if (install_handler(SIGUSR2, async_signal_handler) < 0) {
        THROW_ERROR("failed to install the handler");
    }

    fill_random(g_in, sizeof(g_in));
    memset(g_in, 0, l->reg_size);
    double *increments = (double *)(g_in + l->reg_size);
    for (int i = 0; i < l->reg_size / 8; i++) {
        increments[i] = i + 1;
    }
    memset(g_out, 0, sizeof(g_out));
    g_handler_level = level;
    g_handler_runs = 0;
    g_loop_interrupts = 0;
    g_loop_begin = (uintptr_t)l->spin_op;
    g_loop_end = (uintptr_t)l->spin_end;
    g_stop = 0;
    g_main_thread = pthread_self();

    pthread_t sender;
    if (pthread_create(&sender, NULL, signal_sender, NULL) != 0) {
        THROW_ERROR("failed to create the thread that sends signals");
    }
    long iterations = l->spin_op(&g_stop, g_out, g_in);
    g_stop = 1;
    pthread_join(sender, NULL);

    // No handler must run from now on. Signals that are still pending are ignored.
    sigset_t sigusr2;
    sigemptyset(&sigusr2);
    sigaddset(&sigusr2, SIGUSR2);
    pthread_sigmask(SIG_BLOCK, &sigusr2, NULL);
    int interrupts = g_loop_interrupts;
    int runs = g_handler_runs;
    signal(SIGUSR2, SIG_IGN);
    pthread_sigmask(SIG_UNBLOCK, &sigusr2, NULL);
    close_heap_fds();

    printf("\t\t%s: %d signals, %d interrupted the loop of %ld iterations\n", l->name,
           runs, interrupts, iterations);
    if (interrupts < ASYNC_SIGNALS_MIN) {
        THROW_ERROR("only %d signals have interrupted the loop in %d seconds",
                    interrupts, ASYNC_TIMEOUT_SECS);
    }
    uint8_t *want = g_in;   // the same values, except the accumulator
    double *accumulator = (double *)want;
    for (int i = 0; i < l->reg_size / 8; i++) {
        accumulator[i] = (double)iterations * (i + 1);
    }
    CHECK_REGS(level, want, g_out, "vector registers changed after %d signals", runs);
    return 0;
}

// ============================================================================
// Test case for vfork
// ============================================================================

#define VFORK_REPS  20

// The vfork_exec cases execute this program with this argument, and it does nothing
#define EXEC_CHILD_PATH "/bin/vector_regs"
#define EXEC_CHILD_ARG  "exec_child"

// Calls a function that executes vfork many times with random values in the vector
// registers. The child overwrites the vector registers and makes a system call on the
// thread of the parent: exit, or execve of the program that is given by path and argv.
// When the child has exited or executed the program, the parent must get all its vector
// registers back.
//
// The child has to overwrite the vector registers. The vector state of the parent that
// the LibOS saved when the "syscall" instruction was emulated may be in memory that the
// next exception (like the emulated exit of the child) overwrites. That would not make a
// difference if the child still had the same values in its registers as the parent.
static int run_vfork(int level, op_func_t vfork_op, const char *path,
                     char *const argv[]) {
    for (int i = 0; i < VFORK_REPS; i++) {
        fill_random(g_in, sizeof(g_in));
        memset(g_out, 0, sizeof(g_out));
        struct vr_op op = { 0, (long)path, (long)argv, g_in, g_out };

        long child_pid = vfork_op(&op);

        if (child_pid <= 0) {
            THROW_ERROR("vfork returned %ld (run %d)", child_pid, i);
        }
        int status = 0;
        pid_t waited_pid = waitpid(child_pid, &status, 0);
        if (waited_pid != child_pid || !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            THROW_ERROR("the child %ld did not exit with 0 (waitpid returned %d, status 0x%x, "
                        "run %d)", child_pid, waited_pid, status, i);
        }
        CHECK_REGS(level, g_in, g_out, "vector registers of the parent changed (run %d)", i);
    }
    return 0;
}

// The child exits
static int test_vfork(int level) {
    if (skip_no_syscall_emulation() || skip_level(level)) {
        return 0;
    }
    return run_vfork(level, LEVELS[level].vfork_op, NULL, NULL);
}

// The child executes a program
static int test_vfork_exec(int level) {
    if (skip_no_syscall_emulation() || skip_level(level)) {
        return 0;
    }
    char *const argv[] = { EXEC_CHILD_PATH, EXEC_CHILD_ARG, NULL };
    return run_vfork(level, LEVELS[level].vforkexec_op, EXEC_CHILD_PATH, argv);
}

// ============================================================================
// Test suite main
// ============================================================================

// The test cases for all the levels: test_<name>_sse, test_<name>_avx, test_<name>_avx512
#define DEFINE_LEVEL_TESTS(name) \
    static int test_##name##_sse(void) { return test_##name(LEVEL_SSE); } \
    static int test_##name##_avx(void) { return test_##name(LEVEL_AVX); } \
    static int test_##name##_avx512(void) { return test_##name(LEVEL_AVX512); }

#define LEVEL_TEST_CASES(name) \
    TEST_CASE(test_##name##_sse), \
    TEST_CASE(test_##name##_avx), \
    TEST_CASE(test_##name##_avx512)

DEFINE_LEVEL_TESTS(handlers_overwrite_all_regs)
DEFINE_LEVEL_TESTS(syscall_insn)
DEFINE_LEVEL_TESTS(syscall_insn_with_signal)
DEFINE_LEVEL_TESTS(exception_with_signal)
DEFINE_LEVEL_TESTS(async_signals)
DEFINE_LEVEL_TESTS(vfork)
DEFINE_LEVEL_TESTS(vfork_exec)

static test_case_t test_cases[] = {
    LEVEL_TEST_CASES(handlers_overwrite_all_regs),
    LEVEL_TEST_CASES(syscall_insn),
    LEVEL_TEST_CASES(syscall_insn_with_signal),
    LEVEL_TEST_CASES(exception_with_signal),
    LEVEL_TEST_CASES(async_signals),
    LEVEL_TEST_CASES(vfork),
    LEVEL_TEST_CASES(vfork_exec),
};

// With an argument, only the test cases with this string in their name are run
int main(int argc, const char *argv[]) {
    if (argc >= 2 && strcmp(argv[1], EXEC_CHILD_ARG) == 0) {
        return 0;
    }
    test_case_t selected[ARRAY_SIZE(test_cases)];
    int num_selected = 0;
    for (int i = 0; i < ARRAY_SIZE(test_cases); i++) {
        if (argc < 2 || strstr(test_cases[i].name, argv[1]) != NULL) {
            selected[num_selected++] = test_cases[i];
        }
    }
    if (num_selected == 0) {
        printf("no test case matches %s\n", argv[1]);
        return -1;
    }
    return test_suite_run(selected, num_selected);
}
