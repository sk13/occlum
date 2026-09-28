#include <linux/limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <libgen.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <occlum_pal_api.h>
#include <sys/prctl.h>

// Termination signals sent to occlum-run (e.g., by `docker stop` or Ctrl-C)
// are forwarded to the LibOS process as SIGTERM, which is, besides SIGKILL,
// the only signal that the enclave accepts from outside. A second signal
// terminates occlum-run, and thus the enclave, immediately, as without
// forwarding.
static const int forwarded_signals[] = { SIGTERM, SIGINT };
static sigset_t forwarded_sigset;
static pthread_mutex_t forward_lock = PTHREAD_MUTEX_INITIALIZER;
// The LibOS process that receives the signals, or 0 once it has exited
static int forward_pid = 0;

// Block the forwarded signals in the calling thread and thus in all threads
// created by it later, so that only the forwarding thread receives them.
// Signals that are ignored (e.g., SIGINT in background jobs) stay ignored.
//
// Returns whether there is any signal to forward.
static bool block_forwarded_signals(void) {
    bool any = false;
    sigemptyset(&forwarded_sigset);
    for (size_t i = 0; i < sizeof(forwarded_signals) / sizeof(forwarded_signals[0]); i++) {
        struct sigaction old_action;
        if (sigaction(forwarded_signals[i], NULL, &old_action) == 0 &&
                old_action.sa_handler == SIG_IGN) {
            continue;
        }
        sigaddset(&forwarded_sigset, forwarded_signals[i]);
        any = true;
    }
    pthread_sigmask(SIG_BLOCK, &forwarded_sigset, NULL);
    return any;
}

static void *forward_signals(void *arg) {
    bool has_forwarded = false;
    while (true) {
        int sig;
        if (sigwait(&forwarded_sigset, &sig) != 0) {
            return NULL;
        }

        if (has_forwarded) {
            // Terminate with the default action of the signal
            signal(sig, SIG_DFL);
            pthread_sigmask(SIG_UNBLOCK, &forwarded_sigset, NULL);
            raise(sig);
            return NULL;
        }

        pthread_mutex_lock(&forward_lock);
        if (forward_pid > 0) {
            occlum_pal_kill(forward_pid, SIGTERM);
        }
        pthread_mutex_unlock(&forward_lock);
        has_forwarded = true;
    }
}

int main(int argc, char *argv[]) {
    // Parse arguments
    if (argc < 2) {
        fprintf(stderr, "[ERROR] occlum-run: at least one argument must be provided\n\n");
        fprintf(stderr, "Usage: occlum-run <executable> [<args>]\n");
        return EXIT_FAILURE;
    }

    char **cmd_args = &argv[1];
    char *cmd_path = strdup(argv[1]);
    extern const char **environ;

    // Change cmd_args[0] from program path to program name in place (e.g., "/bin/abc" to "abc")
    char *cmd_path_tmp = strdup(cmd_path);
    const char *program_name = (const char *) basename(cmd_path_tmp);
    memset(cmd_args[0], 0, strlen(cmd_args[0]));
    memcpy(cmd_args[0], program_name, strlen(program_name));

    // Check Occlum PAL version
    int pal_version = occlum_pal_get_version();
    if (pal_version <= 0) {
        return EXIT_FAILURE;
    }

    // Before any thread is created, which would otherwise receive the signals
    bool forward = block_forwarded_signals();

    // Init Occlum PAL
    struct occlum_pal_attr attr = OCCLUM_PAL_ATTR_INITVAL;
    attr.log_level = getenv("OCCLUM_LOG_LEVEL");
    if (occlum_pal_init(&attr) < 0) {
        return EXIT_FAILURE;
    }

    // Use Occlum PAL to execute the cmd
    struct occlum_stdio_fds io_fds = {
        .stdin_fd = STDIN_FILENO,
        .stdout_fd = STDOUT_FILENO,
        .stderr_fd = STDERR_FILENO,
    };
    int exit_status = 0;
    int libos_tid = 0;
    struct occlum_pal_create_process_args create_process_args = {
        .path = (const char *) cmd_path,
        .argv = (const char **) cmd_args,
        .env = environ,
        .stdio = (const struct occlum_stdio_fds *) &io_fds,
        .pid = &libos_tid,
    };
    if (occlum_pal_create_process(&create_process_args) < 0) {
        // Command not found or other internal errors
        return 127;
    }

    // Signals that arrived since they were blocked are pending and are
    // forwarded now
    forward_pid = libos_tid;
    pthread_t forward_thread;
    if (forward && pthread_create(&forward_thread, NULL, forward_signals, NULL) != 0) {
        fprintf(stderr, "[WARN] occlum-run: failed to create the thread to forward signals\n");
        forward = false;
    }
    if (!forward) {
        // Terminate on the signals, as without forwarding
        pthread_sigmask(SIG_UNBLOCK, &forwarded_sigset, NULL);
    }

    struct occlum_pal_exec_args exec_args = {
        .pid = libos_tid,
        .exit_value = &exit_status,
    };
    int exec_ret = occlum_pal_exec(&exec_args);

    pthread_mutex_lock(&forward_lock);
    forward_pid = 0;
    pthread_mutex_unlock(&forward_lock);

    if (exec_ret < 0) {
        // Command not found or other internal errors
        return 127;
    }

    // Convert the exit status to a value in a shell-like encoding
    if (WIFEXITED(exit_status)) { // terminated normally
        exit_status = WEXITSTATUS(exit_status); // [0, 255]
    } else { // killed by signal
        exit_status = 128 + WTERMSIG(exit_status); // [128 + 1, 128 + 64]
    }

    // Destroy Occlum PAL
    occlum_pal_destroy();

    return exit_status;
}
