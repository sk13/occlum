#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "test.h"

// ============================================================================
// The options of an SCTP socket with which libsctp connects (sctp_connectx())
// and binds (sctp_bindx()): their results. sctp_connectx() asks for
// SCTP_SOCKOPT_CONNECTX3 first and, if that fails with ENOPROTOOPT, sets
// SCTP_SOCKOPT_CONNECTX, whose result is the ID of the association: libsctp
// takes the result 0 for "not supported", and any other error of the first
// call for the end of sctp_connectx().
// ============================================================================

#define SOL_SCTP                    132
#define SCTP_SOCKOPT_BINDX_ADD      100
#define SCTP_SOCKOPT_CONNECTX_OLD   107
#define SCTP_SOCKOPT_CONNECTX       110
#define SCTP_SOCKOPT_CONNECTX3      111

// The value of SCTP_SOCKOPT_CONNECTX3: the addresses are behind the pointer, and addr_num is the
// size of the array in bytes (as libsctp sets it)
struct connectx3 {
    int assoc_id;
    int addr_num;
    struct sockaddr *addrs;
};

// Returns -1 and sets skipped if the system has no SCTP
static int skipped;

// A listening SCTP socket (one association per socket) on 127.0.0.1 with a port of the system
static int sctp_listener(struct sockaddr_in *addr) {
    int fd = socket(AF_INET, SOCK_STREAM, IPPROTO_SCTP);
    if (fd < 0) {
        skipped = errno == EPROTONOSUPPORT || errno == ESOCKTNOSUPPORT || errno == EAFNOSUPPORT;
        return -1;
    }
    memset(addr, 0, sizeof(*addr));
    addr->sin_family = AF_INET;
    addr->sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t len = sizeof(*addr);
    if (bind(fd, (struct sockaddr *)addr, sizeof(*addr)) < 0 ||
            getsockname(fd, (struct sockaddr *)addr, &len) < 0 || listen(fd, 1) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

// The client has connected to the listener and the two exchange a message
static int check_connection(int listener, int client) {
    int server = accept(listener, NULL, NULL);
    if (server < 0) {
        THROW_ERROR("accept failed");
    }
    char buf[8];
    if (send(client, "hello", 5, 0) != 5 || recv(server, buf, sizeof(buf), 0) != 5 ||
            memcmp(buf, "hello", 5) != 0) {
        THROW_ERROR("the connection does not carry data");
    }
    close(server);
    return 0;
}

// The result of SCTP_SOCKOPT_CONNECTX is the ID of the association, which is not 0
static int test_connectx_gives_the_association_id(void) {
    struct sockaddr_in addr;
    int listener = sctp_listener(&addr);
    if (listener < 0) {
        if (skipped) {
            printf("SCTP is not supported, skipped\n");
            return 0;
        }
        THROW_ERROR("failed to create the listener");
    }
    int client = socket(AF_INET, SOCK_STREAM, IPPROTO_SCTP);
    long ret = syscall(SYS_setsockopt, client, SOL_SCTP, SCTP_SOCKOPT_CONNECTX, &addr,
                       sizeof(addr));
    if (ret <= 0) {
        THROW_ERROR("SCTP_SOCKOPT_CONNECTX returns %ld instead of the ID of the association", ret);
    }
    if (check_connection(listener, client) < 0) {
        return -1;
    }
    close(client);
    close(listener);
    return 0;
}

// The old option connects and gives back 0
static int test_connectx_old_gives_zero(void) {
    struct sockaddr_in addr;
    int listener = sctp_listener(&addr);
    if (listener < 0) {
        return skipped ? 0 : -1;
    }
    int client = socket(AF_INET, SOCK_STREAM, IPPROTO_SCTP);
    long ret = syscall(SYS_setsockopt, client, SOL_SCTP, SCTP_SOCKOPT_CONNECTX_OLD, &addr,
                       sizeof(addr));
    if (ret != 0) {
        THROW_ERROR("SCTP_SOCKOPT_CONNECTX_OLD returns %ld instead of 0", ret);
    }
    if (check_connection(listener, client) < 0) {
        return -1;
    }
    close(client);
    close(listener);
    return 0;
}

// What libsctp does: the call with SCTP_SOCKOPT_CONNECTX3 either connects, or fails with
// ENOPROTOOPT, which sends libsctp to SCTP_SOCKOPT_CONNECTX; any other error ends sctp_connectx()
static int test_connectx3_lets_libsctp_go_on(void) {
    struct sockaddr_in addr;
    int listener = sctp_listener(&addr);
    if (listener < 0) {
        return skipped ? 0 : -1;
    }
    int client = socket(AF_INET, SOCK_STREAM, IPPROTO_SCTP);
    struct connectx3 param = { 0, (int)sizeof(addr), (struct sockaddr *)&addr };
    socklen_t size = sizeof(param);
    int ret = getsockopt(client, SOL_SCTP, SCTP_SOCKOPT_CONNECTX3, &param, &size);
    if (ret == 0) {
        // The kernel has the option: the socket is connected and the ID is set
        if (param.assoc_id <= 0) {
            THROW_ERROR("SCTP_SOCKOPT_CONNECTX3 sets the ID %d", param.assoc_id);
        }
    } else if (errno == ENOPROTOOPT) {
        long id = syscall(SYS_setsockopt, client, SOL_SCTP, SCTP_SOCKOPT_CONNECTX, &addr,
                          sizeof(addr));
        if (id <= 0) {
            THROW_ERROR("the fallback returns %ld instead of the ID of the association", id);
        }
    } else {
        THROW_ERROR("SCTP_SOCKOPT_CONNECTX3 fails with errno %d, which ends sctp_connectx()",
                    errno);
    }
    if (check_connection(listener, client) < 0) {
        return -1;
    }
    close(client);
    close(listener);
    return 0;
}

// The options that give no other result still give 0
static int test_other_options_give_zero(void) {
    struct sockaddr_in addr;
    int listener = sctp_listener(&addr);
    if (listener < 0) {
        return skipped ? 0 : -1;
    }
    int fd = socket(AF_INET, SOCK_STREAM, IPPROTO_SCTP);
    struct sockaddr_in local;
    memset(&local, 0, sizeof(local));
    local.sin_family = AF_INET;
    local.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    long ret = syscall(SYS_setsockopt, fd, SOL_SCTP, SCTP_SOCKOPT_BINDX_ADD, &local,
                       sizeof(local));
    if (ret != 0) {
        THROW_ERROR("SCTP_SOCKOPT_BINDX_ADD returns %ld instead of 0", ret);
    }
    int on = 1;
    ret = syscall(SYS_setsockopt, fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
    if (ret != 0) {
        THROW_ERROR("SO_REUSEADDR returns %ld instead of 0", ret);
    }
    close(fd);
    close(listener);
    return 0;
}

static test_case_t test_cases[] = {
    TEST_CASE(test_connectx_gives_the_association_id),
    TEST_CASE(test_connectx_old_gives_zero),
    TEST_CASE(test_connectx3_lets_libsctp_go_on),
    TEST_CASE(test_other_options_give_zero),
};

int main(void) {
    return test_suite_run(test_cases, ARRAY_SIZE(test_cases));
}
