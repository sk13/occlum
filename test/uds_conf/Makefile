include ../test_common.mk

# Conformance probe of AF_UNIX SOCK_STREAM sockets: uds_conf.c prints one line "ID: result" per check.
#
# As a test (make -C test test TESTS="uds_conf ..."): the results are compared with those of Linux
# (uds_conf.expected, from a native run of the same source), and the checks whose results are known
# to differ (uds_conf.known) are tolerated. A new difference fails the test. When a LibOS fix lands,
# delete the IDs that it fixed from uds_conf.known ("#! now matches Linux" lines tell which).
#
# The groups that hang the LibOS (or, when it has such a bug, crash it, which aborts the enclave) are
# skipped (-s); remove a group from the list when its bug is fixed:
#   c93_*  listen() while another thread blocks in accept() deadlocks the listener
#
# Native Linux:  gcc -pthread -O2 -o uds_conf uds_conf.c && ./uds_conf -d /tmp   (add -e uds_conf.expected -k /dev/null to compare)
# A long run (a minute or more in Occlum): the groups c13_*, c14_*, c19_*, c22_* wait for timeouts, so
# it is not one of the default TESTS. Run it with
#   make -C test test TESTS=uds_conf               (musl)
#   OCCLUM_TEST_GLIBC=1 make -C test test-glibc TESTS=uds_conf
# or part of it in the enclave, with all the results printed: occlum exec /bin/uds_conf -o c02,c04
EXTRA_C_FLAGS := -Wno-unused-function -Wno-unused-variable -Wno-unused-result -Wno-stringop-overread -Wno-maybe-uninitialized
EXTRA_LINK_FLAGS := -lpthread
BIN_ARGS := -d /tmp -t 40 -x /bin/uds_conf -e /etc/uds_conf.expected -k /etc/uds_conf.known \
	-s c93_relisten_while_accept_blocked

CUSTOM_PRE_BUILD := 1

custom_pre_build:
	@mkdir -p $(BUILD_DIR)/test/image/etc
	@cp uds_conf.expected uds_conf.known $(BUILD_DIR)/test/image/etc/
