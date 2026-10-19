# The directory on the host that the HostFS of the test is mounted from
DEPS_FILE := unix_socket_inodes_host
include ../test_common.mk

EXTRA_C_FLAGS :=
EXTRA_LINK_FLAGS := -lpthread
BIN_ARGS :=

unix_socket_inodes_host:
	@mkdir -p $(BUILD_DIR)/test/$@
