include ../test_common.mk

EXTRA_C_FLAGS :=
EXTRA_LINK_FLAGS :=
BIN_ARGS :=

# A copy of this program, which the network policy of test/Occlum.json does
# not name
all: $(IMAGE_DIR)/bin/network_policy_generic

$(IMAGE_DIR)/bin/network_policy_generic: $(BIN)
	@cp $(BIN) $@
