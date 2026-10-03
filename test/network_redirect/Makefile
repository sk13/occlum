include ../test_common.mk

EXTRA_C_FLAGS :=
EXTRA_LINK_FLAGS := -lpthread
BIN_ARGS :=

# A copy of this program, which the network policy of test/Occlum.json does
# not name
all: $(IMAGE_DIR)/bin/network_redirect_generic

$(IMAGE_DIR)/bin/network_redirect_generic: $(BIN)
	@cp $(BIN) $@
