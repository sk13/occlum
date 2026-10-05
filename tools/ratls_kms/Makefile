include ../../src/sgxenv.mk

# The key server runs in an Occlum enclave as a glibc program, hence cargo
# and not occlum-cargo
SRC_FILES := $(shell find . ../ratls -type f -name '*.rs') Cargo.toml ../ratls/Cargo.toml
RUST_TARGET_DIR := $(BUILD_DIR)/internal/tools/ratls_kms/cargo-target
RUST_OUT_DIR := $(BUILD_DIR)/bin
TARGET_BINARY := $(RUST_OUT_DIR)/ratls_kms

.PHONY: all clean

all: $(SRC_FILES)
	@RUSTC_BOOTSTRAP=1 cargo build --release --target-dir=$(RUST_TARGET_DIR) -Z unstable-options --artifact-dir=$(RUST_OUT_DIR)
	@echo "CARGO (release) => ratls_kms"

clean:
	@cargo clean --target-dir=$(RUST_TARGET_DIR)
	@-$(RM) -f $(TARGET_BINARY)
