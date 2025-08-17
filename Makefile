
.PHONY: all configure build test clean rebuild debug release

BUILD_DIR ?= build
CONFIG ?= Release
CMAKE ?= cmake
ARGS ?=

# fry to auto-detect Cap'n Proto CMake config dir on Debian/Ubuntu containers
# if not found (e.g., on macOS), this stays empty and is not passed to CMake
CapnProto_DIR ?= $(shell sh -c 'if command -v dpkg >/dev/null 2>&1; then dpkg -L libcapnp-dev 2>/dev/null | grep -i CapnProtoConfig.cmake | xargs dirname; fi')
CAPNP_DIR_FLAG := $(if $(CapnProto_DIR),-DCapnProto_DIR=$(CapnProto_DIR),)

all: build

configure:
	mkdir -p $(BUILD_DIR)
	$(CMAKE) -S . -B $(BUILD_DIR) -DCMAKE_BUILD_TYPE=$(CONFIG) $(CAPNP_DIR_FLAG)

build: configure
	$(CMAKE) --build $(BUILD_DIR) -j

run: build
	./$(BUILD_DIR)/stardustd $(ARGS)

test: build
	./$(BUILD_DIR)/stardust_tests

clean:
	rm -rf $(BUILD_DIR)

rebuild: clean build

debug:
	$(MAKE) CONFIG=Debug build

release:
	$(MAKE) CONFIG=Release build


