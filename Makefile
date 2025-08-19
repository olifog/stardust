
.PHONY: all configure build test clean rebuild debug release sync-capnp

CMAKE ?= cmake
ARGS ?=
PRESET ?= dev

# fry to auto-detect Cap'n Proto CMake config dir on Debian/Ubuntu containers
# if not found (e.g., on macOS), this stays empty and is not passed to CMake
CapnProto_DIR ?= $(shell sh -c 'if command -v dpkg >/dev/null 2>&1; then dpkg -L libcapnp-dev 2>/dev/null | grep -i CapnProtoConfig.cmake | xargs dirname; fi')
CAPNP_DIR_FLAG := $(if $(CapnProto_DIR),-DCapnProto_DIR=$(CapnProto_DIR),)

BUILD_DIR := build/$(PRESET)

SRC_CAPNP := schemas/graph.capnp
PY_CAPNP := clients/python/src/stardust/schemas/graph.capnp

all: build

configure:
	$(CMAKE) --preset $(PRESET) $(CAPNP_DIR_FLAG)

build: configure
	$(CMAKE) --build --preset $(PRESET) -j

run: build
	./$(BUILD_DIR)/stardustd $(ARGS)

test: build
	./$(BUILD_DIR)/stardust_tests

clean:
	rm -rf build

rebuild: clean build

debug:
	$(MAKE) PRESET=dev build

release:
	$(MAKE) PRESET=release build

sync-capnp:
	mkdir -p $(dir $(PY_CAPNP))
	cp $(SRC_CAPNP) $(PY_CAPNP)
