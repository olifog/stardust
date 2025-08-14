
.PHONY: all configure build test clean rebuild debug release

BUILD_DIR ?= build
CONFIG ?= Release
CMAKE ?= cmake
ARGS ?=

all: build

configure:
	mkdir -p $(BUILD_DIR)
	$(CMAKE) -S . -B $(BUILD_DIR) -DCMAKE_BUILD_TYPE=$(CONFIG)

build: configure
	$(CMAKE) --build $(BUILD_DIR) -j

run: build
	./build/stardustd $(ARGS)

test: build
	./build/stardust_tests

clean:
	rm -rf $(BUILD_DIR)

rebuild: clean build

debug:
	$(MAKE) CONFIG=Debug build

release:
	$(MAKE) CONFIG=Release build


