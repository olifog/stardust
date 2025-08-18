cmake_minimum_required(VERSION 3.30)
project(stardust C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

find_package(PkgConfig REQUIRED)
pkg_check_modules(LMDB REQUIRED IMPORTED_TARGET lmdb)

find_package(CapnProto REQUIRED)

set(SCHEMAS
  ${CMAKE_SOURCE_DIR}/schemas/graph.capnp
)

capnp_generate_cpp(SCHEMA_SRCS SCHEMA_HDRS ${SCHEMAS})

add_library(stardust STATIC
  src/env.cpp
  src/store.cpp
  ${SCHEMA_SRCS}
)

target_include_directories(stardust PUBLIC
  ${CMAKE_CURRENT_BINARY_DIR}
  ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(stardust PUBLIC
  CapnProto::capnp
  CapnProto::kj
  PkgConfig::LMDB
)

add_library(stardust_rpc STATIC
  src/server.cpp
)
target_link_libraries(stardust_rpc PUBLIC stardust CapnProto::capnp-rpc CapnProto::kj-async)
target_include_directories(stardust_rpc PUBLIC ${CMAKE_SOURCE_DIR}/src ${CMAKE_CURRENT_BINARY_DIR})

# ------------- HTTP server (mongoose) -------------

include(FetchContent)
FetchContent_Declare(
  mongoose
  URL https://github.com/cesanta/mongoose/archive/refs/tags/7.18.tar.gz
)
FetchContent_MakeAvailable(mongoose)

add_library(mongoose STATIC
  ${mongoose_SOURCE_DIR}/mongoose.c
)
target_include_directories(mongoose PUBLIC ${mongoose_SOURCE_DIR})

add_library(stardust_http STATIC
  src/http_server.cpp
)
target_link_libraries(stardust_http PUBLIC stardust mongoose)
target_include_directories(stardust_http PUBLIC ${CMAKE_SOURCE_DIR}/src)

add_executable(stardustd
  src/main.cpp
)
target_link_libraries(stardustd PRIVATE stardust_rpc stardust_http)

add_executable(stardust_client
  src/client_example.cpp
)
target_link_libraries(stardust_client PRIVATE stardust CapnProto::capnp-rpc CapnProto::kj-async)

# ------------- tests -------------

include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/52eb8108c5bdec04579160ae17225d66034bd723.zip
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

add_executable(
  stardust_tests
  tests/integration_rpc_test.cpp
)
target_link_libraries(
  stardust_tests
  PRIVATE
    GTest::gtest_main
    stardust
    stardust_rpc
)
