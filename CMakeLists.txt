cmake_minimum_required(VERSION 3.14)
project(lsm_tree VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Compiler warnings
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(-Wall -Wextra -Wpedantic -Werror)
endif()

# Include directories
include_directories(${CMAKE_SOURCE_DIR})

# Header-only library (for now)
add_library(lsm_core INTERFACE)
target_include_directories(lsm_core INTERFACE ${CMAKE_SOURCE_DIR})

# Test executables
add_executable(memtable_test test/memtable_test.cpp)
target_link_libraries(memtable_test PRIVATE lsm_core pthread)

add_executable(wal_test test/wal_test.cpp)
target_link_libraries(wal_test PRIVATE lsm_core pthread)

# Enable testing
enable_testing()
add_test(NAME memtable_test COMMAND memtable_test)
add_test(NAME wal_test COMMAND wal_test)

# Optional: Build type defaults
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# Release optimizations
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -fsanitize=address,undefined")