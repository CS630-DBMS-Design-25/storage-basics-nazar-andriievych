cmake_minimum_required(VERSION 3.14)
project(storage_cli LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Add include directory
include_directories(${CMAKE_SOURCE_DIR}/include)

# Gather source files
file(GLOB SOURCES
        ${CMAKE_SOURCE_DIR}/src/*.cpp
)

# Gather test sources
file(GLOB TEST_SOURCES
        ${CMAKE_SOURCE_DIR}/tests/*.cpp
)

# Create a library for your main code that can be linked to tests
add_library(storage_lib ${SOURCES})


# GoogleTest setup
include(FetchContent)
FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/refs/heads/main.zip
        DOWNLOAD_EXTRACT_TIMESTAMP TRUE  # Add this to fix the warning
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

enable_testing()

# Add test executable
add_executable(storage_cli_tests ${TEST_SOURCES})

# Link tests with Google Test and your library
target_link_libraries(storage_cli_tests
        PRIVATE
        gtest_main
        gtest
        storage_lib
)

target_include_directories(storage_cli_tests PRIVATE ${CMAKE_SOURCE_DIR}/include)

include(GoogleTest)
gtest_discover_tests(storage_cli_tests)

# Add SQL executable for SQL integration
# SQL CLI executable
add_executable(sql_cli
    src/sql_cli.cpp
    src/sql_lexer.cpp
    src/sql_parser.cpp
    src/sql_ast.cpp
    src/sql_executor.cpp
    src/storage_layer.cpp
)
target_include_directories(sql_cli PRIVATE include)

# Storage CLI executable (original, non-SQL)
add_executable(storage_cli
    src/storage_cli.cpp
    src/storage_layer.cpp
)
target_include_directories(storage_cli PRIVATE include)

target_link_libraries(sql_cli PRIVATE
    # Add any required libraries here, e.g., for gtest or filesystem
)