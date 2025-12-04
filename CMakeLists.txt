cmake_minimum_required(VERSION 3.15)
project(DriverSQL C)

# Set C standard
set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

# Library target for DriverSQL core (object library to reuse)
add_library(driversql_core OBJECT
    driver_sql.c
    driver_sql.h
)

# Platform-specific persistence (macOS/Unix stdio)
add_library(driversql_persist OBJECT
    driver_sql_persist_mac.c
)

# Executable: driver_sql tests
add_executable(driver_sql
    tests.c
    $<TARGET_OBJECTS:driversql_core>
    $<TARGET_OBJECTS:driversql_persist>
)

# Enable warnings
if (CMAKE_C_COMPILER_ID MATCHES "Clang|AppleClang|GNU")
    target_compile_options(driver_sql PRIVATE -Wall -Wextra -Wpedantic)
endif()
