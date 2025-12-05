cmake_minimum_required(VERSION 3.15)
project(DODA C)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

option(DRIVERSQL_FIRMWARE "Build firmware-only target (no tests)" ON)

# Core library
add_library(doda_core OBJECT
    doda_engine.c
    doda_engine.h
    doda_api.h
    doda_timeseries.c
)

target_include_directories(doda_core PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

if (CMAKE_C_COMPILER_ID MATCHES "Clang|AppleClang|GNU")
    target_compile_options(doda_core PRIVATE -Wall -Wextra -Wpedantic)
endif()

if (DRIVERSQL_FIRMWARE)
    target_compile_definitions(doda_core PRIVATE DRIVERSQL_NO_STDIO DRIVERSQL_NO_POINTER_COLUMN)
else()
    # Enable timeseries in core so symbols are compiled
    target_compile_definitions(doda_core PRIVATE DRIVERSQL_TIMESERIES)
    add_executable(doda
        tests.c
        $<TARGET_OBJECTS:doda_core>
    )
    target_include_directories(doda PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    target_compile_definitions(doda PRIVATE DRIVERSQL_TIMESERIES)
    if (CMAKE_C_COMPILER_ID MATCHES "Clang|AppleClang|GNU")
        target_compile_options(doda PRIVATE -Wall -Wextra -Wpedantic)
    endif()
endif()
