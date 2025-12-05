# DODA — Data Ordered Dense Array
A small, deterministic timeseries data store for embedded/IoT firmware.

## Overview
- In-memory, fixed-size arrays; no heap or filesystem.
- Optimized for integer timestamped samples and simple range queries.
- Deterministic operations with bounded memory and timing.

## Key features
- Timeseries first: append samples with INT timestamps; range queries (>=, >, <).
- Primary-key hash on first INT column for O(1) equality lookups.
- Optional per-column sorted index for efficient range scans.
- Safe deletes with slot reuse via a free list.
- Compile-time feature gates to reduce footprint (disable text/float/double/pointers/stdio).

## Build and run
- Prerequisites: CMake 3.15+, Clang/AppleClang (macOS)
- Clean: rm -rf build
- Host/tests (timeseries):
  - cmake -S . -B build -DDRIVERSQL_FIRMWARE=OFF -DDRIVERSQL_TIMESERIES=ON
  - cmake --build build
  - ./build/doda
- Firmware-only library:
  - cmake -S . -B build -DDRIVERSQL_FIRMWARE=ON
  - cmake --build build

## Usage (host/tests)
- Define schema with INT time column; initialize and append; query ranges.
// ...existing code examples...

## Configuration (feature gates)
- DRIVERSQL_NO_STDIO, DRIVERSQL_NO_POINTER_COLUMN
- DRIVERSQL_NO_TEXT, DRIVERSQL_NO_FLOAT, DRIVERSQL_NO_DOUBLE
- DRIVERSQL_MAX_ROWS, DRIVERSQL_MAX_COLUMNS, DRIVERSQL_MAX_TEXT_LEN, DRIVERSQL_HASH_SIZE
- DRIVERSQL_TIMESERIES (enable timeseries helpers)

## Limits and timing
- Capacity: MAX_ROWS; Columns: MAX_COLUMNS.
- Insert: O(1) avg; PK eq: O(1); ranges: O(N) or O(log N + R) with index.
- Deleted slots reused via free_list; DS_ERR_FULL when no free slots.

## Concurrency and ISR safety
- Single-writer, non-reentrant; no internal locks.
- Do not mutate in ISRs; reads only when writers excluded.

## Production checklist
- Schema validation vs feature gates; strict status codes.
- Replace stdio with logging hooks for firmware.
- Static analysis (MISRA/CERT), unit/property tests, CI.

## License
MIT License. See LICENSE.
