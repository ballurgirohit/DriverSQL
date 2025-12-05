/*
 * Copyright (c) 2025 Rohit Ballurgi
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software... [rest of standard MIT short-text]
 * ...
 * MIT License (see LICENSE file for full text)
 */
#pragma once
#include "doda_engine.h"

// Enable this module with -DDRIVERSQL_TIMESERIES
#ifdef DRIVERSQL_TIMESERIES

// Timeseries convenience API built on core without changing core logic
// Assumes a schema with a primary key 'id' (int) and a timestamp column 'time' (int)

typedef struct {
    Table *table;
    const char *time_col; // e.g., "time"
} TSDB;

// Initialize a timeseries DB over an existing Table
static inline void tsdb_init(TSDB *ts, Table *t, const char *time_col) {
    ts->table = t; ts->time_col = time_col;
}

// Append sample with monotonic time (optional check). Returns DSStatus.
static inline DSStatus tsdb_append_int3(TSDB *ts, int id, int time, int value) {
    // Schema assumed: columns {id, time, value}
    const void *vals[3]; vals[0] = &id; vals[1] = &time; vals[2] = &value; return insert_row(ts->table, vals);
}

// Range query on time using core select_where_op; user callback handles rows.
static inline DSStatus tsdb_select_time_ge(const TSDB *ts, int t0, row_callback cb, void *user) {
    return select_where_op(ts->table, ts->time_col, OP_GTE, &t0, cb, user);
}
static inline DSStatus tsdb_select_time_gt(const TSDB *ts, int t0, row_callback cb, void *user) {
    return select_where_op(ts->table, ts->time_col, OP_GT, &t0, cb, user);
}
static inline DSStatus tsdb_select_time_lt(const TSDB *ts, int t1, row_callback cb, void *user) {
    return select_where_op(ts->table, ts->time_col, OP_LT, &t1, cb, user);
}

// Build index on time column for efficient ranges
static inline bool tsdb_build_time_index(TSDB *ts, Index *idx) { return index_build(ts->table, idx, ts->time_col); }

// Delete samples older than cutoff time
static inline DSStatus tsdb_delete_older_than(TSDB *ts, int cutoff_time, size_t *deleted_out) {
    // Scan via select_where_op with OP_LT and delete within callback not supported; instead manual scan
    size_t del = 0; int col = column_index(ts->table, ts->time_col); if (col < 0) { if (deleted_out) *deleted_out = 0; return DS_ERR_NOT_FOUND; }
    for (size_t r = 0; r < ts->table->count; ++r) {
        if (is_deleted(ts->table, r)) continue;
        int v = ts->table->columns[col].data.int_data[r];
        if (v < cutoff_time) { ts->table->free_list[ts->table->free_top++] = (uint16_t)r; ts->table->deleted_bits[r/64] |= (1ULL << (r%64)); del++; }
    }
    if (deleted_out) *deleted_out = del; return DS_OK;
}

#endif // DRIVERSQL_TIMESERIES
