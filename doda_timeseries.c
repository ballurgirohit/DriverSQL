/*
 * Copyright (c) 2025 Rohit Ballurgi
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software... [rest of standard MIT short-text]
 * ...
 * MIT License (see LICENSE file for full text)
 */

#include "doda_api.h"

#ifdef DRIVERSQL_TIMESERIES

void doda_tsdb_init(DodaTSDB *ts, DodaTable *t, const char *time_col) {
    ts->table = t; ts->time_col = time_col;
}

DodaStatus doda_tsdb_append_int3(DodaTSDB *ts, int id, int time, int value) {
    const void *vals[3]; vals[0] = &id; vals[1] = &time; vals[2] = &value; return doda_insert_row(ts->table, vals);
}

DodaStatus doda_tsdb_select_time_ge(const DodaTSDB *ts, int t0, doda_row_callback cb, void *user) {
    return doda_select_where_op(ts->table, ts->time_col, DodaOp_GTE, &t0, cb, user);
}
DodaStatus doda_tsdb_select_time_gt(const DodaTSDB *ts, int t0, doda_row_callback cb, void *user) {
    return doda_select_where_op(ts->table, ts->time_col, DodaOp_GT, &t0, cb, user);
}
DodaStatus doda_tsdb_select_time_lt(const DodaTSDB *ts, int t1, doda_row_callback cb, void *user) {
    return doda_select_where_op(ts->table, ts->time_col, DodaOp_LT, &t1, cb, user);
}

bool doda_tsdb_build_time_index(DodaTSDB *ts, DodaIndex *idx) { return doda_index_build(ts->table, idx, ts->time_col); }

DodaStatus doda_tsdb_delete_older_than(DodaTSDB *ts, int cutoff_time, size_t *deleted_out) {
    size_t del = 0; int col = doda_column_index(ts->table, ts->time_col); if (col < 0) { if (deleted_out) *deleted_out = 0; return DodaStatus_ERR_NOT_FOUND; }
    for (size_t r = 0; r < ts->table->count; ++r) {
        if (doda_is_deleted(ts->table, r)) continue;
        int v = ts->table->columns[col].data.int_data[r];
        if (v < cutoff_time) {
            ts->table->free_list[ts->table->free_top++] = (uint16_t)r;
            ts->table->deleted_bits[r/64] |= (1ULL << (r%64));
            del++;
        }
    }
    if (deleted_out) *deleted_out = del; return DodaStatus_OK;
}

#endif // DRIVERSQL_TIMESERIES
