/*
 * Copyright (c) 2025 Rohit Ballurgi
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software... [rest of standard MIT short-text]
 * ...
 * MIT License (see LICENSE file for full text)
 */

#pragma once
#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>

#ifndef DRIVERSQL_MAX_ROWS
#define DRIVERSQL_MAX_ROWS 256
#endif
#ifndef DRIVERSQL_MAX_COLUMNS
#define DRIVERSQL_MAX_COLUMNS 16
#endif
#ifndef DRIVERSQL_MAX_NAME_LEN
#define DRIVERSQL_MAX_NAME_LEN 32
#endif
#ifndef DRIVERSQL_MAX_TEXT_LEN
#define DRIVERSQL_MAX_TEXT_LEN 64
#endif
#ifndef DRIVERSQL_HASH_SIZE
#define DRIVERSQL_HASH_SIZE 512
#endif

#define MAX_COLUMNS DRIVERSQL_MAX_COLUMNS
#define MAX_NAME_LEN DRIVERSQL_MAX_NAME_LEN
#define MAX_TEXT_LEN DRIVERSQL_MAX_TEXT_LEN
#define MAX_ROWS DRIVERSQL_MAX_ROWS
#define HASH_SIZE DRIVERSQL_HASH_SIZE

// Feature gates
// DRIVERSQL_NO_TEXT, DRIVERSQL_NO_FLOAT, DRIVERSQL_NO_DOUBLE, DRIVERSQL_NO_POINTER_COLUMN, DRIVERSQL_NO_STDIO

typedef enum {
    COL_INT = 0,
#ifndef DRIVERSQL_NO_TEXT
    COL_TEXT = 1,
#endif
    COL_BOOL = 2,
#ifndef DRIVERSQL_NO_FLOAT
    COL_FLOAT = 3,
#endif
#ifndef DRIVERSQL_NO_DOUBLE
    COL_DOUBLE = 4,
#endif
#ifndef DRIVERSQL_NO_POINTER_COLUMN
    COL_POINTER = 5
#endif
} ColumnType;

typedef struct Column {
    char name[MAX_NAME_LEN];
    ColumnType type;
    union {
        int int_data[MAX_ROWS];
#ifndef DRIVERSQL_NO_TEXT
        char text_data[MAX_ROWS][MAX_TEXT_LEN];
#endif
        uint8_t bool_data[MAX_ROWS];
#ifndef DRIVERSQL_NO_FLOAT
        float float_data[MAX_ROWS];
#endif
#ifndef DRIVERSQL_NO_DOUBLE
        double double_data[MAX_ROWS];
#endif
#ifndef DRIVERSQL_NO_POINTER_COLUMN
        void *ptr_data[MAX_ROWS];
#endif
    } data;
} Column;

typedef struct Table {
    char name[MAX_NAME_LEN];
    int column_count;
    Column columns[MAX_COLUMNS];
    size_t capacity;
    size_t count;
    uint64_t deleted_bits[(MAX_ROWS + 63) / 64];
    uint16_t free_list[MAX_ROWS];
    size_t free_top;
    uint16_t pk_hash[HASH_SIZE];
} Table;

typedef struct {
    int column_id;
    uint16_t rows[MAX_ROWS];
    size_t size;
    bool active;
} Index;

typedef void (*row_callback)(const struct Table *t, size_t row, void *user);

typedef enum { OP_EQ = 0, OP_GT, OP_LT, OP_GTE } Op;

typedef enum {
    DS_OK = 0,
    DS_ERR_FULL,
    DS_ERR_UNSUPPORTED,
    DS_ERR_NOT_FOUND,
    DS_ERR_INVALID
} DSStatus;

// Core API
void init_table(Table *t, const char *name, int column_count, const char **col_names, const ColumnType *col_types);
DSStatus insert_row_int_text_int(Table *t, int v0, const char *v1, int v2);
DSStatus insert_row(Table *t, const void *values[]);
DSStatus select_where_eq(const Table *t, const char *col_name, const void *eq_value, row_callback cb, void *user);
DSStatus select_where_op(const Table *t, const char *col_name, Op op, const void *value, row_callback cb, void *user);
DSStatus delete_where_eq(Table *t, const char *col_name, const void *eq_value, size_t *deleted_out);
void free_table(Table *t);

int column_index(const Table *t, const char *col_name);
bool is_deleted(const Table *t, size_t row);
#ifndef DRIVERSQL_NO_STDIO
void print_row(const Table *t, size_t r);
#endif

bool index_build(Table *t, Index *idx, const char *col_name);
void index_drop(Index *idx);
typedef enum { IDX_OK = 0, IDX_UNSUPPORTED, IDX_EMPTY } IndexStatus;
IndexStatus index_select_eq(const Table *t, const Index *idx, const void *value, row_callback cb, void *user);
IndexStatus index_select_op(const Table *t, const Index *idx, Op op, const void *value, row_callback cb, void *user);

// DODA renamed types (backward-compatible typedefs)
typedef ColumnType DodaColumnType;
typedef Table DodaTable;
typedef Index DodaIndex;

typedef void (*doda_row_callback)(const DodaTable *t, size_t row, void *user);

typedef enum { DodaOp_EQ = OP_EQ, DodaOp_GT = OP_GT, DodaOp_LT = OP_LT, DodaOp_GTE = OP_GTE } DodaOp;

typedef enum {
    DodaStatus_OK = DS_OK,
    DodaStatus_ERR_FULL = DS_ERR_FULL,
    DodaStatus_ERR_UNSUPPORTED = DS_ERR_UNSUPPORTED,
    DodaStatus_ERR_NOT_FOUND = DS_ERR_NOT_FOUND,
    DodaStatus_ERR_INVALID = DS_ERR_INVALID
} DodaStatus;

// DODA API aliases
static inline void doda_init_table(DodaTable *t, const char *name, int column_count, const char **col_names, const DodaColumnType *col_types) { init_table((Table*)t, name, column_count, col_names, (const ColumnType*)col_types); }
static inline DodaStatus doda_insert_row_int_text_int(DodaTable *t, int v0, const char *v1, int v2) { return (DodaStatus)insert_row_int_text_int((Table*)t, v0, v1, v2); }
static inline DodaStatus doda_insert_row(DodaTable *t, const void *values[]) { return (DodaStatus)insert_row((Table*)t, values); }
static inline DodaStatus doda_select_where_eq(const DodaTable *t, const char *col_name, const void *eq_value, doda_row_callback cb, void *user) { return (DodaStatus)select_where_eq((const Table*)t, col_name, eq_value, (row_callback)cb, user); }
static inline DodaStatus doda_select_where_op(const DodaTable *t, const char *col_name, DodaOp op, const void *value, doda_row_callback cb, void *user) { return (DodaStatus)select_where_op((const Table*)t, col_name, (Op)op, value, (row_callback)cb, user); }
static inline DodaStatus doda_delete_where_eq(DodaTable *t, const char *col_name, const void *eq_value, size_t *deleted_out) { return (DodaStatus)delete_where_eq((Table*)t, col_name, eq_value, deleted_out); }
static inline void doda_free_table(DodaTable *t) { free_table((Table*)t); }

static inline int doda_column_index(const DodaTable *t, const char *col_name) { return column_index((const Table*)t, col_name); }
static inline bool doda_is_deleted(const DodaTable *t, size_t row) { return is_deleted((const Table*)t, row); }
#ifndef DRIVERSQL_NO_STDIO
static inline void doda_print_row(const DodaTable *t, size_t r) { print_row((const Table*)t, r); }
#endif

static inline bool doda_index_build(DodaTable *t, DodaIndex *idx, const char *col_name) { return index_build((Table*)t, (Index*)idx, col_name); }
static inline void doda_index_drop(DodaIndex *idx) { index_drop((Index*)idx); }
typedef enum { DodaIndexStatus_OK = IDX_OK, DodaIndexStatus_UNSUPPORTED = IDX_UNSUPPORTED, DodaIndexStatus_EMPTY = IDX_EMPTY } DodaIndexStatus;
static inline DodaIndexStatus doda_index_select_eq(const DodaTable *t, const DodaIndex *idx, const void *value, doda_row_callback cb, void *user) { return (DodaIndexStatus)index_select_eq((const Table*)t, (const Index*)idx, value, (row_callback)cb, user); }
static inline DodaIndexStatus doda_index_select_op(const DodaTable *t, const DodaIndex *idx, DodaOp op, const void *value, doda_row_callback cb, void *user) { return (DodaIndexStatus)index_select_op((const Table*)t, (const Index*)idx, (Op)op, value, (row_callback)cb, user); }
