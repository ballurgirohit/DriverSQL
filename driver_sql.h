/*
 * Copyright (c) 2025 [Rohit Ballurgi]
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

#define MAX_COLUMNS 16
#define MAX_NAME_LEN 32
#define MAX_TEXT_LEN 64
#define MAX_ROWS 256
#define HASH_SIZE 512

typedef enum { COL_INT = 0, COL_TEXT = 1, COL_BOOL = 2, COL_FLOAT = 3, COL_DOUBLE = 4, COL_POINTER = 5 } ColumnType;

typedef struct Column {
    char name[MAX_NAME_LEN];
    ColumnType type;
    union {
        int int_data[MAX_ROWS];
        char text_data[MAX_ROWS][MAX_TEXT_LEN];
        uint8_t bool_data[MAX_ROWS];
        float float_data[MAX_ROWS];
        double double_data[MAX_ROWS];
        void *ptr_data[MAX_ROWS];
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

Table *create_table(const char *name, int column_count, const char **col_names, const ColumnType *col_types);
bool insert_row_int_text_int(Table *t, int v0, const char *v1, int v2);
bool insert_row(Table *t, const void *values[]);
void select_where_eq(const Table *t, const char *col_name, const void *eq_value, row_callback cb, void *user);
void select_where_op(const Table *t, const char *col_name, Op op, const void *value, row_callback cb, void *user);
size_t delete_where_eq(Table *t, const char *col_name, const void *eq_value);
void free_table(Table *t);

bool save_table_to_file(const Table *t, const char *path);
Table *load_table_from_file(const char *path);

int column_index(const Table *t, const char *col_name);
bool is_deleted(const Table *t, size_t row);
void print_row(const Table *t, size_t r);

bool index_build(Table *t, Index *idx, const char *col_name);
void index_drop(Index *idx);
typedef enum { IDX_OK = 0, IDX_UNSUPPORTED, IDX_EMPTY } IndexStatus;
IndexStatus index_select_eq(const Table *t, const Index *idx, const void *value, row_callback cb, void *user);
IndexStatus index_select_op(const Table *t, const Index *idx, Op op, const void *value, row_callback cb, void *user);
