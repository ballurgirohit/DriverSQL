/*
 * Copyright (c) 2025 [Rohit Ballurgi]
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software... [rest of standard MIT short-text]
 * ...
 * MIT License (see LICENSE file for full text)
 */

#include "driver_sql.h"
#include <string.h>
#include <stdio.h>

static inline void set_deleted_bit(Table *t, size_t row, bool del) {
    size_t block = row / 64, bit = row % 64;
    uint64_t mask = 1ULL << bit;
    if (del) t->deleted_bits[block] |= mask; else t->deleted_bits[block] &= ~mask;
}

bool is_deleted(const Table *t, size_t row) {
    size_t block = row / 64, bit = row % 64;
    return (t->deleted_bits[block] >> bit) & 1ULL;
}

static inline uint32_t hash32(uint32_t x) {
    x ^= x >> 16; x *= 0x7feb352d; x ^= x >> 15; x *= 0x846ca68b; x ^= x >> 16; return x;
}

static void pk_hash_clear(Table *t) { memset(t->pk_hash, 0, sizeof(t->pk_hash)); }

static bool pk_hash_insert(Table *t, int key, uint16_t row) {
    uint32_t h = hash32((uint32_t)key);
    for (uint32_t i = 0; i < HASH_SIZE; ++i) {
        uint32_t idx = (h + i) & (HASH_SIZE - 1);
        uint16_t slot = t->pk_hash[idx];
        if (slot == 0) { t->pk_hash[idx] = (uint16_t)(row + 1); return true; }
        uint16_t srow = (uint16_t)(slot - 1);
        if (!is_deleted(t, srow) && t->columns[0].data.int_data[srow] == key) return false;
    }
    return false;
}

static int pk_hash_find(const Table *t, int key) {
    uint32_t h = hash32((uint32_t)key);
    for (uint32_t i = 0; i < HASH_SIZE; ++i) {
        uint32_t idx = (h + i) & (HASH_SIZE - 1);
        uint16_t slot = t->pk_hash[idx];
        if (slot == 0) return -1;
        uint16_t row = (uint16_t)(slot - 1);
        if (!is_deleted(t, row) && t->columns[0].data.int_data[row] == key) return (int)row;
    }
    return -1;
}

static void pk_hash_remove(Table *t, int key) {
    uint32_t h = hash32((uint32_t)key);
    for (uint32_t i = 0; i < HASH_SIZE; ++i) {
        uint32_t idx = (h + i) & (HASH_SIZE - 1);
        uint16_t slot = t->pk_hash[idx];
        if (slot == 0) return;
        uint16_t row = (uint16_t)(slot - 1);
        if (!is_deleted(t, row) && t->columns[0].data.int_data[row] == key) { t->pk_hash[idx] = 0; return; }
    }
}

Table *create_table(const char *name, int column_count, const char **col_names, const ColumnType *col_types) {
    static Table table_storage;
    Table *t = &table_storage;
    memset(t, 0, sizeof(*t));
    strncpy(t->name, name, MAX_NAME_LEN - 1);
    t->column_count = column_count;
    t->capacity = MAX_ROWS;
    for (int i = 0; i < column_count; ++i) {
        strncpy(t->columns[i].name, col_names[i], MAX_NAME_LEN - 1);
        t->columns[i].type = col_types[i];
    }
    pk_hash_clear(t);
    return t;
}

int column_index(const Table *t, const char *col_name) {
    for (int i = 0; i < t->column_count; ++i) if (strncmp(t->columns[i].name, col_name, MAX_NAME_LEN) == 0) return i;
    return -1;
}

bool insert_row(Table *t, const void *values[]) {
    if (!t || t->count >= t->capacity) {
        if (t && t->free_top > 0) {
            size_t row = t->free_list[--t->free_top];
            for (int i = 0; i < t->column_count; ++i) {
                Column *c = &t->columns[i];
                switch (c->type) {
                    case COL_INT:    c->data.int_data[row] = *(const int *)values[i]; break;
                    case COL_TEXT:   { const char *s = (const char *)values[i]; strncpy(c->data.text_data[row], s ? s : "", MAX_TEXT_LEN - 1); c->data.text_data[row][MAX_TEXT_LEN - 1] = '\0'; break; }
                    case COL_BOOL:   c->data.bool_data[row] = values[i] ? (uint8_t)(*(const int *)values[i] != 0) : 0; break;
                    case COL_FLOAT:  c->data.float_data[row] = *(const float *)values[i]; break;
                    case COL_DOUBLE: c->data.double_data[row] = *(const double *)values[i]; break;
                    case COL_POINTER:c->data.ptr_data[row] = (void *)values[i]; break;
                }
            }
            int pk = t->columns[0].data.int_data[row];
            if (!pk_hash_insert(t, pk, (uint16_t)row)) return false;
            set_deleted_bit(t, row, false);
            return true;
        }
        return false;
    }
    size_t row = t->count++;
    for (int i = 0; i < t->column_count; ++i) {
        Column *c = &t->columns[i];
        switch (c->type) {
            case COL_INT:    c->data.int_data[row] = *(const int *)values[i]; break;
            case COL_TEXT:   { const char *s = (const char *)values[i]; strncpy(c->data.text_data[row], s ? s : "", MAX_TEXT_LEN - 1); c->data.text_data[row][MAX_TEXT_LEN - 1] = '\0'; break; }
            case COL_BOOL:   c->data.bool_data[row] = values[i] ? (uint8_t)(*(const int *)values[i] != 0) : 0; break;
            case COL_FLOAT:  c->data.float_data[row] = *(const float *)values[i]; break;
            case COL_DOUBLE: c->data.double_data[row] = *(const double *)values[i]; break;
            case COL_POINTER:c->data.ptr_data[row] = (void *)values[i]; break;
        }
    }
    set_deleted_bit(t, row, false);
    int pk = t->columns[0].data.int_data[row];
    if (!pk_hash_insert(t, pk, (uint16_t)row)) return false;
    return true;
}

bool insert_row_int_text_int(Table *t, int v0, const char *v1, int v2) {
    const void *vals[3]; vals[0] = &v0; vals[1] = v1; vals[2] = &v2; return insert_row(t, vals);
}

void select_where_eq(const Table *t, const char *col_name, const void *eq_value, row_callback cb, void *user) {
    int idx = column_index(t, col_name); if (idx < 0) return; const Column *c = &t->columns[idx];
    if (idx == 0 && c->type == COL_INT) { int key = *(const int *)eq_value; int row = pk_hash_find(t, key); if (row >= 0) cb(t, (size_t)row, user); return; }
    switch (c->type) {
        case COL_INT: {
            int key = *(const int *)eq_value;
            for (size_t r = 0; r < t->count; ++r) if (!is_deleted(t, r) && c->data.int_data[r] == key) cb(t, r, user);
            break;
        }
        case COL_TEXT: {
            const char *key = (const char *)eq_value;
            for (size_t r = 0; r < t->count; ++r) if (!is_deleted(t, r) && strncmp(t->columns[idx].data.text_data[r], key, MAX_TEXT_LEN) == 0) cb(t, r, user);
            break;
        }
        case COL_BOOL: {
            uint8_t key = (uint8_t)(*(const int *)eq_value != 0);
            for (size_t r = 0; r < t->count; ++r) if (!is_deleted(t, r) && t->columns[idx].data.bool_data[r] == key) cb(t, r, user);
            break;
        }
        case COL_FLOAT: {
            float key = *(const float *)eq_value;
            for (size_t r = 0; r < t->count; ++r) if (!is_deleted(t, r) && t->columns[idx].data.float_data[r] == key) cb(t, r, user);
            break;
        }
        case COL_DOUBLE: {
            double key = *(const double *)eq_value;
            for (size_t r = 0; r < t->count; ++r) if (!is_deleted(t, r) && t->columns[idx].data.double_data[r] == key) cb(t, r, user);
            break;
        }
        case COL_POINTER: {
            const void *key = eq_value;
            for (size_t r = 0; r < t->count; ++r) if (!is_deleted(t, r) && t->columns[idx].data.ptr_data[r] == key) cb(t, r, user);
            break;
        }
    }
}

void select_where_op(const Table *t, const char *col_name, Op op, const void *value, row_callback cb, void *user) {
    int idx = column_index(t, col_name); if (idx < 0) return; const Column *c = &t->columns[idx];
    if (c->type == COL_INT) {
        int key = *(const int *)value;
        for (size_t r = 0; r < t->count; ++r) {
            if (is_deleted(t, r)) continue; int v = c->data.int_data[r]; bool m = false;
            switch (op) { case OP_EQ: m = (v == key); break; case OP_GT: m = (v > key); break; case OP_LT: m = (v < key); break; case OP_GTE: m = (v >= key); break; }
            if (m) cb(t, r, user);
        }
    } else if (c->type == COL_FLOAT) {
        float key = *(const float *)value;
        for (size_t r = 0; r < t->count; ++r) {
            if (is_deleted(t, r)) continue; float v = t->columns[idx].data.float_data[r]; bool m = false;
            switch (op) { case OP_EQ: m = (v == key); break; case OP_GT: m = (v > key); break; case OP_LT: m = (v < key); break; case OP_GTE: m = (v >= key); break; }
            if (m) cb(t, r, user);
        }
    } else if (c->type == COL_DOUBLE) {
        double key = *(const double *)value;
        for (size_t r = 0; r < t->count; ++r) {
            if (is_deleted(t, r)) continue; double v = t->columns[idx].data.double_data[r]; bool m = false;
            switch (op) { case OP_EQ: m = (v == key); break; case OP_GT: m = (v > key); break; case OP_LT: m = (v < key); break; case OP_GTE: m = (v >= key); break; }
            if (m) cb(t, r, user);
        }
    } else {
        if (op == OP_EQ) select_where_eq(t, col_name, value, cb, user);
    }
}

size_t delete_where_eq(Table *t, const char *col_name, const void *eq_value) {
    int idx = column_index(t, col_name); if (idx < 0) return 0; Column *c = &t->columns[idx]; size_t del = 0;
    if (c->type == COL_INT) {
        int key = *(const int *)eq_value;
        if (idx == 0) {
            int row = pk_hash_find(t, key);
            if (row >= 0 && !is_deleted(t, (size_t)row)) { set_deleted_bit(t, (size_t)row, true); pk_hash_remove(t, key); t->free_list[t->free_top++] = (uint16_t)row; del = 1; }
            return del;
        }
        for (size_t r = 0; r < t->count; ++r) {
            if (is_deleted(t, r)) continue; if (c->data.int_data[r] == key) { set_deleted_bit(t, r, true); t->free_list[t->free_top++] = (uint16_t)r; del++; }
        }
    } else {
        const char *key = (const char *)eq_value;
        for (size_t r = 0; r < t->count; ++r) {
            if (is_deleted(t, r)) continue; if (strncmp(c->data.text_data[r], key, MAX_TEXT_LEN) == 0) { set_deleted_bit(t, r, true); t->free_list[t->free_top++] = (uint16_t)r; del++; }
        }
    }
    return del;
}

void print_row(const Table *t, size_t r) {
    printf("Row %zu: ", r);
    for (int i = 0; i < t->column_count; ++i) {
        const Column *c = &t->columns[i];
        printf("%s=", c->name);
        if (c->type == COL_INT) printf("%d", c->data.int_data[r]);
        else if (c->type == COL_TEXT) printf("%s", c->data.text_data[r]);
        else if (c->type == COL_BOOL) printf("%s", t->columns[i].data.bool_data[r] ? "true" : "false");
        else if (c->type == COL_FLOAT) printf("%g", t->columns[i].data.float_data[r]);
        else if (c->type == COL_DOUBLE) printf("%g", t->columns[i].data.double_data[r]);
        else if (c->type == COL_POINTER) printf("%p", t->columns[i].data.ptr_data[r]);
        if (i + 1 < t->column_count) printf(", ");
    }
    printf("\n");
}

void free_table(Table *t) { (void)t; }

static void sort_rows_by_int(const Table *t, int col, uint16_t *rows, size_t n) {
    for (size_t i = 1; i < n; ++i) {
        uint16_t key = rows[i]; int vkey = t->columns[col].data.int_data[key]; size_t j = i;
        while (j > 0) {
            uint16_t rprev = rows[j-1]; int vprev = t->columns[col].data.int_data[rprev];
            if (vprev <= vkey) break; rows[j] = rows[j-1]; j--;
        }
        rows[j] = key;
    }
}
static void sort_rows_by_float(const Table *t, int col, uint16_t *rows, size_t n) {
    for (size_t i = 1; i < n; ++i) {
        uint16_t key = rows[i]; float vkey = t->columns[col].data.float_data[key]; size_t j = i;
        while (j > 0) {
            uint16_t rprev = rows[j-1]; float vprev = t->columns[col].data.float_data[rprev];
            if (vprev <= vkey) break; rows[j] = rows[j-1]; j--;
        }
        rows[j] = key;
    }
}
static void sort_rows_by_double(const Table *t, int col, uint16_t *rows, size_t n) {
    for (size_t i = 1; i < n; ++i) {
        uint16_t key = rows[i]; double vkey = t->columns[col].data.double_data[key]; size_t j = i;
        while (j > 0) {
            uint16_t rprev = rows[j-1]; double vprev = t->columns[col].data.double_data[rprev];
            if (vprev <= vkey) break; rows[j] = rows[j-1]; j--;
        }
        rows[j] = key;
    }
}
static void sort_rows_by_text(const Table *t, int col, uint16_t *rows, size_t n) {
    for (size_t i = 1; i < n; ++i) {
        uint16_t key = rows[i]; const char *vkey = t->columns[col].data.text_data[key]; size_t j = i;
        while (j > 0) {
            uint16_t rprev = rows[j-1]; const char *vprev = t->columns[col].data.text_data[rprev];
            if (strncmp(vprev, vkey, MAX_TEXT_LEN) <= 0) break; rows[j] = rows[j-1]; j--;
        }
        rows[j] = key;
    }
}

bool index_build(Table *t, Index *idx, const char *col_name) {
    int col = column_index(t, col_name); if (col < 0) { idx->active = false; return false; }
    idx->column_id = col; idx->size = 0; idx->active = true;
    for (size_t r = 0; r < t->count; ++r) if (!is_deleted(t, r)) idx->rows[idx->size++] = (uint16_t)r;
    ColumnType ct = t->columns[col].type;
    if (idx->size == 0) return true;
    if (ct == COL_INT) sort_rows_by_int(t, col, idx->rows, idx->size);
    else if (ct == COL_FLOAT) sort_rows_by_float(t, col, idx->rows, idx->size);
    else if (ct == COL_DOUBLE) sort_rows_by_double(t, col, idx->rows, idx->size);
    else if (ct == COL_TEXT) sort_rows_by_text(t, col, idx->rows, idx->size);
    else { idx->active = false; return false; }
    return true;
}

void index_drop(Index *idx) { idx->active = false; idx->size = 0; idx->column_id = -1; }

static ssize_t idx_lower_bound_int(const Table *t, int col, const Index *idx, int key) {
    size_t lo = 0, hi = idx->size; while (lo < hi) { size_t mid = (lo + hi) >> 1; int v = t->columns[col].data.int_data[idx->rows[mid]]; if (v < key) lo = mid + 1; else hi = mid; } return (ssize_t)lo;
}
static ssize_t idx_lower_bound_float(const Table *t, int col, const Index *idx, float key) {
    size_t lo = 0, hi = idx->size; while (lo < hi) { size_t mid=(lo+hi)>>1; float v=t->columns[col].data.float_data[idx->rows[mid]]; if (v<key) lo=mid+1; else hi=mid; } return (ssize_t)lo;
}
static ssize_t idx_lower_bound_double(const Table *t, int col, const Index *idx, double key) {
    size_t lo = 0, hi = idx->size; while (lo < hi) { size_t mid=(lo+hi)>>1; double v=t->columns[col].data.double_data[idx->rows[mid]]; if (v<key) lo=mid+1; else hi=mid; } return (ssize_t)lo;
}
static ssize_t idx_lower_bound_text(const Table *t, int col, const Index *idx, const char *key) {
    size_t lo=0, hi=idx->size; while (lo<hi){ size_t mid=(lo+hi)>>1; const char *v=t->columns[col].data.text_data[idx->rows[mid]]; if (strncmp(v,key,MAX_TEXT_LEN)<0) lo=mid+1; else hi=mid;} return (ssize_t)lo;
}

IndexStatus index_select_eq(const Table *t, const Index *idx, const void *value, row_callback cb, void *user) {
    if (!idx || !idx->active) return IDX_EMPTY; int col = idx->column_id; ColumnType ct = t->columns[col].type;
    if (ct == COL_INT) {
        int key = *(const int *)value; ssize_t pos = idx_lower_bound_int(t, col, idx, key); if ((size_t)pos >= idx->size) return IDX_OK;
        for (size_t i = (size_t)pos; i < idx->size; ++i) { int v = t->columns[col].data.int_data[idx->rows[i]]; if (v != key) break; cb(t, idx->rows[i], user); }
        return IDX_OK;
    } else if (ct == COL_FLOAT) {
        float key = *(const float *)value; ssize_t pos = idx_lower_bound_float(t, col, idx, key); if ((size_t)pos >= idx->size) return IDX_OK;
        for (size_t i = (size_t)pos; i < idx->size; ++i) { float v = t->columns[col].data.float_data[idx->rows[i]]; if (v != key) break; cb(t, idx->rows[i], user); }
        return IDX_OK;
    } else if (ct == COL_DOUBLE) {
        double key = *(const double *)value; ssize_t pos = idx_lower_bound_double(t, col, idx, key); if ((size_t)pos >= idx->size) return IDX_OK;
        for (size_t i = (size_t)pos; i < idx->size; ++i) { double v = t->columns[col].data.double_data[idx->rows[i]]; if (v != key) break; cb(t, idx->rows[i], user); }
        return IDX_OK;
    } else if (ct == COL_TEXT) {
        const char *key = (const char *)value; ssize_t pos = idx_lower_bound_text(t, col, idx, key); if ((size_t)pos >= idx->size) return IDX_OK;
        for (size_t i = (size_t)pos; i < idx->size; ++i) { const char *v = t->columns[col].data.text_data[idx->rows[i]]; if (strncmp(v, key, MAX_TEXT_LEN) != 0) break; cb(t, idx->rows[i], user); }
        return IDX_OK;
    }
    return IDX_UNSUPPORTED;
}

IndexStatus index_select_op(const Table *t, const Index *idx, Op op, const void *value, row_callback cb, void *user) {
    if (!idx || !idx->active) return IDX_EMPTY; int col = idx->column_id; ColumnType ct = t->columns[col].type;
    if (ct == COL_INT) {
        int key = *(const int *)value; size_t start = (size_t)idx_lower_bound_int(t, col, idx, key);
        if (op == OP_EQ) { for (size_t i = start; i < idx->size; ++i) { int v=t->columns[col].data.int_data[idx->rows[i]]; if (v!=key) break; cb(t, idx->rows[i], user);} return IDX_OK; }
        if (op == OP_LT) { for (size_t i = 0; i < start; ++i) cb(t, idx->rows[i], user); return IDX_OK; }
        size_t s = (op == OP_GT) ? (start + (start < idx->size && t->columns[col].data.int_data[idx->rows[start]] == key)) : start;
        for (size_t i = s; i < idx->size; ++i) cb(t, idx->rows[i], user);
        return IDX_OK;
    } else if (ct == COL_FLOAT) {
        float key = *(const float *)value; size_t start = (size_t)idx_lower_bound_float(t, col, idx, key);
        if (op == OP_EQ) { for (size_t i = start; i < idx->size; ++i) { float v=t->columns[col].data.float_data[idx->rows[i]]; if (v!=key) break; cb(t, idx->rows[i], user);} return IDX_OK; }
        if (op == OP_LT) { for (size_t i = 0; i < start; ++i) cb(t, idx->rows[i], user); return IDX_OK; }
        size_t s = (op == OP_GT) ? (start + (start < idx->size && t->columns[col].data.float_data[idx->rows[start]] == key)) : start;
        for (size_t i = s; i < idx->size; ++i) cb(t, idx->rows[i], user);
        return IDX_OK;
    } else if (ct == COL_DOUBLE) {
        double key = *(const double *)value; size_t start = (size_t)idx_lower_bound_double(t, col, idx, key);
        if (op == OP_EQ) { for (size_t i = start; i < idx->size; ++i) { double v=t->columns[col].data.double_data[idx->rows[i]]; if (v!=key) break; cb(t, idx->rows[i], user);} return IDX_OK; }
        if (op == OP_LT) { for (size_t i = 0; i < start; ++i) cb(t, idx->rows[i], user); return IDX_OK; }
        size_t s = (op == OP_GT) ? (start + (start < idx->size && t->columns[col].data.double_data[idx->rows[start]] == key)) : start;
        for (size_t i = s; i < idx->size; ++i) cb(t, idx->rows[i], user);
        return IDX_OK;
    } else if (ct == COL_TEXT) {
        if (op != OP_EQ) return IDX_UNSUPPORTED; return index_select_eq(t, idx, value, cb, user);
    }
    return IDX_UNSUPPORTED;
}