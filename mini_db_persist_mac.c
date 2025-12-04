#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mini_db.h"

// Platform-specific persistence (macOS/Unix-like using stdio)
// Serialize Table to a binary file and load it back.
// Format:
// [header] name[MAX_NAME_LEN], column_count(int32), count(uint64)
// [columns] repeated: name[MAX_NAME_LEN], type(int32)
// [deleted bitmap] fixed size deleted_bits[]
// [data] for each column:
//   if INT: count * int
//   if TEXT: count * MAX_TEXT_LEN bytes

static bool write_exact(FILE *f, const void *buf, size_t n) { return fwrite(buf, 1, n, f) == n; }
static bool read_exact(FILE *f, void *buf, size_t n) { return fread(buf, 1, n, f) == n; }

bool save_table_to_file(const Table *t, const char *path) {
    FILE *f = fopen(path, "wb");
    if (!f) return false;
    // header
    char name[MAX_NAME_LEN]; memset(name, 0, sizeof(name)); strncpy(name, t->name, MAX_NAME_LEN - 1);
    int32_t colc = t->column_count;
    uint64_t cnt = t->count;
    if (!write_exact(f, name, sizeof(name))) goto fail;
    if (!write_exact(f, &colc, sizeof(colc))) goto fail;
    if (!write_exact(f, &cnt, sizeof(cnt))) goto fail;
    // columns meta
    for (int i = 0; i < t->column_count; ++i) {
        char cname[MAX_NAME_LEN]; memset(cname, 0, sizeof(cname)); strncpy(cname, t->columns[i].name, MAX_NAME_LEN - 1);
        int32_t ctype = (int32_t)t->columns[i].type;
        if (!write_exact(f, cname, sizeof(cname))) goto fail;
        if (!write_exact(f, &ctype, sizeof(ctype))) goto fail;
    }
    // deleted bitmap (fixed size)
    if (!write_exact(f, t->deleted_bits, sizeof(t->deleted_bits))) goto fail;
    // data
    for (int i = 0; i < t->column_count; ++i) {
        if (t->columns[i].type == COL_INT) {
            if (t->count) {
                if (!write_exact(f, t->columns[i].data.int_data, t->count * sizeof(int))) goto fail;
            }
        } else if (t->columns[i].type == COL_TEXT) {
            if (t->count) {
                if (!write_exact(f, t->columns[i].data.text_data, t->count * MAX_TEXT_LEN)) goto fail;
            }
        } else {
            // For other types, you can extend serialization as needed; currently skipped
        }
    }
    fclose(f);
    return true;
fail:
    fclose(f);
    return false;
}

Table *load_table_from_file(const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;

    char name[MAX_NAME_LEN];
    int32_t colc;
    uint64_t cnt;
    if (!read_exact(f, name, sizeof(name))) goto fail;
    if (!read_exact(f, &colc, sizeof(colc))) goto fail;
    if (!read_exact(f, &cnt, sizeof(cnt))) goto fail;

    // read column meta
    char col_names[MAX_COLUMNS][MAX_NAME_LEN];
    ColumnType types[MAX_COLUMNS];
    for (int i = 0; i < colc; ++i) {
        if (!read_exact(f, col_names[i], MAX_NAME_LEN)) goto fail;
        int32_t ctype;
        if (!read_exact(f, &ctype, sizeof(ctype))) goto fail;
        types[i] = (ColumnType)ctype;
    }

    const char *col_name_ptrs[MAX_COLUMNS];
    for (int i = 0; i < colc; ++i) col_name_ptrs[i] = col_names[i];

    Table *t = create_table(name, colc, col_name_ptrs, types);
    if (!t) goto fail;

    // deleted bitmap (fixed size)
    if (!read_exact(f, t->deleted_bits, sizeof(t->deleted_bits))) { free_table(t); goto fail; }

    // data
    t->count = (cnt > t->capacity) ? t->capacity : (size_t)cnt;
    for (int i = 0; i < t->column_count; ++i) {
        if (t->columns[i].type == COL_INT) {
            if (t->count) {
                if (!read_exact(f, t->columns[i].data.int_data, t->count * sizeof(int))) { free_table(t); goto fail; }
            }
        } else if (t->columns[i].type == COL_TEXT) {
            if (t->count) {
                if (!read_exact(f, t->columns[i].data.text_data, t->count * MAX_TEXT_LEN)) { free_table(t); goto fail; }
            }
        } else {
            // For other types, you can extend deserialization as needed; currently skipped
        }
    }

    fclose(f);
    return t;
fail:
    fclose(f);
    return NULL;
}
