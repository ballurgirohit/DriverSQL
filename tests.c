#include <stdio.h>
#include "driver_sql.h"

static void print_cb(const Table *tab, size_t row, void *user) {
    (void)user; print_row(tab, row);
}

void test_db(void) {
    const char *cols[] = {"id", "name", "age"};
    ColumnType types[] = {COL_INT, COL_TEXT, COL_INT};
    Table *t = create_table("people", 3, cols, types);

    // Insert rows using typed helper [INT, TEXT, INT]
    insert_row_int_text_int(t, 1, "Alice", 30);
    insert_row_int_text_int(t, 2, "Bob", 22);
    insert_row_int_text_int(t, 3, "Cara", 22);
    insert_row_int_text_int(t, 4, "Dave", 30);

    printf("All rows before delete:\n");
    for (size_t r = 0; r < t->count; ++r) if (!is_deleted(t, r)) print_row(t, r);

    // Select examples
    printf("\nSelect age == 22:\n");
    int target_age = 22; select_where_eq(t, "age", &target_age, print_cb, NULL);

    printf("\nSelect age > 25:\n");
    int gt_age = 25; select_where_op(t, "age", OP_GT, &gt_age, print_cb, NULL);

    printf("\nSelect age < 30:\n");
    int lt_age = 30; select_where_op(t, "age", OP_LT, &lt_age, print_cb, NULL);

    printf("\nSelect age >= 30:\n");
    int gte_age = 30; select_where_op(t, "age", OP_GTE, &gte_age, print_cb, NULL);

    // Delete name == "Dave"
    const char *target_name = "Dave";
    size_t del = delete_where_eq(t, "name", target_name);
    printf("\nDeleted %zu rows where name == 'Dave'\n", del);

    printf("\nAll rows after delete:\n");
    for (size_t r = 0; r < t->count; ++r) if (!is_deleted(t, r)) print_row(t, r);

    // Persistence demo: save and reload
    printf("\nPersist people to people.db and reload:\n");
    Table *tp = create_table("people", 3, cols, types);
    insert_row_int_text_int(tp, 5, "Zoe", 27);
    insert_row_int_text_int(tp, 6, "Max", 31);
    save_table_to_file(tp, "people.db");
    free_table(tp);
    Table *t_loaded = load_table_from_file("people.db");
    if (t_loaded) {
        for (size_t r = 0; r < t_loaded->count; ++r) if (!is_deleted(t_loaded, r)) print_row(t_loaded, r);
        free_table(t_loaded);
    }

    free_table(t);
}

static void test_db_many(void) {
    const char *cols[] = {"id", "name", "age", "city", "salary", "dept"};
    ColumnType types[] = {COL_INT, COL_TEXT, COL_INT, COL_TEXT, COL_INT, COL_TEXT};
    Table *t = create_table("employees", 6, cols, types);

    // Insert rows using generic values array
    const void *vals[6];
    int id, age, salary; const char *name, *city, *dept;

    id=1001; name="Alice"; age=30; city="NY"; salary=120000; dept="Eng";
    vals[0]=&id; vals[1]=name; vals[2]=&age; vals[3]=city; vals[4]=&salary; vals[5]=dept; insert_row(t, vals);
    id=1002; name="Bob"; age=26; city="SF"; salary=105000; dept="Eng";
    vals[0]=&id; vals[1]=name; vals[2]=&age; vals[3]=city; vals[4]=&salary; vals[5]=dept; insert_row(t, vals);
    id=1003; name="Cara"; age=40; city="LA"; salary=98000; dept="HR";
    vals[0]=&id; vals[1]=name; vals[2]=&age; vals[3]=city; vals[4]=&salary; vals[5]=dept; insert_row(t, vals);
    id=1004; name="Dan"; age=33; city="NY"; salary=135000; dept="Sales";
    vals[0]=&id; vals[1]=name; vals[2]=&age; vals[3]=city; vals[4]=&salary; vals[5]=dept; insert_row(t, vals);
    id=1005; name="Eve"; age=29; city="SF"; salary=150000; dept="Eng";
    vals[0]=&id; vals[1]=name; vals[2]=&age; vals[3]=city; vals[4]=&salary; vals[5]=dept; insert_row(t, vals);

    printf("\n[employees] All rows:\n");
    for (size_t r = 0; r < t->count; ++r) if (!is_deleted(t, r)) print_row(t, r);

    printf("\nSelect dept == 'Eng':\n");
    const char *target_dept = "Eng"; select_where_eq(t, "dept", target_dept, print_cb, NULL);

    printf("\nSelect salary == 135000:\n");
    int target_salary = 135000; select_where_eq(t, "salary", &target_salary, print_cb, NULL);

    printf("\nSelect salary >= 120000:\n");
    int gte_salary = 120000; select_where_op(t, "salary", OP_GTE, &gte_salary, print_cb, NULL);

    printf("\nSelect age < 30:\n");
    int lt_age2 = 30; select_where_op(t, "age", OP_LT, &lt_age2, print_cb, NULL);

    printf("\nDelete city == 'SF'\n");
    const char *target_city = "SF";
    size_t del = delete_where_eq(t, "city", target_city);
    printf("Deleted %zu rows where city == 'SF'\n", del);

    printf("\n[employees] Remaining rows:\n");
    for (size_t r = 0; r < t->count; ++r) if (!is_deleted(t, r)) print_row(t, r);

    // Persistence demo: save and reload employees
    printf("\nPersist employees to employees.db and reload:\n");
    const char *cols2[] = {"id", "name", "age", "city", "salary", "dept"};
    ColumnType types2[] = {COL_INT, COL_TEXT, COL_INT, COL_TEXT, COL_INT, COL_TEXT};
    Table *te = create_table("employees", 6, cols2, types2);
    id=2001; name="Neil"; age=45; city="NY"; salary=160000; dept="Exec";
    vals[0]=&id; vals[1]=name; vals[2]=&age; vals[3]=city; vals[4]=&salary; vals[5]=dept; insert_row(te, vals);
    id=2002; name="Ola"; age=34; city="SF"; salary=110000; dept="Eng";
    vals[0]=&id; vals[1]=name; vals[2]=&age; vals[3]=city; vals[4]=&salary; vals[5]=dept; insert_row(te, vals);
    save_table_to_file(te, "employees.db");
    free_table(te);
    Table *te_loaded = load_table_from_file("employees.db");
    if (te_loaded) {
        for (size_t r = 0; r < te_loaded->count; ++r) if (!is_deleted(te_loaded, r)) print_row(te_loaded, r);
        free_table(te_loaded);
    }

    free_table(t);
}

// Read-only test: load persisted DBs and perform queries
static void test_db_read_only(void) {
    printf("\n[read-only] Load people.db and query:\n");
    Table *t_people = load_table_from_file("people.db");
    if (t_people) {
        printf("All loaded people rows:\n");
        for (size_t r = 0; r < t_people->count; ++r) if (!is_deleted(t_people, r)) print_row(t_people, r);
        // Queries
        int age_gt = 25; select_where_op(t_people, "age", OP_GT, &age_gt, print_cb, NULL);
        const char *name_eq = "Zoe"; select_where_eq(t_people, "name", name_eq, print_cb, NULL);
        free_table(t_people);
    } else {
        printf("people.db not found or failed to load.\n");
    }

    printf("\n[read-only] Load employees.db and query:\n");
    Table *t_emp = load_table_from_file("employees.db");
    if (t_emp) {
        printf("All loaded employee rows:\n");
        for (size_t r = 0; r < t_emp->count; ++r) if (!is_deleted(t_emp, r)) print_row(t_emp, r);
        // Queries
        const char *dept_eq = "Eng"; select_where_eq(t_emp, "dept", dept_eq, print_cb, NULL);
        int salary_gte = 120000; select_where_op(t_emp, "salary", OP_GTE, &salary_gte, print_cb, NULL);
        free_table(t_emp);
    } else {
        printf("employees.db not found or failed to load.\n");
    }
}

int main(void) {
    test_db();
    test_db_many();
    test_db_read_only();
    return 0;
}
