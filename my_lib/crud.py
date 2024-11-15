import sqlite3
from my_lib.util import log_tests


def get_table_columns(database_name: str, table_name: str):
    conn = sqlite3.connect("data/" + database_name)
    c = conn.cursor()
    to_execute = f"SELECT name FROM pragma_table_info('{table_name}')"
    log_tests("Executing query...")
    log_tests(to_execute, issql=True)
    columns = c.execute(to_execute).fetchall()
    return [
        (str(column).strip(")").strip("(").strip(",").strip("'")) for column in columns
    ]


def get_primary_key(cursor, table_name):
    return cursor.execute(
        f"SELECT name FROM pragma_table_info('{table_name}') where pk"
    ).fetchone()[0]


def read_data(database_name: str, table_name: str, data_id: int):
    conn = sqlite3.connect("data/" + database_name)
    c = conn.cursor()
    to_execute = (
        f"select * from {table_name} where {get_primary_key(c, table_name)} = {data_id}"
    )
    log_tests("Executing query...")
    log_tests(to_execute, issql=True)
    result = c.execute(to_execute).fetchall()
    c.close()
    conn.close()
    if result:
        return result
    else:
        return None


def read_all_data(database_name: str, table_name: str):
    conn = sqlite3.connect("data/" + database_name)
    c = conn.cursor()
    to_execute = f"select * from {table_name}"
    log_tests("Executing query...")
    log_tests(to_execute, issql=True)
    result = c.execute(to_execute).fetchall()
    c.close()
    conn.close()
    if result:
        return result
    else:
        return None


def save_data(database_name: str, table_name: str, row: list):
    conn = sqlite3.connect("data/" + database_name)
    c = conn.cursor()
    col_names = ", ".join(get_table_columns(database_name, table_name))
    data_values = "', '".join(row)
    to_execute = f"INSERT INTO {table_name} ({col_names}) VALUES ('{data_values}')"
    log_tests("Executing query...")
    log_tests(to_execute, issql=True)
    c.execute(to_execute)
    conn.commit()
    c.close()
    conn.close()
    return "Save Successful"


def delete_data(database_name: str, table_name: str, data_id: int):
    conn = sqlite3.connect("data/" + database_name)
    c = conn.cursor()
    to_execute = (
        f"delete from {table_name} where {get_primary_key(c, table_name)} = {data_id}"
    )
    log_tests("Executing query...")
    log_tests(to_execute, issql=True)
    c.execute(to_execute)
    conn.commit()
    c.close()
    conn.close()
    return "Delete Successful"


def update_data(
    database_name: str, table_name: str, things_to_update: dict, data_id: int
):
    conn = sqlite3.connect("data/" + database_name)
    c = conn.cursor()
    set_values = ", ".join(
        [(k + "='" + v + "'") for (k, v) in things_to_update.items()]
    )
    to_execute = f"UPDATE {table_name} SET {set_values} WHERE {get_primary_key(c, table_name)} = {data_id}"
    log_tests("Executing query...")
    log_tests(to_execute, issql=True)
    c.execute(to_execute)
    conn.commit()
    c.close()
    conn.close()
    return "Update Successful"
