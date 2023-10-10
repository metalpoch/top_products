from os import path
import sqlite3 as sql

DEPARTMENTS_TABLE = """
CREATE TABLE IF NOT EXISTS walmart_departments (
    id INTEGER PRIMARY KEY,
    department  TEXT NOT NULL,
    super_department_number INTEGER NOT NULL,
    super_department TEXT NOT NULL,
    super_departments_id INTEGER NOT NULL,
    last_update TEXT NOT NULL
);"""

TOP_PRODUCTS_TABLE = """
CREATE TABLE IF NOT EXISTS top_products (
    id INTEGER PRIMARY KEY,
    department_id INTEGER NOT NULL,
    category_name TEXT NOT NULL,
    sub_category_name TEXT NOT NULL,
    product_name TEXT NOT NULL,
    is_two_day_eligible INTEGER NOT NULL,
    total_offers INTEGER NOT NULL,
    isbn TEXT NOT NULL,
    issn TEXT NOT NULL,
    exists_for_seller INTEGER NOT NULL,
    rank INTEGER NOT NULL,
    brand TEXT NOT NULL,
    last_update TEXT NOT NULL,
    FOREIGN KEY (department_id) REFERENCES walmart_departments(id)
);"""


class Database:
    def __init__(self, db_dir: str) -> None:
        self.db_dir = path.join(db_dir, "db.sqlite")
        self.__create_table_if_not_exists()

    def __create_table_if_not_exists(self) -> None:
        conn = sql.connect(self.db_dir)
        cur = conn.cursor()
        for script in (DEPARTMENTS_TABLE, TOP_PRODUCTS_TABLE):
            cur.executescript(script)
        conn.commit()
        conn.close()

    def insert_or_replace_many(self, table: str, data: list):
        conn = sql.connect(self.db_dir)
        cur = conn.cursor()

        for row in data:
            fields = "?" + ", ?" * (len(row) - 1)
            value_data = list(row.values())
            cur.execute(
                f"INSERT OR REPLACE INTO {table} VALUES ({fields})",
                value_data,
            )
        conn.commit()
        conn.close()

    def get(self, table: str, fields: list):
        conn = sql.connect(self.db_dir)
        cur = conn.cursor()
        cur.execute(f"SELECT {', '.join(fields)} FROM {table}")
        data = cur.fetchall()
        conn.commit()
        conn.close()

        return [x[0] for x in data]
