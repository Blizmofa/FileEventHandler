import json
import sqlite3
from logger import Logger


class CustomContextManager:
    """
    Custom Context Manager Class to manage DB resources with the 'with' key word.
    """
    def __init__(self, name: str):
        """
        Initializing DB.
        """
        self.name = name
        self.class_logger = Logger("DB Context Manager")

    def __enter__(self):
        """
        Opens the connection.
        """
        self.conn = sqlite3.connect(self.name)
        self.class_logger.logger.debug(f"Connected to '{self.name}' successfully.")
        return self.conn.cursor()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Closes the connection.
        """
        if self.conn:
            self.conn.commit()
            self.conn.close()
            self.class_logger.logger.debug(f"Saved data and closed the connection to '{self.name}' successfully.")


class DB:
    """
    ServerDB class for creating and customize the server needed SQLite tables.
    DB is written with sql parameterized queries to prevent SQL Injection.
    """
    def __init__(self, name: str):
        """
        Class Constructor.
        """
        self.name = name
        self.class_logger = Logger('DB')

    def create_table(self, table_name: str, columns: str) -> None:
        """
        Creates a table according to a given table name.
        :param table_name: For the table name.
        :param columns: For the table columns.
        """
        try:
            with CustomContextManager(self.name) as cur:
                cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})")
                self.class_logger.logger.debug(f"Created Table '{table_name}' successfully.")
        except sqlite3.Error as err:
            self.class_logger.logger.error(f"Error creating table {err}.")
            raise CreateTableError(f"[!] Unable to create table '{table_name}'.")

    def insert_value(self, table_name: str, table_column: str, value: str) -> None:
        """
        Inserting a new value to a given database table.
        :param table_name: For the table to insert values to.
        :param table_column: For the column to insert values to.
        :param value: For the value to insert.
        """
        try:
            with CustomContextManager(self.name) as cur:
                cur.execute(f"INSERT INTO {table_name} ({table_column}) VALUES(?)", (value,))
                self.class_logger.logger.debug(f"Inserted '{value}' to '{table_name}' successfully.")
        except sqlite3.Error as err:
            self.class_logger.logger.error(f"Error inserting '{value}' to table {err}.")
            raise InsertError(f"[!] Unable to insert '{value}' to '{table_name}'.")

    def insert_if_not_exists(self, table_name: str, table_column: str, value: str) -> bool:
        """
        Inserting a new value to a given database table only if it doesn't already exist.
        :param table_name: For the table to insert values to.
        :param table_column: For the column to insert values to.
        :param value: For the value to insert.
        :return: True if the value has been inserted successfully, False otherwise.
        """
        try:
            with CustomContextManager(self.name) as cur:
                cur.execute(f"SELECT * FROM {table_name} WHERE {table_column}=?", (value,))
                result = cur.fetchone()
                if result is None:
                    # Value does not exist and has been inserted successfully
                    try:
                        self.insert_value(table_name, table_column, value)
                        return True
                    except InsertError:
                        self.class_logger.logger.error(f"[!] Unable to insert '{value}' to '{table_name}'.")
                        return False
                else:
                    self.class_logger.logger.debug(f"'{value}' Exists in '{table_name}'")
                    return False
        except sqlite3.Error as err:
            self.class_logger.logger.error(f"Error inserting '{value}' from '{table_name}' {err}.")

    def update_table(self, table_name: str, column_to_update: str, value: str, current_table_column: str,
                     existing_value: str) -> None:
        """
        Updates an existing table with a given value.
        :param table_name: For the existing table to update.
        :param column_to_update: For the table column to update.
        :param value: For the new value to add.
        :param current_table_column: For the existing table column.
        :param existing_value: For the existing table value.
        """
        try:
            with CustomContextManager(self.name) as cur:
                cur.execute(f"UPDATE {table_name} SET {column_to_update} = ? WHERE {current_table_column} = ?",
                                    (value, existing_value))
                self.class_logger.logger.debug(f"Inserted '{value}' to '{column_to_update}' in '{table_name}' successfully.")
        except sqlite3.Error as err:
            self.class_logger.logger.error(f"Error updating table {err}.")
            raise UpdateError(f"[!] Unable to update '{value}' in '{table_name}'")

    def delete_value(self, table_name: str, table_column: str, value_to_delete: str) -> None:
        """
        Deleting a given value from a given table.
        :param table_name: For the table to delete the value from.
        :param table_column: For the table column to delete from.
        :param value_to_delete: For the value to delete.
        """
        try:
            with CustomContextManager(self.name) as cur:
                cur.execute(f"DELETE FROM {table_name} WHERE {table_column} = ?", (value_to_delete,))
                self.class_logger.logger.debug(f"Deleted '{value_to_delete}' from '{table_name}' successfully.")
        except sqlite3.Error as err:
            self.class_logger.logger.error(f"Error deleting values from '{table_name}' {err}.")
            raise DeleteError(f"[!] Unable to delete '{value_to_delete}' from '{table_name}'.")

    def select_value(self, table_name: str, table_column: str) -> None:
        """
        Selects a specific table value and return it.
        :param table_name: For the table to select from.
        :param table_column: For the table column to select from.
        :return: The table value after unpacking.
        """
        try:
            with CustomContextManager(self.name) as cur:
                cur.execute(f"SELECT {table_column} FROM {table_name}")
                value = cur.fetchone()[0]
                if value is None:
                    raise NotFoundError(f"[!] Unable to retrieve value from '{table_name}'")
                else:
                    return value
        except sqlite3.Error as err:
            self.class_logger.logger.error(f"Error retrieving value from '{table_name}', Error: {err}.")

    def export_table_to_json(self, table_name: str) -> None:
        """
        Exports the given table data to a JSON file.
        :param table_name: For the table to export.
        """
        try:
            with CustomContextManager(self.name) as cur:
                cur.execute(f"SELECT * FROM {table_name}")
                data = cur.fetchall()
                if data is None:
                    raise NotFoundError(f"[!] Unable to retrieve data from '{table_name}'.")
                else:
                    temp = json.dumps(data)
                    with open(f"{self.name}.json", 'w') as out_file:
                        out_file.write(temp)
        except sqlite3.Error as err:
            self.class_logger.logger.error(f"Error retrieving data from '{table_name}', Error: {err}.")


"""
Custom Exception Classes for raising high-level Exceptions,
and make DB error handling more informative.
"""


class NotFoundError(Exception):
    pass


class CreateTableError(Exception):
    pass


class InsertError(Exception):
    pass


class UpdateError(Exception):
    pass


class DeleteError(Exception):
    pass

