import os
import duckdb as db
from app.ingestion.loaders.utils import logging


class DuckDBDatabase:

    def __init__(self, db_path: str, ddl_folder: str = "ddl"):
        self.db_path = db_path
        self.ddl_folder = ddl_folder
        self.con = None

    def connect(self):
        """ Establishes a connection to the DuckDB database. """
        if not self.con:
            self.con = db.connect(self.db_path)
        return self.con

    def disconnect(self):
        """ Closes the connection to the DuckDB database. """
        if self.con:
            self.con.close()
            self.con = None

    def create_tables_from_files(self):
        """
        Creates tables in the database by reading DDL scripts from files.
        """
        con = self.connect()
        for filename in os.listdir(self.ddl_folder):
            if filename.endswith(".sql"):
                filepath = os.path.join(self.ddl_folder, filename)
                with open(filepath, 'r') as f:
                    schema = f.read()
                con.execute(schema)
        con.commit()

    def drop_database_file(self):
        """
        Drops the database file if it exists.
        """
        if os.path.isfile(self.db_path):
            try:
                os.remove(self.db_path)
                logging.info(f"Database file {self.db_path} dropped successfully.")
            except OSError as e:
                logging.error(f"Failed to drop database file: {e}")
