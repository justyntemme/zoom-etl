import pymysql
import logging
import tabulate
from pymysql.constants import CLIENT


class DBAccessor:
    def __init__(
        self, host, user, password, database, port=3306, logger=logging.getLogger()
    ):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.connection = None
        self.logger = logger

    def ensure_connection(self):
        if self.connection is None:
            self.logger.info("No active connection found. Attempting to connect.")
            self.connect()

    def connect(self):
        try:
            self.connection = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                port=self.port,
                client_flag=CLIENT.MULTI_STATEMENTS,
            )
            self.logger.info("Connected to MySQL database.")
        except pymysql.Error as e:
            self.logger.info(f"Failed to connect to MySQL database: {e}")

    def execute_query(self, query, params=()):
        self.ensure_connection()

        if self.connection is None:
            self.logger.error("Failed to execute query: No database connection found")
            return None

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                if query.lstrip().upper().startswith(("CREATE", "INSERT", "UPDATE", "DELETE")):
                    self.connection.commit()
                    return cursor.rowcount
                else:
                    return cursor.fetchall()
        except pymysql.Error as e:
            self.connection.rollback()
            self.logger.info(f"Query execution failed: {e}")


    def analyze_table(self, table_name):
        cursor = self.connection.cursor()
        cursor.execute(f"SHOW TABLE STATUS LIKE '{table_name}'")
        table_status = cursor.fetchone()
        cursor.execute(f"SHOW INDEX FROM {table_name}")
        indexes = cursor.fetchall()

        # Calculate performance metrics
        query_execution_time = self.get_query_execution_time(table_name)
        rows = table_status[4]

        print(f"Table: {table_name}")
        print("Performance Metrics:")
        print(tabulate.tabulate([
            ["Query Execution Time", query_execution_time],
            ["Rows", rows]
        ], headers=["Metric", "Value"], tablefmt="grid"))

        print("\nIndexes:")
        print(tabulate.tabulate(indexes, headers=["Table", "Non_unique", "Key_name", "Seq_in_index", "Column_name", "Collation", "Cardinality", "Sub_part", "Packed", "Null", "Index_type", "Comment"], tablefmt="grid"))

    def get_query_execution_time(self, table_name):
        cursor = self.connection.cursor()
        cursor.execute(f"EXPLAIN ANALYZE SELECT * FROM {table_name}")
        data = cursor.fetchall()[-1]
        execution_time = data[0]
        return execution_time

    def close_connection(self):
        if self.connection:
            self.connection.close()
            self.connection = None
            self.logger.info("MySQL database connection closed.")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # For Linting 
        _ = exc_type
        _ = exc_val
        _ = exc_tb
        self.close_connection()
