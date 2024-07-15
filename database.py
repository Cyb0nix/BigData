import psycopg2

class DatabaseHandler:
    def __init__(self, dbname, user, password, host="localhost", port="5432"):
        """Initializes the database handler with connection details."""
        self.connection = psycopg2.connect(
            dbname=dbname, user=user, password=password, host=host, port=port
        )
        self.cursor = self.connection.cursor()

    def execute_query(self, query, parameters=None):
        """Executes a SQL query with optional parameters."""
        try:
            self.cursor.execute(query, parameters)
            self.connection.commit()
        except psycopg2.Error as e:
            print(f"Database error: {e}")
            self.connection.rollback()

    def close(self):
        """Closes the database connection."""
        self.cursor.close()
        self.connection.close()