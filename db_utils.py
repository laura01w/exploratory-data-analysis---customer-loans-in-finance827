from sqlalchemy import create_engine
import pandas as pd

class RDSDatabaseConnector:
    def __init__(self, host, port, database, user, password):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.engine = self._create_engine()

    def _create_engine(self):
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        return create_engine(f"{DATABASE_TYPE}+{DBAPI}://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}")

    def execute_query(self, query):
        try:
            with self.engine.connect() as connection:
                result = connection.execute(query)
                return result
        except Exception as e:
            print(f"Error executing query: {e}")
            return None

    def fetch_data(self, query):
        try:
            with self.engine.connect() as connection:
                result = connection.execute(query)
                data = pd.DataFrame(result.fetchall(), columns=result.keys())
                return data
        except Exception as e:
            print(f"Error fetching data: {e}")
            return None
