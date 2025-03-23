import os
from google.cloud.sql.connector import Connector
from dotenv import load_dotenv

load_dotenv()

class CloudSQLConnection:

    def __init__(self) -> None:

        self.config = {
            'instance_connection_string': os.getenv('CLOUD_SQL_INSTANCE_NAME'),
            'driver': 'pg8000',
            'user': os.getenv('CLOUD_SQL_USER'),
            'password': os.getenv('CLOUD_SQL_PASSWORD'),
            'db': os.getenv('CLOUD_SQL_DATABASE')
        }

    def __enter__(self):

        self.connector = Connector(refresh_strategy="LAZY")
        self.connection = self.connector.connect(**self.config)
        self.cursor = self.connection.cursor()

        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        
        try:
            self.connection.commit()
    
        except Exception as e:
            self.connection.rollback()
            print(f'Database transaction failed and rolled back: {e}')

        finally:
            if self.connection:
                self.cursor.close()
                self.connection.close()
                self.connector.close()
