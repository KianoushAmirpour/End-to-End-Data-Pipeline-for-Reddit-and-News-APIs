import os
import psycopg2
from . import utils
from io import StringIO


class DataWarehouseModule:
    
    def __init__(self):
        
        self.logger = utils.setup_logger(__name__)
        self.connection, self.cursor = self._connect()
        
    def _connect(self):
        
        try:
            db_credentials = {
                            'dbname': os.environ.get('DB_NAME'),
                            'user': os.environ.get('DB_USER'),
                            'password': os.environ.get('DB_PASSWORD'),
                            'host': os.environ.get('DB_HOST'),
                            'port': os.environ.get('DB_PORT')
                             }
            self.connection = psycopg2.connect(**db_credentials)
            self.cursor = self.connection.cursor()
            return self.connection, self.cursor
        except Exception as e:
            self.logger.exception(f"Connection to database was unsuccessful due to: {str(e)}.")

    def copy_to_staging_tables(self, key):
        try:
            copy_query = f"""
                COPY staging.{key}
                FROM stdin
                WITH CSV DELIMITER ',' HEADER
                ;
                """
            with open(f'tmp/{key}.csv', 'r',  encoding='utf-8') as f:
                file_like_object = StringIO(f.read())
                self.cursor.copy_expert(sql=copy_query, file=file_like_object)
                self.connection.commit()
        except Exception as e:
            self.logger.exception(f"Copying the data to staging tables was unseccessful due to: {str(e)}.")



