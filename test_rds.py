import os
import datetime
import mysql.connector
from mysql.connector import errorcode
from dotenv import load_dotenv
from urllib.parse import urlparse
from logger import logger

# Load environment variables
load_dotenv()

class DatabaseManager:
    def __init__(self):
        # Default fallback to the provided credentials
        self.db_url = os.getenv("DATABASE_URL")
        self.conn = None

    def _parse_db_url(self, db_url):
        url = urlparse(db_url)
        dbname = url.path[1:] if url.path.startswith('/') else url.path
        return {
            'user': url.username,
            'password': url.password,
            'host': url.hostname,
            'port': url.port or 3306,
            'database': dbname
        }

    def connect(self):
        config = self._parse_db_url(self.db_url)
        dbname = config.pop('database')
        
        # Try both 'root' and 'admin' if one fails with Access Denied
        usernames = [config['user'], 'admin']
        last_err = None
        
        for user in usernames:
            try:
                config['user'] = user
                logger.info(f"Attempting to connect to RDS as '{user}'...")
                self.conn = mysql.connector.connect(**config)
                
                with self.conn.cursor() as cur:
                    cur.execute(f"CREATE DATABASE IF NOT EXISTS {dbname}")
                
                self.conn.database = dbname
                self.conn.autocommit = True
                logger.info(f"Connected successfully as '{user}'.")
                return
            except mysql.connector.Error as err:
                last_err = err
                if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                    logger.warning(f"Access denied for user '{user}'.")
                    continue
                else:
                    raise
        
        logger.error(f"Failed to connect after trying multiple usernames: {last_err}")
        raise last_err

    def initialize_schema(self):
        schema_path = os.path.join(os.path.dirname(__file__), 'schema.sql')
        if os.path.exists(schema_path):
            with open(schema_path, 'r') as f:
                schema_sql = f.read()
            commands = [cmd.strip() for cmd in schema_sql.split(';') if cmd.strip()]
            with self.conn.cursor() as cur:
                for command in commands:
                    cur.execute(command)
            logger.info("Schema initialized.")

    def store_dummy_data(self):
        """Stores some sandbox data to verify it works."""
        now = datetime.datetime.now()
        dummy_data = [
            (12345, "TEST_STOCK_1", now, 100.5, 105.0, 99.0, 102.5, 5000),
            (67890, "TEST_STOCK_2", now, 200.0, 210.0, 195.0, 205.0, 3000)
        ]
        
        sql = """
        INSERT IGNORE INTO stock_data (instrument_token, symbol, timestamp, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        with self.conn.cursor() as cur:
            cur.executemany(sql, dummy_data)
        logger.info(f"Stored {len(dummy_data)} dummy records.")

    def close(self):
        if self.conn:
            self.conn.close()

if __name__ == "__main__":
    db = DatabaseManager()
    try:
        db.connect()
        db.initialize_schema()
        db.store_dummy_data()
        print("SUCCESS: Data successfully stored in the RDS instance!")
        db.close()
    except Exception as e:
        print(f"FAILED: {e}")
