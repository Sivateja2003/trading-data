import os
import mysql.connector
from mysql.connector import errorcode
from dotenv import load_dotenv
from urllib.parse import urlparse
from logger import logger

# Load environment variables
load_dotenv()

class DatabaseManager:
    def __init__(self):
        # Default fallback to the provided credentials if .env is missing
        default_url = ""
        self.db_url = os.getenv("DATABASE_URL", default_url)
        self.conn = None

    def _parse_db_url(self, db_url):
        """Parses the DATABASE_URL into a dictionary compatible with mysql.connector."""
        try:
            url = urlparse(db_url)
            # url.path includes the leading '/'
            dbname = url.path[1:] if url.path.startswith('/') else url.path
            return {
                'user': url.username,
                'password': url.password,
                'host': url.hostname,
                'port': url.port or 3306,
                'database': dbname
            }
        except Exception as e:
            logger.error(f"Failed to parse DATABASE_URL: {e}")
            raise

    def connect(self):
        """Establishes connection to MySQL via URL and initializes schema."""
        config = self._parse_db_url(self.db_url)
        dbname = config.pop('database')
        
        try:
            # First, connect without a database to ensure we can create it if missing
            self.conn = mysql.connector.connect(**config)
            with self.conn.cursor() as cur:
                cur.execute(f"CREATE DATABASE IF NOT EXISTS {dbname}")
            
            # Now reconnect with the database selected
            config['database'] = dbname
            self.conn.database = dbname
            self.conn.autocommit = True
            logger.info(f"Successfully connected to MySQL database '{dbname}'.")
            
            # Initialize schema
            self.initialize_schema()
        except mysql.connector.Error as err:
            logger.error(f"Error connecting to MySQL: {err}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during connection: {e}")
            raise

    def initialize_schema(self):
        """Runs schema.sql to ensure tables exist."""
        schema_path = os.path.join(os.path.dirname(__file__), 'schema.sql')
        if os.path.exists(schema_path):
            try:
                with open(schema_path, 'r') as f:
                    schema_sql = f.read()
                
                # Split schema_sql into individual commands as MySQL connector's execute doesn't like multiple statements by default
                commands = [cmd.strip() for cmd in schema_sql.split(';') if cmd.strip()]
                
                with self.conn.cursor() as cur:
                    for command in commands:
                        cur.execute(command)
                logger.info("Database schema initialized successfully.")
            except Exception as e:
                logger.error(f"Error initializing schema: {e}")
                raise
        else:
            logger.warning("schema.sql not found. Skipping initialization.")

    def close(self):
        """Closes the connection."""
        if self.conn:
            self.conn.close()
            logger.info("MySQL connection closed.")

    def get_last_timestamp(self, instrument_token):
        """Fetches the latest timestamp for a given instrument."""
        query = "SELECT MAX(timestamp) FROM stock_data WHERE instrument_token = %s"
        try:
            if not self.conn:
                self.connect()
            with self.conn.cursor() as cur:
                cur.execute(query, (instrument_token,))
                res = cur.fetchone()
                return res[0] if res else None
        except Exception as e:
            logger.error(f"Error fetching last timestamp: {e}")
            return None

    def upsert_data(self, data_list):
        """Batch inserts data using INSERT IGNORE (MySQL's version of ON CONFLICT DO NOTHING)."""
        if not data_list:
            return
            
        sql = """
        INSERT IGNORE INTO stock_data (instrument_token, symbol, timestamp, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            if not self.conn:
                self.connect()
            with self.conn.cursor() as cur:
                cur.executemany(sql, data_list)
            logger.info(f"Successfully upserted {len(data_list)} records.")
        except Exception as e:
            logger.error(f"Error during upsert: {e}")
            raise
