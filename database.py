import os
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from logger import logger

# Load environment variables
load_dotenv()

class DatabaseManager:
    def __init__(self):
        self.db_url = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/stock_db")
        self.conn = None

    def connect(self):
        """Establishes connection to PostgreSQL via URL and initializes schema."""
        try:
            self.conn = psycopg2.connect(self.db_url)
            self.conn.autocommit = True
            logger.info("Successfully connected to PostgreSQL via DATABASE_URL.")
            
            # Initialize schema
            self.initialize_schema()
        except Exception as e:
            logger.error(f"Error connecting to PostgreSQL: {e}")
            raise

    def initialize_schema(self):
        """Runs schema.sql to ensure tables exist."""
        schema_path = os.path.join(os.path.dirname(__file__), 'schema.sql')
        if os.path.exists(schema_path):
            try:
                with open(schema_path, 'r') as f:
                    schema_sql = f.read()
                with self.conn.cursor() as cur:
                    cur.execute(schema_sql)
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
            logger.info("PostgreSQL connection closed.")

    def get_last_timestamp(self, instrument_token):
        """Fetches the latest timestamp for a given instrument."""
        query = "SELECT MAX(timestamp) FROM stock_data WHERE instrument_token = %s"
        with self.conn.cursor() as cur:
            cur.execute(query, (instrument_token,))
            res = cur.fetchone()
            return res[0] if res else None

    def upsert_data(self, data_list):
        """Batch inserts data with ON CONFLICT DO NOTHING (UPSERT)."""
        if not data_list:
            return
            
        sql = """
        INSERT INTO stock_data (instrument_token, symbol, timestamp, open, high, low, close, volume)
        VALUES %s
        ON CONFLICT (instrument_token, timestamp) DO NOTHING
        """
        try:
            with self.conn.cursor() as cur:
                execute_values(cur, sql, data_list)
            logger.info(f"Successfully upserted {len(data_list)} records.")
        except Exception as e:
            logger.error(f"Error during upsert: {e}")
            raise
