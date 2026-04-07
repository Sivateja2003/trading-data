import os
import mysql.connector
from dotenv import load_dotenv
from urllib.parse import urlparse
from logger import logger

# Load environment variables
load_dotenv()

class RDSVerifier:
    def __init__(self):
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
        self.conn = mysql.connector.connect(**config)
        self.conn.autocommit = True
        logger.info("Connected to RDS for verification.")

    def check_data(self):
        """Queries the database to verify stored records."""
        query = "SELECT instrument_token, symbol, timestamp, close FROM stock_data ORDER BY timestamp DESC LIMIT 5"
        try:
            with self.conn.cursor(dictionary=True) as cur:
                cur.execute(query)
                rows = cur.fetchall()
                
                if not rows:
                    print("⚠️ No data found in 'stock_data' table.")
                    return
                
                print("✅ Found data in 'stock_data' table!")
                print("-" * 50)
                print(f"{'Token':<10} | {'Symbol':<15} | {'Timestamp':<20} | {'Close':<10}")
                print("-" * 50)
                for row in rows:
                    print(f"{row['instrument_token']:<10} | {row['symbol']:<15} | {str(row['timestamp']):<20} | {row['close']:<10}")
                print("-" * 50)
        except Exception as e:
            print(f"❌ Error checking data: {e}")

    def close(self):
        if self.conn:
            self.conn.close()

if __name__ == "__main__":
    verifier = RDSVerifier()
    try:
        verifier.connect()
        verifier.check_data()
        verifier.close()
    except Exception as e:
        print(f"❌ Verification failed: {e}")
