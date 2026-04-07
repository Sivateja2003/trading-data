import time
import datetime
import pandas as pd
from kite_auth import KiteAuthenticator
from database import DatabaseManager
from utils import get_instrument_tokens
from logger import logger

# Configuration
STOCKS = ["RELIANCE", "TCS", "INFY", "HDFCBANK"]
INTERVAL = "day"
BACKFILL_DAYS = 365
RETRY_ATTEMPTS = 3
RATE_LIMIT_SLEEP = 0.4  # Slightly more than 0.3s to be safe

def fetch_with_retry(kite, instrument_token, from_date, to_date):
    """Fetches historical data with retry logic."""
    for attempt in range(RETRY_ATTEMPTS):
        try:
            data = kite.historical_data(instrument_token, from_date, to_date, INTERVAL)
            return data
        except Exception as e:
            logger.warning(f"Attempt {attempt+1} failed for token {instrument_token}: {e}")
            if attempt < RETRY_ATTEMPTS - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                logger.error(f"All retries failed for token {instrument_token}.")
                raise

def run_pipeline():
    logger.info("Starting stock data pipeline...")
    
    # Initial Auth
    auth = KiteAuthenticator()
    kite = auth.authenticate()
    
    # Database initialization
    db = DatabaseManager()
    db.connect()
    
    # Get Instrument Tokens
    tokens_map = get_instrument_tokens(kite, STOCKS)
    
    today = datetime.date.today()
    
    for symbol in STOCKS:
        token = tokens_map.get(symbol)
        if not token:
            logger.warning(f"No token found for {symbol}. Skipping.")
            continue
            
        logger.info(f"Processing {symbol} (Token: {token})...")
        
        # Get last date from DB
        last_date = db.get_last_timestamp(token)
        
        if last_date:
            # Incremental logic: start from last known date (overlap + UPSERT)
            from_date = last_date.date()
            logger.info(f"Incremental update for {symbol} from {from_date}.")
        else:
            # Backfill logic
            from_date = today - datetime.timedelta(days=BACKFILL_DAYS)
            logger.info(f"Historical backfill for {symbol} from {from_date}.")
            
        try:
            # Fetch data
            raw_data = fetch_with_retry(kite, token, from_date, today)
            
            if not raw_data:
                logger.info(f"No new data for {symbol}.")
                continue
                
            # Transform data
            formatted_records = []
            for item in raw_data:
                # Zerodha hands back: date, open, high, low, close, volume
                formatted_records.append((
                    token,
                    symbol,
                    item["date"],
                    item["open"],
                    item["high"],
                    item["low"],
                    item["close"],
                    item["volume"]
                ))
            
            # Batch Upsert
            db.upsert_data(formatted_records)
            
        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}")
            
        # Rate limit
        time.sleep(RATE_LIMIT_SLEEP)
        
    db.close()
    logger.info("Pipeline run completed.")

if __name__ == "__main__":
    run_pipeline()
