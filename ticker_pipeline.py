import os
import time
import datetime
from kiteconnect import KiteTicker
from kite_auth import KiteAuthenticator
from database import DatabaseManager
from utils import get_instrument_tokens
from logger import logger

# Configuration
STOCKS = ["RELIANCE", "TCS", "INFY", "HDFCBANK"]
BUFFER_SIZE = 50  # Flush after 50 ticks
FLUSH_INTERVAL = 1  # Or every 2 seconds

class TickerPipeline:
    def __init__(self):
        self.auth = KiteAuthenticator()
        self.kite = self.auth.authenticate()
        self.db = DatabaseManager()
        self.db.connect()
        
        # Get instrument tokens
        self.tokens_map = get_instrument_tokens(self.kite, STOCKS)
        self.reverse_tokens_map = {v: k for k, v in self.tokens_map.items()}
        self.tokens = list(self.tokens_map.values())
        
        # Buffer for ticks
        self.buffer = []
        self.last_flush_time = time.time()
        
        # Initialize KWS
        self.kws = KiteTicker(self.auth.api_key, self.auth.access_token)
        self.kws.on_ticks = self.on_ticks
        self.kws.on_connect = self.on_connect
        self.kws.on_close = self.on_close
        self.kws.on_error = self.on_error
        self.kws.on_reconnect = self.on_reconnect

    def on_ticks(self, ws, ticks):
        """Callback for incoming ticks."""
        current_time = datetime.datetime.now()
        
        for tick in ticks:
            token = tick.get("instrument_token")
            symbol = self.reverse_tokens_map.get(token, "UNKNOWN")
            
            # Format: (instrument_token, symbol, timestamp, open, high, low, close, volume)
            # For real-time ticks, we use last_price as close. 
            # We can also use high/low/open from the tick's daily OHLC if available.
            ohlc = tick.get("ohlc", {})
            record = (
                token,
                symbol,
                current_time,
                ohlc.get("open", tick.get("last_price")),
                ohlc.get("high", tick.get("last_price")),
                ohlc.get("low", tick.get("last_price")),
                tick.get("last_price"),
                tick.get("volume", 0)
            )
            self.buffer.append(record)
        
        # Check if we should flush
        if len(self.buffer) >= BUFFER_SIZE or (time.time() - self.last_flush_time) >= FLUSH_INTERVAL:
            self.flush_buffer()

    def flush_buffer(self):
        """Flushes the buffer to the database."""
        if not self.buffer:
            return
            
        try:
            logger.info(f"Flushing {len(self.buffer)} ticks to database...")
            self.db.upsert_data(self.buffer)
            self.buffer = []
            self.last_flush_time = time.time()
        except Exception as e:
            logger.error(f"Error flushing buffer: {e}")

    def on_connect(self, ws, response):
        """Callback on successful connection."""
        logger.info("Successfully connected to WebSocket ticker.")
        # Subscribe to tokens
        ws.subscribe(self.tokens)
        # Set mode to FULL to get OHLC and volume
        ws.set_mode(ws.MODE_FULL, self.tokens)
        logger.info(f"Subscribed to tokens: {self.tokens}")

    def on_close(self, ws, code, reason):
        """Callback on connection close."""
        logger.warning(f"WebSocket connection closed: {code} - {reason}")
        self.flush_buffer()

    def on_error(self, ws, code, reason):
        """Callback on connection error."""
        logger.error(f"WebSocket error: {code} - {reason}")

    def on_reconnect(self, ws, attempts_count):
        """Callback on reconnection attempts."""
        logger.info(f"Reconnecting... attempt {attempts_count}")

    def run(self):
        """Starts the ticker."""
        logger.info("Starting Ticker Pipeline...")
        # connect(threaded=False) so it blocks the main thread
        self.kws.connect()

if __name__ == "__main__":
    pipeline = TickerPipeline()
    try:
        pipeline.run()
    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user.")
        pipeline.flush_buffer()
        pipeline.db.close()
