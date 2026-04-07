import os
import json
from logger import logger

INSTRUMENTS_FILE = ".instruments.json"

def get_instrument_tokens(kite, symbols=["RELIANCE", "TCS", "INFY", "HDFCBANK"], exchange="NSE"):
    """Fetches and caches instrument tokens for the given symbols."""
    # Try to load from cache
    if os.path.exists(INSTRUMENTS_FILE):
        with open(INSTRUMENTS_FILE, "r") as f:
            tokens = json.load(f)
            # Check if all requested symbols are in cache
            if all(s in tokens for s in symbols):
                logger.info("Loaded instrument tokens from cache.")
                return tokens
    
    # Fetch all instruments
    logger.info("Fetching instruments from Kite API...")
    instruments = kite.instruments(exchange)
    
    tokens = {}
    for inst in instruments:
        if inst["tradingsymbol"] in symbols:
            tokens[inst["tradingsymbol"]] = inst["instrument_token"]
            
    # Save to cache
    with open(INSTRUMENTS_FILE, "w") as f:
        json.dump(tokens, f)
    
    logger.info("Saved instrument tokens to cache.")
    return tokens
