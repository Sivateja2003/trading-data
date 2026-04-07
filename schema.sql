-- Initial schema for stock data storage

CREATE TABLE IF NOT EXISTS stock_data (
    instrument_token BIGINT,
    symbol TEXT,
    timestamp TIMESTAMP,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume BIGINT,
    PRIMARY KEY (instrument_token, timestamp)
);

-- Index for faster lookups by symbol and timestamp
CREATE INDEX IF NOT EXISTS idx_stock_data_symbol_timestamp ON stock_data (symbol, timestamp);
