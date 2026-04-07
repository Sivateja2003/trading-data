-- Initial schema for stock data storage (MySQL Compatible)

CREATE TABLE IF NOT EXISTS stock_data (
    instrument_token BIGINT,
    symbol VARCHAR(255),
    timestamp DATETIME(6),
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume BIGINT,
    PRIMARY KEY (instrument_token, timestamp),
    INDEX idx_stock_data_symbol_timestamp (symbol, timestamp)
);
