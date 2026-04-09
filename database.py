"""
MySQL database helpers for storing and retrieving historical candle data.

Tables used:
  stock_data  — OHLCV candles keyed by (instrument_token, timestamp)
  fetch_log   — tracks which (symbol, exchange, interval, from_date, to_date)
                queries have been fetched; used for exact-range duplicate detection
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from urllib.parse import urlparse

import pandas as pd
from dotenv import load_dotenv

ENV_FILE = os.path.join(os.path.dirname(__file__), ".env")

try:
    import pymysql
    import pymysql.cursors
except ImportError:
    print(
        "ERROR: PyMySQL is not installed.\n"
        "Run: pip install pymysql"
    )
    sys.exit(1)


def _get_connection():
    load_dotenv(dotenv_path=ENV_FILE)
    url = os.getenv("MYSQL_URL", "")
    if not url:
        sys.exit("ERROR: MYSQL_URL not set in .env file.")

    parsed   = urlparse(url)
    host     = parsed.hostname
    port     = parsed.port or 3306
    user     = parsed.username
    password = parsed.password
    database = parsed.path.lstrip("/")

    try:
        print(f"[DEBUG] Connecting to MySQL: {user}@{host}:{port}/{database}")
        conn = pymysql.connect(
            host=host, port=port, user=user,
            password=password, database=database,
            connect_timeout=10,
            cursorclass=pymysql.cursors.DictCursor,
        )
        print("[DEBUG] Connected to MySQL successfully")
        return conn
    except pymysql.Error as exc:
        print(
            f"ERROR: Cannot connect to MySQL at {user}@{host}:{port}/{database}.\n"
            f"  {exc}\n"
            "Check MYSQL_URL in your .env file."
        )
        sys.exit(1)
    except Exception as exc:
        print(f"ERROR: Unexpected error connecting to MySQL: {exc}")
        sys.exit(1)


_CREATE_FETCH_LOG_SQL = """
CREATE TABLE IF NOT EXISTS fetch_log (
    symbol        VARCHAR(50)  NOT NULL,
    exchange      VARCHAR(20)  NOT NULL,
    interval_type VARCHAR(20)  NOT NULL,
    from_date     DATE         NOT NULL,
    to_date       DATE         NOT NULL,
    fetched_at    DATETIME     DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, exchange, interval_type, from_date, to_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def ensure_table() -> None:
    """Verify stock_data is accessible and create fetch_log if needed."""
    conn   = _get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM stock_data LIMIT 1")
    cursor.execute(_CREATE_FETCH_LOG_SQL)
    conn.commit()
    cursor.close()
    conn.close()


def data_exists(
    symbol: str,
    exchange: str,
    interval: str,
    from_dt: datetime,
    to_dt: datetime,
) -> bool:
    """
    Return True only if this exact query (symbol+exchange+interval+date range)
    was previously fetched and logged in fetch_log.
    """
    conn   = _get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT COUNT(*) AS cnt FROM fetch_log
         WHERE symbol        = %s
           AND exchange      = %s
           AND interval_type = %s
           AND from_date     <= %s
           AND to_date       >= %s
        """,
        (
            symbol.upper(),
            exchange.upper(),
            interval,
            from_dt.date(),
            to_dt.date(),
        ),
    )
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return int(row["cnt"]) > 0


def save_to_db(
    df: pd.DataFrame,
    symbol: str,
    instrument_token: int,
    exchange: str,
    interval: str,
    from_dt: datetime,
    to_dt: datetime,
) -> int:
    """
    Insert candles into stock_data and log the query in fetch_log.
    Duplicate candle rows (instrument_token, timestamp) are silently skipped.

    Returns
    -------
    Number of candle rows actually inserted.
    """
    if df.empty:
        return 0

    conn   = _get_connection()
    cursor = conn.cursor()
    rows_inserted = 0

    for ts, row in df.iterrows():
        cursor.execute(
            """
            INSERT IGNORE INTO stock_data
                (instrument_token, symbol, timestamp, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                instrument_token,
                symbol.upper(),
                ts.to_pydatetime(),
                float(row["open"]),
                float(row["high"]),
                float(row["low"]),
                float(row["close"]),
                int(row["volume"]),
            ),
        )
        rows_inserted += cursor.rowcount

    # Log the fetched query range so future identical queries are detected
    cursor.execute(
        """
        INSERT IGNORE INTO fetch_log
            (symbol, exchange, interval_type, from_date, to_date)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (
            symbol.upper(),
            exchange.upper(),
            interval,
            from_dt.date(),
            to_dt.date(),
        ),
    )

    conn.commit()
    cursor.close()
    conn.close()
    return rows_inserted
