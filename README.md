# Zerodha Stock Data Pipeline

A robust, production-ready data pipeline for fetching and storing stock data from Zerodha's Kite Connect API into PostgreSQL.

## Features
- **Auto-Authentication**: Caches historical tokens and handles daily re-auth with user prompts.
- **Incremental Updates**: Uses an overlap+UPSERT strategy to handle holidays and weekends seamlessly.
- **Rate-Limited**: Respects Zerodha's API limits.
- **Error-Resistant**: Includes retry logic and exponential backoff.
- **Logging**: Full visibility into pipeline progress and failures.
- **Scheduling**: Simple Python scheduler or external Task Scheduler support.

## Prerequisites
1.  Python 3.8+
2.  PostgreSQL (or Docker installed)
3.  Zerodha API Key & Secret

## Setup
1.  **Clone/Create Project**:
    ```bash
    pip install -r requirements.txt
    ```
2.  **Environment Setup**:
    - Copy `.env.example` to `.env` and fill in your Zerodha credentials and `DATABASE_URL` (e.g. `postgresql://user:password@host:port/dbname`).
3.  **Database**:
    - If using Docker: `docker-compose up -d`
    - Otherwise, ensure your PostgreSQL server is accessible via the `DATABASE_URL` and run `schema.sql`.

## Usage
### Initial Backfill / Manual Update
```bash
python data_pipeline.py
```
- On the first run of the day, it will print a login URL.
- Login in your browser, copy the `request_token` from the URL, and paste it into the terminal.

### Scheduled Updates
```bash
python scheduler.py
```
- This will keep the script running and trigger `data_pipeline.py` every day at 18:00 (Post-market).

## Project Structure
- `kite_auth.py`: Handles Zerodha's complex login flow.
- `database.py`: Clean PostgreSQL interface with batch UPSERT logic.
- `data_pipeline.py`: The heart of the logic (fetching, transforming, and storing).
- `utils.py`: Fetches and caches instrument tokens.
- `logger.py`: Centralized logging.
- `schema.sql`: Table structure for production reliability.
- `scheduler.py`: Easy-to-use scheduling wrapper.
