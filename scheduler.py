import time
import schedule
import subprocess
from logger import logger

def run_job():
    logger.info("Running scheduled job...")
    try:
        # Run the pipeline script
        result = subprocess.run(["python", "data_pipeline.py"], capture_output=True, text=True)
        logger.info(result.stdout)
        if result.stderr:
            logger.error(result.stderr)
    except Exception as e:
        logger.error(f"Error running scheduled job: {e}")

# Schedule for 8:30 PM (after market close)
schedule.every().day.at("20:30").do(run_job)

logger.info("Scheduler started. Waiting for 20:30 local time...")

while True:
    schedule.run_pending()
    time.sleep(60) # check every minute
