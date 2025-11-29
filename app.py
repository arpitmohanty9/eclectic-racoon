# app.py
import logging
import os
import signal
import sys
import time
from dotenv import load_dotenv, dotenv_values
from watchdog.observers import Observer
from file_watcher import CSVFileHandler

# Load .env values into environment
load_dotenv()  # merges .env into os.environ
config = dotenv_values(".env")

WATCH_DIR = config.get("WATCH_DIR") or os.environ.get("WATCH_DIR") or "input_files"
ARCHIVE_DIR = config.get("ARCHIVE_DIR") or os.environ.get("ARCHIVE_DIR") or "archive"
MAX_RETRIES = int(config.get("MAX_RETRIES") or os.environ.get("MAX_RETRIES") or 3)
RETRY_BASE_SECONDS = int(config.get("RETRY_BASE_SECONDS") or os.environ.get("RETRY_BASE_SECONDS") or 1)
LOG_LEVEL = (config.get("LOG_LEVEL") or os.environ.get("LOG_LEVEL") or "INFO").upper()
LOG_FILE = config.get("LOG_FILE") or os.environ.get("LOG_FILE") or ""
PROCESSED_MARKER = config.get("PROCESSED_MARKER") or os.environ.get("PROCESSED_MARKER") or ".processed"
WRITE_PROCESSED_OUTPUT = (config.get("WRITE_PROCESSED_OUTPUT") or os.environ.get("WRITE_PROCESSED_OUTPUT") or "True").lower() in ("1","true","yes")

# Setup logging
logging_handlers = [logging.StreamHandler(sys.stdout)]
if LOG_FILE:
    logging_handlers.append(logging.FileHandler(LOG_FILE))

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=logging_handlers
)

def main():
    logging.info("Starting CSV watcher")
    os.makedirs(WATCH_DIR, exist_ok=True)
    os.makedirs(ARCHIVE_DIR, exist_ok=True)

    event_handler = CSVFileHandler(
        archive_dir=ARCHIVE_DIR,
        max_retries=MAX_RETRIES,
        retry_base=RETRY_BASE_SECONDS,
        processed_marker=PROCESSED_MARKER,
        write_processed_output=WRITE_PROCESSED_OUTPUT
    )

    observer = Observer()
    observer.schedule(event_handler, WATCH_DIR, recursive=False)
    observer.start()

    # Graceful shutdown handling
    def _shutdown(signum, frame):
        logging.info("Received signal %s. Stopping observer...", signum)
        observer.stop()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        while observer.is_alive():
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Keyboard interrupt received; stopping.")
        observer.stop()
    observer.join()
    logging.info("CSV watcher stopped.")

if __name__ == "__main__":
    main()
