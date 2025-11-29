# file_watcher.py
import os
import time
import logging
from watchdog.events import FileSystemEventHandler
from typing import Optional
from processor import CSVProcessor
from utils import timestamped_name, safe_move
from dotenv import dotenv_values

# load defaults from .env (caller/app.py will also load, but defensively here)
_config = dotenv_values(".env") or {}

def _getenv(key: str, default=None, cast_type=None):
    v = _config.get(key) or os.environ.get(key) or default
    if cast_type and v is not None:
        return cast_type(v)
    return v

class CSVFileHandler(FileSystemEventHandler):
    def __init__(self,
                 archive_dir: str,
                 max_retries: int = 3,
                 retry_base: int = 1,
                 processed_marker: str = ".processed",
                 write_processed_output: bool = True,
                 processor: Optional[CSVProcessor] = None):
        super().__init__()
        self.archive_dir = archive_dir
        self.max_retries = max_retries
        self.retry_base = retry_base
        self.processed_marker = processed_marker
        self.write_processed_output = write_processed_output
        self.processor = processor or CSVProcessor()

    def on_created(self, event):
        # Called when a file appears in the directory
        if event.is_directory:
            return

        filename = os.path.basename(event.src_path)

        # Ignore already-processed files
        if self.processed_marker in filename:
            logging.debug("Ignoring file (already processed marker present): %s", filename)
            return

        if not filename.lower().endswith(".csv"):
            logging.info("Ignoring non-CSV file: %s", filename)
            return

        logging.info("Detected new file: %s", filename)
        self._attempt_processing(event.src_path, filename)

    def _attempt_processing(self, full_path: str, filename: str):
        """
        Try to process with retries and exponential backoff.
        """
        for attempt in range(1, self.max_retries + 1):
            try:
                logging.info("Processing attempt %d/%d for %s", attempt, self.max_retries, filename)
                processed_rows = self.processor.process(full_path)

                # Optionally write the processed output into the archive (same name pattern)
                new_name = timestamped_name(filename, marker=self.processed_marker)
                archive_path = os.path.join(self.archive_dir, new_name)

                if self.write_processed_output:
                    # Write processed CSV to a temporary path in archive, then remove original
                    temp_out = archive_path + ".tmp"
                    self.processor.write_csv(temp_out, processed_rows)
                    # Move original file into archive as well (keep original if needed)
                    safe_move(full_path, archive_path + ".orig")
                    # Move written processed file into final name
                    safe_move(temp_out, archive_path)
                    logging.info("Processed output written to: %s", archive_path)
                else:
                    # If we don't write processed output, simply move original to archive with timestamp
                    archive_path = os.path.join(self.archive_dir, new_name)
                    safe_move(full_path, archive_path)
                    logging.info("Moved original file to archive as: %s", archive_path)

                # Success -> break retry loop
                return

            except Exception as exc:
                logging.exception("Error processing %s on attempt %d: %s", filename, attempt, exc)
                if attempt < self.max_retries:
                    backoff = self.retry_base * (2 ** (attempt - 1))
                    logging.warning("Retrying %s after %s seconds...", filename, backoff)
                    time.sleep(backoff)
                else:
                    logging.error("Exceeded max retries for %s. Moving file to failed folder.", filename)
                    # Move to a failed/ quarantine area for manual inspection
                    failed_dir = os.path.join(self.archive_dir, "failed")
                    os.makedirs(failed_dir, exist_ok=True)
                    failed_name = f"{filename}.failed"
                    failed_path = os.path.join(failed_dir, failed_name)
                    try:
                        safe_move(full_path, failed_path)
                        logging.info("Moved failed file to: %s", failed_path)
                    except Exception as move_exc:
                        logging.exception("Failed to move failed file %s: %s", filename, move_exc)
