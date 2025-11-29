import os
import time
import logging
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from csv_processor import CSVFileProcessor

INPUT_DIR = "input_files"
ARCHIVE_DIR = "archive"

# ---------------------------------------
# Logging Setup
# ---------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),  # print to console
        logging.FileHandler("app.log")  # write to file
    ]
)

class CSVHandler(FileSystemEventHandler):
    """
    Handles new file creation events in the INPUT_DIR
    """

    def on_created(self, event):
        if event.is_directory:
            #its an event for a directory
            #can be ignored.
            return
        
        filename = os.path.basename(event.src_path)

        # ignore already processed files.
        if ".processed" in filename:
            #ignore processed files
            return
        
        #process only CSV 
        if not filename.endswith(".csv"):
            logging.info("Ignoring non-CSV file: {filename}")
            return

        #process the file
        logging.info("Detected new file : {filename}")
        full_path = event.src_path
        processor = CSVFileProcessor(full_path)
        try:
            result = processor.run()
        except Exception as e:
            logging.error(f"Processing failed for {filename}: {e}")
            return
        
        # Rename + move to archive
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base = filename[:-4]
        new_name = f"{base}_{timestamp}.processed.csv"
        archive_path = os.path.join(ARCHIVE_DIR, new_name)
        os.makedirs(ARCHIVE_DIR, exist_ok=True)

        os.rename(full_path, archive_path)
        logging.info(f"Moved processed file to archive as: {archive_path}")


def main():
    logging.info("File watcher started with logging enabled.")
    event_handler = CSVHandler()
    observer = Observer()
    
    observer.schedule(event_handler, INPUT_DIR, recursive=False)
    observer.start()

    try:
        #keep the program alive
        while True:
            pass
    except KeyboardInterrupt:
        logging.info("File watcher stopped.")
        observer.stop()
    finally:
        observer.join()

if __name__ == "__main__":
    main()
