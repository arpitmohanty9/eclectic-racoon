import os
import time
import logging
from datetime import datetime
from csv_processor import CSVFileProcessor

INPUT_DIR = "input_files"
ARCHIVE_DIR = "archive"
POLL_SECONDS = 5

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


def process_new_files():
    os.makedirs(ARCHIVE_DIR, exist_ok=True)

    while True:
        # Find files ending in .csv
        files = [f for f in os.listdir(INPUT_DIR) 
                 if f.endswith(".csv") and ".processed" not in f]

        if not files:
            print("No new files detected. Waiting...")
            time.sleep(POLL_SECONDS)
            continue

        for filename in files:
            full_path = os.path.join(INPUT_DIR, filename)
            print(f"Processing file: {filename}")

            processor = CSVFileProcessor(full_path)
            try:
                result = processor.run()
            except Exception as e:
                logging.error(f"Processing failed for {filename}: {e}")
                continue

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base = filename[:-4]
            new_name = f"{base}_{timestamp}.processed.csv"
            archive_path = os.path.join(ARCHIVE_DIR, new_name)

            os.rename(full_path, archive_path)
            logging.info(f"File archived as: {archive_path}")

        time.sleep(POLL_SECONDS)

        
def main():
    logging.info("File watcher started with logging enabled.")
    process_new_files()


if __name__ == "__main__":
    main()
