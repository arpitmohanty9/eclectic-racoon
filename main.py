import os
import time
from datetime import datetime
from csv_processor import CSVFileProcessor

INPUT_DIR = "input_files"

def process_new_files():
    while True:
        # Find files ending in .csv
        files = [f for f in os.listdir(INPUT_DIR) 
                 if f.endswith(".csv") and ".processed" not in f]

        if not files:
            print("No new files detected. Waiting...")
            time.sleep(5)
            continue

        for filename in files:
            full_path = os.path.join(INPUT_DIR, filename)
            print(f"Processing file: {filename}")

            processor = CSVFileProcessor(full_path)
            result = processor.run()

            print("Processed rows:")
            for row in result:
                print(row)

            # Rename after processing
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            new_name = f"{filename.rstrip('.csv')}_{timestamp}.processed.csv"
            new_path = os.path.join(INPUT_DIR, new_name)
            os.rename(full_path, new_path)

            print(f"Renamed file to: {new_name}")

        # Sleep before checking again
        time.sleep(5)

def main():
    print("Starting file watcher...")
    process_new_files()

if __name__ == "__main__":
    main()
