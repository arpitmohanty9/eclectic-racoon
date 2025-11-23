import csv

class CSVFileProcessor:
    def __init__(self, file_path, has_header=True):
        self.file_path = file_path
        self.has_header = has_header

    def read_csv(self):
        """Reads the CSV and returns structured data."""
        try:
            with open(self.file_path, newline="", encoding="utf-8") as f:
                reader = csv.reader(f)

                if self.has_header:
                    header = next(reader, None)
                    return [
                        dict(zip(header, row)) 
                        for row in reader
                    ]
                else:
                    return [row for row in reader]
        except FileNotFoundError:
            print(f"[ERROR] File not found: {self.file_path}")
            return []
        except Exception as e:
            print(f"[ERROR] Failed to read CSV: {e}")
            return []

    def process_data(self, rows):
        """
        Modify this method as needed.
        Example: uppercase all strings.
        """
        processed = []

        for row in rows:
            if isinstance(row, dict):
                processed.append({
                    k: v.upper() if isinstance(v, str) else v
                    for k, v in row.items()
                })
            else:
                processed.append([
                    v.upper() if isinstance(v, str) else v
                    for v in row
                ])
        return processed

    def run(self):
        rows = self.read_csv()
        if not rows:
            return []
        return self.process_data(rows)
