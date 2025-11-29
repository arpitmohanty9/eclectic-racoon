import csv
from typing import List, Dict, Union

class CSVProcessor:

    """
    Reusable CSV Processor class
    Override transform_row for custom logic
    """

    def __init__(self, has_header: bool= False):
        self.has_header = has_header
    
    def read_csv(self,path:str) -> List[Union[Dict[str, str],List[str]]]:
        """
        Read CSV and return a List of Dict(if header is present) or Lists
        """
        # read csv the file and create a list or  dictionary if there is a header
        rows = []
        with open(path, newline="", encoding="utf-8") as csvfile:
            if self.has_header:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    rows.append(row)
            
            else:
                reader = csv.reader(csvfile)
                for row in reader:
                    rows.append(row)
            
        return rows    

    def transform_row(self,row: Union[List[str],Dict[str,str]]):
        """
        Default transformation: uppercase string values.
        Override in subclasses for custom behavior.
        """
        # transform row , method to take the list of dictionary and do any custom Transform on them.
        if isinstance(row,Dict):
            return {k : v.upper() if isinstance(v,str) else v for k,v in row.items()}
        else:
            return [v.upper() if isinstance(v,str) else v for v in row]


    def process(self,path: str) -> List[Union[Dict[str, str], List[str]]]:
        """
        Full process pipeline: read -> transform -> return processed rows.
        """
        rows = self.read_csv(path)
        return [self.transform_row(r) for r in rows]
    
    def write_csv(self, path: str, rows: List[Union[Dict[str, str], List[str]]]):
        """
        Writes processed rows to the given path.
        If rows are dicts, write header inferred from first row keys.
        """
        if not rows:
            return

        with open(path, "w", newline="", encoding="utf-8") as f:
            if isinstance(rows[0], dict):
                fieldnames = list(rows[0].keys())
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for r in rows:
                    writer.writerow(r)
            else:
                writer = csv.writer(f)
                for r in rows:
                    writer.writerow(r)