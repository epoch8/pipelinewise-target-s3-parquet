import os
import csv
import pyarrow as pa
from pyarrow.parquet import ParquetWriter
from abc import ABC



class FileHandler(ABC):
    def __init__(self, target) -> None:
        self.suffix = None
        self.target = target
        raise NotImplementedError

    def write_record_to_file(self, stream_name, filename, record):
        ...



class CSVFileHandler(FileHandler):
    def __init__(self, target):
        self.suffix = ".csv"
        self.target = target

    def write_record_to_file(self, stream_name, filename, record) -> None:
        file_is_empty = (not os.path.isfile(filename)) or os.stat(filename).st_size == 0
        delimiter = self.target.config.get('delimiter', ',')
        quotechar = self.target.config.get('quotechar', '"')
        if stream_name not in self.target.headers and not file_is_empty:
            with open(filename, 'r') as csvfile:
                reader = csv.reader(
                    csvfile,
                    delimiter=delimiter,
                    quotechar=quotechar
                    )
                first_line = next(reader)
                self.target.headers[stream_name] = first_line if first_line else record.keys()
        else:
            self.target.headers[stream_name] = record.keys()
        with open(filename, 'a') as csvfile:
            writer = csv.DictWriter(
                csvfile,
                self.target.headers[stream_name],
                extrasaction='ignore',
                delimiter=delimiter,
                quotechar=quotechar
                )
            if file_is_empty:
                writer.writeheader()

            writer.writerow(record)


class ParquetFileHandler(FileHandler):
    def __init__(self, target):
        self.suffix = ".parquet"
        self.target = target
        
    def write_record_to_file(self, stream_name, filename, record):
        dataframe = self.create_dataframe(record)
        with open(filename, "wb") as f:
            ParquetWriter(
                f, dataframe.schema
            ).write_table(dataframe)
        # explicit memory management. This can be usefull when working on very large data groups
        del dataframe
    
    @staticmethod
    def create_dataframe(list_dict):
        fields = set()
        for d in list_dict:
            fields = fields.union(d.keys())
        dataframe = pa.table({f: [row.get(f) for row in list_dict] for f in fields})
        return dataframe