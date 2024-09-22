import csv
from core.filter import Filter
from datetime import datetime
import time

class Data:
    def __init__(self, data_path, has_header=True):
        self.data_path = data_path
        self.has_header = has_header
        self.read_data()
    
    def read_data(self):
        with open(self.data_path, "r") as f:
            start_time = datetime.now() 
            reader = csv.reader(f)
            self.column_data = {}
            processed_row = 0

            if self.has_header:
                self.header = next(reader, None)
                for col_name in self.header:
                    self.column_data[col_name] = []
            
            for row in reader:
                for h, v in zip(self.header, row):
                    if not v:
                        self.column_data[h].append(None)
                        continue

                    value = v
                    try:
                        value = int(value)
                    except ValueError as e:
                        try:
                            value = float(value)
                        except Exception as e:
                            value = v

                    self.column_data[h].append(value)
                    
                processed_row += 1
                if processed_row % 100000 == 0:
                    print(f"Processed {processed_row} row, took {(datetime.now() - start_time).total_seconds()} s")

            print(f"Processed {processed_row} row, took {(datetime.now() - start_time).total_seconds()} s")

    def filter(self, filter_query: Filter):
        if not filter_query.col in self.column_data:
            raise "filter column does not exist"

        data = self.column_data[filter_query.col]
        counter = 0
        for val in data:
            if val and filter_query.execute(val) == True:
                counter += 1

        return counter