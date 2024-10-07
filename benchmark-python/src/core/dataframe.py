import csv
from core.filter import Filter
from datetime import datetime
from itertools import batched
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from multiprocessing.managers import SharedMemoryManager 

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

    def filter_multithread(self, filter_query: Filter):
        if not filter_query.col in self.column_data:
            raise "filter column does not exist"

        n_thread = 5
        data = self.column_data[filter_query.col]
        counter = 0

        with ThreadPoolExecutor(max_workers=n_thread) as executor:
            futures = []
            for batch in batched(data, n_thread):
                futures.append(executor.submit(filter_internal, filter_query, batch))
            
            for future in futures:
                counter += future.result()  

        return counter
    
    def filter_multiprocess_no_shared_memory(self, filter_query: Filter):
        if not filter_query.col in self.column_data:
            raise "filter column does not exist"

        n_thread = 5
        data = self.column_data[filter_query.col]
        counter = 0

        with ProcessPoolExecutor(max_workers=n_thread) as executor:
            futures = []
            for batch in batched(data, n_thread):
                futures.append(executor.submit(filter_internal, filter_query, batch))
            
            for future in futures:
                counter += future.result()  

        return counter
     
    def filter_multiprocess_share_memory(self, filter_query: Filter):
        if not filter_query.col in self.column_data:
            raise "filter column does not exist"

        n_process = 1
        data = self.column_data[filter_query.col]
        counter = 0

        smm = SharedMemoryManager()
        smm.start()
        sl = smm.ShareableList(data)

        with ProcessPoolExecutor(max_workers=n_process) as executor:
            futures = []
            default_partition_sz = int(len(data)/n_process)
            start_idx = 0
            while start_idx < len(data):
                if (start_idx + default_partition_sz < len(data)):
                    futures.append(executor.submit(filter_partition_data, filter_query, sl, start_idx, default_partition_sz))
                    start_idx += default_partition_sz
                else:
                    sz = len(data) - start_idx
                    futures.append(executor.submit(filter_partition_data, filter_query, sl, start_idx, sz))
                    break

            for future in futures:
                counter += future.result() 

        smm.shutdown()
        return counter

def filter_internal(filter_query, data):
    res = 0
    for val in data:
        if val and filter_query.execute(val) == True:
            res += 1
    return res

def filter_partition_data(filter_query, share_list, start_idx, sz):
    res = 0
    for i in range(start_idx, start_idx + sz):
        if share_list[i] and filter_query.execute(share_list[i]) == True:
            res += 1
    return res
