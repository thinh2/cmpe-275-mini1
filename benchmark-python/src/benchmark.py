import sys
from core import Data, Filter, FilterOp
from datetime import datetime 
from enum import Enum
import json
'''
void registration_state_filter(Data &data) {
    Filter<std::string> filter("CA", FilterOp::Eq, "Registration State");
    query_filter<std::string>(data, filter);
    std::cout << "Finish" << std::endl;
}
'''

###
class ProcessingType(Enum):
    SingleThread = 1
    MultiThread = 2
    MultiProcess = 3
    MultiProcessShareMemory = 4

def query(data: Data, process_type: ProcessingType, filter_query: Filter):
    start_time = datetime.now()

    match process_type:
        case ProcessingType.SingleThread:
            result = data.filter(filter_query)
        case ProcessingType.MultiThread:
            result = data.filter_multithread(filter_query)
        case ProcessingType.MultiProcess:
            result = data.filter_multiprocess_no_shared_memory(filter_query)
        case ProcessingType.MultiProcessShareMemory:
            result = data.filter_multiprocess_share_memory(filter_query)
        
    print(f"There are {result} records satisfy the filter")

    end_time = datetime.now()
    print(f"Query tooks {(end_time - start_time).total_seconds() * 10**3} ms")

def build_filter(cfg): 
    match cfg["op"]:
        case "eq":
            op = FilterOp.EQ
        case "gt":
            op = FilterOp.GT
        case "lt":
            op = FilterOp.LT
        case "lte":
            op = FilterOp.LTE
        case "gte":
            op = FilterOp.GTE
    return Filter(cfg["col"], op, cfg["val"])

if __name__ == '__main__':
    print(sys.argv[1])
    with open(sys.argv[1], 'r') as f:
        config = json.load(f)
    print(config)
    data = Data(config["data_path"])
    filter_query = build_filter(config["filter"])
    
    match config["filter_mode"]:
        case "single-thread":
            filter_mode = ProcessingType.SingleThread
        case "multi-thread":
            filter_mode = ProcessingType.MultiThread
        case "multi-process-no-share-memory":
            filter_mode = ProcessingType.MultiProcess
        case "multi-process-share-memory":
            filter_mode = ProcessingType.MultiProcessShareMemory
        
    query(data, filter_mode, filter_query)
