import sys
from core import Data, Filter, FilterOp
from datetime import datetime 


'''
void registration_state_filter(Data &data) {
    Filter<std::string> filter("CA", FilterOp::Eq, "Registration State");
    query_filter<std::string>(data, filter);
    std::cout << "Finish" << std::endl;
}
'''

###
def registration_state_filter(data: Data):
    start_time = datetime.now()
    filter_query = Filter("Registration State", FilterOp.EQ, "CA")

    result = data.filter(filter_query)
    print(f"There are {result} records satisfy the filter")

    end_time = datetime.now()
    print(f"Query tooks {(end_time - start_time).total_seconds() * 10**3} ms")

if __name__ == '__main__':
    print(sys.argv[1])
    data = Data(sys.argv[1])
    registration_state_filter(data)
