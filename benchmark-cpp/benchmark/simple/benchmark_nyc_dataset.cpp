#include "benchmark.h"
#include "dataframe.h"
#include "filter.h"

#include <string>

void registration_state_filter(Data &data) {
    Filter<std::string> filter("CA", FilterOp::Eq, "Registration State");
    query_filter<std::string>(data, filter);
    std::cout << "Finish" << std::endl;
}

int main(int argc, char* argv[]) {
    std::cout << argv[1] << std::endl;
    Data data(argv[1], true);
    registration_state_filter(data);
}