#include "benchmark.h"
#include "dataframe.h"
#include "filter.h"
#include "nlohmann/json.hpp"

#include <string>
#include <fstream>

using json = nlohmann::json;

void registration_state_filter_single_thread(Data &data) {
    Filter<std::string> filter("CA", FilterOp::Eq, "Registration State");
    query_filter<std::string>(data, filter);
    std::cout << "Finish" << std::endl;
}

void registration_state_filter_multithread(Data &data) {
    Filter<std::string> filter("CA", FilterOp::Eq, "Registration State");
    query_filter_multithread<std::string>(data, filter);
    std::cout << "Finish" << std::endl;
}

Filter<std::string> build_filter(json &cfg) {
    FilterOp op;
    if (cfg["filter"]["op"] == "eq") op = FilterOp::Eq;
    return Filter<std::string>(cfg["filter"]["val"], op, cfg["filter"]["col"]);
}

int main(int argc, char* argv[]) {
    std::cout << "config path: " << argv[1] << std::endl;
    std::ifstream f(argv[1]);
    json config = json::parse(f);
    
    bool lazy_load = false;
    if (config.contains("data_mode") && config["data_mode"] == "lazy-load") {
        lazy_load = true;
    }
    Data data(config["data_path"], true, lazy_load);

    auto filter = build_filter(config);

    if (config["filter_mode"] == "single-thread") {
        query_filter<std::string>(data, filter);
    }
    if (config["filter_mode"] == "multi-thread") {
        query_filter_multithread<std::string>(data, filter);
    }

    return 0;
}