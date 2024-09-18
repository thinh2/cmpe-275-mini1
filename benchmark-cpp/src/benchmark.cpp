#include "aggregate.h"
#include "filter.h"
#include "dataframe.h"
#include "benchmark.h"

template<typename T>
void query_aggregation(Data &input, Aggregate<T> agg) {
    std::vector<T> column_data = input.get_column_data<T>(agg.get_column());
    for (auto ele : column_data) {
        agg.execute(ele);
    }
    std::cout << "agg_output" << " " << agg.get_stat() << std::endl;
}

template<typename T>
void query_filter(Data &input, Filter<T> filter) {
    std::chrono::steady_clock::time_point begin_time = std::chrono::steady_clock::now();
    int count = 0;
    std::vector<T> column_data = input.get_column_data<T>(filter.get_column());
    for (int i = 0; i < column_data.size(); i++) {
        if (filter.execute(column_data[i])) {
            count++;
        }
    }
    std::cout << "there are " << count << " rows satisfy the filter" << std::endl;
    std::chrono::steady_clock::time_point end_time = std::chrono::steady_clock::now();
    auto tookMs = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - begin_time).count();
    std::cout << "query_filter took " << tookMs <<  "ms" << std::endl; 
}

template<typename T, typename U>
void query_aggregation_filter(const Data &input, Aggregate<T> agg, Filter<U> filter) {

}
