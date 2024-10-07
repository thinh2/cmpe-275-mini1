#include "gtest/gtest.h"

#include "benchmark.h"
#include "dataframe.h"

TEST(TestBenchmark, TestAggregationBench) {
    Data data("/Users/thinhbui/Thinh/Learning/sjsu/fall-2024/cmpe-275/mini-1/benchmark-2/test/testdata/header_no_null.csv", true, false);
    Aggregate<long long> agg("age", AggOp::Sum);
    query_aggregation<long long>(data, agg); 
}

TEST(TestBenchmark, TestFilterBench) {
    Data data("/Users/thinhbui/Thinh/Learning/sjsu/fall-2024/cmpe-275/mini-1/benchmark-2/test/testdata/header_no_null.csv", true, false);
    Filter<long long> filter(12, FilterOp::Gt, "age");
    query_filter<long long>(data, filter); 
}