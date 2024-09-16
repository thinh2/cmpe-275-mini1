#include "gtest/gtest.h"
#include "aggregate.h"

TEST(TestAggregate, AggregateSum) {
    std::vector<int> arr{1, 2, 3, 4, 5};
    Aggregate<int> agg("test", AggOp::Sum);
    for (auto ele : arr) {
        agg.execute(ele);
    }
    ASSERT_EQ(agg.get_stat(), 15);
}

TEST(TestAggregate, AggregateMax) {
    std::vector<int> arr{3, 1, 4, 2};
    Aggregate<int> agg("test", AggOp::Max);
    for (auto ele: arr) {
        agg.execute(ele);
    }
    ASSERT_EQ(agg.get_stat(), 4);
}