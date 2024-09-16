#include "gtest/gtest.h"
#include "filter.h"

TEST(TestFilter, FilterGreaterThan) {
    std::vector<int> arr{3, 4, 5, 1};
    std::vector<bool> expected_result{true, true, true, false};
    std::vector<bool> output;
    Filter<int> filter(2, FilterOp::Gt, "tmp"); 
    for (auto ele: arr) {
       output.push_back(filter.execute(ele)); 
    }
    ASSERT_EQ(output, expected_result);
}