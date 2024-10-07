#include "gtest/gtest.h"
#include "dataframe.h"
#include <filesystem>

TEST(TestDataFrame, TestReadCSVNoNull) {
    Data data("testdata/header_no_null.csv", true, false);
    std::vector<long long> expected_long = std::vector<long long>{12, 13, 14};
    std::vector<double> expected_double = std::vector<double>{3.5, 3.1, 2.5};
    std::vector<std::string> expected_string = std::vector<std::string>{"TT", "II", "ss"};

    std::vector<long long> result_long = data.get_column_data<long long>("age");
    std::vector<double> result_double = data.get_column_data<double>("gpa"); 
    std::vector<std::string> result_string = data.get_column_data<std::string>("name");

    ASSERT_EQ(result_long, expected_long);
    ASSERT_EQ(result_double, expected_double);
    ASSERT_EQ(result_string, expected_string);
}

TEST(TestDataFrame, TestReadCSVWithNull) {
    Data data("testdata/header_with_null_value.csv", true, false);
    std::vector<long long> expected_long = std::vector<long long>{12, 13, 0, 13, 14, 14};
    std::vector<double> expected_double = std::vector<double>{0.0, 3.1, 3.5, 0.0, 2.2, 0.0};
    std::vector<std::string> expected_string = std::vector<std::string>{"TT", "II", "TT", "", "", ""};

    std::vector<long long> result_long = data.get_column_data<long long>("age");
    std::vector<double> result_double = data.get_column_data<double>("gpa"); 
    std::vector<std::string> result_string = data.get_column_data<std::string>("name");

    ASSERT_EQ(result_long, expected_long);
    ASSERT_EQ(result_double, expected_double);
    ASSERT_EQ(result_string, expected_string);
}