#pragma once

#include <vector>
#include <unordered_map>

#include "csv.hpp"
#include "column.h"

class Metadata;

class Data {
    public:
        Data(const std::string &csv_path, const bool header, const bool lazy_load);
        template<typename T>
        std::vector<T> get_column_data(const Column&);
    protected:
        std::tuple<ColumnType, int> get_column_index(const Column &);
        bool metadata_has(const std::string &col);
        void metadata_insert(const std::string &col, ColumnType col_type, int prefix_null);
        void insert_data(const std::string &col, csv::CSVField csv_value);
    private:
        void parse_data(std::vector<std::string> col);

        std::vector<std::vector<long long>> long_vector; 
        std::vector<std::vector<double>> double_vector;
        std::vector<std::vector<std::string>> string_vector;
        std::unordered_map<std::string, std::tuple<ColumnType, int>> metadata;

        bool lazy_load;
        csv::CSVReader csv_reader;
        std::vector<std::string> col_name;
        std::unordered_map<std::string, int> col_to_idx;
};

