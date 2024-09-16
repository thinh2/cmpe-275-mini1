#include "dataframe.h"
#include "column.h"

#include <tuple>

using std::get;

Data::Data(const std::string &csv_path, const bool header) {
    bool first = true; 

    std::unordered_map<std::string, size_t> null_prefix;
    csv::CSVFormat format;
    int n_row = 0;
    if (header) {
        format.header_row(0);
    } else {
        format.no_header();
    }

    // Make it work for the happy path: the first row is non-null, we can derive
    // all the value type from it
    csv::CSVReader reader(csv_path, format);

    std::vector<std::string> col_name = reader.get_col_names();
    for (csv::CSVRow& row: reader) {
        for (int i = 0; i < row.size(); i++) {
            if (!metadata_has(col_name[i])) {
                if (row[i].is_float()) {
                    metadata_insert(col_name[i], ColumnType::Double, n_row);
                } else if (row[i].is_int()) {
                    metadata_insert(col_name[i], ColumnType::Int, n_row);
                } else if (row[i].is_str()) {
                    metadata_insert(col_name[i], ColumnType::String, n_row);
                }
            }

            if (metadata_has(col_name[i])) {
                insert_data(col_name[i], row[i]);
            }
        }
        n_row++;
    }
}

std::tuple<ColumnType, int> Data::get_column_index(const Column & col) {
    if (metadata.find(col) == metadata.end()) {
        throw std::runtime_error("column metadata does not exist");
    }

    return metadata[col];
}

bool Data::metadata_has(const std::string &col) { 
    return metadata.find(col) != metadata.end();
}

void Data::metadata_insert(const std::string &col, ColumnType col_type, int prefix_null) {
    switch (col_type) {
    case ColumnType::Int:
        metadata[col] = std::make_tuple(ColumnType::Int, long_vector.size());
        long_vector.push_back(std::vector<long long>(prefix_null, 0));
        break;
    case ColumnType::Double:
        metadata[col] = std::make_tuple(ColumnType::Double, double_vector.size());
        double_vector.push_back(std::vector<double>(prefix_null, 0.0));
        break;
    case ColumnType::String:
        metadata[col] = std::make_tuple(ColumnType::String, string_vector.size());
        string_vector.push_back(std::vector<std::string>(prefix_null, ""));
        break;
    }
}

void Data::insert_data(const std::string &col, csv::CSVField csv_value) {
    std::tuple<ColumnType, int> col_info = get_column_index(col);
    switch (std::get<ColumnType>(col_info)) {
    case ColumnType::Double: {
        double double_value = 0.0;
        if (csv_value.is_float()) {
           double_value = csv_value.get<double>(); 
        }
        double_vector[std::get<int>(col_info)].push_back(double_value);
        break;
    }
    case ColumnType::Int: {
        long long long_value = 0;
        if (csv_value.is_int()) {
            long_value = csv_value.get<long long>();
        }
        long_vector[std::get<int>(col_info)].push_back(long_value);
        break;
    }
    case ColumnType::String: {
        std::string string_value = "";
        if (csv_value.is_str()) {
            string_value = csv_value.get<std::string>();
        }
        string_vector[std::get<int>(col_info)].push_back(string_value);
        break;
    }
    }
}

template<>
std::vector<long long> Data::get_column_data(const Column& column) {
    std::tuple<ColumnType, int> col_info = get_column_index(column);
    if (std::get<ColumnType>(col_info) != ColumnType::Int) {
        throw std::runtime_error("not a int column");
    }
    return long_vector[std::get<int>(col_info)];
}

template<>
std::vector<double> Data::get_column_data<double>(const Column& column) {
    std::tuple<ColumnType, int> col_info = get_column_index(column);
    if (std::get<ColumnType>(col_info) != ColumnType::Double) {
        throw std::runtime_error("not a double column");
    }
    return double_vector[std::get<int>(col_info)];
}

template<>
std::vector<std::string> Data::get_column_data<std::string>(const Column& column) {
    std::tuple<ColumnType, int> col_info = get_column_index(column);
    if (std::get<ColumnType>(col_info) != ColumnType::String) {
        throw std::runtime_error("not a string column");
    }
    return string_vector[std::get<int>(col_info)];
}
