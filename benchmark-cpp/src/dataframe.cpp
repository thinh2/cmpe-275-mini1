#include "dataframe.h"
#include "column.h"

#include <tuple>

using std::get;

Data::Data(const std::string &csv_path, const bool header, const bool lazy_load): lazy_load(lazy_load), long_vector{}, double_vector{}, string_vector{}, metadata{}, csv_reader(csv::CSVReader(csv_path)) {
    bool first = true;

    std::unordered_map<std::string, size_t> null_prefix;
    csv::CSVFormat format;
    if (header) {
        format.header_row(0);
    } else {
        format.no_header();
    }

    // init long_vector/string_vector
    // Make it work for the happy path: the first row is non-null, we can derive
    // all the value type from it
    csv_reader = csv::CSVReader(csv_path, format);
    col_name = csv_reader.get_col_names(); // convert it to unordered_map?
    for (int i = 0; i < col_name.size(); i++) {
        col_to_idx[col_name[i]] = i; 
    }
    if (lazy_load) {
        return;
    } else {
        parse_data(col_name);
    }
    // TODO: try to remove the unordered_map<string, ...> to vector<tuple>
}

void Data::parse_data(std::vector<std::string> col) {
    int n_row = 0;

    std::vector<int> col_idx;
    for (auto val: col) {
        if (col_to_idx.find(val) == col_to_idx.end()) {
            throw std::runtime_error("column does not exist");
        }
        col_idx.push_back(col_to_idx[val]);
    }

    std::chrono::steady_clock::time_point begin_time = std::chrono::steady_clock::now();
    std::cout << "Start reading data" << std::endl;

    // convert vector<string> col to get the index value?
    for (csv::CSVRow& row: csv_reader) {
        for (int i = 0; i < col_idx.size(); i++) {
            //std::cerr << "col_idx: " << col_idx[i] << " col: " << col[i] << std::endl;
            csv::CSVField field = row[col_idx[i]];
            if (!metadata_has(col[i])) {
                if (field.is_float()) {
                    metadata_insert(col[i], ColumnType::Double, n_row);
                } else if (field.is_int()) {
                    metadata_insert(col[i], ColumnType::Int, n_row);
                } else if (field.is_str()) {
                    metadata_insert(col[i], ColumnType::String, n_row);
                }
            }

            if (metadata_has(col[i])) {
                insert_data(col[i], field);
            }
        }
        n_row++;
        if (n_row % 100000 == 0) {
            std::chrono::steady_clock::time_point end_time = std::chrono::steady_clock::now();
            auto tookSeconds = std::chrono::duration_cast<std::chrono::seconds>(end_time - begin_time).count();
            std::cout << "processed " << n_row << " rows, took " << tookSeconds << " seconds" << std::endl;
        }
    }

    std::chrono::steady_clock::time_point end_time = std::chrono::steady_clock::now();
    auto tookMins = std::chrono::duration_cast<std::chrono::minutes>(end_time - begin_time).count();
    std::cout << "processed data: " << n_row << " rows, took " << tookMins << " mins" << std::endl;

    //TODO: reset csv_reader
}

std::tuple<ColumnType, int> Data::get_column_index(const Column &col)
{
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
    if (lazy_load) {
        parse_data({column});
    }

    std::tuple<ColumnType, int> col_info = get_column_index(column);
    if (std::get<ColumnType>(col_info) != ColumnType::Int) {
        throw std::runtime_error("not a int column");
    }
    return long_vector[std::get<int>(col_info)];
}

template<>
std::vector<double> Data::get_column_data<double>(const Column& column) {
    if (lazy_load) {
        parse_data({column});
    }

    std::tuple<ColumnType, int> col_info = get_column_index(column);
    if (std::get<ColumnType>(col_info) != ColumnType::Double) {
        throw std::runtime_error("not a double column");
    }
    return double_vector[std::get<int>(col_info)];
}

template<>
std::vector<std::string> Data::get_column_data<std::string>(const Column& column) {
    if (lazy_load) {
        parse_data({column});
    }

    std::tuple<ColumnType, int> col_info = get_column_index(column);
    if (std::get<ColumnType>(col_info) != ColumnType::String) {
        throw std::runtime_error("not a string column");
    }
    return string_vector[std::get<int>(col_info)];
}
