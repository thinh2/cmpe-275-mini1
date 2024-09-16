#pragma once

#include <vector>

#include "dataframe.h"

class Select {
    public:
        Select(std::vector<std::string> &columns);
        std::vector<std::string> get_columns();
        void execute(Data &input);
    private:
        std::vector<std::string> columns;
};

