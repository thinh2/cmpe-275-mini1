#pragma once

#include "column.h"

enum class FilterOp {
    Gt, Lt, Gte, Lte, Eq
};

template<typename T>
class Filter {
    public:
        Filter(T value, FilterOp op, Column col);
        bool execute(T value);
        Column get_column();
    private:
        T cmp_val;
        FilterOp op;
        Column col;
};
