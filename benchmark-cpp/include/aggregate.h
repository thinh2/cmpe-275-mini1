#pragma once

#include "column.h"

enum class AggOp {
    Sum, Avg, Max, Min, Count, 
};

template<typename T>
class Aggregate {
    public:
        Aggregate(Column agg_col, AggOp agg_op);
        void execute(T value);
        T get_stat();
        Column get_column();
    private:
        AggOp op;
        T agg_val;
        int n_element;
        Column col;
};
