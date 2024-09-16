#include "aggregate.h"
#include "column.h"

template <typename T>
Aggregate<T>::Aggregate(Column agg_col, AggOp agg_op) {
    this->col = agg_col;
    this->n_element = 0;
    this->agg_val = 0;
    this->op = agg_op;
}

template <typename T>
void Aggregate<T>::execute(T value) {
    n_element++;
    switch (op) {
    case AggOp::Sum:
        agg_val += value;
        break;
    case AggOp::Max:
        if (n_element == 1) {
            agg_val = value;
        } else {
            agg_val = std::max<T>(agg_val, value);
        }
        break;
    case AggOp::Min:
        if (n_element == 1) {
            agg_val = value;
        } else {
            agg_val = std::min<T>(agg_val, value);
        }
        break;
    case AggOp::Avg:
        agg_val += value;
        break;
    case AggOp::Count:
        agg_val++; 
        break;
    }
}

template <typename T>
T Aggregate<T>::get_stat()
{
    switch (op) {
    case AggOp::Sum:
    case AggOp::Max:
    case AggOp::Min:
    case AggOp::Count:
        return agg_val;
    case AggOp::Avg:
        return agg_val/(double)n_element; 
    }
}

template <typename T>
Column Aggregate<T>::get_column()
{
    return col;
}
