#include "filter.h"
#include "column.h"

template<typename T>
Filter<T>::Filter(T value, FilterOp op, Column col) {
    this->cmp_val = value;
    this->op = op;
    this->col = col;
}

template <typename T>
bool Filter<T>::execute(T val)
{
    switch (op) {
    case FilterOp::Gt:
        return val > cmp_val;
    case FilterOp::Lt:
        return val < cmp_val; 
    case FilterOp::Eq:
        return (val == cmp_val);
    case FilterOp::Gte:
        return val >= cmp_val;
    case FilterOp::Lte:
        return val <= cmp_val;
    }
}

template <typename T>
Column Filter<T>::get_column()
{
    return col;
}
