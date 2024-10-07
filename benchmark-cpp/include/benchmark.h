#pragma once

#include "dataframe.h"
#include "aggregate.h"
#include "filter.h"

template<typename T>
void query_aggregation(Data &input, Aggregate<T> agg);

template<typename T>
void query_filter(Data &input, Filter<T> filter);

template<typename T>
void query_filter_multithread(Data &input, Filter<T> filter);