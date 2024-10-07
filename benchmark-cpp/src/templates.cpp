#include "aggregate.h"
#include "aggregate.cpp"

#include "filter.h"
#include "filter.cpp"

#include "benchmark.h"
#include "benchmark.cpp"

typedef long long ll;
template class Aggregate<int>;
template class Aggregate<ll>;
template class Aggregate<double>;

template class Filter<int>;
template class Filter<ll>;
template class Filter<double>;
template class Filter<std::string>;

template void query_aggregation<long long>(Data &input, Aggregate<long long> agg);
template void query_aggregation<double>(Data &input, Aggregate<double> agg);
//template void query_aggregation<std::string>(Data &input, Aggregate<std::string> agg);

template void query_filter<long long>(Data &input, Filter<long long> filter);
template void query_filter<double>(Data &input, Filter<double> filter);
template void query_filter<std::string>(Data &input, Filter<std::string> filter);

template void query_filter_multithread<long long>(Data &input, Filter<long long> filter);
template void query_filter_multithread<double>(Data &input, Filter<double> filter);
template void query_filter_multithread<std::string>(Data &input, Filter<std::string> filter);