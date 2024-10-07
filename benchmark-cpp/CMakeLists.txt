cmake_minimum_required(VERSION 3.30)

project(benchmark)

set(CMAKE_CXX_STANDARD 17)
#set(CMAKE_CXX_FLAGS "-fopenmp -lprofiler -L /opt/homebrew/Cellar/gperftools/2.16/lib/")
#set(CMAKE_CXX_COMPILER "/opt/homebrew/bin/gcc-14")

set(CMAKE_CXX_FLAGS "-fopenmp")

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)
FetchContent_MakeAvailable(googletest)

FetchContent_Declare(
  json 
  URL https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz
)
FetchContent_MakeAvailable(json)

FetchContent_Declare(
  csv  
  GIT_REPOSITORY https://github.com/vincentlaucsb/csv-parser.git
  GIT_SHALLOW TRUE 
  GIT_TAG 2.2.1
) 

FetchContent_MakeAvailable(csv)

add_library(core STATIC
  src/templates.cpp
  src/aggregate.cpp
  src/filter.cpp
  src/dataframe.cpp
  src/benchmark.cpp
)
target_link_libraries(core csv)

include_directories(include)
enable_testing()

add_executable(
  unit_test
  test/aggregate.cpp
  test/filter.cpp
  test/dataframe.cpp
  test/benchmark.cpp
)
target_include_directories(unit_test PRIVATE csv)

target_link_libraries(
  unit_test
  GTest::gtest_main
  core
)


include(GoogleTest)
gtest_discover_tests(unit_test)

add_executable(
  benchmark
  benchmark/benchmark.cpp
)

target_link_libraries(
  benchmark
  nlohmann_json::nlohmann_json
  core
)
