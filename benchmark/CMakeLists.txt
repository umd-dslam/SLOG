project(benchmark)

add_library(benchmark basic_workload.cpp)
target_link_libraries(benchmark
  common
  module
  proto)
