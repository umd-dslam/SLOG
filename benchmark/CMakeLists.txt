project(benchmark)

add_library(benchmark workload_generator.cpp)

target_link_libraries(benchmark
  common
  module
  proto)