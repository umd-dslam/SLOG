project(benchmark)

add_library(benchmark 
  stored_procedures.cpp
  workload_generator.cpp)

target_link_libraries(benchmark
  common
  module
  proto)