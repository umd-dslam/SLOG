project(benchmark)

add_library(benchmark 
  commands.cpp
  workload_generator.cpp)

target_link_libraries(benchmark
  common
  module
  proto)