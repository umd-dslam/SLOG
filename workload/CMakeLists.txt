project(workload)

add_library(workload basic_workload.cpp)
target_link_libraries(workload
  common
  module
  proto)
