project(workload)

add_library(workload 
  basic_workload.cpp
  single_machine_workload.cpp
  remastering_workload.cpp)
target_link_libraries(workload
  module
  offline_data_reader
  proto)
