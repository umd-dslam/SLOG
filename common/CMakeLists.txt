project(common)

add_library(common
  configuration.cpp
  mmessage.cpp
  proto_utils.cpp
  service_utils.cpp
  types.h)
target_link_libraries(common proto)

add_library(batch_log batch_log.cpp)
target_link_libraries(batch_log proto)

add_library(offline_data_reader offline_data_reader.cpp)
target_link_libraries(offline_data_reader proto)

add_library(test_utils test_utils.cpp)
target_link_libraries(test_utils
  common
  connection
  module
  proto)