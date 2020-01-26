project(common)

add_library(common
  configuration.cpp
  mmessage.cpp
  proto_utils.cpp
  service_utils.cpp
  types.h)
target_link_libraries(common proto)

add_library(async_log async_log.cpp)
target_link_libraries(async_log proto)

add_library(test_utils test_utils.cpp)
target_link_libraries(test_utils
  common
  connection
  module
  proto)