project(common)

add_library(common
  configuration.cpp
  mmessage.cpp
  proto_utils.cpp
  service_utils.cpp)
target_link_libraries(common proto)

add_library(test_utils test_utils.cpp)
target_link_libraries(test_utils
  common
  connection
  module
  storage
  proto)