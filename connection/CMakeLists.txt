project(connection)

add_library(connection broker.cpp sender.cpp)

target_link_libraries(connection 
  common
  proto)