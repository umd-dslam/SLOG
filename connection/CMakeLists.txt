project(connection)

set(SOURCES
  broker.cpp
  channel.cpp)

add_library(connection ${SOURCES})

target_link_libraries(connection 
  common
  proto)