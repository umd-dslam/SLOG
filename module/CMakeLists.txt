project(module)

set(SOURCES
  basic_module.cpp
  forwarder.cpp
  module.cpp
  server.cpp
  sequencer.cpp
  scheduler.cpp
  workload_generator.cpp)

add_library(module ${SOURCES})

target_link_libraries(module
  common
  connection
  proto
  storage)