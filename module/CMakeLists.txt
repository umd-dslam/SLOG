project(module)

add_library(base_module 
  base/basic_module.cpp
  base/module.cpp)

target_link_libraries(base_module
  common
  connection
  proto)

add_library(module
  forwarder.cpp
  orderer.cpp
  server.cpp
  sequencer.cpp
  scheduler.cpp)

target_link_libraries(module
  base_module
  storage)