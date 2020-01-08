project(module)

add_library(base_module 
  base/basic_module.cpp
  base/module.cpp)

target_link_libraries(base_module
  common
  connection
  proto)

add_library(scheduler_components
  scheduler_components/deterministic_lock_manager.cpp
  scheduler_components/worker.cpp)

target_link_libraries(scheduler_components common)

add_library(module
  forwarder.cpp
  orderer.cpp
  server.cpp
  sequencer.cpp
  scheduler.cpp)

target_link_libraries(module
  base_module
  scheduler_components
  storage)