project(module)

set(SOURCES
  client.cpp
  basic_module.cpp
  forwarder.cpp
  module.cpp
  server.cpp
)

add_library(module ${SOURCES})

target_link_libraries(module
    common
    connection
    proto
    storage)