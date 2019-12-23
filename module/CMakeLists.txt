project(module)

set(SOURCES
  client.cpp
  module.cpp
  server.cpp
)

add_library(module ${SOURCES})