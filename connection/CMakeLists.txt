project(connection)

set(SOURCES
  broker.cpp
  channel.cpp
  mmessage.cpp
)

add_library(connection ${SOURCES})