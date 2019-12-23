project(common)

set(SOURCES
  configuration.cpp
  mmessage.cpp
  proto_utils.cpp
  test_utils.cpp
  service_utils.cpp
)

add_library(common ${SOURCES})