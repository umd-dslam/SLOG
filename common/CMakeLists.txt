project(common)

set(SOURCES
  configuration.cpp
  mmessage.cpp
  test_utils.cpp
)

add_library(common ${SOURCES})