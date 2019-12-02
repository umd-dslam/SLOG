cmake_minimum_required(VERSION 3.13.4)
project(slog)

set(CMAKE_CXX_FLAGS "-std=c++14 -W -Wextra -Wall")

# Add root directory to include path
include_directories(${PROJECT_SOURCE_DIR})

# All sublibraries
add_subdirectory(storage)

set(LIBS
  storage)

# Create an executable for the server
add_executable(slog_server machine/server.cpp)
# Link to libraries
target_link_libraries(slog_server ${LIBS})

enable_testing()
find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})

# Add tests
file(GLOB_RECURSE TEST_SOURCES RELATIVE ${PROJECT_SOURCE_DIR} test/*.cpp)
set(TEST_OUTPUT_DIR ${CMAKE_BINARY_DIR}/test)
foreach(TEST ${TEST_SOURCES})
  # Extract file names (w/o extension)
  get_filename_component(TEST_NAME ${TEST} NAME_WE)
  # Set compile target
  add_executable(${TEST_NAME} ${TEST})

  #link to libraries
  target_link_libraries(${TEST_NAME} ${LIBS} GTest::GTest GTest::Main)

  # move tests to 'test' folder and add to test execution
  set_target_properties(${TEST_NAME} PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${TEST_OUTPUT_DIR})
  add_test(
    NAME ${TEST_NAME}
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    COMMAND ${TEST_OUTPUT_DIR}/${TEST_NAME})

endforeach(TEST)