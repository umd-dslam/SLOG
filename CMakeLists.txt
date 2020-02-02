cmake_minimum_required(VERSION 3.12.4)
project(slog)

set(DEPENDENCIES_DIR ".dep")
list(APPEND CMAKE_PREFIX_PATH ${CMAKE_SOURCE_DIR}/${DEPENDENCIES_DIR})
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "-W -Wextra -Wall")
set(CMAKE_CXX_FLAGS_DEBUG "-g")

find_package(ZeroMQ REQUIRED)
include_directories(${ZeroMQ_INCLUDE_DIR})
find_package(cppzmq REQUIRED)
include_directories(${cppzmq_INCLUDE_DIR})

# Set this so that proto filenames do not have to be unique across directories
find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})
if (protobuf_VERBOSE)
  message(STATUS "Using Protocol Buffers ${Protobuf_VERSION}")
endif()

find_package(Glog REQUIRED)
include_directories(${GLOG_INCLUDE_DIRS})

find_package(gflags REQUIRED)
include_directories(${GFLAGS_INCLUDE_DIRS})

find_package(Readline REQUIRED)
include_directories(${READLINE_INCLUDE_DIR})

# Add root and build directory to include path. The build directory inclusion is
# necessary for protobuf
include_directories(${CMAKE_SOURCE_DIR})
include_directories(${CMAKE_BINARY_DIR})

# ================================== Executables ========================================
add_subdirectory(proto)
add_subdirectory(benchmark)
add_subdirectory(common)
add_subdirectory(connection)
add_subdirectory(module)
add_subdirectory(paxos)
set(SLOG_LIBS
  batch_log
  benchmark
  common
  connection
  module
  paxos
  proto)

# All libraries to be linked to executables
set(EXTERNAL_LIBS
  ${Protobuf_LIBRARIES}
  ${ZeroMQ_STATIC_LIBRARY}
  ${cppzmq_STATIC_LIBRARY}
  ${GLOG_LIBRARIES}
  ${GFLAGS_LIBRARIES}
  ${READLINE_LIBRARIES})

# Create an executable for the services
add_executable(slog service/slog.cpp)
target_link_libraries(slog ${SLOG_LIBS} ${EXTERNAL_LIBS})

add_executable(bm service/bm.cpp)
target_link_libraries(bm ${SLOG_LIBS} ${EXTERNAL_LIBS})

add_executable(client service/client.cpp)
target_link_libraries(client ${SLOG_LIBS} ${EXTERNAL_LIBS})

# =================================== Tests =============================================
find_package(GTest)
if (GTEST_FOUND)
  message(STATUS "GTest found. Tests will be built")
  include_directories(${GTEST_INCLUDE_DIRS})
  enable_testing()
  add_subdirectory(test)
else()
  message(STATUS "GTest not found. No test will be built")
endif()