cmake_minimum_required(VERSION 3.13.4)
project(slog)

set(DEPENDENCIES_DIR ".dep")
set(CMAKE_PREFIX_PATH ${CMAKE_SOURCE_DIR}/${DEPENDENCIES_DIR})
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

find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})

find_package(Glog REQUIRED)
include_directories(${GLOG_INCLUDE_DIRS})

find_package(gflags REQUIRED)
include_directories(${GFLAGS_INCLUDE_DIRS})

# Add root and build directory to include path. The build directory inclusion is
# necessary for protobuf
include_directories(${CMAKE_SOURCE_DIR})
include_directories(${CMAKE_BINARY_DIR})

# ================================== Server =============================================

# Proto needs to be added before everything else so that protobuf gets to generate files
# needed in subsequent libraries
add_subdirectory(benchmark)
add_subdirectory(common)
add_subdirectory(connection)
add_subdirectory(module)
add_subdirectory(paxos)
add_subdirectory(proto)
add_subdirectory(storage)
set(SLOG_LIBS
  benchmark
  common
  connection
  module
  paxos
  proto
  storage)

# All libraries to be linked to executables
set(EXTERNAL_LIBS
  ${Protobuf_LIBRARIES}
  ${ZeroMQ_STATIC_LIBRARY}
  ${cppzmq_STATIC_LIBRARY}
  ${GLOG_LIBRARIES}
  ${GFLAGS_LIBRARIES})

# Create an executable for the services
add_executable(slog service/slog.cpp)
target_link_libraries(slog ${SLOG_LIBS} ${EXTERNAL_LIBS})

add_executable(bm service/bm.cpp)
target_link_libraries(bm ${SLOG_LIBS} ${EXTERNAL_LIBS})

# =================================== Tests =============================================

enable_testing()
add_subdirectory(test)