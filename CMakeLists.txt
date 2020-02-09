cmake_minimum_required(VERSION 3.12.4)
project(slog)

#========================================
#               Options
#========================================

option(BUILD_CLIENT "Build the client" ON)


#========================================
#               Setup
#========================================

set(DEPENDENCIES_DIR ".deps")
list(APPEND CMAKE_PREFIX_PATH ${CMAKE_SOURCE_DIR}/${DEPENDENCIES_DIR})
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "-W -Wextra -Wall")
set(CMAKE_CXX_FLAGS_DEBUG "-g")

find_package(ZeroMQ REQUIRED)
include_directories(${ZeroMQ_INCLUDE_DIR})
find_package(cppzmq REQUIRED)
include_directories(${cppzmq_INCLUDE_DIR})

SET(Protobuf_USE_STATIC_LIBS ON)
find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})
if (protobuf_VERBOSE)
  message(STATUS "Using Protocol Buffers ${Protobuf_VERSION}")
endif()

find_package(Glog REQUIRED)
include_directories(${GLOG_INCLUDE_DIR})

find_package(gflags REQUIRED)
include_directories(${GFLAGS_INCLUDE_DIRS})

if (BUILD_CLIENT)
  find_package(Readline REQUIRED)
  include_directories(${READLINE_INCLUDE_DIR})
endif()

# Add root and build directory to include path. The build directory inclusion is
# necessary for protobuf
include_directories(${CMAKE_SOURCE_DIR})
include_directories(${CMAKE_BINARY_DIR})


#========================================
#               Protobuf
#========================================

set(PROTO_PREFIX proto)
# Turn this off so that protobuf_generate_cpp does not append directory
# of each .proto file to the protobuf include path. Instead, the current
# directory is used as the include path. Even if Protobuf_IMPORT_DIRS is
# set to be the current directory, we still don't want those additional
# include paths; otherwise, protoc would use the longest path to match
# with the prefix of each file name and generate code with relative
# import.
set(PROTOBUF_GENERATE_CPP_APPEND_PATH OFF)
# Generating code from protobuf files is done here because we want
# to do absolute import in the .proto file so that the importing style
# is consistent across the codebase. More importantly, generated python
# code will also use absolute import, which goes well with python 3
# (implicit relative import is disable since python 3).
set(PROTOS 
  ${PROTO_PREFIX}/api.proto
  ${PROTO_PREFIX}/configuration.proto
  ${PROTO_PREFIX}/internal.proto
  ${PROTO_PREFIX}/machine_id.proto
  ${PROTO_PREFIX}/transaction.proto)

# Generate cpp code
protobuf_generate_cpp(PROTO_SRCS_CPP PROTO_HDRS ${PROTOS})
add_library(proto ${PROTO_SRCS_CPP})
target_link_libraries(proto ${Protobuf_LIBRARIES})

#========================================
#            Subdirectories
#========================================

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
  ${GLOG_STATIC_LIB}
  ${GFLAGS_LIBRARIES}
  ${READLINE_LIBRARIES})
message(STATUS "External libs: ${EXTERNAL_LIBS}")


#========================================
#            Executables
#========================================

# Create an executable for the services
add_executable(slog service/slog.cpp)
target_link_libraries(slog ${SLOG_LIBS} ${EXTERNAL_LIBS})

add_executable(bm service/bm.cpp)
target_link_libraries(bm ${SLOG_LIBS} ${EXTERNAL_LIBS})

if (BUILD_CLIENT)
  add_executable(client service/client.cpp)
  target_link_libraries(client ${SLOG_LIBS} ${EXTERNAL_LIBS})
endif()


#========================================
#                Tests
#========================================

find_package(GTest)
if (GTEST_FOUND)
  message(STATUS "GTest found. Tests will be built")
  include_directories(${GTEST_INCLUDE_DIRS})
  enable_testing()
  add_subdirectory(test)
else()
  message(STATUS "GTest not found. No test will be built")
endif()