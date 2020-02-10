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

# Generating code from protobuf files is done here because we want
# to do absolute import in .proto files, consistent with the rest of
# the codebase. More importantly, as implicit relative import is disabled
# in python 3, we need absolute import for the generated python code to work.

# Turn this off so that "protobuf_generate_cpp" won't append directory
# of each .proto file to the protobuf include path, which should contain only
# the project root directory. The reason is that protoc uses the longest path
# to match with the prefix of each file name, generating relative import instead
# of absolute import. For example, if the include path has:
#   "slog/something/proto" and "slog/"
# The path "slog/something/proto/messageA.proto" will become "messageA.proto",
# because it is matched with "slog/something/proto". On the other hand, if only
# "slog/" is present, we get "something/proto/messageA.proto", which is absolute
# with respect to the project root "slog/"
set(PROTOBUF_GENERATE_CPP_APPEND_PATH OFF)
set(PROTO_PREFIX proto)

# Generate cpp code
protobuf_generate_cpp(PROTO_CPP_SRCS PROTO_CPP_HEADERS
  ${PROTO_PREFIX}/api.proto
  ${PROTO_PREFIX}/configuration.proto
  ${PROTO_PREFIX}/internal.proto
  ${PROTO_PREFIX}/machine_id.proto
  ${PROTO_PREFIX}/offline_data.proto
  ${PROTO_PREFIX}/transaction.proto)
add_library(proto ${PROTO_CPP_SRCS} ${PROTO_CPP_HEADERS})
target_link_libraries(proto ${Protobuf_LIBRARIES})

# Generate python code
protobuf_generate_python(PROTO_PY_SRCS
  ${PROTO_PREFIX}/offline_data.proto)

# Copy generated python code to the "tools" directory
set(PROTO_PY_DEST_DIR ${CMAKE_SOURCE_DIR}/tools)
set(PROTO_PY_DEST_SRCS "")
foreach(FULL_PATH ${PROTO_PY_SRCS})
  # /base/path/relative/path/file_name.py => relative/path/file_name.py
  file(
    RELATIVE_PATH
    RELATIVE_DIR_AND_FILE_NAME
    ${CMAKE_BINARY_DIR}
    ${FULL_PATH})
  
  # relative/path/file_name.py => relative/path
  get_filename_component(RELATIVE_DIR ${RELATIVE_DIR_AND_FILE_NAME} DIRECTORY)

  set(PROTO_PY_DEST_SRC ${PROTO_PY_DEST_DIR}/${RELATIVE_DIR_AND_FILE_NAME})
  add_custom_command(
    OUTPUT ${PROTO_PY_DEST_SRC}
    COMMAND mkdir -p ${PROTO_PY_DEST_DIR}/${RELATIVE_DIR} && cp ${FULL_PATH} ${PROTO_PY_DEST_SRC}
    DEPENDS ${FULL_PATH}
  )
  list(APPEND PROTO_PY_DEST_SRCS ${PROTO_PY_DEST_SRC})
endforeach(FULL_PATH)

# Trigger the python code generation and copy commands
add_custom_target(proto_py ALL DEPENDS ${PROTO_PY_DEST_SRCS})

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