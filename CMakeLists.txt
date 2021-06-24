cmake_minimum_required(VERSION 3.16.3)

file(READ "VERSION" _VERSION)
string(STRIP ${_VERSION} VERSION)
project(slog VERSION ${VERSION})

set(CMAKE_CXX_STANDARD 17)

if (NOT CMAKE_CONFIGURATION_TYPES AND NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()

configure_file("version.h.in" "version.h")

message(STATUS "Version: ${CMAKE_PROJECT_VERSION}")
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")

#========================================
#               Options
#========================================

option(BUILD_SLOG_CLIENT           "Build the client"                      ON)
option(BUILD_SLOG_TESTS            "Build the tests"                       ON)
option(ENABLE_TXN_EVENT_RECORDING  "Enable transaction events recording"   ON)
set(REMASTER_PROTOCOL "COUNTERLESS" CACHE STRING "Protocol for remastering (\"SIMPLE\", \"PER_KEY\", \"COUNTERLESS\", \"NONE\")")
set(LOCK_MANAGER "RMA" CACHE STRING "Lock manager (\"OLD\", \"DDR\", \"RMA\")")

message(STATUS "Options:")
message(STATUS "  BUILD_SLOG_CLIENT = ${BUILD_SLOG_CLIENT}")
message(STATUS "  ENABLE_TXN_EVENT_RECORDING = ${ENABLE_TXN_EVENT_RECORDING}")
message(STATUS "  REMASTER_PROTOCOL = ${REMASTER_PROTOCOL}")
message(STATUS "  LOCK_MANAGER = ${LOCK_MANAGER}")

#========================================
#               Dependencies
#========================================

include(FetchContent)

FetchContent_Declare(libzmq
  GIT_REPOSITORY  https://github.com/zeromq/libzmq.git
  GIT_TAG         v4.3.3
)
FetchContent_GetProperties(libzmq)
if(NOT libzmq_POPULATED)
  message("Populating: ZeroMQ")
  FetchContent_Populate(libzmq)
  set(ZMQ_BUILD_TESTS OFF CACHE INTERNAL "Build tests for ZeroMQ" FORCE)
  add_subdirectory(${libzmq_SOURCE_DIR} ${libzmq_BINARY_DIR})
endif()

FetchContent_Declare(cppzmq
  GIT_REPOSITORY  https://github.com/zeromq/cppzmq.git
  GIT_TAG         v4.7.1
)
FetchContent_GetProperties(cppzmq)
if(NOT cppzmq_POPULATED)
  message("Populating: cppzmq")
  FetchContent_Populate(cppzmq)
  set(CPPZMQ_BUILD_TESTS OFF CACHE INTERNAL "Build tests for cppzmq" FORCE)
  add_subdirectory(${cppzmq_SOURCE_DIR} ${cppzmq_BINARY_DIR})
endif()

FetchContent_Declare(gflags
  GIT_REPOSITORY  https://github.com/gflags/gflags.git
  GIT_TAG         v2.2.2
)
FetchContent_GetProperties(gflags)
if(NOT gflags_POPULATED)
  message("Populating: gflags")
  FetchContent_Populate(gflags)
  set(GFLAGS_BUILD_TESTING OFF CACHE INTERNAL "Build tests")
  # Needed to work with glog
  set(GFLAGS_NAMESPACE "google" CACHE INTERNAL "Set namespace for gflags")
  add_subdirectory(${gflags_SOURCE_DIR} ${gflags_BINARY_DIR})
endif()

FetchContent_Declare(glog
  GIT_REPOSITORY  https://github.com/google/glog.git
  GIT_TAG         v0.4.0
)
FetchContent_GetProperties(glog)
if(NOT glog_POPULATED)
  message("Populating: glog")
  FetchContent_Populate(glog)
  set(WITH_GFLAGS FALSE CACHE INTERNAL "Build glog with gflags")
  set(BUILD_TESTING FALSE CACHE INTERNAL "Build tests")
  add_subdirectory(${glog_SOURCE_DIR} ${glog_BINARY_DIR})
endif()

FetchContent_Declare(rapidjson
  GIT_REPOSITORY  https://github.com/Tencent/rapidjson
  GIT_TAG         v1.1.0
  GIT_SHALLOW     TRUE
)
FetchContent_GetProperties(rapidjson)
if (NOT rapidjson_POPULATED)
  message("Populating: rapidjson")
  FetchContent_Populate(rapidjson)
  add_library(rapidjson INTERFACE)
  target_include_directories(rapidjson INTERFACE "${rapidjson_SOURCE_DIR}/include")
endif()

FetchContent_Declare(protobuf
  GIT_REPOSITORY  https://github.com/protocolbuffers/protobuf.git
  GIT_TAG         v3.14.0
)
FetchContent_GetProperties(protobuf)
if (NOT protobuf_POPULATED)
  message("Populating: protobuf")
  FetchContent_Populate(protobuf)
  set(protobuf_BUILD_TESTS OFF CACHE INTERNAL "Build protobuf tests" FORCE)
  add_subdirectory(${protobuf_SOURCE_DIR}/cmake ${protobuf_BINARY_DIR})
endif()

#========================================
#               Protobuf
#========================================

# Call this so that we can use protobuf_generate_cpp and protobuf_generate_python
# Other results are discarded
find_package(Protobuf QUIET)

# Generating code from protobuf files is done here instead of in a CMakeLists.txt
# in the 'proto' directory because, otherwise, we cannot do absolute import in the
# .proto files as well as in Python.

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
  ${PROTO_PREFIX}/offline_data.proto
  ${PROTO_PREFIX}/modules.proto
  ${PROTO_PREFIX}/transaction.proto)
add_library(proto ${PROTO_CPP_SRCS} ${PROTO_CPP_HEADERS})
target_include_directories(proto PUBLIC ${CMAKE_BINARY_DIR})
target_link_libraries(proto PUBLIC protobuf::libprotobuf)

# Generate python code
protobuf_generate_python(PROTO_PY_SRCS
  ${PROTO_PREFIX}/configuration.proto
  ${PROTO_PREFIX}/offline_data.proto
  ${PROTO_PREFIX}/modules.proto
  ${PROTO_PREFIX}/transaction.proto)

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
#            Build flags
#========================================

set(CMAKE_CXX_FLAGS "-Wall -Wextra")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")
set(CMAKE_CXX_FLAGS_DEBUG  "-g -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS_ASAN "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address")
set(CMAKE_CXX_FLAGS_UBSAN "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=undefined")
set(CMAKE_CXX_FLAGS_TSAN "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=thread")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELEASE} ${CMAKE_CXX_FLAGS_DEBUG} -DNDEBUG")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

#========================================
#            Library
#========================================

set(SLOG_CORE_SRC_DIRS common connection data_structure execution module paxos storage workload)
add_library(slog-core STATIC "")
foreach (DIR ${SLOG_CORE_SRC_DIRS})
  add_subdirectory(${DIR})
endforeach()
target_include_directories(slog-core PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(slog-core
  PUBLIC
    proto
    glog::glog
    cppzmq-static
    rapidjson)

set(ENABLE_REMASTER TRUE)
string(TOUPPER ${REMASTER_PROTOCOL} REMASTER_PROTOCOL_)
string(TOUPPER ${LOCK_MANAGER} LOCK_MANAGER_)
if (REMASTER_PROTOCOL_ STREQUAL "SIMPLE")
  if (NOT LOCK_MANAGER_ STREQUAL "OLD")
    message(FATAL_ERROR "SIMPLE remaster protocol is only compatible with OLD lock manager")
  endif()
  target_compile_definitions(slog-core PUBLIC REMASTER_PROTOCOL_SIMPLE)
elseif (REMASTER_PROTOCOL_ STREQUAL "PER_KEY")
  if (NOT LOCK_MANAGER_ STREQUAL "OLD")
    message(FATAL_ERROR "PER_KEY remaster protocol is only compatible with OLD lock manager")
  endif()
  target_compile_definitions(slog-core PUBLIC REMASTER_PROTOCOL_PER_KEY)
elseif (REMASTER_PROTOCOL_ STREQUAL "COUNTERLESS")
  if (LOCK_MANAGER_ STREQUAL "OLD")
    message(FATAL_ERROR "COUNTERLESS remaster protocol is not compatible with OLD lock manager")
  endif()
  target_compile_definitions(slog-core PUBLIC REMASTER_PROTOCOL_COUNTERLESS)
elseif (REMASTER_PROTOCOL_ STREQUAL "NONE")
  set(ENABLE_REMASTER FALSE)
else()
  message(FATAL_ERROR "Invalid REMASTER_PROTOCOL. It must be one of: \"SIMPLE\", \"PER_KEY\", \"COUNTERLESS\", or \"NONE\"")
endif()

if (LOCK_MANAGER_ STREQUAL "OLD")
  target_compile_definitions(slog-core PUBLIC LOCK_MANAGER_OLD)
elseif (LOCK_MANAGER_ STREQUAL "RMA")
  target_compile_definitions(slog-core PUBLIC LOCK_MANAGER_RMA)
elseif (LOCK_MANAGER_ STREQUAL "DDR")
  target_compile_definitions(slog-core PUBLIC LOCK_MANAGER_DDR)
else()
  message(FATAL_ERROR "Invalid LOCK_MANAGER. It must be one of: \"OLD\", \"RMA\", or \"DDR\"")
endif()

if (ENABLE_REMASTER)
  target_compile_definitions(slog-core PUBLIC ENABLE_REMASTER)
endif()

if (ENABLE_TXN_EVENT_RECORDING)
  target_compile_definitions(slog-core PUBLIC ENABLE_TXN_EVENT_RECORDING)
endif()

#========================================
#            Executables
#========================================

# Create an executable for each service
add_executable(slog service/slog.cpp service/service_utils.h)
target_link_libraries(slog PRIVATE slog-core gflags::gflags)

if (BUILD_SLOG_CLIENT)
  add_executable(client service/client.cpp service/service_utils.h)
  target_link_libraries(client
    PRIVATE
      slog-core
      gflags::gflags
  )
endif()

add_executable(benchmark service/benchmark.cpp service/service_utils.h)
target_link_libraries(benchmark
  PRIVATE
    slog-core
    gflags::gflags
)

add_executable(scheduler_benchmark service/scheduler_benchmark.cpp)
target_link_libraries(scheduler_benchmark
  PRIVATE
    slog-core
    gflags::gflags
)

#========================================
#                Tests
#========================================

if (BUILD_SLOG_TESTS)
  enable_testing()
  add_subdirectory(test)
else()
  message(STATUS "BUILD_SLOG_TESTS is off. No test will be built")
endif()

#========================================
#              clang-format
#========================================

find_program(CLANG_FORMAT "clang-format")
if (CLANG_FORMAT)
  set(ALL_SRC_FILES)
  foreach (SRC_DIR ${SLOG_CORE_SRC_DIRS} service test)
    file(GLOB_RECURSE SRC_FILES 
      ${PROJECT_SOURCE_DIR}/${SRC_DIR}/*.cpp
      ${PROJECT_SOURCE_DIR}/${SRC_DIR}/*.h)
    list(APPEND ALL_SRC_FILES ${SRC_FILES})
  endforeach()

  add_custom_target(
    format
    ${CLANG_FORMAT}
    -i
    -style=file
    ${ALL_SRC_FILES}
  )
else()
  message(STATUS "Could not find clang-format")
endif()