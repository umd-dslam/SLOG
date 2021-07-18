include(GoogleTest)

FetchContent_Declare(googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG        release-1.10.0
)
FetchContent_GetProperties(googletest)
if(NOT googletest_POPULATED)
  message("Populating: googletest")
  FetchContent_Populate(googletest)
  add_subdirectory(${googletest_SOURCE_DIR} ${googletest_BINARY_DIR})
endif()

add_library(test-utils test_utils.cpp)
target_link_libraries(test-utils PUBLIC slog-core)

macro(add_slog_test TESTFILE)
  get_filename_component(TESTNAME ${TESTFILE} NAME_WE)
  add_executable(${TESTNAME} ${TESTFILE})
  target_link_libraries(${TESTNAME} 
    PRIVATE
      gmock
      gtest
      gtest_main
      test-utils)
  gtest_discover_tests(${TESTNAME}
    PROPERTIES 
      RUN_SERIAL TRUE
      TIMEOUT    5)
endmacro()

add_slog_test(common/string_utils_test.cpp)
add_slog_test(connection/broker_and_sender_test.cpp)
add_slog_test(connection/zmq_utils_test.cpp)
add_slog_test(data_structure/batch_log_test.cpp)
add_slog_test(data_structure/concurrent_hash_map_test.cpp)
add_slog_test(e2e/e2e_test.cpp)
add_slog_test(execution/tpcc/table_test.cpp)
add_slog_test(execution/tpcc/transaction_test.cpp)
add_slog_test(module/forwarder_test.cpp)
add_slog_test(module/interleaver_test.cpp)
add_slog_test(module/scheduler_components/ddr_lock_manager_test.cpp)
add_slog_test(module/scheduler_components/old_lock_manager_test.cpp)
add_slog_test(module/scheduler_components/per_key_remaster_manager_test.cpp)
add_slog_test(module/scheduler_components/rma_lock_manager_test.cpp)
add_slog_test(module/scheduler_components/simple_remaster_manager_test.cpp)
add_slog_test(module/scheduler_test.cpp)
add_slog_test(module/sequencer_test.cpp)
add_slog_test(paxos/paxos_test.cpp)
add_slog_test(storage/mem_only_storage_test.cpp)