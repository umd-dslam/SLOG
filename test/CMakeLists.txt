include(GoogleTest)

add_library(test_utils test_utils.cpp)
target_link_libraries(test_utils PUBLIC libslog)

macro(add_slog_test TESTFILE)
  get_filename_component(TESTNAME ${TESTFILE} NAME_WE)
  add_executable(${TESTNAME} ${TESTFILE})
  target_link_libraries(${TESTNAME} 
    PRIVATE
      GTest::GTest
      GTest::Main
      test_utils)
  gtest_discover_tests(${TESTNAME}
    PROPERTIES RUN_SERIAL TRUE)
endmacro()

add_slog_test(common/message_pool_test.cpp)
add_slog_test(common/string_utils_test.cpp)
add_slog_test(common/transaction_holder_test.cpp)
add_slog_test(connection/broker_and_sender_test.cpp)
add_slog_test(connection/zmq_utils_test.cpp)
add_slog_test(data_structure/batch_log_test.cpp)
add_slog_test(data_structure/concurrent_hash_map_test.cpp)
add_slog_test(e2e/e2e_test.cpp)
add_slog_test(module/forwarder_test.cpp)
add_slog_test(module/interleaver_test.cpp)
add_slog_test(module/scheduler_components/commands_test.cpp)
add_slog_test(module/scheduler_components/deterministic_lock_manager_deprecated_test.cpp)
add_slog_test(module/scheduler_components/deterministic_lock_manager_test.cpp)
add_slog_test(module/scheduler_components/per_key_remaster_manager_test.cpp)
add_slog_test(module/scheduler_components/simple_remaster_manager_test.cpp)
add_slog_test(module/scheduler_test.cpp)
add_slog_test(module/sequencer_test.cpp)
add_slog_test(paxos/paxos_test.cpp)
add_slog_test(storage/mem_only_storage_test.cpp)