project(test)

# Don't forget to add all of the tests here so that they
# are linked properly
set(ALL_TESTS
  batch_log_test
  batch_interleaver_test
  broker_test
  channel_test
  commands_test
  deterministic_lock_manager_test
  e2e_test
  forwarder_test
  mem_only_storage_test
  mmessage_test
  paxos_test
  proto_utils_test
  scheduler_test
  server_test
  sequencer_test)

add_executable(batch_log_test data_structure/batch_log_test.cpp)
target_link_libraries(batch_log_test batch_log proto)
  
add_executable(batch_interleaver_test
  module/scheduler_components/batch_interleaver_test.cpp)
target_link_libraries(batch_interleaver_test scheduler_components proto)

add_executable(broker_test connection/broker_test.cpp)
target_link_libraries(broker_test test_utils)

add_executable(channel_test connection/channel_test.cpp)
target_link_libraries(channel_test test_utils)

add_executable(commands_test module/scheduler_components/commands_test.cpp)
target_link_libraries(commands_test test_utils)

add_executable(deterministic_lock_manager_test 
  module/scheduler_components/deterministic_lock_manager_test.cpp)
target_link_libraries(deterministic_lock_manager_test test_utils)

add_executable(e2e_test e2e/e2e_test.cpp)
target_link_libraries(e2e_test test_utils)

add_executable(forwarder_test module/forwarder_test.cpp)
target_link_libraries(forwarder_test test_utils)

add_executable(mem_only_storage_test storage/mem_only_storage_test.cpp)
target_link_libraries(mem_only_storage_test)

add_executable(mmessage_test common/mmessage_test.cpp)
target_link_libraries(mmessage_test test_utils)

add_executable(paxos_test paxos/paxos_test.cpp)
target_link_libraries(paxos_test test_utils paxos)

add_executable(proto_utils_test common/proto_utils_test.cpp)
target_link_libraries(proto_utils_test common proto)

add_executable(scheduler_test module/scheduler_test.cpp)
target_link_libraries(scheduler_test test_utils)

add_executable(server_test module/server_test.cpp)
target_link_libraries(server_test test_utils)

add_executable(sequencer_test module/sequencer_test.cpp)
target_link_libraries(sequencer_test test_utils)

foreach(TEST ${ALL_TESTS})
  target_link_libraries(${TEST} ${EXTERNAL_LIBS} GTest::GTest GTest::Main)
  add_test(
    NAME ${TEST}
    COMMAND ${TEST})
endforeach(TEST)