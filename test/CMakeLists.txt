project(test)

set(ALL_TESTS
  mmessage_test
  broker_test
  channel_test
  mem_only_storage_test
  server_test)

add_executable(mmessage_test common/mmessage_test.cpp)
target_link_libraries(mmessage_test 
  common)

add_executable(broker_test connection/broker_test.cpp)
target_link_libraries(broker_test 
  common 
  connection)

add_executable(channel_test connection/channel_test.cpp)
target_link_libraries(channel_test 
  common
  connection)

add_executable(mem_only_storage_test storage/mem_only_storage_test.cpp)
target_link_libraries(mem_only_storage_test 
  storage)

add_executable(server_test module/server_test.cpp)
target_link_libraries(server_test
  common
  connection
  module
  storage
  proto)

foreach(TEST ${ALL_TESTS})
  target_link_libraries(${TEST} ${EXTERNAL_LIBS} GTest::GTest GTest::Main)
  add_test(
    NAME ${TEST}
    COMMAND ${TEST})
endforeach(TEST)