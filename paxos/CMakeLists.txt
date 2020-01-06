project(paxos)

set(SOURCES
  acceptor.cpp
  simple_multi_paxos.cpp
  leader.cpp
  quorum_tracker.cpp)

add_library(paxos ${SOURCES})

target_link_libraries(paxos base_module connection proto)