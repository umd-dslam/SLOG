project(data_structure)

add_library(batch_log batch_log.cpp)
target_link_libraries(batch_log proto)