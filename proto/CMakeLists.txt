project(proto)

set(PROTOS 
  api.proto
  configuration.proto
  internal.proto
  machine_id.proto
  transaction.proto)

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTOS})

add_library(proto ${PROTO_SRCS})
target_link_libraries(proto ${Protobuf_LIBRARIES})