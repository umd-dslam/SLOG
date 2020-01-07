project(slog_proto)

set(PROTOS 
  api.proto
  configuration.proto
  internal.proto
  transaction.proto)

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTOS})

add_library(proto ${PROTO_SRCS})