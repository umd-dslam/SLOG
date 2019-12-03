project(slog_proto)

set(PROTOS config.proto)

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTOS})

add_library(proto ${PROTO_SRCS})