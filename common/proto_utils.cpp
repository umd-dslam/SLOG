#include "proto_utils.h"

namespace slog {

proto::SlogIdentifier MakeSlogIdentifier(uint32_t replica, uint32_t partition) {
  proto::SlogIdentifier slog_id;
  slog_id.set_replica(replica);
  slog_id.set_partition(partition);
  return slog_id;
}

}