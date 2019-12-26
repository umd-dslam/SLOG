#pragma once

#include "common/mmessage.h"
#include "proto/internal.pb.h"

namespace slog {

inline internal::MachineId MakeMachineIdProto(uint32_t replica, uint32_t partition) {
  internal::MachineId machine_id;  
  machine_id.set_replica(replica);
  machine_id.set_partition(partition);
  return machine_id;
}

inline std::string MakeMachineId(uint32_t replica, uint32_t partition) {
  return std::to_string(replica) + ":" + std::to_string(partition);
}

}