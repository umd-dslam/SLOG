#include "proto_utils.h"

using std::to_string;

namespace slog {

MachineId MakeMachineId(uint32_t replica, uint32_t partition) {
  MachineId machine_id;
  machine_id.set_replica(replica);
  machine_id.set_partition(partition);
  return machine_id;
}

std::string MachineIdToString(const MachineId machine_id) {
  return to_string(machine_id.replica()) + ":" + to_string(machine_id.partition());
}

}