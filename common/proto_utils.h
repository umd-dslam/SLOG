#pragma once

#include "common/mmessage.h"
#include "proto/internal.pb.h"

namespace slog {

using internal::MachineId;

MachineId MakeMachineId(uint32_t replica, uint32_t partition);
std::string MachineIdToString(const MachineId machine_id);

template<typename Req, typename Res>
void GetRequestAndPrepareResponse(Req& request, Res& response, const MMessage& msg) {
  CHECK(msg.GetProto(request));
  response.set_stream_id(request.stream_id());
}

}