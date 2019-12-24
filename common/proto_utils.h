#pragma once

#include "common/mmessage.h"
#include "proto/internal.pb.h"

namespace slog {

using internal::SlogIdentifier;

SlogIdentifier MakeSlogId(uint32_t replica, uint32_t partition);
std::string SlogIdToString(const SlogIdentifier slog_id);

template<typename Req, typename Res>
void GetRequestAndPrepareResponse(Req& request, Res& response, const MMessage& msg) {
  CHECK(msg.GetProto(request));
  response.set_stream_id(request.stream_id());
}

}