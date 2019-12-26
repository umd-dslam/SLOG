#pragma once

#include <chrono>
#include <thread>
#include <set>
#include <unordered_map>

#include <zmq.hpp>

#include "common/configuration.h"
#include "common/types.h"
#include "connection/broker.h"
#include "module/module.h"
#include "storage/lookup_master_index.h"

using std::shared_ptr;
using std::unordered_map;

namespace slog {

class Server : public Module, public ChannelHolder {
public:
  Server(
      shared_ptr<const Configuration> config,
      shared_ptr<zmq::context_t> context,
      Broker& broker,
      shared_ptr<LookupMasterIndex<Key, Metadata>> lookup_master_index);

private:
  void SetUp() final;
  void Loop() final;

  void HandleAPIRequest(MMessage&& msg);
  void HandleInternalRequest(MMessage&& msg);
  void HandleInternalResponse(MMessage&& msg);

  TxnId NextTxnId();

  shared_ptr<const Configuration> config_;
  zmq::socket_t socket_;
  std::vector<zmq::pollitem_t> poll_items_;

  shared_ptr<LookupMasterIndex<Key, Metadata>> lookup_master_index_;

  uint32_t server_id_;
  uint32_t txn_id_counter_;
  unordered_map<TxnId, MMessage> pending_response_;
  std::set<std::pair<TimePoint, TxnId>> response_time_;
};

} // namespace slog