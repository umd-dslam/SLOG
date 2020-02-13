#pragma once

#include <chrono>
#include <thread>
#include <set>
#include <unordered_map>

#include <zmq.hpp>

#include "common/configuration.h"
#include "common/types.h"
#include "connection/broker.h"
#include "module/base/module.h"
#include "storage/lookup_master_index.h"

using std::shared_ptr;
using std::unordered_map;

namespace slog {

struct PendingResponse {
  MMessage response;
  Transaction* txn;
  std::unordered_set<uint32_t> awaited_partitions;
  bool initialized;
};

/**
 * A Server serves external requests from the clients. It also answers
 * requests about mastership of data.
 * 
 * INPUT:  External TransactionRequest and LookUpMasterRequest
 * 
 * OUTPUT: For external TransactionRequest, it forwards the txn internally
 *         to appropriate modules and waits for internal responses before
 *         responding back to the client with an external TransactionResponse.
 * 
 *         For LookUpMasterRequest, a LookUpMasterResponse is sent back to
 *         the requester.
 */
class Server : public Module, ChannelHolder {
public:
  Server(
      ConfigurationPtr config,
      zmq::context_t& context,
      Broker& broker,
      shared_ptr<LookupMasterIndex<Key, Metadata>> lookup_master_index);

private:
  void SetUp() final;
  void Loop() final;

  bool HasMessageFromChannel() const;
  bool HasMessageFromClient() const;

  void HandleAPIRequest(MMessage&& msg);
  void HandleInternalRequest(
      internal::Request&& req,
      string&& from_machine_id,
      string&& from_channel);

  void ProcessLookUpMasterRequest(
      internal::LookupMasterRequest* lookup_master,
      string&& from_machine_id,
      string&& from_channel);
  void ProcessForwardSubtxnRequest(
      internal::ForwardSubtransaction* forward_sub_txn,
      string&& from_machine_id);

  void SendAPIResponse(TxnId txn_id);

  TxnId NextTxnId();

  ConfigurationPtr config_;
  zmq::socket_t client_socket_;
  std::vector<zmq::pollitem_t> poll_items_;

  shared_ptr<LookupMasterIndex<Key, Metadata>> lookup_master_index_;

  uint32_t server_id_;
  TxnId txn_id_counter_;
  unordered_map<TxnId, PendingResponse> pending_responses_;
};

} // namespace slog