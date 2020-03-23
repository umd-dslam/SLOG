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
#include "proto/api.pb.h"

using std::shared_ptr;
using std::unordered_map;

namespace slog {

struct PendingResponse {
  MMessage response;
  uint32_t stream_id;
};

struct CompletedTransaction {
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

  void SetUp() final;
  void Loop() final;

private:

  bool HasMessageFromChannel() const;
  bool HasMessageFromClient() const;

  void HandleAPIRequest(MMessage&& msg);
  void HandleInternalRequest(
      internal::Request&& req,
      string&& from_machine_id,
      string&& from_channel);
  void HandleInternalResponse(internal::Response&& res);

  void ProcessLookUpMasterRequest(
      internal::LookupMasterRequest* lookup_master,
      string&& from_machine_id,
      string&& from_channel);

  /**
   * After a transaction is processed by different partitions, each
   * involving partition will send a sub-transaction with the processing
   * result to the coordinating server. The coordinating server will be
   * in charge of merging these sub-transactions and responding back to
   * the client.
   */
  void ProcessCompletedSubtxn(internal::CompletedSubtransaction* completed_subtxn);

  void SendAPIResponse(TxnId txn_id, api::Response&& res);

  TxnId NextTxnId();

  ConfigurationPtr config_;
  zmq::socket_t client_socket_;
  std::vector<zmq::pollitem_t> poll_items_;

  shared_ptr<LookupMasterIndex<Key, Metadata>> lookup_master_index_;

  uint32_t server_id_;
  TxnId txn_id_counter_;
  unordered_map<TxnId, PendingResponse> pending_responses_;
  unordered_map<TxnId, CompletedTransaction> completed_txns_;
};

} // namespace slog