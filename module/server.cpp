#include "module/server.h"

#include "common/constants.h"
#include "common/json_utils.h"
#include "connection/zmq_utils.h"
#include "proto/internal.pb.h"

using std::move;

namespace slog {

namespace {
void ValidateTransaction(Transaction* txn) {
  txn->set_status(TransactionStatus::ABORTED);
  if (txn->keys().empty()) {
    txn->set_abort_reason("Txn accesses no key");
    return;
  }
  txn->set_status(TransactionStatus::NOT_STARTED);
}
}  // namespace

Server::FinishedTransaction::FinishedTransaction(size_t involved_partitions)
    : remaining_partitions_(involved_partitions) {}

bool Server::FinishedTransaction::AddSubTxn(EnvelopePtr&& new_req) {
  DCHECK(new_req != nullptr);

  remaining_partitions_--;

  if (req_ == nullptr) {
    req_ = std::move(new_req);
  } else {
    auto& subtxn = new_req->request().finished_subtxn();
    auto txn = req_->mutable_request()->mutable_finished_subtxn()->mutable_txn();
    MergeTransaction(*txn, subtxn.txn());
  }

  return remaining_partitions_ == 0;
}

Transaction* Server::FinishedTransaction::ReleaseTxn() {
  if (req_ == nullptr) return nullptr;
  return req_->mutable_request()->mutable_finished_subtxn()->release_txn();
}

Server::Server(const std::shared_ptr<Broker>& broker, const MetricsRepositoryManagerPtr& metrics_manager,
               std::chrono::milliseconds poll_timeout)
    : NetworkedModule(broker, kServerChannel, metrics_manager, poll_timeout), txn_id_counter_(0) {}

/***********************************************
                Initialization
***********************************************/

void Server::Initialize() {
  string endpoint = "tcp://*:" + std::to_string(config()->server_port());
  zmq::socket_t client_socket(*context(), ZMQ_ROUTER);
  client_socket.set(zmq::sockopt::rcvhwm, 0);
  client_socket.set(zmq::sockopt::sndhwm, 0);
  client_socket.bind(endpoint);

  LOG(INFO) << "Bound Server to: " << endpoint;

  // Tell other machines that the current one is online
  internal::Envelope env;
  env.mutable_request()->mutable_signal();
  for (MachineId m : config()->all_machine_ids()) {
    if (m != config()->local_machine_id()) {
      offline_machines_.insert(m);
      Send(env, m, kServerChannel);
    }
  }

  AddCustomSocket(move(client_socket));
}

/***********************************************
                  API Requests
***********************************************/

bool Server::OnCustomSocket() {
  auto& socket = GetCustomSocket(0);

  zmq::message_t identity;
  if (!socket.recv(identity, zmq::recv_flags::dontwait)) {
    return false;
  }
  if (!identity.more()) {
    LOG(ERROR) << "Invalid message from client: Only identity part is found";
    return false;
  }
  api::Request request;
  if (!RecvDeserializedProtoWithEmptyDelim(socket, request)) {
    LOG(ERROR) << "Invalid message from client: Body is not a proto";
    return false;
  }

  // While this is called txn id, we use it for any kind of request
  auto txn_id = NextTxnId();
  auto res = pending_responses_.try_emplace(txn_id, move(identity), request.stream_id());
  CHECK(res.second) << "Duplicate transaction id: " << txn_id;

  switch (request.type_case()) {
    case api::Request::kTxn: {
      auto txn = request.mutable_txn()->release_txn();
      auto txn_internal = txn->mutable_internal();

      txn_internal->set_id(txn_id);
      txn_internal->set_coordinating_server(config()->local_machine_id());

      RECORD(txn_internal, TransactionEvent::ENTER_SERVER);

      ValidateTransaction(txn);
      if (txn->status() == TransactionStatus::ABORTED) {
        SendTxnToClient(txn);
        break;
      }

      RECORD(txn_internal, TransactionEvent::EXIT_SERVER_TO_FORWARDER);

      // Send to forwarder
      auto env = NewEnvelope();
      env->mutable_request()->mutable_forward_txn()->set_allocated_txn(txn);
      Send(move(env), kForwarderChannel);
      break;
    }
    case api::Request::kStats: {
      auto env = NewEnvelope();
      env->mutable_request()->mutable_stats()->set_id(txn_id);
      env->mutable_request()->mutable_stats()->set_level(request.stats().level());

      // Send to appropriate module based on provided information
      switch (request.stats().module()) {
        case ModuleId::SERVER:
          ProcessStatsRequest(env->request().stats());
          break;
        case ModuleId::FORWARDER:
          Send(move(env), kForwarderChannel);
          break;
        case ModuleId::MHORDERER:
          Send(move(env), kMultiHomeOrdererChannel);
          break;
        case ModuleId::SEQUENCER:
          Send(move(env), kSequencerChannel);
          break;
        case ModuleId::SCHEDULER:
          Send(move(env), kSchedulerChannel);
          break;
        default:
          LOG(ERROR) << "Invalid module for stats request";
          break;
      }
      break;
    }
    case api::Request::kMetrics: {
      metrics_manager().AggregateAndFlushToDisk(request.metrics().prefix());
      api::Response response;
      response.mutable_metrics();
      SendResponseToClient(txn_id, std::move(response));
      break;
    }
    default:
      pending_responses_.erase(txn_id);
      LOG(ERROR) << "Unexpected request type received: \"" << CASE_NAME(request.type_case(), api::Request) << "\"";
      break;
  }
  return true;
}

/***********************************************
              Internal Requests
***********************************************/

void Server::OnInternalRequestReceived(EnvelopePtr&& env) {
  switch (env->request().type_case()) {
    case internal::Request::kSignal:
      LOG(INFO) << "Machine " << env->from() << " is online";
      offline_machines_.erase(env->from());
      if (offline_machines_.empty()) {
        LOG(INFO) << "All machines are online";
      }
      break;
    case internal::Request::kFinishedSubtxn:
      ProcessFinishedSubtxn(move(env));
      break;
    default:
      LOG(ERROR) << "Unexpected request type received: \"" << CASE_NAME(env->request().type_case(), internal::Request)
                 << "\"";
  }
}

void Server::ProcessFinishedSubtxn(EnvelopePtr&& env) {
  auto finished_subtxn = env->mutable_request()->mutable_finished_subtxn();
  auto txn_internal = finished_subtxn->mutable_txn()->mutable_internal();

  RECORD(txn_internal, TransactionEvent::RETURN_TO_SERVER);

  auto txn_id = finished_subtxn->txn().internal().id();
  if (pending_responses_.count(txn_id) == 0) {
    return;
  }

  auto res = finished_txns_.try_emplace(txn_id, txn_internal->involved_partitions_size());
  auto& finished_txn = res.first->second;
  if (finished_txn.AddSubTxn(std::move(env))) {
    SendTxnToClient(finished_txn.ReleaseTxn());
    finished_txns_.erase(txn_id);
  }
}

void Server::ProcessStatsRequest(const internal::StatsRequest& stats_request) {
  using rapidjson::StringRef;

  int level = stats_request.level();

  rapidjson::Document stats;
  stats.SetObject();
  auto& alloc = stats.GetAllocator();

  // Add stats for current transactions in the system
  stats.AddMember(StringRef(TXN_ID_COUNTER), txn_id_counter_, alloc);
  stats.AddMember(StringRef(NUM_PENDING_RESPONSES), pending_responses_.size(), alloc);
  stats.AddMember(StringRef(NUM_PARTIALLY_FINISHED_TXNS), finished_txns_.size(), alloc);
  if (level >= 1) {
    stats.AddMember(StringRef(PENDING_RESPONSES),
                    ToJsonArrayOfKeyValue(
                        pending_responses_, [](const auto& resp) { return resp.stream_id; }, alloc),
                    alloc);

    stats.AddMember(StringRef(PARTIALLY_FINISHED_TXNS),
                    ToJsonArray(
                        finished_txns_, [](const auto& p) { return p.first; }, alloc),
                    alloc);
  }

  rapidjson::StringBuffer buf;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
  stats.Accept(writer);

  auto env = NewEnvelope();
  env->mutable_response()->mutable_stats()->set_id(stats_request.id());
  env->mutable_response()->mutable_stats()->set_stats_json(buf.GetString());
  OnInternalResponseReceived(move(env));
}

/***********************************************
              Internal Responses
***********************************************/

void Server::OnInternalResponseReceived(EnvelopePtr&& env) {
  if (env->response().type_case() != internal::Response::kStats) {
    LOG(ERROR) << "Unexpected response type received: \"" << CASE_NAME(env->response().type_case(), internal::Response)
               << "\"";
  }
  api::Response response;
  auto stats_response = response.mutable_stats();
  stats_response->set_allocated_stats_json(env->mutable_response()->mutable_stats()->release_stats_json());
  SendResponseToClient(env->response().stats().id(), move(response));
}

/***********************************************
                    Helpers
***********************************************/

void Server::SendTxnToClient(Transaction* txn) {
  RECORD(txn->mutable_internal(), TransactionEvent::EXIT_SERVER_TO_CLIENT);

  api::Response response;
  auto txn_response = response.mutable_txn();
  txn_response->set_allocated_txn(txn);
  SendResponseToClient(txn->internal().id(), move(response));
}

void Server::SendResponseToClient(TxnId txn_id, api::Response&& res) {
  auto it = pending_responses_.find(txn_id);
  if (it == pending_responses_.end()) {
    LOG(ERROR) << "Cannot find info to response back to client for txn: " << txn_id;
    return;
  }
  auto& socket = GetCustomSocket(0);
  // Stream id is for the client to match request/response
  res.set_stream_id(it->second.stream_id);
  // Send identity to the socket to select the client to response to
  socket.send(it->second.identity, zmq::send_flags::sndmore);
  // Send the actual message
  SendSerializedProtoWithEmptyDelim(socket, res);

  pending_responses_.erase(txn_id);
}

TxnId Server::NextTxnId() {
  txn_id_counter_++;
  return txn_id_counter_ * kMaxNumMachines + config()->local_machine_id();
}

}  // namespace slog