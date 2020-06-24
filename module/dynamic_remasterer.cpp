#include "module/dynamic_remasterer.h"

#include <glog/logging.h>

#include "common/constants.h"
#include "common/proto_utils.h"

namespace slog {

using internal::Request;
using internal::Response;

DynamicRemasterer::DynamicRemasterer(const ConfigurationPtr& config, const shared_ptr<Broker>& broker) 
  : NetworkedModule(broker, DYNAMIC_REMASTERER_CHANNEL),
    config_(config) {}

void DynamicRemasterer::HandleInternalRequest(
    Request&& req,
    string&& /* from_machine_id */) {
  if (req.type_case() != Request::kDynamicRemasterForward) {
    LOG(ERROR) << "Unexpected request type received: \""
               << CASE_NAME(req.type_case(), Request) << "\"";
    return;
  }

  auto txn = req.mutable_dynamic_remaster_forward()->release_txn();
  MaybeRemasterKeys(txn);
}

void DynamicRemasterer::HandleInternalResponse(
    Response&& res,
    string&& /* from_machine_id */) {
  
}

void DynamicRemasterer::MaybeRemasterKeys(Transaction* txn) {
  std::unordered_set<Key> local_keys;
  for (auto& key_pair : txn->read_set()) {
    auto& key = key_pair.first;
    local_keys.insert(key);
  }
  for (auto& key_pair : txn->write_set()) {
    auto& key = key_pair.first;
    local_keys.insert(key);
  }

  auto coordinating_replica = txn->internal().coordinating_server().replica();

  for (auto& key : local_keys) {
    auto& access_history = key_accesses_[key];

    // Note: local accesses aren't recorded, besides removing the streak of another replica
    if (access_history.replica != coordinating_replica) {
      access_history.replica = coordinating_replica;
      access_history.repeats = 1;
    } else {
      if (coordinating_replica != config_->GetLocalReplica()) {
        // streak continues
        access_history.repeats += 1;
        // TODO put in config
        if (access_history.repeats >= 3) {
          LOG(INFO) << "remaster key: " << key;
          // TODO: fire remaster
        }
      }
    }
  }
}

} // namespace slog