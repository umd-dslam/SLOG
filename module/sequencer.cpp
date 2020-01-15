#include "module/sequencer.h"

namespace slog {

Sequencer::Sequencer(
    shared_ptr<Configuration> config,
    Broker& broker)
  : BasicModule(
        broker.AddChannel(SEQUENCER_CHANNEL),
        config->GetBatchDuration()),
    config_(config) {}

void Sequencer::HandleInternalRequest(
    internal::Request&& req,
    string&& from_machine_id) {
  LOG(INFO) << "Received something";
}

void Sequencer::HandleWakeUp() {
  LOG(INFO) << "Finished a batch. Sending out...";
}

} // namespace slog