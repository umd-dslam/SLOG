#include "module/sequencer.h"

namespace slog {

Sequencer::Sequencer(
    shared_ptr<Configuration> config,
    Broker& broker)
  : BasicModule(broker.AddChannel(SEQUENCER_CHANNEL)),
    config_(config) {}

void Sequencer::HandleInternalRequest(
    internal::Request&& req,
    string&& from_machine_id,
    string&& from_channel) {
  // TODO: implement batching of transactions
}



} // namespace slog