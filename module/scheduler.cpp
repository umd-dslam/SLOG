#include "module/scheduler.h"

namespace slog {

Scheduler::Scheduler(
    shared_ptr<Configuration> config,
    Broker& broker)
  : BasicModule(broker.AddChannel(SCHEDULER_CHANNEL)),
    config_(config) {}

void Scheduler::SetUp() {

}

void Scheduler::HandleInternalRequest(
    internal::Request&& req,
    string&& from_machine_id,
    string&& from_channel) {

}

} // namespace slog