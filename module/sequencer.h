#pragma once

#include "common/configuration.h"
#include "connection/broker.h"
#include "module/base/basic_module.h"

using std::shared_ptr;

namespace slog {

class Sequencer : public BasicModule {
public:
  Sequencer(
      shared_ptr<Configuration> config,
      Broker& broker);

protected:
  void HandleInternalRequest(
      internal::Request&& req,
      string&& from_machine_id) final;

  void HandleWakeUp() final;

private:
  shared_ptr<Configuration> config_;
};

} // namespace slog