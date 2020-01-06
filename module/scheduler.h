#pragma once

#include "common/configuration.h"
#include "connection/broker.h"
#include "module/base/basic_module.h"

using std::shared_ptr;

namespace slog {

class Scheduler : public BasicModule {
public:
  Scheduler(
      shared_ptr<Configuration> config,
      Broker& broker);

protected:
  void SetUp() final;

  void HandleInternalRequest(
      internal::Request&& req,
      string&& from_machine_id,
      string&& from_channel) final;

private:
  shared_ptr<Configuration> config_;
};

} // namespace slog