#pragma once

#include <vector>

#include "common/configuration.h"
#include "connection/broker.h"
#include "module/module.h"
#include "module/server.h"
#include "module/forwarder.h"
#include "storage/mem_only_storage.h"
#include "proto/internal.pb.h"

using std::string;
using std::shared_ptr;
using std::unique_ptr;

namespace slog {

using ConfigVec = std::vector<shared_ptr<Configuration>>;

internal::Request MakeEchoRequest(const string& data);
internal::Response MakeEchoResponse(const string& data);

ConfigVec MakeTestConfigurations(
    string&& prefix,
    int num_replicas, 
    int num_partitions);

class TestSlog {
public:
  TestSlog(shared_ptr<Configuration> config);
  TestSlog& Data(Key&& key, Record&& record);
  TestSlog& WithServerAndClient();
  TestSlog& WithForwarder();
  Channel* AddChannel(const string& name);

  void StartInNewThreads();

private:
  shared_ptr<Configuration> config_;
  shared_ptr<zmq::context_t> server_context_;
  shared_ptr<MemOnlyStorage> storage_;
  Broker broker_;
  unique_ptr<ModuleRunner> server_;
  unique_ptr<ModuleRunner> forwarder_;

  zmq::context_t client_context_;
  zmq::socket_t client_socket_;
};

} // namespace slog