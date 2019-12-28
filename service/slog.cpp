#include "common/proto_utils.h"
#include "common/service_utils.h"
#include "common/configuration.h"
#include "connection/broker.h"
#include "module/forwarder.h"
#include "module/scheduler.h"
#include "module/server.h"
#include "module/sequencer.h"
#include "proto/internal.pb.h"
#include "storage/mem_only_storage.h"

DEFINE_string(config, "slog.conf", "Path to the configuration file");
DEFINE_string(address, "", "Address of the local machine");
DEFINE_uint32(replica, 0, "Replica number of the local machine");
DEFINE_uint32(partition, 0, "Partition number of the local machine");

using namespace slog;
using namespace std;

using std::make_shared;

int main(int argc, char* argv[]) {
  InitializeService(argc, argv);
  
  auto config = Configuration::FromFile(
      FLAGS_config, 
      FLAGS_address,
      FLAGS_replica,
      FLAGS_partition);
  auto context = make_shared<zmq::context_t>(1);
  Broker broker(config, context);

  auto storage = make_shared<MemOnlyStorage>();

  auto server = MakeRunnerFor<Server>(config, *context, broker, storage);
  auto forwarder = MakeRunnerFor<Forwarder>(config, broker);
  auto sequencer = MakeRunnerFor<Sequencer>(config, broker);
  auto scheduler = MakeRunnerFor<Scheduler>(config, broker);

  // Only start the Broker after it is used to initialized all the modules
  broker.StartInNewThread();

  forwarder->StartInNewThread();
  sequencer->StartInNewThread();
  scheduler->StartInNewThread();
  server->Start();

  return 0;
}