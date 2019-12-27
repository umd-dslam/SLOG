#include "common/proto_utils.h"
#include "common/service_utils.h"
#include "common/configuration.h"
#include "connection/broker.h"
#include "module/server.h"
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
      "slog.conf", 
      FLAGS_address,
      FLAGS_replica,
      FLAGS_partition);
  auto context = make_shared<zmq::context_t>(1);
  auto storage = make_shared<MemOnlyStorage>();
  Broker broker(config, context);
  auto server = MakeRunnerFor<Server>(config, *context, broker, storage);

  broker.StartInNewThread();
  server->Start();

  return 0;
}