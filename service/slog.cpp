#include "common/proto_utils.h"
#include "common/service_utils.h"
#include "common/configuration.h"
#include "connection/broker.h"
#include "module/server.h"
#include "proto/internal.pb.h"

DEFINE_string(config, "slog.conf", "Path to the configuration file");
DEFINE_string(address, "", "Address of the local machine");
DEFINE_uint32(replica, 0, "Replica number of the local machine");
DEFINE_uint32(partition, 0, "Partition number of the local machine");

using namespace slog;
using namespace std;

int main(int argc, char* argv[]) {
  InitializeService(argc, argv);
  
  auto slog_id = MakeSlogId(FLAGS_replica, FLAGS_partition);
  auto config = Configuration::FromFile(
      "slog.conf", 
      FLAGS_address,
      slog_id);
  auto context = std::make_shared<zmq::context_t>(1);

  Broker broker(config, context);
  Server server(config, context, broker);

  broker.StartInNewThread();

  server.Start();

  return 0;
}