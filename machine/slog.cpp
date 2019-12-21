#include <iostream>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "common/configuration.h"
#include "connection/broker.h"
#include "machine/client.h"
#include "machine/server.h"
#include "proto/internal.pb.h"

DEFINE_string(config, "slog.conf", "Path to the configuration file");
DEFINE_string(address, "", "Address of the local machine");
DEFINE_uint32(replica, 0, "Replica number of the local machine");
DEFINE_uint32(partition, 0, "Partition number of the local machine");

using namespace slog;
using namespace std;

int main(int argc, char* argv[]) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Verify that the version of the library that we linked against is
  // compatible with the version of the headers we compiled against
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  proto::SlogIdentifier slog_id;
  slog_id.set_replica(FLAGS_replica);
  slog_id.set_partition(FLAGS_partition);
  auto config = Configuration::FromFile(
      "slog.conf", 
      FLAGS_address,
      slog_id);
  auto context = std::make_shared<zmq::context_t>(1);

  Broker broker(config, context);

  LOG(INFO) << "Broker started";
  
  while (true) {}

  return 0;
}