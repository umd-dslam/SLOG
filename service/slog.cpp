#include <iostream>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "common/proto_utils.h"
#include "common/configuration.h"
#include "connection/broker.h"
#include "module/server.h"
#include "proto/slog_identifier.pb.h"

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

  auto slog_id = MakeSlogId(FLAGS_replica, FLAGS_partition);
  auto config = Configuration::FromFile(
      "slog.conf", 
      FLAGS_address,
      slog_id);
  auto context = std::make_shared<zmq::context_t>(1);

  Broker broker(config, context);
  broker.Start();
  LOG(INFO) << "Broker started";
  
  while (true) {}

  return 0;
}