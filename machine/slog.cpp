#include <iostream>
#include <glog/logging.h>

#include "common/configuration.h"
#include "connection/broker.h"
#include "machine/client.h"
#include "machine/server.h"

using namespace slog;
using namespace std;

int main(int argc, char* argv[]) {
  // Verify that the version of the library that we linked against is
  // compatible with the version of the headers we compiled against
  GOOGLE_PROTOBUF_VERIFY_VERSION;
  google::InitGoogleLogging(argv[0]);
  auto config = Configuration::FromFile("slog.conf", 0);
  zmq::context_t context(1);

  Broker broker(config, context);
  LOG(ERROR) << "Broker started";

  return 0;
}