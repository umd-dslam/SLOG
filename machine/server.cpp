#include <iostream>
#include <glog/logging.h>
#include "common/configuration.h"

using namespace slog;
using namespace std;

int main(int argc, char* argv[]) {
  // Verify that the version of the library that we linked against is
  // compatible with the version of the headers we compiled against
  GOOGLE_PROTOBUF_VERIFY_VERSION;
  google::InitGoogleLogging(argv[0]);
  auto config = Configuration::FromFile("slog.conf", 0);
  LOG(ERROR) << "Local port: " << config.GetLocalPort();
  return 0;
}