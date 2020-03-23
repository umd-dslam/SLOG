#include "service_utils.h"

#include "google/protobuf/stubs/common.h"

namespace slog {

void InitializeService(int* argc, char*** argv) {
  google::InitGoogleLogging((*argv)[0]);
  google::InstallFailureSignalHandler();
  gflags::ParseCommandLineFlags(argc, argv, true /* remove_flags */);
  FLAGS_logtostderr = true;
  FLAGS_colorlogtostderr = true;
  // Verify that the version of the library that we linked against is
  // compatible with the version of the headers we compiled against
  GOOGLE_PROTOBUF_VERIFY_VERSION;
}

} // namespace slog