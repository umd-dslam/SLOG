#pragma once

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "google/protobuf/stubs/common.h"
namespace slog {

inline void InitializeService(int* argc, char*** argv) {
  google::InitGoogleLogging((*argv)[0]);
  google::InstallFailureSignalHandler();
  google::ParseCommandLineFlags(argc, argv, true /* remove_flags */);
  FLAGS_logtostderr = true;
  FLAGS_colorlogtostderr = true;
  // Verify that the version of the library that we linked against is
  // compatible with the version of the headers we compiled against
  GOOGLE_PROTOBUF_VERIFY_VERSION;
}

}  // namespace slog