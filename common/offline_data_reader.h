#pragma once

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream.h>

#include "proto/offline_data.pb.h"

namespace slog {

class OfflineDataReader {
 public:
  OfflineDataReader(int fd);
  ~OfflineDataReader();

  uint32_t GetNumDatums();
  bool HasNextDatum();
  Datum GetNextDatum();

 private:
  google::protobuf::io::ZeroCopyInputStream* raw_input_;
  google::protobuf::io::CodedInputStream* coded_input_;
  uint32_t num_datums_;
  uint32_t num_read_datums_;
};

}  // namespace slog