#include "common/offline_data_reader.h"

#include <glog/logging.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

using google::protobuf::io::CodedInputStream;
using google::protobuf::io::FileInputStream;

namespace slog {

OfflineDataReader::OfflineDataReader(int fd)
    : raw_input_(new FileInputStream(fd)), coded_input_(new CodedInputStream(raw_input_)) {
  if (!coded_input_->ReadVarint32(&num_datums_)) {
    LOG(FATAL) << "Error while reading data file";
  }
  num_read_datums_ = 0;
}

OfflineDataReader::~OfflineDataReader() {
  delete coded_input_;
  delete raw_input_;
}

uint32_t OfflineDataReader::GetNumDatums() { return num_datums_; }

bool OfflineDataReader::HasNextDatum() { return num_read_datums_ < num_datums_; }

Datum OfflineDataReader::GetNextDatum() {
  int sz;
  // Read the size of the next datum
  if (!coded_input_->ReadVarintSizeAsInt(&sz)) {
    LOG(FATAL) << "Error while reading data file";
  }
  std::string buf;
  // Read the datum given the size
  if (!coded_input_->ReadString(&buf, sz)) {
    LOG(FATAL) << "Error while reading data file";
  }

  Datum datum;
  // Parse raw bytes into protobuf object
  datum.ParseFromString(buf);

  num_read_datums_++;

  return datum;
}

}  // namespace slog