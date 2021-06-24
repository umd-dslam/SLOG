#pragma once

#include "common/types.h"

namespace slog {

class LookupMasterIndex {
 public:
  virtual bool GetMasterMetadata(const Key& key, Metadata& metadata) const = 0;
};

}  // namespace slog