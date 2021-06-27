#pragma once

namespace slog {
namespace tpcc {

class TPCCTransaction {
 public:
  virtual ~TPCCTransaction() = default;
  bool Execute() {
    if (!Read()) {
      return false;
    }
    Compute();
    if (!Write()) {
      return false;
    }
    return true;
  }
  virtual bool Read() = 0;
  virtual void Compute() = 0;
  virtual bool Write() = 0;
};

}  // namespace tpcc
}  // namespace slog