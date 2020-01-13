#pragma once

#include "module/base/module.h"

using std::string;

namespace slog {

class Outputter : public Module {
public:
  static const string WORKER_OUT;

protected:
  void SetUp() final;
  void Loop() final;

};

} // namespace slog