#include "module/base/module.h"

#include "common/constants.h"

#include <glog/logging.h>

using std::shared_ptr;
using std::unique_ptr;

namespace slog {

ModuleRunner::ModuleRunner(const shared_ptr<Module>& module) 
  : module_(module),
    running_(false) {}

ModuleRunner::~ModuleRunner() {
  running_ = false;
  LOG(INFO) << module_->name() << " exitting...";
  thread_.join();
}

void ModuleRunner::StartInNewThread() {
  if (running_) {
    throw std::runtime_error("The module has already started");
  }
  running_ = true;
  thread_ = std::thread(&ModuleRunner::Run, this);
}

void ModuleRunner::Start() {
  if (running_) {
    throw std::runtime_error("The module has already started");
  }
  running_ = true;
  Run();
}

void ModuleRunner::Run() {
  module_->SetUp();
  while (running_) {
    module_->Loop();
  }
}

} // namespace slog