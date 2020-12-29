#include "module/base/module.h"

#include <glog/logging.h>

#include "common/constants.h"
#include "common/thread_utils.h"

using std::shared_ptr;
using std::unique_ptr;

namespace slog {

ModuleRunner::ModuleRunner(const shared_ptr<Module>& module) : module_(module), running_(false) {}

ModuleRunner::~ModuleRunner() {
  running_ = false;
  LOG(INFO) << "Stopping " << module_->name();
  thread_.join();
}

void ModuleRunner::StartInNewThread(int cpu) {
  if (running_) {
    throw std::runtime_error("The module has already started");
  }
  running_ = true;
  thread_ = std::thread(&ModuleRunner::Run, this);
  PinToCpu(thread_.native_handle(), cpu);
}

void ModuleRunner::Start(int cpu) {
  if (running_) {
    throw std::runtime_error("The module has already started");
  }
  running_ = true;
  PinToCpu(pthread_self(), cpu);
  Run();
}

void ModuleRunner::Run() {
  module_->SetUp();
  while (running_) {
    module_->Loop();
  }
}

void ModuleRunner::Stop() { running_ = false; }

}  // namespace slog