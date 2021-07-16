#include "module/base/module.h"

#include <glog/logging.h>

#include "common/constants.h"
#include "common/thread_utils.h"

using std::shared_ptr;
using std::unique_ptr;

namespace slog {

ModuleRunner::ModuleRunner(const shared_ptr<Module>& module) : module_(module), running_(false), setup_(false) {}

ModuleRunner::~ModuleRunner() {
  running_ = false;
  if (thread_.joinable()) {
    thread_.join();
    LOG(INFO) << module_->name() << " - thread stopped";
  }
}

void ModuleRunner::Start(std::optional<uint32_t> cpu) {
  if (running_) {
    throw std::runtime_error("The module has already started");
  }
  running_ = true;
  if (cpu.has_value()) {
    PinToCpu(pthread_self(), cpu.value());
    LOG(INFO) << module_->name() << " - pinned to CPU " << cpu.value();
  }
  Run();
}

void ModuleRunner::StartInNewThread(std::optional<uint32_t> cpu) {
  if (running_) {
    throw std::runtime_error("The module has already started");
  }
  running_ = true;
  thread_ = std::thread(&ModuleRunner::Run, this);
  SetThreadName(thread_.native_handle(), module_->name().c_str());
  if (cpu.has_value()) {
    PinToCpu(thread_.native_handle(), cpu.value());
    LOG(INFO) << module_->name() << " - pinned to CPU " << cpu.value();
  }
}

void ModuleRunner::Run() {
  if (!setup_) {
    module_->SetUp();
    setup_ = true;
  }
  while (running_) {
    if (module_->Loop()) Stop();
  }
}

void ModuleRunner::Stop() { running_ = false; }

}  // namespace slog