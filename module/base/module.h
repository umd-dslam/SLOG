#pragma once

#include <atomic>
#include <optional>
#include <thread>
#include <vector>

namespace slog {

/**
 * An interface for a module in SLOG. Most modules are extended from NetworkedModule
 * instead of directly from this class. A module only contains the instructions for
 * what to run. It has to be coupled with a ModuleRunner to be able to run.
 */
class Module {
 public:
  Module() = default;
  Module(const Module&) = delete;
  const Module& operator=(const Module&) = delete;
  virtual ~Module() {}

  /**
   * To be called before the main loop. This gives a chance to perform
   * all neccessary one-time initialization.
   */
  virtual void SetUp(){};

  /**
   * Contains the actions to be perform in one iteration of the main loop.
   * If this method returns true, the loop will stop.
   */
  virtual bool Loop() = 0;

  /**
   * Module name
   */
  virtual std::string name() const = 0;
};

/**
 * A ModuleRunner executes a Module. Its execution can either live in
 * a new thread or in the same thread as its caller.
 */
class ModuleRunner {
 public:
  ModuleRunner(const std::shared_ptr<Module>& module);
  ~ModuleRunner();

  void Start(std::optional<uint32_t> cpu = {});
  void StartInNewThread(std::optional<uint32_t> cpu = {});

  void Stop();

  std::shared_ptr<Module> module() { return module_; }
  bool is_running() const { return running_; }
  bool set_up() const { return setup_; }

 private:
  void Run();

  std::shared_ptr<Module> module_;
  std::thread thread_;
  std::atomic<bool> running_;
  std::atomic<bool> setup_;
};

/**
 * Helper function for creating a ModuleRunner.
 */
template <typename T, typename... Args>
inline std::unique_ptr<ModuleRunner> MakeRunnerFor(Args&&... args) {
  return std::make_unique<ModuleRunner>(std::make_shared<T>(std::forward<Args>(args)...));
}

}  // namespace slog