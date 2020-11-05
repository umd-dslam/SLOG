#pragma once

#include <atomic>
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
  Module(const std::string& name) : name_(name) {};
  Module(const Module&) = delete;
  const Module& operator=(const Module&) = delete;
  virtual ~Module() {}

  /**
   * To be called before the main loop. This gives a chance to perform 
   * all neccessary one-time initialization.
   */
  virtual void SetUp() {};

  /**
   * Contains the actions to be perform in one iteration of the main loop
   */
  virtual void Loop() = 0;

  const std::string& name() const { return name_; }

private:
  std::string name_;
};

/**
 * A ModuleRunner executes a Module. Its execution can either live in
 * a new thread or in the same thread as its caller.
 */
class ModuleRunner {
public:
  ModuleRunner(const std::shared_ptr<Module>& module);
  ~ModuleRunner();

  void Start();
  void StartInNewThread();

private:
  void Run();

  std::shared_ptr<Module> module_;
  std::thread thread_;
  std::atomic<bool> running_;
};

/**
 * Helper function for creating a ModuleRunner.
 */
template<typename T, typename... Args>
inline std::unique_ptr<ModuleRunner>
MakeRunnerFor(Args&&... args)
{
  return std::make_unique<ModuleRunner>(
      std::make_shared<T>(std::forward<Args>(args)...));
}

} // namespace slog