#include "module/base/networked_module.h"

#include <array>
#include <iostream>

#include "test/test_utils.h"

using namespace slog;
using namespace std;

class TestModule : public NetworkedModule {
 public:
  TestModule(const std::shared_ptr<Broker>& broker, int socket_number, array<int, 2> weights)
      : NetworkedModule(broker, socket_number, nullptr, kModuleTimeout),
        socket_number_(socket_number),
        weights_(weights) {}

  std::string name() const override { return "Test-Module"; }

  const std::string& result() const { return result_; }

 protected:
  void Initialize() final {
    zmq::socket_t socket(*context(), ZMQ_PULL);
    socket.bind(MakeInProcChannelAddress(socket_number_ + 1));
    AddCustomSocket(move(socket));
    SetMainVsCustomSocketWeights(weights_);
  }

  void OnInternalRequestReceived(EnvelopePtr&&) final { result_ += "#"; }

  bool OnCustomSocket() final {
    auto& socket = GetCustomSocket(0);
    auto env = RecvEnvelope(socket, true /* dont_wait */);
    if (env == nullptr) {
      return false;
    }
    result_ += "*";
    return true;
  }

 private:
  int socket_number_;
  array<int, 2> weights_;
  std::string result_;
};

void SendEmptyEnv(Sender& sender, int socket_number) {
  auto env = make_unique<internal::Envelope>();
  env->set_from(0);
  env->mutable_request();
  sender.Send(move(env), socket_number);
}

int main() {
  auto config = MakeTestConfigurations("", 1, 1)[0];
  auto broker = Broker::New(config);
  Sender sender(config, broker->context());

  int msgs = 100;
  int socket_number = 0;

  {
    auto m = MakeRunnerFor<TestModule>(broker, socket_number, array<int, 2>({1, 1}));
    cout << "1:1" << endl;
    for (int i = 0; i < msgs; i++) {
      SendEmptyEnv(sender, socket_number);
      SendEmptyEnv(sender, socket_number + 1);
    }
    m->StartInNewThread();
    this_thread::sleep_for(500ms);
    cout << static_pointer_cast<TestModule>(m->module())->result() << endl;
  }
  {
    socket_number += 2;
    auto m = MakeRunnerFor<TestModule>(broker, socket_number, array<int, 2>({5, 1}));
    cout << "5:1" << endl;
    for (int i = 0; i < msgs; i++) {
      SendEmptyEnv(sender, socket_number);
      SendEmptyEnv(sender, socket_number + 1);
    }
    m->StartInNewThread();
    this_thread::sleep_for(500ms);
    cout << static_pointer_cast<TestModule>(m->module())->result() << endl;
  }
  {
    socket_number += 2;
    auto m = MakeRunnerFor<TestModule>(broker, socket_number, array<int, 2>({10, 1}));
    cout << "10:1" << endl;
    for (int i = 0; i < msgs; i++) {
      SendEmptyEnv(sender, socket_number);
      SendEmptyEnv(sender, socket_number + 1);
    }
    m->StartInNewThread();
    this_thread::sleep_for(500ms);
    cout << static_pointer_cast<TestModule>(m->module())->result() << endl;
  }
  {
    socket_number += 2;
    auto m = MakeRunnerFor<TestModule>(broker, socket_number, array<int, 2>({1, 10}));
    cout << "1:10" << endl;
    for (int i = 0; i < msgs; i++) {
      SendEmptyEnv(sender, socket_number);
      SendEmptyEnv(sender, socket_number + 1);
    }
    m->StartInNewThread();
    this_thread::sleep_for(500ms);
    cout << static_pointer_cast<TestModule>(m->module())->result() << endl;
  }
  {
    socket_number += 2;
    auto m = MakeRunnerFor<TestModule>(broker, socket_number, array<int, 2>({3, 2}));
    cout << "3:2" << endl;
    for (int i = 0; i < msgs; i++) {
      SendEmptyEnv(sender, socket_number);
      SendEmptyEnv(sender, socket_number + 1);
    }
    m->StartInNewThread();
    this_thread::sleep_for(500ms);
    cout << static_pointer_cast<TestModule>(m->module())->result() << endl;
  }
}