#include "module/base/networked_module.h"

#include <array>
#include <iostream>

#include "test/test_utils.h"

using namespace slog;
using namespace std;

class TestModule : public NetworkedModule {
 public:
  TestModule(const std::shared_ptr<Broker>& broker, int socket_number, array<int, 2> weights)
      : NetworkedModule("test_module", broker, socket_number, nullptr, kModuleTimeout),
        socket_number_(socket_number),
        weights_(weights) {}

 protected:
  void Initialize() final {
    zmq::socket_t socket(*context(), ZMQ_PULL);
    socket.bind(MakeInProcChannelAddress(socket_number_ + 1));
    AddCustomSocket(move(socket));
    SetMainVsCustomSocketWeights(weights_);
  }

  void OnInternalRequestReceived(EnvelopePtr&&) final { cout << "#"; }

  bool OnCustomSocket() final {
    auto& socket = GetCustomSocket(0);
    auto env = RecvEnvelope(socket, true /* dont_wait */);
    if (env == nullptr) {
      return false;
    }
    cout << "*";
    return true;
  }

 private:
  int socket_number_;
  array<int, 2> weights_;
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

  int msgs = 50;
  int socket_number = 0;

  {
    auto m1 = MakeRunnerFor<TestModule>(broker, socket_number, array<int, 2>({1, 1}));
    m1->StartInNewThread();
    cout << "1:1" << endl;
    for (int i = 0; i < msgs; i++) {
      SendEmptyEnv(sender, socket_number);
      SendEmptyEnv(sender, socket_number + 1);
    }
    this_thread::sleep_for(100ms);
    cout << endl;
  }
  {
    socket_number += 2;
    auto m1 = MakeRunnerFor<TestModule>(broker, socket_number, array<int, 2>({5, 1}));
    m1->StartInNewThread();
    cout << "5:1" << endl;
    for (int i = 0; i < msgs; i++) {
      SendEmptyEnv(sender, socket_number);
      SendEmptyEnv(sender, socket_number + 1);
    }
    this_thread::sleep_for(100ms);
    cout << endl;
  }
  {
    socket_number += 2;
    auto m1 = MakeRunnerFor<TestModule>(broker, socket_number, array<int, 2>({10, 1}));
    m1->StartInNewThread();
    cout << "10:1" << endl;
    for (int i = 0; i < msgs; i++) {
      SendEmptyEnv(sender, socket_number);
      SendEmptyEnv(sender, socket_number + 1);
    }
    this_thread::sleep_for(100ms);
    cout << endl;
  }
  {
    socket_number += 2;
    auto m1 = MakeRunnerFor<TestModule>(broker, socket_number, array<int, 2>({3, 2}));
    m1->StartInNewThread();
    cout << "3:2" << endl;
    for (int i = 0; i < msgs; i++) {
      SendEmptyEnv(sender, socket_number);
      SendEmptyEnv(sender, socket_number + 1);
    }
    this_thread::sleep_for(100ms);
    cout << endl;
  }
}