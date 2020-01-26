#include <vector>
#include <condition_variable>

#include <gtest/gtest.h>

#include "common/proto_utils.h"
#include "common/test_utils.h"
#include "paxos/simple_multi_paxos.h"

using namespace slog;
using namespace std;

using Pair = pair<uint32_t, uint32_t>;

class TestSimpleMultiPaxos : public SimpleMultiPaxos {
public:
  TestSimpleMultiPaxos(
      Broker& broker,
      const vector<string>& group_members,
      const string& me)
    : SimpleMultiPaxos("test", broker, group_members, me) {}

  Pair Poll() {
    unique_lock<mutex> lock(m_);
    // Wait until committed_ is not null
    bool ok = cv_.wait_for(
        lock, milliseconds(2000), [this]{return committed_ != nullptr;});
    if (!ok) {
      CHECK(false) << "Poll timed out";
    }
    Pair ret = *committed_;
    committed_.reset();
    return ret;
  }

protected:
  void OnCommit(uint32_t slot, uint32_t value) final {
    {
      lock_guard<mutex> g(m_);
      CHECK(committed_ == nullptr)
          << "The result needs to be read before committing another one";
      committed_.reset(new Pair(slot, value));
    }
    cv_.notify_all();
  }

private:
  unique_ptr<Pair> committed_;
  mutex m_;
  condition_variable cv_;
};

class PaxosTest : public ::testing::Test {
protected:
  void AddAndStartNewPaxos(
      shared_ptr<Configuration> config) {
    AddAndStartNewPaxos(
        config,
        config->GetAllMachineIds(),
        config->GetLocalMachineIdAsString());
  }

  void AddAndStartNewPaxos(
      shared_ptr<Configuration> config,
      const vector<string>& members,
      const string& me) {
    auto context = make_shared<zmq::context_t>(1);
    auto broker = new Broker(config, context, 5 /* timeout_ms */);
    auto paxos = make_shared<TestSimpleMultiPaxos>(*broker, members, me);
    auto paxos_runner = new ModuleRunner(paxos);
    auto client = new SimpleMultiPaxosClient(*paxos, "test");

    broker->StartInNewThread();
    paxos_runner->StartInNewThread();

    contexts_.push_back(context);
    broker_.emplace_back(broker);
    paxos_runner_.emplace_back(paxos_runner);
    
    paxi.push_back(paxos);
    clients.emplace_back(client);
  }

  vector<shared_ptr<TestSimpleMultiPaxos>> paxi;
  vector<unique_ptr<PaxosClient>> clients;

private:
  vector<shared_ptr<zmq::context_t>> contexts_;
  vector<unique_ptr<Broker>> broker_;
  vector<unique_ptr<ModuleRunner>> paxos_runner_;
};

TEST_F(PaxosTest, ProposeWithoutForwarding) {
  auto configs = MakeTestConfigurations("paxos", 1, 3);
  for (auto config : configs) {
    AddAndStartNewPaxos(config);
  }

  clients[0]->Propose(111);
  for (auto& paxos : paxi) {
    auto ret = paxos->Poll();
    ASSERT_EQ(0, ret.first);
    ASSERT_EQ(111, ret.second);
  }
}

TEST_F(PaxosTest, ProposeWithForwarding) {
  auto configs = MakeTestConfigurations("paxos", 1, 3);
  for (auto config : configs) {
    AddAndStartNewPaxos(config);
  }

  clients[1]->Propose(111);
  for (auto& paxos : paxi) {
    auto ret = paxos->Poll();
    ASSERT_EQ(0, ret.first);
    ASSERT_EQ(111, ret.second);
  }
}

TEST_F(PaxosTest, ProposeMultipleValues) {
  auto configs = MakeTestConfigurations("paxos", 1, 3);
  for (auto config : configs) {
    AddAndStartNewPaxos(config);
  }

  clients[0]->Propose(111);
  for (auto& paxos : paxi) {
    auto ret = paxos->Poll();
    ASSERT_EQ(0, ret.first);
    ASSERT_EQ(111, ret.second);
  }

  clients[1]->Propose(222);
  for (auto& paxos : paxi) {
    auto ret = paxos->Poll();
    ASSERT_EQ(1, ret.first);
    ASSERT_EQ(222, ret.second);
  }

  clients[2]->Propose(333);
  for (auto& paxos : paxi) {
    auto ret = paxos->Poll();
    ASSERT_EQ(2, ret.first);
    ASSERT_EQ(333, ret.second);
  }
}

TEST_F(PaxosTest, MultiRegionsWithNonMembers) {
  auto configs = MakeTestConfigurations("paxos", 2, 2);
  vector<string> members;
  auto member_part = configs.front()->GetGlobalPaxosMemberPartition();
  for (uint32_t rep = 0; rep < configs.front()->GetNumReplicas(); rep++) {
    members.emplace_back(MakeMachineId(rep, member_part));
  }
  for (auto config : configs) {
    AddAndStartNewPaxos(config, members, config->GetLocalMachineIdAsString());
  }

  auto non_member = (member_part + 1) % configs.front()->GetNumPartitions(); 
  clients[non_member]->Propose(111);
  for (auto& paxos : paxi) {
    if (paxos->IsMember()) {
      auto ret = paxos->Poll();
      ASSERT_EQ(0, ret.first);
      ASSERT_EQ(111, ret.second);
    }
  }
}