#include "module/base/module.h"

#include "common/constants.h"
#include "common/mmessage.h"

using std::shared_ptr;
using std::unique_ptr;

namespace slog {

ModuleRunner::ModuleRunner(const shared_ptr<Module>& module) 
  : module_(module),
    running_(false) {}

ModuleRunner::~ModuleRunner() {
  running_ = false;
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

ChannelHolder::ChannelHolder(unique_ptr<Channel>&& channel) 
  : channel_(std::move(channel)) {}

void ChannelHolder::Send(
    const google::protobuf::Message& request_or_response,
    const string& to_machine_id,
    const string& to_channel,
    bool has_more) {
  MMessage message;
  message.SetIdentity(to_machine_id);
  message.Set(MM_PROTO, request_or_response);
  message.Set(MM_FROM_CHANNEL, channel_->GetName());
  message.Set(MM_TO_CHANNEL, to_channel);
  Send(std::move(message), has_more);
}

void ChannelHolder::SendSameMachine(
    const google::protobuf::Message& request_or_response,
    const string& to_channel,
    bool has_more) {
  MMessage message;
  message.Set(MM_PROTO, request_or_response);
  message.Set(MM_FROM_CHANNEL, channel_->GetName());
  message.Set(MM_TO_CHANNEL, to_channel);
  Send(std::move(message), has_more);
}

void ChannelHolder::SendSameChannel(
    const google::protobuf::Message& request_or_response,
    const string& to_machine_id,
    bool has_more) {
  MMessage message;
  message.SetIdentity(to_machine_id);
  message.Set(MM_PROTO, request_or_response);
  message.Set(MM_FROM_CHANNEL, channel_->GetName());
  message.Set(MM_TO_CHANNEL, channel_->GetName());
  Send(std::move(message), has_more);
}

void ChannelHolder::Send(MMessage&& message, bool has_more) {
  channel_->Send(message, has_more);
}

void ChannelHolder::ReceiveFromChannel(MMessage& message) {
  channel_->Receive(message);
}

zmq::pollitem_t ChannelHolder::GetChannelPollItem() const {
  return channel_->GetPollItem();
}

const shared_ptr<zmq::context_t>& ChannelHolder::GetContext() const {
  return channel_->GetContext();
}

} // namespace slog