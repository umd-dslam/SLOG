#include "module/module.h"

#include "common/constants.h"
#include "common/mmessage.h"

namespace slog {

ModuleRunner::ModuleRunner(Module* module) 
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

ChannelHolder::ChannelHolder(unique_ptr<Channel>&& listener) 
  : channel_(std::move(listener)) {}

void ChannelHolder::Send(
    const google::protobuf::Message& request_or_response,
    const string& to_machine_id,
    const string& to_channel) {
  MMessage message;
  message.SetIdentity(to_machine_id);
  message.Set(MM_PROTO, request_or_response);
  message.Set(MM_FROM_CHANNEL, channel_->GetName());
  message.Set(MM_TO_CHANNEL, to_channel);
  Send(std::move(message));
}

void ChannelHolder::SendSameMachine(
    const google::protobuf::Message& request_or_response,
    const string& to_channel) {
  MMessage message;
  message.Set(MM_PROTO, request_or_response);
  message.Set(MM_FROM_CHANNEL, channel_->GetName());
  message.Set(MM_TO_CHANNEL, to_channel);
  Send(std::move(message));
}

void ChannelHolder::SendSameChannel(
    const google::protobuf::Message& request_or_response,
    const string& to_machine_id) {
  MMessage message;
  message.SetIdentity(to_machine_id);
  message.Set(MM_PROTO, request_or_response);
  message.Set(MM_FROM_CHANNEL, channel_->GetName());
  message.Set(MM_TO_CHANNEL, channel_->GetName());
  Send(std::move(message));
}

void ChannelHolder::Send(MMessage&& message) {
  channel_->Send(message);
}

void ChannelHolder::ReceiveFromChannel(MMessage& message) {
  channel_->Receive(message);
}

zmq::pollitem_t ChannelHolder::GetChannelPollItem() const {
  return channel_->GetPollItem();
}


} // namespace slog