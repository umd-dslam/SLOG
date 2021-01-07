#include "connection/broker.h"

#include <glog/logging.h>

#include <sstream>
#include <unordered_set>

#include "common/proto_utils.h"
#include "common/thread_utils.h"
#include "connection/zmq_utils.h"
#include "proto/internal.pb.h"

using std::move;
using std::pair;
using std::string;
using std::unordered_set;

namespace slog {

using internal::Request;

Broker::Broker(const ConfigurationPtr& config, const shared_ptr<zmq::context_t>& context,
               std::chrono::milliseconds poll_timeout_ms)
    : config_(config),
      context_(context),
      poll_timeout_ms_(poll_timeout_ms),
      socket_(*context, ZMQ_PULL),
      running_(false),
      is_synchronized_(false) {
  // Remove all limits on the message queue
  socket_.set(zmq::sockopt::rcvhwm, 0);
}

Broker::~Broker() {
  running_ = false;
  LOG(INFO) << "Stopping Broker";
  thread_.join();
}

void Broker::StartInNewThread(std::optional<uint32_t> cpu) {
  if (running_) {
    return;
  }
  running_ = true;
  thread_ = std::thread(&Broker::Run, this);
  if (cpu.has_value()) {
    PinToCpu(pthread_self(), cpu.value());
  }
}

void Broker::Stop() { running_ = false; }

void Broker::AddChannel(Channel chan) {
  CHECK(!running_) << "Cannot add new channel. The broker has already been running";
  CHECK(channels_.count(chan) == 0) << "Channel \"" << chan << "\" already exists";

  zmq::socket_t new_channel(*context_, ZMQ_PUSH);
  new_channel.set(zmq::sockopt::sndhwm, 0);
  new_channel.connect("inproc://channel_" + std::to_string(chan));
  channels_.insert_or_assign(chan, move(new_channel));
}

const std::shared_ptr<zmq::context_t>& Broker::context() const { return context_; }

std::string Broker::GetEndpointByMachineId(MachineId machine_id) {
  std::unique_lock<std::mutex> lock(mutex_);
  while (!is_synchronized_) {
    cv_.wait(lock);
  }
  lock.unlock();

  // Once we can reach this point, we'll always be able to reach this point
  // and the machine_id_to_endpoint_ map becomes read-only.
  auto endpoint_it = machine_id_to_endpoint_.find(machine_id);
  if (endpoint_it == machine_id_to_endpoint_.end()) {
    LOG(FATAL) << "Invalid machine id: " << machine_id;
  }
  return endpoint_it->second;
}

MachineId Broker::GetLocalMachineId() const { return config_->local_machine_id(); }

string Broker::MakeEndpoint(const string& addr) const {
  std::stringstream endpoint;
  const auto& protocol = config_->protocol();
  endpoint << protocol << "://";
  if (addr.empty()) {
    if (protocol == "ipc") {
      endpoint << config_->local_address();
    } else {
      endpoint << "*";
    }
  } else {
    endpoint << addr;
  }
  auto port = config_->broker_port();
  if (port > 0) {
    endpoint << ":" << port;
  }
  return endpoint.str();
}

bool Broker::InitializeConnection() {
  // Bind the router to its endpoint
  socket_.bind(MakeEndpoint());
  LOG(INFO) << "Bound Broker to: " << MakeEndpoint();

  // Prepare a READY message
  Request request;
  auto ready = request.mutable_broker_ready();
  ready->set_ip_address(config_->local_address());
  ready->set_machine_id(config_->local_machine_id());

  // Connect to all other machines and send the READY message
  for (const auto& addr : config_->all_addresses()) {
    zmq::socket_t tmp_socket(*context_, ZMQ_PUSH);

    auto endpoint = MakeEndpoint(addr);
    tmp_socket.connect(endpoint);

    SendSerializedProto(tmp_socket, request);

    // See comment in class declaration
    tmp_sockets_.push_back(move(tmp_socket));
    LOG(INFO) << "Sent READY message to " << endpoint;
  }

  // This set represents the membership of all machines in the network.
  // Each machine is identified with its replica and partition. Each broker
  // needs to receive the READY message from all other machines to start working.
  unordered_set<MachineId> needed_machine_ids;
  for (uint32_t rep = 0; rep < config_->num_replicas(); rep++) {
    for (uint32_t part = 0; part < config_->num_partitions(); part++) {
      needed_machine_ids.insert(config_->MakeMachineId(rep, part));
    }
  }

  LOG(INFO) << "Waiting for READY messages from other machines...";
  zmq::pollitem_t item = GetSocketPollItem();
  while (running_) {
    if (zmq::poll(&item, 1, poll_timeout_ms_)) {
      zmq::message_t msg;
      if (!socket_.recv(msg, zmq::recv_flags::dontwait)) {
        continue;
      }

      if (!DeserializeProto(request, msg) || !request.has_broker_ready()) {
        LOG(INFO) << "Received a message while broker is not READY. "
                  << "Saving for later";
        unhandled_incoming_messages_.push_back(move(msg));
        continue;
      }

      // Use the information in each READY message to build up the translation maps
      const auto& ready = request.broker_ready();
      const auto& addr = ready.ip_address();
      const auto machine_id = ready.machine_id();
      const auto [replica, partition] = config_->UnpackMachineId(machine_id);

      if (needed_machine_ids.count(machine_id) == 0) {
        continue;
      }

      LOG(INFO) << "Received READY message from " << addr << " (rep: " << replica << ", part: " << partition << ")";

      machine_id_to_endpoint_.insert_or_assign(machine_id, MakeEndpoint(addr));
      needed_machine_ids.erase(machine_id);
    }

    if (needed_machine_ids.empty()) {
      LOG(INFO) << "All READY messages received";
      return true;
    }
  }
  return false;
}

void Broker::Run() {
  if (!InitializeConnection()) {
    LOG(ERROR) << "Unable to initialize connection";
    return;
  }

  // Notify threads waiting in GetEndpointByMachineId() that all brokers
  // has been synchronized
  std::unique_lock<std::mutex> lock(mutex_);
  is_synchronized_ = true;
  lock.unlock();
  cv_.notify_all();

  // Handle the unhandled messages received during initializing
  for (size_t i = 0; i < unhandled_incoming_messages_.size(); i++) {
    HandleIncomingMessage(move(unhandled_incoming_messages_[i]));
  }
  unhandled_incoming_messages_.clear();

  auto poll_item = GetSocketPollItem();
  while (running_) {
    // Wait until a message arrived at one of the sockets
    if (zmq::poll(&poll_item, 1, poll_timeout_ms_)) {
      for (int i = 0; i < 100; i++) {
        // Socket just received a message
        if (zmq::message_t msg; socket_.recv(msg, zmq::recv_flags::dontwait)) {
          HandleIncomingMessage(move(msg));
        } else {
          break;
        }
      }
    }
  }  // while-loop
}

void Broker::HandleIncomingMessage(zmq::message_t&& msg) {
  Channel chan_id;
  if (!ParseChannel(chan_id, msg)) {
    LOG(ERROR) << "Message without channel info";
    return;
  }
  auto chan_it = channels_.find(chan_id);
  if (chan_it == channels_.end()) {
    LOG(ERROR) << "Unknown channel: \"" << chan_id << "\". Dropping message";
    return;
  }
  chan_it->second.send(msg, zmq::send_flags::none);
}

zmq::pollitem_t Broker::GetSocketPollItem() { return {static_cast<void*>(socket_), 0, ZMQ_POLLIN, 0}; }

}  // namespace slog