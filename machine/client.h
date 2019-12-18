#pragma once

#include <thread>

#include <zmq.hpp>

class Client {
public:
  Client() {
    zmq::context_t context(1);
    socket_ = std::make_unique<zmq::socket_t>(context, ZMQ_DEALER);
    thread_ = std::thread(&Client::Run, this);
  }

  ~Client() {
    thread_.join();
  }

  void Run() {
    
  }

private: 
  std::thread thread_;
  std::unique_ptr<zmq::socket_t> socket_;
};