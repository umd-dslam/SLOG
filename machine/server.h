#pragma once

#include <thread>

#include <zmq.hpp>

class Server {
public:
  Server() {
    zmq::context_t context(1);
    socket_ = std::make_unique<zmq::socket_t>(context, ZMQ_PAIR);
    thread_ = std::thread(&Server::Run, this);
  }

  ~Server() {
    thread_.join();
  }

  void Run() {
    
  }

private: 
  std::thread thread_;
  std::unique_ptr<zmq::socket_t> socket_;
};