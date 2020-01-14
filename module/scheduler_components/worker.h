#pragma once

#include <zmq.hpp>

#include "benchmark/stored_procedures.h"
#include "common/types.h"
#include "module/base/module.h"
#include "proto/transaction.pb.h"
#include "storage/storage.h"

using std::shared_ptr;
using std::unique_ptr;

namespace slog {

class Worker : public Module {
public:
  Worker(
      zmq::context_t& context,
      shared_ptr<Storage<Key, Record>> storage);
  void SetUp() final;
  void Loop() final;

private:
  void ProcessTransaction(Transaction* txn);
  void ResponseToScheduler(TxnId txn_id);
  void ForwardToOutputter(Transaction* txn);

  zmq::socket_t scheduler_socket_;
  zmq::socket_t outputter_socket_;
  shared_ptr<Storage<Key, Record>> storage_;
  unique_ptr<StoredProcedures> stored_procedures_;
};

} // namespace slog