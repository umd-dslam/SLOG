#pragma once

#include <vector>

#include "common/configuration.h"
#include "common/types.h"
#include "execution/tpcc/constants.h"
#include "proto/transaction.pb.h"
#include "workload/workload.h"

using std::vector;

namespace slog {

class TPCCWorkload : public Workload {
 public:
  TPCCWorkload(const ConfigurationPtr& config, uint32_t region, const std::string& params_str,
               std::pair<int, int> id_slot, const uint32_t seed = std::random_device()());

  std::pair<Transaction*, TransactionProfile> NextTransaction();

 private:
  void NewOrder(Transaction& txn, TransactionProfile& pro, int w_id, int partition);
  void Payment(Transaction& txn, TransactionProfile& pro, int w_id, int partition);
  void OrderStatus(Transaction& txn, int w_id);
  void Deliver(Transaction& txn, int w_id);
  void StockLevel(Transaction& txn, int w_id);

  std::vector<int> SelectRemoteWarehouses(int partition);

  ConfigurationPtr config_;
  uint32_t local_region_;
  std::vector<int> distance_ranking_;
  int zipf_coef_;
  vector<vector<vector<int>>> warehouse_index_;
  std::mt19937 rg_;
  TxnId client_txn_id_counter_;
  std::vector<int> txn_mix_;

  struct TPCCIds {
    TPCCIds(int i = 1) : o_id(tpcc::kOrdPerDist + i), no_o_id(1), h_id(i + 1) {}
    int o_id;
    int no_o_id;
    int h_id;
  };

  class TPCCIdGenerator {
   public:
    TPCCIdGenerator() = default;
    TPCCIdGenerator(int w, int init, int step) : max_o_id_(tpcc::kOrdPerDist), step_(step) {
      ids_.reserve(w);
      for (int i = 0; i < w; i++) {
        auto& warehouse = ids_.emplace_back();
        for (size_t j = 0; j < warehouse.size(); j++) {
          warehouse[j] = TPCCIds(init);
        }
      }
    }

    int NextOId(int w_id, int d_id) {
      auto o_id = ids_[w_id - 1][d_id - 1].o_id++;
      max_o_id_ = std::max(max_o_id_, o_id);
      return o_id;
    }

    int NextNOOId(int w_id, int d_id) { return ids_[w_id - 1][d_id - 1].no_o_id++; }

    int NextHId(int w_id, int d_id) { return ids_[w_id - 1][d_id - 1].h_id++; }

    int max_o_id() const { return max_o_id_; }

   private:
    std::vector<std::array<TPCCIds, tpcc::kDistPerWare>> ids_;
    int max_o_id_;
    int step_;
  } id_generator_;
};

}  // namespace slog