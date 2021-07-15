#include "workload/tpcc_workload.h"

#include <glog/logging.h>

#include <random>

#include "common/proto_utils.h"
#include "execution/tpcc/constants.h"
#include "execution/tpcc/transaction.h"

using std::bernoulli_distribution;
using std::iota;
using std::sample;
using std::to_string;
using std::unordered_set;

namespace slog {
namespace {

// Partition that is used in a single-partition transaction.
// Use a negative number to select a random partition for
// each transaction
constexpr char PARTITION[] = "sp_partition";
// Max number of regions to select warehouse from
constexpr char HOMES[] = "homes";
// Zipf coefficient for selecting regions to access in a txn. Must be non-negative.
// The lower this is, the more uniform the regions are selected
constexpr char MH_ZIPF[] = "mh_zipf";
// Colon-separated list of % of the 5 txn types. Default is: "45:43:4:4:4"
constexpr char TXN_MIX[] = "mix";
// Only send single-home transactions
constexpr char SH_ONLY[] = "sh_only";

const RawParamMap DEFAULT_PARAMS = {
    {PARTITION, "-1"}, {HOMES, "2"}, {MH_ZIPF, "0"}, {TXN_MIX, "45:43:4:4:4"}, {SH_ONLY, "0"}};

template <typename G>
int NURand(G& g, int A, int x, int y) {
  std::uniform_int_distribution<> rand1(0, A);
  std::uniform_int_distribution<> rand2(x, y);
  return (rand1(g) | rand2(g)) % (y - x + 1) + x;
}

template <typename T, typename G>
T SampleOnce(G& g, const std::vector<T>& source) {
  CHECK(!source.empty());
  size_t i = std::uniform_int_distribution<size_t>(0, source.size() - 1)(g);
  return source[i];
}

}  // namespace

TPCCWorkload::TPCCWorkload(const ConfigurationPtr& config, uint32_t region, const string& params_str,
                           std::pair<int, int> id_slot, const uint32_t seed)
    : Workload(DEFAULT_PARAMS, params_str),
      config_(config),
      local_region_(region),
      distance_ranking_(config->distance_ranking_from(region)),
      zipf_coef_(params_.GetInt32(MH_ZIPF)),
      rg_(seed),
      client_txn_id_counter_(0) {
  name_ = "tpcc";
  CHECK(config_->proto_config().has_tpcc_partitioning()) << "TPC-C workload is only compatible with TPC-C partitioning";

  auto num_replicas = config_->num_replicas();
  if (distance_ranking_.empty()) {
    for (size_t i = 0; i < num_replicas; i++) {
      if (i != local_region_) {
        distance_ranking_.push_back(i);
      }
    }
    if (zipf_coef_ > 0) {
      LOG(WARNING) << "Distance ranking is not provided. MH_ZIPF is reset to 0.";
      zipf_coef_ = 0;
    }
  }

  CHECK_EQ(distance_ranking_.size(), num_replicas - 1) << "Distance ranking size must match the number of regions";

  auto num_partitions = config_->num_partitions();
  for (size_t i = 0; i < num_partitions; i++) {
    vector<vector<int>> partitions(num_replicas);
    warehouse_index_.push_back(partitions);
  }
  auto num_warehouses = config_->proto_config().tpcc_partitioning().warehouses();
  for (int i = 0; i < num_warehouses; i++) {
    int partition = i % num_partitions;
    int home = i / num_partitions % num_replicas;
    warehouse_index_[partition][home].push_back(i + 1);
  }
  id_generator_ = TPCCIdGenerator(num_warehouses, id_slot.first, id_slot.second);

  auto txn_mix_str = Split(params_.GetString(TXN_MIX), ":");
  CHECK_EQ(txn_mix_str.size(), 5) << "There must be exactly 5 values for txn mix";
  for (const auto& t : txn_mix_str) {
    txn_mix_.push_back(std::stoi(t));
  }
}

std::pair<Transaction*, TransactionProfile> TPCCWorkload::NextTransaction() {
  TransactionProfile pro;

  pro.client_txn_id = client_txn_id_counter_;
  pro.is_multi_partition = false;
  pro.is_multi_home = false;

  auto num_partitions = config_->num_partitions();
  auto partition = params_.GetInt32(PARTITION);
  if (partition < 0) {
    partition = std::uniform_int_distribution<>(0, num_partitions - 1)(rg_);
  }

  const auto& selectable_w = warehouse_index_[partition][local_region_];
  CHECK(!selectable_w.empty()) << "Not enough warehouses";
  int w = SampleOnce(rg_, selectable_w);

  Transaction* txn = new Transaction();
  std::discrete_distribution<> select_tpcc_txn(txn_mix_.begin(), txn_mix_.end());
  switch (select_tpcc_txn(rg_)) {
    case 0:
      NewOrder(*txn, pro, w, partition);
      break;
    case 1:
      Payment(*txn, pro, w, partition);
      break;
    case 2:
      OrderStatus(*txn, w);
      break;
    case 3:
      Deliver(*txn, w);
      break;
    case 4:
      StockLevel(*txn, w);
      break;
    default:
      LOG(FATAL) << "Invalid txn choice";
  }

  txn->mutable_internal()->set_id(client_txn_id_counter_);
  client_txn_id_counter_++;

  return {txn, pro};
}

void TPCCWorkload::NewOrder(Transaction& txn, TransactionProfile& pro, int w_id, int partition) {
  auto txn_adapter = std::make_shared<tpcc::TxnKeyGenStorageAdapter>(txn);

  auto remote_warehouses = SelectRemoteWarehouses(partition);
  int d_id = std::uniform_int_distribution<>(1, tpcc::kDistPerWare)(rg_);
  int c_id = NURand(rg_, 1023, 1, tpcc::kCustPerDist);
  int o_id = id_generator_.NextOId(w_id, d_id);
  int i_w_id = partition + static_cast<int>(local_region_ * config_->num_partitions()) + 1;
  auto datetime = std::chrono::system_clock::now().time_since_epoch().count();
  std::array<tpcc::NewOrderTxn::OrderLine, tpcc::kLinePerOrder> ol;
  std::bernoulli_distribution is_remote(0.01);
  std::uniform_int_distribution<> quantity_rnd(1, 10);
  for (size_t i = 0; i < tpcc::kLinePerOrder; i++) {
    auto supply_w_id = w_id;
    if (is_remote(rg_) && !remote_warehouses.empty()) {
      supply_w_id = remote_warehouses[i % remote_warehouses.size()];
      pro.is_multi_home = true;
    }
    ol[i] = tpcc::NewOrderTxn::OrderLine({
        .id = static_cast<int>(i),
        .supply_w_id = supply_w_id,
        .item_id = NURand(rg_, 8191, 1, tpcc::kMaxItems),
        .quantity = quantity_rnd(rg_),
    });
  }

  tpcc::NewOrderTxn new_order_txn(txn_adapter, w_id, d_id, c_id, o_id, datetime, i_w_id, ol);
  new_order_txn.Read();
  new_order_txn.Write();
  txn_adapter->Finialize();

  auto procedure = txn.mutable_code()->add_procedures();
  procedure->add_args("new_order");
  procedure->add_args(to_string(w_id));
  procedure->add_args(to_string(d_id));
  procedure->add_args(to_string(c_id));
  procedure->add_args(to_string(o_id));
  procedure->add_args(to_string(datetime));
  procedure->add_args(to_string(i_w_id));
  for (const auto& l : ol) {
    auto order_lines = txn.mutable_code()->add_procedures();
    order_lines->add_args(to_string(l.id));
    order_lines->add_args(to_string(l.supply_w_id));
    order_lines->add_args(to_string(l.item_id));
    order_lines->add_args(to_string(l.quantity));
  }
}

void TPCCWorkload::Payment(Transaction& txn, TransactionProfile& pro, int w_id, int partition) {
  auto txn_adapter = std::make_shared<tpcc::TxnKeyGenStorageAdapter>(txn);

  auto remote_warehouses = SelectRemoteWarehouses(partition);
  std::uniform_int_distribution<> d_id_rnd(1, tpcc::kDistPerWare);
  int c_id = NURand(rg_, 1023, 1, tpcc::kCustPerDist);
  auto datetime = std::chrono::system_clock::now().time_since_epoch().count();
  std::uniform_int_distribution<> quantity_rnd(1, 10);
  std::bernoulli_distribution is_remote(0.01);

  auto d_id = d_id_rnd(rg_);
  auto c_w_id = w_id;
  auto c_d_id = d_id;
  auto h_id = id_generator_.NextHId(w_id, d_id);
  auto amount = std::uniform_int_distribution<int64_t>(100, 500000)(rg_);
  if (is_remote(rg_) && !remote_warehouses.empty()) {
    c_w_id = SampleOnce(rg_, remote_warehouses);
    c_d_id = d_id_rnd(rg_);
    pro.is_multi_home = true;
  }
  tpcc::PaymentTxn payment_txn(txn_adapter, w_id, d_id, c_w_id, c_d_id, c_id, amount, datetime, h_id);
  payment_txn.Read();
  payment_txn.Write();
  txn_adapter->Finialize();

  auto procedure = txn.mutable_code()->add_procedures();
  procedure->add_args("payment");
  procedure->add_args(to_string(w_id));
  procedure->add_args(to_string(d_id));
  procedure->add_args(to_string(c_w_id));
  procedure->add_args(to_string(c_d_id));
  procedure->add_args(to_string(c_id));
  procedure->add_args(to_string(amount));
  procedure->add_args(to_string(datetime));
  procedure->add_args(to_string(h_id));
}

void TPCCWorkload::OrderStatus(Transaction& txn, int w_id) {
  auto txn_adapter = std::make_shared<tpcc::TxnKeyGenStorageAdapter>(txn);

  auto d_id = std::uniform_int_distribution<>(1, tpcc::kDistPerWare)(rg_);
  int c_id = NURand(rg_, 1023, 1, tpcc::kCustPerDist);
  auto max_o_id = id_generator_.max_o_id();
  auto o_id = std::uniform_int_distribution<>(max_o_id - 5, max_o_id)(rg_);

  tpcc::OrderStatusTxn order_status_txn(txn_adapter, w_id, d_id, c_id, o_id);
  order_status_txn.Read();
  txn_adapter->Finialize();

  auto procedure = txn.mutable_code()->add_procedures();
  procedure->add_args("order_status");
  procedure->add_args(to_string(w_id));
  procedure->add_args(to_string(d_id));
  procedure->add_args(to_string(c_id));
  procedure->add_args(to_string(o_id));
}

void TPCCWorkload::Deliver(Transaction& txn, int w_id) {
  auto txn_adapter = std::make_shared<tpcc::TxnKeyGenStorageAdapter>(txn);
  int c_id = NURand(rg_, 1023, 1, tpcc::kCustPerDist);
  auto d_id = std::uniform_int_distribution<>(1, tpcc::kDistPerWare)(rg_);
  auto no_o_id = id_generator_.NextNOOId(w_id, d_id);
  auto datetime = std::chrono::system_clock::now().time_since_epoch().count();
  auto carrier = std::uniform_int_distribution<>(1, 10)(rg_);
  tpcc::DeliverTxn deliver(txn_adapter, w_id, d_id, no_o_id, c_id, carrier, datetime);
  deliver.Read();
  deliver.Write();
  txn_adapter->Finialize();

  auto procedure = txn.mutable_code()->add_procedures();
  procedure->add_args("deliver");
  procedure->add_args(to_string(w_id));
  procedure->add_args(to_string(d_id));
  procedure->add_args(to_string(no_o_id));
  procedure->add_args(to_string(c_id));
  procedure->add_args(to_string(carrier));
  procedure->add_args(to_string(datetime));
}

void TPCCWorkload::StockLevel(Transaction& txn, int w_id) {
  auto txn_adapter = std::make_shared<tpcc::TxnKeyGenStorageAdapter>(txn);

  auto d_id = std::uniform_int_distribution<>(1, tpcc::kDistPerWare)(rg_);
  auto o_id = id_generator_.max_o_id();
  std::array<int, tpcc::StockLevelTxn::kTotalItems> i_ids;
  std::uniform_int_distribution<> i_id_rnd(1, tpcc::kMaxItems);
  for (size_t i = 0; i < i_ids.size(); i++) {
    i_ids[i] = i_id_rnd(rg_);
  }
  tpcc::StockLevelTxn stock_level(txn_adapter, w_id, d_id, o_id, i_ids);
  stock_level.Read();
  txn_adapter->Finialize();

  auto procedure = txn.mutable_code()->add_procedures();
  procedure->add_args("stock_level");
  procedure->add_args(to_string(w_id));
  procedure->add_args(to_string(d_id));
  procedure->add_args(to_string(o_id));
  auto items = txn.mutable_code()->add_procedures();
  for (auto i_id : i_ids) {
    items->add_args(to_string(i_id));
  }
}

std::vector<int> TPCCWorkload::SelectRemoteWarehouses(int partition) {
  if (params_.GetInt32(SH_ONLY) == 1) {
    return {SampleOnce(rg_, warehouse_index_[partition][local_region_])};
  }

  auto num_replicas = config_->num_replicas();
  auto max_num_homes = std::min(params_.GetUInt32(HOMES), num_replicas);
  if (max_num_homes < 2) {
    return {};
  }
  auto num_homes = std::uniform_int_distribution{2U, max_num_homes}(rg_);
  auto remote_warehouses = zipf_sample(rg_, zipf_coef_, distance_ranking_, num_homes - 1);

  for (size_t i = 0; i < remote_warehouses.size(); i++) {
    auto r = remote_warehouses[i];
    remote_warehouses[i] = SampleOnce(rg_, warehouse_index_[partition][r]);
  }

  return remote_warehouses;
}

}  // namespace slog