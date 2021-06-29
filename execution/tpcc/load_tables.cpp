#include "execution/tpcc/load_tables.h"

#include <algorithm>
#include <random>
#include <thread>

#include "common/string_utils.h"
#include "execution/tpcc/table.h"

namespace slog {
namespace tpcc {

namespace {
const size_t kRandStrPoolSize = 1000000;
const std::string kCharacters("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz ");
}  // namespace

class PartitionedTPCCDataLoader {
 public:
  PartitionedTPCCDataLoader(const StorageAdapterPtr& storage_adapter, int from_w, int to_w, int step, int seed)
      : rg_(seed), str_gen_(seed), storage_adapter_(storage_adapter), from_w_(from_w), to_w_(to_w), step_(step) {}

  void Load() {
    LoadWarehouse();
    LoadCustomer();
    LoadOrder();
  }

  static void LoadItem(const StorageAdapterPtr& storage_adapter, const std::vector<int>& rep_w) {
    Table<ItemSchema> item(storage_adapter);
    LOG(INFO) << "Loading " << kMaxItems << " items for each of " << rep_w.size() << " representative warehouses";

    std::mt19937 rg;
    RandomStringGenerator str_rnd;
    std::uniform_int_distribution<> price_rnd(100, 10000);
    for (int id = 1; id <= kMaxItems; id++) {
      auto name = MakeFixedTextScalar<24>(str_rnd(24));
      auto price = MakeInt32Scalar(price_rnd(rg));
      auto data = MakeFixedTextScalar<50>(str_rnd(50));
      for (auto w_id : rep_w) {
        item.Insert({
            MakeInt32Scalar(w_id),
            MakeInt32Scalar(id),
            MakeInt32Scalar(id),
            name,
            price,
            data,
        });
      }
    }
  }

 private:
  std::mt19937 rg_;
  RandomStringGenerator str_gen_;

  StorageAdapterPtr storage_adapter_;
  int from_w_;
  int to_w_;
  int step_;

  void LoadStock(int w_id) {
    Table<StockSchema> stock(storage_adapter_);

    std::uniform_int_distribution<> quantity_rng(10, 100);
    for (int id = 1; id <= kMaxItems; id++) {
      stock.Insert({
          MakeInt32Scalar(w_id),
          MakeInt32Scalar(id),
          MakeInt16Scalar(quantity_rng(rg_)),
          MakeFixedTextScalar<240>(str_gen_(240)),
          MakeInt32Scalar(0),
          MakeInt16Scalar(0),
          MakeInt16Scalar(0),
          MakeFixedTextScalar<50>(str_gen_(50)),
      });
    }
  }

  void LoadDistrict(int w_id) {
    Table<DistrictSchema> district(storage_adapter_);

    std::uniform_int_distribution<> tax_rnd(10, 20);
    for (int id = 1; id <= kDistPerWare; id++) {
      // clang-format off
      district.Insert({MakeInt32Scalar(w_id),
                      MakeInt8Scalar(id),
                      MakeFixedTextScalar<10>(str_gen_(10)),
                      MakeFixedTextScalar<71>(str_gen_(71)),
                      MakeInt32Scalar(tax_rnd(rg_)),
                      MakeInt64Scalar(300000),
                      MakeInt32Scalar(3001)});
      // clang-format on
    }
  }

  void LoadWarehouse() {
    Table<WarehouseSchema> warehouse(storage_adapter_);

    std::uniform_int_distribution<> tax_rnd(10, 20);
    for (int id = from_w_; id < to_w_; id += step_) {
      LOG(INFO) << "Loading data for warehouse: " << id << std::endl;
      // clang-format off
      warehouse.Insert({MakeInt32Scalar(id),
                        MakeFixedTextScalar<10>(str_gen_(10)),
                        MakeFixedTextScalar<71>(str_gen_(71)),
                        MakeInt32Scalar(tax_rnd(rg_)),
                        MakeInt64Scalar(300000000)});
      // clang-format on
      LoadStock(id);
      LoadDistrict(id);
    }
  }

  void LoadCustomer() {
    Table<CustomerSchema> customer(storage_adapter_);
    Table<HistorySchema> history(storage_adapter_);

    std::uniform_int_distribution<> discount_rnd(0, 50);
    std::bernoulli_distribution credit_rnd;
    for (int w_id = from_w_; w_id < to_w_; w_id += step_) {
      LOG(INFO) << "Loading customers in warehouse: " << w_id << std::endl;
      for (int d_id = 1; d_id <= kDistPerWare; d_id++) {
        for (int id = 1; id <= kCustPerDist; id++) {
          customer.Insert({MakeInt32Scalar(w_id), MakeInt8Scalar(d_id), MakeInt32Scalar(id),
                           MakeFixedTextScalar<34>(str_gen_(34)), MakeFixedTextScalar<71>(str_gen_(71)),
                           MakeFixedTextScalar<16>(str_gen_(16)), MakeInt64Scalar(1234567890),
                           MakeFixedTextScalar<2>(credit_rnd(rg_) ? "GC" : "BC"), MakeInt64Scalar(50000),
                           MakeInt32Scalar(discount_rnd(rg_)), MakeInt64Scalar(-10), MakeInt64Scalar(100),
                           MakeInt16Scalar(1), MakeInt16Scalar(0), MakeFixedTextScalar<250>(str_gen_(250))});
          // clang-format off
          history.Insert({MakeInt32Scalar(w_id),
                          MakeInt8Scalar(d_id),
                          MakeInt32Scalar(id),
                          MakeInt32Scalar(1),
                          MakeInt8Scalar(d_id),
                          MakeInt32Scalar(w_id),
                          MakeInt64Scalar(1234567890),
                          MakeInt32Scalar(100),
                          MakeFixedTextScalar<24>(str_gen_(24))});
          // clang-format on
        }
      }
    }
  }

  void LoadOrder() {
    Table<OrderSchema> order(storage_adapter_);
    Table<NewOrderSchema> new_order(storage_adapter_);
    Table<OrderLineSchema> order_line(storage_adapter_);

    std::uniform_int_distribution<> carrier_id_rnd(1, 10);
    std::uniform_int_distribution<> item_rnd(1, kMaxItems);
    for (int w_id = from_w_; w_id < to_w_; w_id += step_) {
      LOG(INFO) << "Loading orders in warehouse: " << w_id << std::endl;
      for (int d_id = 1; d_id <= kDistPerWare; d_id++) {
        std::vector<int> cust_id(kCustPerDist);
        std::iota(cust_id.begin(), cust_id.end(), 1);
        std::shuffle(cust_id.begin(), cust_id.end(), rg_);
        for (int id = 1; id <= kOrdPerDist; id++) {
          // clang-format off
          order.Insert({MakeInt32Scalar(w_id),
                        MakeInt8Scalar(d_id),
                        MakeInt32Scalar(id),
                        MakeInt32Scalar(cust_id[id - 1]),
                        MakeInt64Scalar(1234567890),
                        MakeInt8Scalar(id > 2100 ? 0 : carrier_id_rnd(rg_)),
                        MakeInt8Scalar(kLinePerOrder), MakeInt8Scalar(1)});
          // clang-format on
          if (id > 2100) {
            new_order.Insert({MakeInt32Scalar(w_id), MakeInt8Scalar(d_id), MakeInt32Scalar(id), MakeInt8Scalar(0)});
          }
          for (int ol_id = 1; ol_id <= kLinePerOrder; ol_id++) {
            // clang-format off
            order_line.Insert({MakeInt32Scalar(w_id),
                              MakeInt8Scalar(d_id),
                              MakeInt32Scalar(id),
                              MakeInt8Scalar(ol_id),
                              MakeInt32Scalar(item_rnd(rg_)),
                              MakeInt32Scalar(w_id),
                              MakeInt64Scalar(id > 2100 ? 0 : 1234567890),
                              MakeInt8Scalar(5),
                              MakeInt32Scalar(0),
                              MakeFixedTextScalar<24>(str_gen_(24))});
            // clang-format on
          }
        }
      }
    }
  }
};

void LoadTables(const StorageAdapterPtr& storage_adapter, int W, int num_replicas, int num_partitions, int partition,
                int num_threads) {
  LOG(INFO) << "Generating ~" << W / num_partitions << " warehouses using " << num_threads << " threads. ";

  std::vector<int> rep_w;
  int rep_w_counter = partition;
  for (int i = 0; i < num_replicas; i++) {
    rep_w.push_back(rep_w_counter + 1);
    rep_w_counter += num_partitions;
  }
  PartitionedTPCCDataLoader::LoadItem(storage_adapter, rep_w);

  std::atomic<int> num_done = 0;
  auto LoadFn = [&](int from_w, int to_w, int seed) {
    PartitionedTPCCDataLoader loader(storage_adapter, from_w, to_w, num_partitions, seed);
    loader.Load();
    num_done++;
  };
  std::vector<std::thread> threads;
  int range = W / num_threads + 1;
  for (int i = 0; i < num_threads; i++) {
    int range_start = i * range;
    int partition_of_range_start = range_start % num_partitions;
    int distance_to_next_in_partition_key = (partition - partition_of_range_start + num_partitions) % num_partitions;
    int from_key = range_start + distance_to_next_in_partition_key;
    int to_key = std::min((i + 1) * range, W);
    threads.emplace_back(LoadFn, from_key + 1, to_key + 1, i);
  }
  while (num_done < num_threads) {
    std::this_thread::sleep_for(std::chrono::seconds(5));
  }
  for (auto& t : threads) {
    t.join();
  }
}

}  // namespace tpcc
}  // namespace slog