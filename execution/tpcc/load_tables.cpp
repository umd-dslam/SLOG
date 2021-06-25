#include "execution/tpcc/load_tables.h"

#include <algorithm>
#include <iostream>
#include <random>

#include "execution/tpcc/table.h"

namespace slog {
namespace tpcc {

namespace {

const size_t kRandStrPoolSize = 1000000;
const std::string kCharacters("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz ");

std::unique_ptr<std::mt19937> rg;
std::string rnd_str_pool;
size_t rnd_str_pool_offset;

std::string RandStr(size_t len) {
  if (rnd_str_pool_offset + len >= rnd_str_pool.size()) {
    rnd_str_pool_offset = 0;
  }
  auto res = rnd_str_pool.substr(rnd_str_pool_offset, len);
  rnd_str_pool_offset += len;
  return res;
}

void LoadItem(const StorageAdapterPtr& storage_adapter, int partition) {
  Table<ItemSchema> item(storage_adapter);

  std::cout << "Loading " << kMaxItems << " items" << std::endl;

  std::uniform_int_distribution<> price_rnd(100, 10000);
  for (int id = 1; id <= kMaxItems; id++) {
    item.Insert({
        MakeInt32Scalar(partition),
        MakeInt32Scalar(id),
        MakeInt32Scalar(id),
        MakeFixedTextScalar<24>(RandStr(24)),
        MakeInt32Scalar(price_rnd(*rg)),
        MakeFixedTextScalar<50>(RandStr(50)),
    });
  }
}

void LoadStock(const StorageAdapterPtr& storage_adapter, int w_id) {
  Table<StockSchema> stock(storage_adapter);

  std::uniform_int_distribution<> quantity_rng(10, 100);
  for (int id = 1; id <= kMaxItems; id++) {
    stock.Insert({
        MakeInt32Scalar(w_id),
        MakeInt32Scalar(id),
        MakeInt16Scalar(quantity_rng(*rg)),
        MakeFixedTextScalar<24>(RandStr(24)),
        MakeFixedTextScalar<24>(RandStr(24)),
        MakeFixedTextScalar<24>(RandStr(24)),
        MakeFixedTextScalar<24>(RandStr(24)),
        MakeFixedTextScalar<24>(RandStr(24)),
        MakeFixedTextScalar<24>(RandStr(24)),
        MakeFixedTextScalar<24>(RandStr(24)),
        MakeFixedTextScalar<24>(RandStr(24)),
        MakeFixedTextScalar<24>(RandStr(24)),
        MakeFixedTextScalar<24>(RandStr(24)),
        MakeInt32Scalar(0),
        MakeInt16Scalar(0),
        MakeInt16Scalar(0),
        MakeFixedTextScalar<50>(RandStr(50)),
    });
  }
}

void LoadDistrict(const StorageAdapterPtr& storage_adapter, int w_id) {
  Table<DistrictSchema> district(storage_adapter);

  std::uniform_int_distribution<> tax_rnd(10, 20);
  for (int id = 1; id <= kDistPerWare; id++) {
    // clang-format off
    district.Insert({MakeInt32Scalar(w_id),
                     MakeInt8Scalar(id),
                     MakeFixedTextScalar<10>(RandStr(10)),
                     MakeFixedTextScalar<20>(RandStr(20)),
                     MakeFixedTextScalar<20>(RandStr(20)),
                     MakeFixedTextScalar<20>(RandStr(20)),
                     MakeFixedTextScalar<2>(RandStr(2)),
                     MakeFixedTextScalar<9>(RandStr(9)),
                     MakeInt32Scalar(tax_rnd(*rg)),
                     MakeInt64Scalar(300000),
                     MakeInt32Scalar(3001)});
    // clang-format on
  }
}

void LoadWarehouse(const StorageAdapterPtr& storage_adapter, int W) {
  Table<WarehouseSchema> warehouse(storage_adapter);

  std::uniform_int_distribution<> tax_rnd(10, 20);
  for (int id = 1; id <= W; id++) {
    std::cout << "Loading data for warehouse: " << id << std::endl;
    // clang-format off
    warehouse.Insert({MakeInt32Scalar(id),
                      MakeFixedTextScalar<10>(RandStr(10)),
                      MakeFixedTextScalar<20>(RandStr(20)),
                      MakeFixedTextScalar<20>(RandStr(20)),
                      MakeFixedTextScalar<20>(RandStr(20)),
                      MakeFixedTextScalar<2>(RandStr(2)),
                      MakeFixedTextScalar<9>(RandStr(9)),
                      MakeInt32Scalar(tax_rnd(*rg)),
                      MakeInt64Scalar(300000000)});
    // clang-format on
    LoadStock(storage_adapter, id);
    LoadDistrict(storage_adapter, id);
  }
}

void LoadCustomer(const StorageAdapterPtr& storage_adapter, int W) {
  Table<CustomerSchema> customer(storage_adapter);
  Table<HistorySchema> history(storage_adapter);

  std::uniform_int_distribution<> discount_rnd(0, 50);
  std::bernoulli_distribution credit_rnd;
  for (int w_id = 1; w_id <= W; w_id++) {
    std::cout << "Loading customers in warehouse: " << w_id << std::endl;
    for (int d_id = 1; d_id <= kDistPerWare; d_id++) {
      for (int id = 1; id <= kCustPerDist; id++) {
        customer.Insert({MakeInt32Scalar(w_id),
                         MakeInt8Scalar(d_id),
                         MakeInt32Scalar(id),
                         MakeFixedTextScalar<16>(RandStr(16)),
                         MakeFixedTextScalar<2>("OE"),
                         MakeFixedTextScalar<16>(RandStr(16)),
                         MakeFixedTextScalar<20>(RandStr(20)),
                         MakeFixedTextScalar<20>(RandStr(20)),
                         MakeFixedTextScalar<20>(RandStr(20)),
                         MakeFixedTextScalar<2>(RandStr(2)),
                         MakeFixedTextScalar<9>(RandStr(9)),
                         MakeFixedTextScalar<16>(RandStr(16)),
                         MakeInt64Scalar(1234567890),
                         MakeFixedTextScalar<2>(credit_rnd(*rg) ? "GC" : "BC"),
                         MakeInt64Scalar(50000),
                         MakeInt32Scalar(discount_rnd(*rg)),
                         MakeInt64Scalar(-10),
                         MakeInt64Scalar(100),
                         MakeInt16Scalar(1),
                         MakeInt16Scalar(0),
                         MakeFixedTextScalar<250>(RandStr(250))});
        // clang-format off
        history.Insert({MakeInt32Scalar(w_id),
                        MakeInt8Scalar(d_id),
                        MakeInt32Scalar(id),
                        MakeInt32Scalar(1),
                        MakeInt8Scalar(d_id),
                        MakeInt32Scalar(w_id),
                        MakeInt64Scalar(1234567890),
                        MakeInt32Scalar(100),
                        MakeFixedTextScalar<24>(RandStr(24))});
        // clang-format on
      }
    }
  }
}

void LoadOrder(const StorageAdapterPtr& storage_adapter, int W) {
  Table<OrderSchema> order(storage_adapter);
  Table<NewOrderSchema> new_order(storage_adapter);
  Table<OrderLineSchema> order_line(storage_adapter);

  std::uniform_int_distribution<> carrier_id_rnd(1, 10);
  std::uniform_int_distribution<> item_rnd(1, kMaxItems);
  for (int w_id = 1; w_id <= W; w_id++) {
    std::cout << "Loading orders in warehouse: " << w_id << std::endl;
    for (int d_id = 1; d_id <= kDistPerWare; d_id++) {
      std::vector<int> cust_id(kCustPerDist);
      std::iota(cust_id.begin(), cust_id.end(), 1);
      std::shuffle(cust_id.begin(), cust_id.end(), *rg);
      for (int id = 1; id <= kOrdPerDist; id++) {
        // clang-format off
        order.Insert({MakeInt32Scalar(w_id),
                      MakeInt8Scalar(d_id),
                      MakeInt32Scalar(id),
                      MakeInt32Scalar(cust_id[id - 1]),
                      MakeInt64Scalar(1234567890),
                      MakeInt8Scalar(id > 2100 ? 0 : carrier_id_rnd(*rg)),
                      MakeInt8Scalar(kLinePerOrder), MakeInt8Scalar(1)});
        // clang-format on
        if (id > 2100) {
          new_order.Insert({MakeInt32Scalar(w_id), MakeInt8Scalar(d_id), MakeInt32Scalar(id)});
        }
        for (int ol_id = 1; ol_id <= kLinePerOrder; ol_id++) {
          // clang-format off
          order_line.Insert({MakeInt32Scalar(w_id),
                             MakeInt8Scalar(d_id),
                             MakeInt32Scalar(id),
                             MakeInt8Scalar(ol_id),
                             MakeInt32Scalar(item_rnd(*rg)),
                             MakeInt32Scalar(w_id),
                             MakeInt64Scalar(id > 2100 ? 0 : 1234567890),
                             MakeInt8Scalar(5),
                             MakeInt32Scalar(0),
                             MakeFixedTextScalar<24>(RandStr(24))});
          // clang-format on
        }
      }
    }
  }
}

}  // namespace

void LoadTables(const StorageAdapterPtr& storage_adapter, int W, int partition) {
  rg = std::make_unique<std::mt19937>();
  rnd_str_pool.clear();
  rnd_str_pool.reserve(kRandStrPoolSize);
  rnd_str_pool_offset = 0;
  std::uniform_int_distribution<uint32_t> char_rnd(0, kCharacters.size() - 1);
  for (size_t i = 0; i < kRandStrPoolSize; i++) {
    rnd_str_pool.push_back(kCharacters[char_rnd(*rg)]);
  }

  LoadItem(storage_adapter, partition);
  LoadWarehouse(storage_adapter, W);
  LoadCustomer(storage_adapter, W);
  LoadOrder(storage_adapter, W);
}

}  // namespace tpcc
}  // namespace slog