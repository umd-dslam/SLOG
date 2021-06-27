#include "execution/tpcc/transaction.h"

#include <gtest/gtest.h>

#include <iostream>

#include "common/proto_utils.h"
#include "execution/tpcc/load_tables.h"
#include "execution/tpcc/metadata_initializer.h"
#include "execution/tpcc/new_order.h"
#include "execution/tpcc/table.h"
#include "storage/mem_only_storage.h"

using namespace std;
using namespace slog;
using namespace slog::tpcc;

class TransactionTest : public ::testing::Test {
 protected:
  void SetUp() {
    storage = std::make_shared<MemOnlyStorage>();
    auto metadata_initializer = std::make_shared<TPCCMetadataInitializer>(2, 1);
    kv_storage_adapter = std::make_shared<KVStorageAdapter>(storage, metadata_initializer);
    LoadTables(kv_storage_adapter, W, 1);
  }

  void FlushAndRefreshTxn() {
    for (const auto& kv : txn.keys()) {
      const auto& key = kv.key();
      const auto& value = kv.value_entry();
      if (!value.new_value().empty()) {
        Record record(value.new_value());
        record.SetMetadata(value.metadata());
        storage->Write(key, record);
      }
    }
    for (const auto& key : txn.deleted_keys()) {
      ASSERT_TRUE(storage->Delete(key));
    }
    for (auto& kv : *(txn.mutable_keys())) {
      const auto& key = kv.key();
      Record record;
      if (storage->Read(key, record)) {
        auto value = kv.mutable_value_entry();
        value->set_value(record.to_string());
        value->clear_new_value();
      }
    }
    txn.clear_deleted_keys();
  }

  Transaction txn;
  const int W = 1;
  std::shared_ptr<Storage> storage;
  std::shared_ptr<KVStorageAdapter> kv_storage_adapter;
};

TEST_F(TransactionTest, DISABLED_InspectStorage) {
  {
    cout << "Warehouse\n";
    Table<WarehouseSchema> warehouse(kv_storage_adapter);
    vector<vector<ScalarPtr>> rows;
    for (int w_id = 1; w_id <= W; w_id++) {
      rows.push_back(warehouse.Select({MakeInt32Scalar(w_id)}));
    }
    Table<WarehouseSchema>::PrintRows(rows);
    cout << endl;
  }
  {
    cout << "District\n";
    Table<DistrictSchema> district(kv_storage_adapter);
    vector<vector<ScalarPtr>> rows;
    for (int w_id = 1; w_id <= W; w_id++) {
      for (int d_id = kDistPerWare - 2; d_id <= kDistPerWare; d_id++) {
        rows.push_back(district.Select({MakeInt32Scalar(w_id), MakeInt8Scalar(d_id)}));
      }
    }
    Table<DistrictSchema>::PrintRows(rows);
    cout << endl;
  }
  {
    cout << "Customer\n";
    Table<CustomerSchema> customer(kv_storage_adapter);
    vector<vector<ScalarPtr>> rows;
    for (int w_id = 1; w_id <= W; w_id++) {
      for (int d_id = kDistPerWare - 2; d_id <= kDistPerWare; d_id++) {
        for (int c_id = kCustPerDist - 2; c_id <= kCustPerDist; c_id++) {
          rows.push_back(customer.Select({MakeInt32Scalar(w_id), MakeInt8Scalar(d_id), MakeInt32Scalar(c_id)}));
        }
      }
    }
    Table<CustomerSchema>::PrintRows(rows);
    cout << endl;
  }
  {
    cout << "History\n";
    Table<HistorySchema> history(kv_storage_adapter);
    vector<vector<ScalarPtr>> rows;
    for (int w_id = 1; w_id <= W; w_id++) {
      for (int d_id = kDistPerWare - 2; d_id <= kDistPerWare; d_id++) {
        for (int c_id = kCustPerDist - 2; c_id <= kCustPerDist; c_id++) {
          rows.push_back(
              history.Select({MakeInt32Scalar(w_id), MakeInt8Scalar(d_id), MakeInt32Scalar(c_id), MakeInt32Scalar(1)}));
        }
      }
    }
    Table<HistorySchema>::PrintRows(rows);
    cout << endl;
  }
  {
    cout << "NewOrder\n";
    Table<NewOrderSchema> new_order(kv_storage_adapter);
    vector<vector<ScalarPtr>> rows;
    for (int w_id = 1; w_id <= W; w_id++) {
      for (int d_id = kDistPerWare - 2; d_id <= kDistPerWare; d_id++) {
        for (int o_id = kOrdPerDist - 2; o_id <= kOrdPerDist; o_id++) {
          rows.push_back(new_order.Select({MakeInt32Scalar(w_id), MakeInt8Scalar(d_id), MakeInt32Scalar(o_id)}));
        }
      }
    }
    Table<NewOrderSchema>::PrintRows(rows);
    cout << endl;
  }
  {
    cout << "Order\n";
    Table<OrderSchema> order(kv_storage_adapter);
    vector<vector<ScalarPtr>> rows;
    for (int w_id = 1; w_id <= W; w_id++) {
      for (int d_id = kDistPerWare - 2; d_id <= kDistPerWare; d_id++) {
        for (int o_id = kOrdPerDist - 2; o_id <= kOrdPerDist; o_id++) {
          rows.push_back(order.Select({MakeInt32Scalar(w_id), MakeInt8Scalar(d_id), MakeInt32Scalar(o_id)}));
        }
      }
    }
    Table<OrderSchema>::PrintRows(rows);
    cout << endl;
  }
  {
    cout << "OrderLine\n";
    Table<OrderLineSchema> order_line(kv_storage_adapter);
    vector<vector<ScalarPtr>> rows;
    for (int w_id = 1; w_id <= W; w_id++) {
      for (int d_id = kDistPerWare - 2; d_id <= kDistPerWare; d_id++) {
        for (int o_id = kOrdPerDist - 2; o_id <= kOrdPerDist; o_id++) {
          for (int ol_id = 1; ol_id <= kLinePerOrder; ol_id++) {
            rows.push_back(order_line.Select(
                {MakeInt32Scalar(w_id), MakeInt8Scalar(d_id), MakeInt32Scalar(o_id), MakeInt8Scalar(ol_id)}));
          }
        }
      }
    }
    Table<OrderLineSchema>::PrintRows(rows);
    cout << endl;
  }
  {
    cout << "Item\n";
    Table<ItemSchema> item(kv_storage_adapter);
    vector<vector<ScalarPtr>> rows;
    for (int w_id = 1; w_id <= W; w_id++) {
      for (int i_id = 1; i_id <= 5; i_id++) {
        rows.push_back(item.Select({MakeInt32Scalar(w_id), MakeInt32Scalar(i_id)}));
      }
    }
    Table<ItemSchema>::PrintRows(rows);
    cout << endl;
  }
  {
    cout << "Stock\n";
    Table<StockSchema> stock(kv_storage_adapter);
    vector<vector<ScalarPtr>> rows;
    for (int i_id = 1; i_id <= 10; i_id++) {
      rows.push_back(stock.Select({MakeInt32Scalar(1), MakeInt32Scalar(i_id)}));
    }
    Table<StockSchema>::PrintRows(rows);
    cout << endl;
  }
}

TEST_F(TransactionTest, NewOrder) {
  {
    auto key_gen_adapter = std::make_shared<TxnKeyGenStorageAdapter>(txn);
    std::vector<NewOrderTxn::OrderLine> ol;
    for (int i = 1; i <= kLinePerOrder; i++) {
      ol.push_back(NewOrderTxn::OrderLine{.id = i, .supply_w_id = 1, .item_id = i * 10, .quantity = 4});
    }
    NewOrderTxn new_order_txn(key_gen_adapter, 1, 2, 5, 5000, 1234567890, ol, 100);
    new_order_txn.Read();
    new_order_txn.Write();
    key_gen_adapter->Finialize();
  }
  FlushAndRefreshTxn();
  {
    auto txn_adapter = std::make_shared<TxnStorageAdapter>(txn);
    std::vector<NewOrderTxn::OrderLine> ol;
    for (int i = 1; i <= kLinePerOrder; i++) {
      ol.push_back(NewOrderTxn::OrderLine{.id = i, .supply_w_id = 1, .item_id = i * 10, .quantity = 4});
    }
    NewOrderTxn new_order_txn(txn_adapter, 1, 2, 5, 5000, 1234567890, ol, 100);
    ASSERT_TRUE(new_order_txn.Execute());
  }
}