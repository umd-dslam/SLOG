#include "execution/tpcc/transaction.h"

#include <gtest/gtest.h>

#include <iostream>

#include "common/proto_utils.h"
#include "execution/tpcc/load_tables.h"
#include "execution/tpcc/metadata_initializer.h"
#include "execution/tpcc/table.h"
#include "execution/tpcc/transaction.h"
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
    LoadTables(kv_storage_adapter, W, 1, 0, 1);
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
    for (int i_id = 1; i_id <= 5; i_id++) {
      rows.push_back(item.Select({MakeInt32Scalar(0), MakeInt32Scalar(i_id)}));
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

TEST_F(TransactionTest, DISABLED_NewOrder) {
  int w_id = 1;
  int d_id = 2;
  int c_id = 5;
  int o_id = 5000;
  int64_t datetime = 1234567890;
  int w_i_id = 100;
  std::array<NewOrderTxn::OrderLine, kLinePerOrder> ol;
  for (int i = 0; i < static_cast<int>(ol.size()); i++) {
    ol[i] = NewOrderTxn::OrderLine{.id = i + 1, .supply_w_id = 1, .item_id = (i + 1) * 10, .quantity = 4};
  }
  {
    auto key_gen_adapter = std::make_shared<TxnKeyGenStorageAdapter>(txn);
    NewOrderTxn new_order_txn(key_gen_adapter, w_id, d_id, c_id, o_id, datetime, w_i_id, ol);
    new_order_txn.Read();
    new_order_txn.Write();
    key_gen_adapter->Finialize();
  }
  FlushAndRefreshTxn();
  {
    auto txn_adapter = std::make_shared<TxnStorageAdapter>(txn);
    NewOrderTxn new_order_txn(txn_adapter, w_id, d_id, c_id, o_id, datetime, w_i_id, ol);
    ASSERT_TRUE(new_order_txn.Execute());
  }
}

TEST_F(TransactionTest, DISABLED_Payment) {
  int w_id = 1;
  int d_id = 2;
  int c_w_id = 2;
  int c_d_id = 3;
  int c_id = 4;
  int64_t amount = 10000;
  int64_t datetime = 1234567890;
  int h_id = 33333;
  {
    auto key_gen_adapter = std::make_shared<TxnKeyGenStorageAdapter>(txn);
    PaymentTxn payment_txn(key_gen_adapter, w_id, d_id, c_w_id, c_d_id, c_id, amount, datetime, h_id);
    payment_txn.Read();
    payment_txn.Write();
    key_gen_adapter->Finialize();
  }
  FlushAndRefreshTxn();
  {
    auto txn_adapter = std::make_shared<TxnStorageAdapter>(txn);
    PaymentTxn payment_txn(txn_adapter, w_id, d_id, c_w_id, c_d_id, c_id, amount, datetime, h_id);
    ASSERT_TRUE(payment_txn.Execute());
  }
}

TEST_F(TransactionTest, DISABLED_OrderStatus) {
  int w_id = 1;
  int d_id = 2;
  int c_id = 2;
  int o_id = 3;
  {
    auto key_gen_adapter = std::make_shared<TxnKeyGenStorageAdapter>(txn);
    OrderStatusTxn order_status_txn(key_gen_adapter, w_id, d_id, c_id, o_id);
    order_status_txn.Read();
    order_status_txn.Write();
    key_gen_adapter->Finialize();
  }
  FlushAndRefreshTxn();
  {
    auto txn_adapter = std::make_shared<TxnStorageAdapter>(txn);
    OrderStatusTxn order_status_txn(txn_adapter, w_id, d_id, c_id, o_id);
    ASSERT_TRUE(order_status_txn.Execute());
  }
}

TEST_F(TransactionTest, DISABLED_Deliver) {
  int w_id = 1;
  int d_id = 2;
  int no_o_id = 2;
  int c_id = 3;
  int o_carrier = 123;
  int64_t datetime = 1234567890;
  {
    auto key_gen_adapter = std::make_shared<TxnKeyGenStorageAdapter>(txn);
    DeliverTxn deliver(key_gen_adapter, w_id, d_id, no_o_id, c_id, o_carrier, datetime);
    deliver.Read();
    deliver.Write();
    key_gen_adapter->Finialize();
  }
  FlushAndRefreshTxn();
  {
    auto txn_adapter = std::make_shared<TxnStorageAdapter>(txn);
    DeliverTxn deliver(txn_adapter, w_id, d_id, no_o_id, c_id, o_carrier, datetime);
    ASSERT_TRUE(deliver.Execute());
  }
}

TEST_F(TransactionTest, DISABLED_StockLevel) {
  int w_id = 1;
  int d_id = 2;
  int o_id = 2;
  std::array<int, StockLevelTxn::kTotalItems> i_ids;
  for (int i = 0; i < StockLevelTxn::kTotalItems; i++) {
    i_ids[i] = i;
  }
  {
    auto key_gen_adapter = std::make_shared<TxnKeyGenStorageAdapter>(txn);
    StockLevelTxn stock_level(key_gen_adapter, w_id, d_id, o_id, i_ids);
    stock_level.Read();
    stock_level.Write();
    key_gen_adapter->Finialize();
  }
  FlushAndRefreshTxn();
  {
    auto txn_adapter = std::make_shared<TxnStorageAdapter>(txn);
    StockLevelTxn stock_level(txn_adapter, w_id, d_id, o_id, i_ids);
    ASSERT_TRUE(stock_level.Execute());
  }
}
