#pragma once

#include "execution/tpcc/storage_adapter.h"
#include "execution/tpcc/table.h"
#include "execution/tpcc/transaction.h"

namespace slog {
namespace tpcc {

class NewOrderTxn : public TPCCTransaction {
 public:
  struct OrderLine {
    int id;
    int supply_w_id;
    int item_id;
    int quantity;
  };

  NewOrderTxn(const StorageAdapterPtr& storage_adapter, int w_id, int d_id, int c_id, int o_id, int64_t datetime,
              const std::vector<OrderLine>& ol, int w_i_id);
  bool Read() final;
  void Compute() final;
  bool Write() final;

 private:
  Table<WarehouseSchema> warehouse_;
  Table<DistrictSchema> district_;
  Table<CustomerSchema> customer_;
  Table<NewOrderSchema> new_order_;
  Table<OrderSchema> order_;
  Table<OrderLineSchema> order_line_;
  Table<ItemSchema> item_;
  Table<StockSchema> stock_;

  struct OrderLineScalar {
    Int8ScalarPtr a_id;
    Int32ScalarPtr a_supply_w_id;
    Int32ScalarPtr a_item_id;
    Int8ScalarPtr a_quantity;
    Int32ScalarPtr amount = MakeInt32Scalar();
    FixedTextScalarPtr dist_info = MakeFixedTextScalar();
    Int16ScalarPtr s_quantity = MakeInt16Scalar();
    Int32ScalarPtr i_price = MakeInt32Scalar();
  };

  // Arguments
  Int32ScalarPtr a_w_id_;
  Int8ScalarPtr a_d_id_;
  Int32ScalarPtr a_c_id_;
  Int32ScalarPtr a_o_id_;
  Int64ScalarPtr datetime_;
  std::vector<OrderLineScalar> a_ol_;
  Int32ScalarPtr w_i_id_;

  // Read results
  Int32ScalarPtr w_tax_ = MakeInt32Scalar();
  Int32ScalarPtr c_discount_ = MakeInt32Scalar();
  FixedTextScalarPtr c_last_ = MakeFixedTextScalar();
  FixedTextScalarPtr c_credit_ = MakeFixedTextScalar();
  Int32ScalarPtr d_tax_ = MakeInt32Scalar();
  Int32ScalarPtr d_next_o_id_ = MakeInt32Scalar();

  // Computed values
  Int32ScalarPtr new_d_next_o_id_ = MakeInt32Scalar();
  Int8ScalarPtr all_local_ = MakeInt8Scalar();
};

}  // namespace tpcc
}  // namespace slog