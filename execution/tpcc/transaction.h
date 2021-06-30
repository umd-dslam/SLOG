#pragma once

#include <array>

#include "execution/tpcc/constants.h"
#include "execution/tpcc/table.h"

namespace slog {
namespace tpcc {

class TPCCTransaction {
 public:
  virtual ~TPCCTransaction() = default;
  bool Execute() {
    if (!Read()) {
      return false;
    }
    Compute();
    if (!Write()) {
      return false;
    }
    return true;
  }
  virtual bool Read() = 0;
  virtual void Compute() = 0;
  virtual bool Write() = 0;

  const std::string& error() const { return error_; }

 protected:
  void SetError(const std::string& error) {
    if (error_.empty()) error_ = error;
  }

 private:
  std::string error_;
};

class NewOrderTxn : public TPCCTransaction {
 public:
  struct OrderLine {
    int id;
    int supply_w_id;
    int item_id;
    int quantity;
  };

  NewOrderTxn(const StorageAdapterPtr& storage_adapter, int w_id, int d_id, int c_id, int o_id, int64_t datetime,
              int i_w_id, const std::array<OrderLine, kLinePerOrder>& ol);
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
  std::array<OrderLineScalar, kLinePerOrder> a_ol_;
  Int32ScalarPtr i_w_id_;

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

class PaymentTxn : public TPCCTransaction {
 public:
  PaymentTxn(const StorageAdapterPtr& storage_adapter, int w_id, int d_id, int c_w_id, int c_d_id, int c_id,
             int64_t amount, int64_t datetime, int h_id);
  bool Read() final;
  void Compute() final;
  bool Write() final;

 private:
  Table<WarehouseSchema> warehouse_;
  Table<DistrictSchema> district_;
  Table<CustomerSchema> customer_;
  Table<HistorySchema> history_;

  // Arguments
  Int32ScalarPtr a_w_id_;
  Int8ScalarPtr a_d_id_;
  Int32ScalarPtr a_c_w_id_;
  Int8ScalarPtr a_c_d_id_;
  Int32ScalarPtr a_c_id_;
  Int32ScalarPtr a_amount_;
  Int64ScalarPtr datetime_;
  Int32ScalarPtr a_h_id_;

  // Read results
  FixedTextScalarPtr w_name_ = MakeFixedTextScalar();
  FixedTextScalarPtr w_address_ = MakeFixedTextScalar();
  Int64ScalarPtr w_ytd_ = MakeInt64Scalar();
  FixedTextScalarPtr d_name_ = MakeFixedTextScalar();
  FixedTextScalarPtr d_address_ = MakeFixedTextScalar();
  Int64ScalarPtr d_ytd_ = MakeInt64Scalar();
  FixedTextScalarPtr c_full_name = MakeFixedTextScalar();
  FixedTextScalarPtr c_address_ = MakeFixedTextScalar();
  FixedTextScalarPtr c_phone_ = MakeFixedTextScalar();
  Int64ScalarPtr c_since_ = MakeInt64Scalar();
  FixedTextScalarPtr c_credit_ = MakeFixedTextScalar();
  Int64ScalarPtr c_credit_lim_ = MakeInt64Scalar();
  Int32ScalarPtr c_discount_ = MakeInt32Scalar();
  Int64ScalarPtr c_balance_ = MakeInt64Scalar();
  Int64ScalarPtr c_ytd_payment_ = MakeInt64Scalar();
  Int16ScalarPtr c_payment_cnt_ = MakeInt16Scalar();
  FixedTextScalarPtr c_data_ = MakeFixedTextScalar();

  // Computed values
  Int64ScalarPtr new_w_ytd_ = MakeInt64Scalar();
  Int64ScalarPtr new_d_ytd_ = MakeInt64Scalar();
  Int64ScalarPtr new_c_balance_ = MakeInt64Scalar();
  Int64ScalarPtr new_c_ytd_payment_ = MakeInt64Scalar();
  Int16ScalarPtr new_c_payment_cnt_ = MakeInt16Scalar();
  FixedTextScalarPtr new_h_data_ = MakeFixedTextScalar();
};

class OrderStatusTxn : public TPCCTransaction {
 public:
  OrderStatusTxn(const StorageAdapterPtr& storage_adapter, int w_id, int d_id, int c_id, int o_id);
  bool Read() final;
  void Compute() final {}
  bool Write() final { return true; }

 private:
  Table<CustomerSchema> customer_;
  Table<OrderSchema> order_;
  Table<OrderLineSchema> order_line_;

  // Arguments
  Int32ScalarPtr a_w_id_;
  Int8ScalarPtr a_d_id_;
  Int32ScalarPtr a_c_id_;
  Int32ScalarPtr a_o_id_;
};

class DeliverTxn : public TPCCTransaction {
 public:
  DeliverTxn(const StorageAdapterPtr& storage_adapter, int w_id, int d_id, int no_o_id, int c_id, int o_carrier,
             int64_t datetime);
  bool Read() final;
  void Compute() final;
  bool Write() final;

 private:
  Table<CustomerSchema> customer_;
  Table<NewOrderSchema> new_order_;
  Table<OrderSchema> order_;
  Table<OrderLineSchema> order_line_;

  // Arguments
  Int32ScalarPtr a_w_id_;
  Int8ScalarPtr a_d_id_;
  Int32ScalarPtr a_no_o_id_;
  Int32ScalarPtr a_c_id_;
  Int8ScalarPtr a_o_carrier_;
  Int64ScalarPtr datetime_;

  // Read results
  Int32ScalarPtr sum_o_amount_ = MakeInt32Scalar();
  Int64ScalarPtr c_balance_ = MakeInt64Scalar();
  Int16ScalarPtr c_delivery_cnt_ = MakeInt16Scalar();

  // Computed values
  Int64ScalarPtr new_c_balance_ = MakeInt64Scalar();
  Int16ScalarPtr new_c_delivery_cnt_ = MakeInt16Scalar();
};

class StockLevelTxn : public TPCCTransaction {
 public:
  constexpr static int kTotalItems = 200;
  StockLevelTxn(const StorageAdapterPtr& storage_adapter, int w_id, int d_id, int o_id,
                const std::array<int, kTotalItems>& i_ids);
  bool Read() final;
  void Compute() final {}
  bool Write() final { return true; }

 private:
  Table<DistrictSchema> district_;
  Table<OrderLineSchema> order_line_;
  Table<StockSchema> stock_;

  // Arguments
  Int32ScalarPtr a_w_id_;
  Int8ScalarPtr a_d_id_;
  Int32ScalarPtr a_o_id_;
  std::array<Int32ScalarPtr, kTotalItems> a_i_ids_;
};

}  // namespace tpcc
}  // namespace slog