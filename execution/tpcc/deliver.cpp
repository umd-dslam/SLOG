#include "execution/tpcc/transaction.h"

namespace slog {
namespace tpcc {

DeliverTxn::DeliverTxn(const StorageAdapterPtr& storage_adapter, int w_id, int d_id, int no_o_id, int c_id,
                       int o_carrier, int64_t datetime)
    : customer_(storage_adapter), new_order_(storage_adapter), order_(storage_adapter), order_line_(storage_adapter) {
  a_w_id_ = MakeInt32Scalar(w_id);
  a_d_id_ = MakeInt8Scalar(d_id);
  a_no_o_id_ = MakeInt32Scalar(no_o_id);
  a_c_id_ = MakeInt32Scalar(c_id);
  a_o_carrier_ = MakeInt8Scalar(o_carrier);
  datetime_ = MakeInt64Scalar(datetime);
}

bool DeliverTxn::Read() {
  order_.Select({a_w_id_, a_d_id_, a_no_o_id_}, {OrderSchema::Column::C_ID});

  auto ol_id = MakeInt8Scalar();
  for (int i = 1; i <= kLinePerOrder; i++) {
    ol_id->value = i;
    auto res = order_line_.Select({a_w_id_, a_d_id_, a_no_o_id_, ol_id}, {OrderLineSchema::Column::AMOUNT});
    if (!res.empty()) {
      sum_o_amount_->value += UncheckedCast<Int32Scalar>(res[0])->value;
    }
  }
  auto res = customer_.Select({a_w_id_, a_d_id_, a_c_id_},
                              {CustomerSchema::Column::BALANCE, CustomerSchema::Column::DELIVERY_CNT});
  if (!res.empty()) {
    c_balance_ = UncheckedCast<Int64Scalar>(res[0]);
    c_delivery_cnt_ = UncheckedCast<Int16Scalar>(res[1]);
  }

  return true;
}

void DeliverTxn::Compute() {
  new_c_balance_->value = c_balance_->value + sum_o_amount_->value;
  new_c_delivery_cnt_->value = c_delivery_cnt_->value + 1;
}

bool DeliverTxn::Write() {
  new_order_.Delete({a_w_id_, a_d_id_, a_no_o_id_});
  order_.Update({a_w_id_, a_d_id_, a_no_o_id_}, {OrderSchema::Column::CARRIER_ID}, {a_o_carrier_});
  auto ol_id = MakeInt8Scalar();
  for (int i = 1; i <= kLinePerOrder; i++) {
    ol_id->value = i;
    order_line_.Update({a_w_id_, a_d_id_, a_no_o_id_, ol_id}, {OrderLineSchema::Column::DELIVERY_D}, {datetime_});
  }
  customer_.Update({a_w_id_, a_d_id_, a_c_id_}, {CustomerSchema::Column::BALANCE, CustomerSchema::Column::DELIVERY_CNT},
                   {new_c_balance_, new_c_delivery_cnt_});
  return true;
}

}  // namespace tpcc
}  // namespace slog