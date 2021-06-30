#include "execution/tpcc/transaction.h"

namespace slog {
namespace tpcc {

OrderStatusTxn::OrderStatusTxn(const StorageAdapterPtr& storage_adapter, int w_id, int d_id, int c_id, int o_id)
    : customer_(storage_adapter), order_(storage_adapter), order_line_(storage_adapter) {
  a_w_id_ = MakeInt32Scalar(w_id);
  a_d_id_ = MakeInt8Scalar(d_id);
  a_c_id_ = MakeInt32Scalar(c_id);
  a_o_id_ = MakeInt32Scalar(o_id);
}

bool OrderStatusTxn::Read() {
  customer_.Select({a_w_id_, a_d_id_, a_c_id_}, {CustomerSchema::Column::FULL_NAME, CustomerSchema::Column::BALANCE});
  order_.Select({a_w_id_, a_d_id_, a_o_id_}, {OrderSchema::Column::ENTRY_D, OrderSchema::Column::CARRIER_ID});
  auto ol_number = MakeInt8Scalar();
  for (int i = 1; i <= kLinePerOrder; i++) {
    ol_number->value = i;
    order_line_.Select(
        {a_w_id_, a_d_id_, a_o_id_, ol_number},
        {OrderLineSchema::Column::I_ID, OrderLineSchema::Column::SUPPLY_W_ID, OrderLineSchema::Column::QUANTITY,
         OrderLineSchema::Column::AMOUNT, OrderLineSchema::Column::DELIVERY_D});
  }

  return true;
}

}  // namespace tpcc
}  // namespace slog