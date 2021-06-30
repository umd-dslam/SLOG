#include "execution/tpcc/transaction.h"

namespace slog {
namespace tpcc {

StockLevelTxn::StockLevelTxn(const StorageAdapterPtr& storage_adapter, int w_id, int d_id, int o_id,
                             const std::array<int, kTotalItems>& i_ids)
    : district_(storage_adapter), order_line_(storage_adapter), stock_(storage_adapter) {
  a_w_id_ = MakeInt32Scalar(w_id);
  a_d_id_ = MakeInt8Scalar(d_id);
  a_o_id_ = MakeInt32Scalar(o_id);
  for (int i = 0; i < kTotalItems; i++) {
    a_i_ids_[i] = MakeInt32Scalar(i_ids[i]);
  }
}

bool StockLevelTxn::Read() {
  district_.Select({a_w_id_, a_d_id_}, {DistrictSchema::Column::NEXT_O_ID});
  auto o_id = MakeInt32Scalar();
  auto ol_number = MakeInt8Scalar();
  for (int i = a_o_id_->value - 20; i < a_o_id_->value; i++) {
    o_id->value = i;
    for (int j = 0; j < kLinePerOrder; j++) {
      ol_number->value = j;
      order_line_.Select({a_w_id_, a_d_id_, o_id, ol_number}, {OrderLineSchema::Column::I_ID});
    }
  }
  for (int i = 0; i < kTotalItems; i++) {
    stock_.Select({a_w_id_, a_i_ids_[i]}, {StockSchema::Column::QUANTITY});
  }
  return true;
}

}  // namespace tpcc
}  // namespace slog