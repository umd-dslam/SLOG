#include "execution/tpcc/transaction.h"

namespace slog {
namespace tpcc {

PaymentTxn::PaymentTxn(const StorageAdapterPtr& storage_adapter, int w_id, int d_id, int c_w_id, int c_d_id, int c_id,
                       int64_t amount, int64_t datetime, int h_id)
    : warehouse_(storage_adapter), district_(storage_adapter), customer_(storage_adapter), history_(storage_adapter) {
  a_w_id_ = MakeInt32Scalar(w_id);
  a_d_id_ = MakeInt8Scalar(d_id);
  a_c_w_id_ = MakeInt32Scalar(c_w_id);
  a_c_d_id_ = MakeInt8Scalar(c_d_id);
  a_c_id_ = MakeInt32Scalar(c_id);
  a_amount_ = MakeInt32Scalar(amount);
  datetime_ = MakeInt64Scalar(datetime);
  a_h_id_ = MakeInt32Scalar(h_id);
}

bool PaymentTxn::Read() {
  bool ok = true;
  if (auto res = warehouse_.Select(
          {a_w_id_}, {WarehouseSchema::Column::NAME, WarehouseSchema::Column::ADDRESS, WarehouseSchema::Column::YTD});
      !res.empty()) {
    w_name_ = UncheckedCast<FixedTextScalar>(res[0]);
    w_address_ = UncheckedCast<FixedTextScalar>(res[1]);
    w_ytd_ = UncheckedCast<Int64Scalar>(res[2]);
  } else {
    SetError("Warehouse does not exist");
    ok = false;
  }

  if (auto res = district_.Select({a_w_id_, a_d_id_}, {DistrictSchema::Column::NAME, DistrictSchema::Column::ADDRESS,
                                                       DistrictSchema::Column::YTD});
      !res.empty()) {
    d_name_ = UncheckedCast<FixedTextScalar>(res[0]);
    d_address_ = UncheckedCast<FixedTextScalar>(res[1]);
    d_ytd_ = UncheckedCast<Int64Scalar>(res[2]);
  } else {
    SetError("District does not exist");
    ok = false;
  }

  if (auto res = customer_.Select(
          {a_c_w_id_, a_c_d_id_, a_c_id_},
          {CustomerSchema::Column::FULL_NAME, CustomerSchema::Column::ADDRESS, CustomerSchema::Column::PHONE,
           CustomerSchema::Column::SINCE, CustomerSchema::Column::CREDIT, CustomerSchema::Column::CREDIT_LIM,
           CustomerSchema::Column::DISCOUNT, CustomerSchema::Column::BALANCE, CustomerSchema::Column::YTD_PAYMENT,
           CustomerSchema::Column::PAYMENT_CNT, CustomerSchema::Column::DATA});
      !res.empty()) {
    c_full_name = UncheckedCast<FixedTextScalar>(res[0]);
    c_address_ = UncheckedCast<FixedTextScalar>(res[1]);
    c_phone_ = UncheckedCast<FixedTextScalar>(res[2]);
    c_since_ = UncheckedCast<Int64Scalar>(res[3]);
    c_credit_ = UncheckedCast<FixedTextScalar>(res[4]);
    c_credit_lim_ = UncheckedCast<Int64Scalar>(res[5]);
    c_discount_ = UncheckedCast<Int32Scalar>(res[6]);
    c_balance_ = UncheckedCast<Int64Scalar>(res[7]);
    c_ytd_payment_ = UncheckedCast<Int64Scalar>(res[8]);
    c_payment_cnt_ = UncheckedCast<Int16Scalar>(res[9]);
    c_data_ = UncheckedCast<FixedTextScalar>(res[10]);
  } else {
    SetError("Customer does not exist");
    ok = false;
  }

  return ok;
}

void PaymentTxn::Compute() {
  new_w_ytd_->value = w_ytd_->value + a_amount_->value;
  new_d_ytd_->value = d_ytd_->value + a_amount_->value;
  new_c_balance_->value = c_balance_->value - a_amount_->value;
  new_c_ytd_payment_->value = c_ytd_payment_->value + a_amount_->value;
  new_c_payment_cnt_->value = c_payment_cnt_->value + 1;
  new_h_data_->buffer = w_name_->buffer + "    " + d_name_->buffer;
}

bool PaymentTxn::Write() {
  bool ok = true;
  if (!warehouse_.Update({a_w_id_}, {WarehouseSchema::Column::YTD}, {new_w_ytd_})) {
    SetError("Cannot update Warehouse");
    ok = false;
  }
  if (!district_.Update({a_w_id_, a_d_id_}, {DistrictSchema::Column::YTD}, {new_d_ytd_})) {
    SetError("Cannot update District");
    ok = false;
  }
  if (!customer_.Update({a_c_w_id_, a_c_d_id_, a_c_id_},
                        {CustomerSchema::Column::BALANCE, CustomerSchema::Column::YTD_PAYMENT,
                         CustomerSchema::Column::PAYMENT_CNT, CustomerSchema::Column::DATA},
                        {new_c_balance_, new_c_ytd_payment_, new_c_payment_cnt_, c_data_})) {
    SetError("Cannot update Customer");
    ok = false;
  }

  history_.Insert({a_w_id_, a_d_id_, a_c_id_, a_h_id_, a_c_d_id_, a_c_w_id_, datetime_, a_amount_, new_h_data_});

  return ok;
}

}  // namespace tpcc
}  // namespace slog