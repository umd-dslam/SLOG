#include "execution/tpcc/constants.h"
#include "execution/tpcc/transaction.h"

namespace slog {
namespace tpcc {

NewOrderTxn::NewOrderTxn(const StorageAdapterPtr& storage_adapter, int w_id, int d_id, int c_id, int o_id,
                         int64_t datetime, int i_w_id, const std::array<OrderLine, kLinePerOrder>& ol)
    : warehouse_(storage_adapter),
      district_(storage_adapter),
      customer_(storage_adapter),
      new_order_(storage_adapter),
      order_(storage_adapter),
      order_line_(storage_adapter),
      item_(storage_adapter),
      stock_(storage_adapter) {
  a_w_id_ = MakeInt32Scalar(w_id);
  a_d_id_ = MakeInt8Scalar(d_id);
  a_c_id_ = MakeInt32Scalar(c_id);
  a_o_id_ = MakeInt32Scalar(o_id);
  datetime_ = MakeInt64Scalar(datetime);
  for (size_t i = 0; i < ol.size(); i++) {
    a_ol_[i] = OrderLineScalar{.a_id = MakeInt8Scalar(ol[i].id),
                               .a_supply_w_id = MakeInt32Scalar(ol[i].supply_w_id),
                               .a_item_id = MakeInt32Scalar(ol[i].item_id),
                               .a_quantity = MakeInt8Scalar(ol[i].quantity)};
  }
  i_w_id_ = MakeInt32Scalar(i_w_id);
}

bool NewOrderTxn::Read() {
  bool ok = true;
  if (auto res = warehouse_.Select({a_w_id_}, {WarehouseSchema::Column::TAX}); !res.empty()) {
    w_tax_ = UncheckedCast<Int32Scalar>(res[0]);
  } else {
    SetError("Warehouse does not exist");
    ok = false;
  }

  if (auto res = customer_.Select(
          {a_w_id_, a_d_id_, a_c_id_},
          {CustomerSchema::Column::DISCOUNT, CustomerSchema::Column::FULL_NAME, CustomerSchema::Column::CREDIT});
      !res.empty()) {
    c_discount_ = UncheckedCast<Int32Scalar>(res[0]);
    c_last_ = UncheckedCast<FixedTextScalar>(res[1]);
    c_credit_ = UncheckedCast<FixedTextScalar>(res[2]);
  } else {
    SetError("The customer does not exist");
    ok = false;
  }

  if (auto res = district_.Select({a_w_id_, a_d_id_}, {DistrictSchema::Column::TAX, DistrictSchema::Column::NEXT_O_ID});
      !res.empty()) {
    d_tax_ = UncheckedCast<Int32Scalar>(res[0]);
    d_next_o_id_ = UncheckedCast<Int32Scalar>(res[1]);
  } else {
    SetError("The district does not exist");
    ok = false;
  }

  for (auto& l : a_ol_) {
    auto item_res = item_.Select({i_w_id_, l.a_item_id},
                                 {ItemSchema::Column::PRICE, ItemSchema::Column::NAME, ItemSchema::Column::DATA});
    if (!item_res.empty()) {
      l.i_price = UncheckedCast<Int32Scalar>(item_res[0]);
    } else {
      SetError("The item does not exist");
      ok = false;
    }
    auto stock_res =
        stock_.Select({l.a_supply_w_id, l.a_item_id}, {StockSchema::Column::QUANTITY, StockSchema::Column::ALL_DIST});
    if (!stock_res.empty()) {
      l.s_quantity = UncheckedCast<Int16Scalar>(stock_res[0]);
      std::string dist_info(reinterpret_cast<const char*>(stock_res[1]->data()), 24);
      l.dist_info = MakeFixedTextScalar<24>(dist_info);
    } else {
      SetError("Stock of the item does not exist");
      ok = false;
    }
  }

  return ok;
}

void NewOrderTxn::Compute() {
  new_d_next_o_id_->value = d_next_o_id_->value + 1;

  bool all_local = true;
  for (auto& l : a_ol_) {
    if (!(*l.a_supply_w_id == *a_w_id_)) {
      all_local = false;
    }
    l.amount->value = l.a_quantity->value * l.i_price->value;
    if (l.s_quantity->value > l.a_quantity->value) {
      l.s_quantity->value -= l.a_quantity->value;
    } else {
      l.s_quantity->value -= l.a_quantity->value - 91;
    }
  }
  all_local_->value = all_local;
}

bool NewOrderTxn::Write() {
  bool ok = true;
  auto null_carrier_id = MakeInt8Scalar(0);
  auto ol_cnt = MakeInt8Scalar(a_ol_.size());
  auto null_delivery_d = MakeInt64Scalar(0);

  if (!district_.Update({a_w_id_, a_d_id_}, {DistrictSchema::Column::NEXT_O_ID}, {new_d_next_o_id_})) {
    SetError("Cannot update District");
    ok = false;
  }
  if (!order_.Insert({a_w_id_, a_d_id_, a_o_id_, a_c_id_, datetime_, null_carrier_id, ol_cnt, all_local_})) {
    SetError("Cannot insert into Order");
    ok = false;
  }
  if (!new_order_.Insert({a_w_id_, a_d_id_, a_o_id_, MakeInt8Scalar()})) {
    SetError("Cannot insert into NewOrder");
    ok = false;
  }
  for (const auto& l : a_ol_) {
    if (!stock_.Update({l.a_supply_w_id, l.a_item_id}, {StockSchema::Column::QUANTITY}, {l.s_quantity})) {
      SetError("Cannot update Stock");
      ok = false;
    }
    if (!order_line_.Insert({a_w_id_, a_d_id_, a_o_id_, l.a_id, l.a_item_id, l.a_supply_w_id, null_delivery_d,
                             l.a_quantity, l.amount, l.dist_info})) {
      SetError("Cannot insert to OrderLine");
      ok = false;
    }
  }

  return ok;
}

}  // namespace tpcc
}  // namespace slog