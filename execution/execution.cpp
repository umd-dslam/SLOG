#include "execution/execution.h"

namespace slog {

void Execution::ApplyWrites(const Transaction& txn, const SharderPtr& sharder,
                            const std::shared_ptr<Storage>& storage) {
  for (const auto& kv : txn.keys()) {
    const auto& key = kv.key();
    const auto& value = kv.value_entry();
    if (!sharder->is_local_key(key) || value.type() == KeyType::READ) {
      continue;
    }
    Record new_record;
    new_record.SetMetadata(value.metadata());
    new_record.SetValue(value.new_value());
    storage->Write(key, new_record);
  }
  for (const auto& key : txn.deleted_keys()) {
    storage->Delete(key);
  }
}

}  // namespace slog