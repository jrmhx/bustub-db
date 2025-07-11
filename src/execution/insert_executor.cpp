//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <optional>
#include "common/macros.h"
#include "common/rid.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

#include "execution/executors/insert_executor.h"

namespace bustub {

/**
 * Construct a new InsertExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The insert plan to be executed
 * @param child_executor The child executor from which inserted tuples are pulled
 */
InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  auto *catalog = exec_ctx->GetCatalog();
  BUSTUB_ASSERT(catalog != nullptr, "invalid catalog");
  table_info_ = catalog->GetTable(plan->GetTableOid()).get();
  BUSTUB_ASSERT(table_info_ != nullptr, "invalid table");
  child_executor_ = std::move(child_executor);
}

/** Initialize the insert */
void InsertExecutor::Init() {
  child_executor_->Init();
  produced_ = false;
}

/**
 * Yield the number of rows inserted into the table.
 * @param[out] tuple The integer tuple indicating the number of rows inserted into the table
 * @param[out] rid The next tuple RID produced by the insert (ignore, not used)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: InsertExecutor::Next() does not use the `rid` out-parameter.
 * NOTE: InsertExecutor::Next() returns true with number of inserted rows produced only once.
 */
auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (produced_) {
    return false;
  }
  int inserted = 0;
  Tuple t;
  RID r;
  while (child_executor_->Next(&t, &r)) {
    TupleMeta meta{exec_ctx_->GetTransaction()->GetTransactionId(), false};
    auto rid_opt = table_info_->table_->InsertTuple(meta, t);
    if (rid_opt != std::nullopt) {
      auto *txn = exec_ctx_->GetTransaction();
      auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
      for (auto index_info : indexes) {
        index_info->index_->InsertEntry(
            t.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
            rid_opt.value(), txn);
      }
      ++inserted;
    } else {
      // unsuccessful insert
      return false;
    }
  }
  *tuple = Tuple({ValueFactory::GetIntegerValue(inserted)}, &plan_->OutputSchema());
  produced_ = true;
  return true;
}

}  // namespace bustub
