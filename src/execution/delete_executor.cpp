//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "common/macros.h"
#include "type/value_factory.h"

#include "execution/executors/delete_executor.h"

namespace bustub {

/**
 * Construct a new DeleteExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The delete plan to be executed
 * @param child_executor The child executor that feeds the delete
 */
DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  BUSTUB_ASSERT(plan != nullptr, "invalid plan");
  plan_ = plan;
  auto *catalog = exec_ctx->GetCatalog();
  BUSTUB_ASSERT(catalog != nullptr, "invalid catalog");
  table_info_ = catalog->GetTable(plan_->GetTableOid()).get();
  BUSTUB_ASSERT(table_info_ != nullptr, "invalid table_info");
  child_executor_.reset(child_executor.release());
}

/** Initialize the delete */
void DeleteExecutor::Init() {
  child_executor_->Init();
  produced_ = false;
}

/**
 * Yield the number of rows deleted from the table.
 * @param[out] tuple The integer tuple indicating the number of rows deleted from the table
 * @param[out] rid The next tuple RID produced by the delete (ignore, not used)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: DeleteExecutor::Next() does not use the `rid` out-parameter.
 * NOTE: DeleteExecutor::Next() returns true with the number of deleted rows produced only once.
 */
auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (produced_) {
    return false;
  }
  int deleted = 0;
  Tuple t;
  RID r;
  while (child_executor_->Next(&t, &r)) {
    auto meta = table_info_->table_->GetTupleMeta(r);
    meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(meta, r);
    auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    auto *txn = exec_ctx_->GetTransaction();
    for (auto &index_info : indexes) {
      index_info->index_->DeleteEntry(
          t.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()), r, txn);
    }
    ++deleted;
  }
  *tuple = Tuple({ValueFactory::GetIntegerValue(deleted)}, &plan_->OutputSchema());
  produced_ = true;
  return true;
}

}  // namespace bustub
