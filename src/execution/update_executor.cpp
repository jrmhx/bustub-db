//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "common/macros.h"

#include "execution/executors/update_executor.h"

namespace bustub {

/**
 * Construct a new UpdateExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The update plan to be executed
 * @param child_executor The child executor that feeds the update
 */
UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  BUSTUB_ASSERT(plan != nullptr, "invalid plan");
  plan_ = plan;
  auto * catalog = exec_ctx->GetCatalog();
  BUSTUB_ASSERT(catalog != nullptr, "invalid catalog");
  table_info_ = catalog->GetTable(plan_->GetTableOid()).get();
  BUSTUB_ASSERT(table_info_ != nullptr, "invalid table_info");
  child_executor_ = std::move(child_executor);
}

/** Initialize the update */
void UpdateExecutor::Init() { 
  child_executor_->Init();
}

/**
 * Yield the next tuple from the update.
 * @param[out] tuple The next tuple produced by the update
 * @param[out] rid The next tuple RID produced by the update (ignore this)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: UpdateExecutor::Next() does not use the `rid` out-parameter.
 */
auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple t;
  RID r;

  while (child_executor_->Next(&t, &r)) {
    // mark the old tuple as deleted
    auto meta = table_info_->table_->GetTupleMeta(r);
    meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(meta, r);

    // evaluate expressions to get the new values
    std::vector<Value> values;
    values.reserve(plan_->target_expressions_.size());
    for (const auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(&t, table_info_->schema_));
    }
    Tuple new_tuple(values, &table_info_->schema_);

    auto new_rid = table_info_->table_->InsertTuple(TupleMeta{exec_ctx_->GetTransaction()->GetTransactionId(), false}, new_tuple);

    // update all indexes
    if (new_rid.has_value()) {
      auto txn = exec_ctx_->GetTransaction();
      auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
      for (auto &index_info : indexes) {
        index_info->index_->DeleteEntry(
          t.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()), 
          r, 
          txn
        );
        index_info->index_->InsertEntry(
          new_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
          new_rid.value(), 
          txn
        );
      }
    } else {
      return false;
    }
  }

  return true;
}

}  // namespace bustub
