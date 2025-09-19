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

#include <cstdint>
#include <memory>
#include <optional>
#include <sstream>
#include <vector>
#include "common/exception.h"
#include "common/macros.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "fmt/ostream.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

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
  auto *catalog = exec_ctx->GetCatalog();
  BUSTUB_ASSERT(catalog != nullptr, "invalid catalog");
  table_info_ = catalog->GetTable(plan_->GetTableOid());
  BUSTUB_ASSERT(table_info_ != nullptr, "invalid table_info");
  child_executor_ = std::move(child_executor);
  txn_ = exec_ctx_->GetTransaction();
  txn_mgr_ = exec_ctx_->GetTransactionManager();
}

/** Initialize the update */
void UpdateExecutor::Init() {
  child_executor_->Init();
  produced_ = false;
}

/**
 * Yield the next tuple from the update.
 * @param[out] tuple the number of rows being updated in the table (not include the rows update in the indices)
 * @param[out] rid The next tuple RID produced by the update (ignore this)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: UpdateExecutor::Next() does not use the `rid` out-parameter.
 */
auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (produced_) {
    return false;
  }
  Tuple t;
  RID r;
  uint32_t updated = 0;

  while (child_executor_->Next(&t, &r)) {
    // MVCC makes the txn can only write on the tuple version that they can see
    // if the to-be-written tuple's latest version is not visable to txn_ there is a write write conflict

    //atomic fetch meta, tuple, undo_link
    const auto [base_meta, base_tuple, base_ulink] = GetTupleAndUndoLink(txn_mgr_, table_info_->table_.get(), r);
    // check write write conflict
    if (IsWriteWriteConflict(txn_, base_meta)) {
      txn_->SetTainted(); // tained means marked to be aborted but havent aborted yet
      std::ostringstream error_msg;
      fmt::print(error_msg, "txn{} encountered a write write conflict and is marked as tained", txn_->GetTransactionIdHumanReadable());
      throw ExecutionException(error_msg.str());
    }
    std::vector<Value> values;
    values.reserve(plan_->target_expressions_.size());
    for (const auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(&base_tuple, table_info_->schema_));
    }
    Tuple updated_tuple(values, &table_info_->schema_);
    auto updated_meta = TupleMeta{txn_->GetTransactionTempTs(), false};
    UndoLink updated_ulink = base_ulink.value_or(UndoLink{});
    if (base_meta.ts_ <= txn_->GetReadTs()) {
      auto ulog = GenerateNewUndoLog(
        &table_info_->schema_, 
        &base_tuple, 
        &updated_tuple, 
        base_meta.ts_, 
        base_ulink.value_or(UndoLink{})
      );
      updated_ulink = txn_->AppendUndoLog(ulog);
      
    } else if (base_meta.ts_ == txn_->GetTransactionTempTs()) {
      auto base_ulog = txn_mgr_->GetUndoLog(base_ulink.value());
      auto ulog = GenerateUpdatedUndoLog(&table_info_->schema_, &base_tuple, &updated_tuple, base_ulog);
      txn_->ModifyUndoLog(base_ulink->prev_log_idx_, ulog);
    }
    
    const auto old_meta = base_meta;
    const auto old_tuple = base_tuple;
    auto success_write = UpdateTupleAndUndoLink(
      txn_mgr_, 
      r, 
      updated_ulink, 
      table_info_->table_.get(), 
      txn_, 
      updated_meta, 
      updated_tuple,
      [old_meta, old_tuple, r] (const TupleMeta &meta, const Tuple &tuple, const RID rid, const std::optional<UndoLink> ulink) {
        return (
          r == rid &&
          old_meta == meta && 
          IsTupleContentEqual(old_tuple, tuple) &&
          ulink != std::nullopt
        );
      }
    );
    if (success_write) {
      txn_->AppendWriteSet(table_info_->oid_, r);
      //update index
      auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
      for (auto &index_info : indexes) {
        
        index_info->index_->DeleteEntry(
          base_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()), 
          r, 
          txn_
        );
        auto success_index_update = index_info->index_->InsertEntry(
          updated_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
          r, 
          txn_
        );
        if (!success_index_update) {
          txn_->SetTainted();
          std::ostringstream error_msg;
          fmt::print(error_msg, "txn{} failed to update index and is marked as tained", txn_->GetTransactionIdHumanReadable());
          throw ExecutionException(error_msg.str());
        }
      }
      ++updated;
    } else {
      txn_->SetTainted();
      std::ostringstream error_msg;
      fmt::print(error_msg, "txn{} encountered a write write conflict and is marked as tained", txn_->GetTransactionIdHumanReadable());
      throw ExecutionException(error_msg.str());
    }
  }
  *tuple = Tuple({ValueFactory::GetIntegerValue(updated)}, &plan_->OutputSchema());
  produced_ = true;
  return true;
}

}  // namespace bustub
