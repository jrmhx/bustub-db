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
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "fmt/ostream.h"
#include <optional>
#include <sstream>
#include "storage/table/tuple.h"
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
  txn_ = exec_ctx->GetTransaction();
  txn_mgr_ = exec_ctx->GetTransactionManager();
  table_info_ = catalog->GetTable(plan_->GetTableOid());
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
    const auto [base_meta, base_tuple, base_ulink] = GetTupleAndUndoLink(txn_mgr_, table_info_->table_.get(), r);
    if (IsWriteWriteConflict(txn_, base_meta)) {
      txn_->SetTainted(); // tained means marked to be aborted but havent aborted yet
      std::ostringstream error_msg;
      fmt::print(error_msg, "txn{} encountered a write write conflict and is marked as tained", txn_->GetTransactionIdHumanReadable());
      throw ExecutionException(error_msg.str());
    } else {
      const auto old_meta = base_meta;
      const auto old_tuple = base_tuple;
      TupleMeta updated_meta = {txn_->GetTransactionTempTs(), true};
      UndoLink updated_ulink = base_ulink.value_or(UndoLink{});
      if (base_meta.ts_ <= txn_->GetReadTs()) {
        auto ulog = GenerateNewUndoLog(
          &table_info_->schema_, 
          &base_tuple, nullptr, 
          base_meta.ts_,
          base_ulink.value_or(UndoLink{})
        );
        updated_ulink = txn_->AppendUndoLog(ulog);
      } else if (base_meta.ts_ == txn_->GetTransactionTempTs()) {
        auto base_ulog = txn_mgr_->GetUndoLog(base_ulink.value());
        auto ulog = GenerateUpdatedUndoLog(&table_info_->schema_, &base_tuple, nullptr, base_ulog);
        txn_->ModifyUndoLog(base_ulink->prev_log_idx_, ulog);
        updated_ulink = base_ulink.value_or(UndoLink{});
      }
      auto success_write = UpdateTupleAndUndoLink(
        txn_mgr_, 
        r, 
        updated_ulink, 
        table_info_->table_.get(), 
        txn_, 
        updated_meta, 
        base_tuple,
        [old_meta, old_tuple, r] (const TupleMeta &meta, const Tuple &tuple, const RID rid, const std::optional<UndoLink> ulink) {
          auto pass = (
            r == rid &&
            old_meta == meta && 
            IsTupleContentEqual(old_tuple, tuple) &&
            ulink != std::nullopt
          );

          // debug
          if (!pass) {
            fmt::println(stderr, "=== DELETE CHECK FAILED ===");
            fmt::println(stderr, "\tr: {}/{}, rid: {}/{}", r.GetPageId(), r.GetSlotNum(), rid.GetPageId(), rid.GetSlotNum());
            fmt::println(stderr, "\told_meta: ts={}, is_deleted={}", old_meta.ts_, old_meta.is_deleted_);
            fmt::println(stderr, "\tmeta: ts={}, is_deleted={}", meta.ts_, meta.is_deleted_);
            fmt::println(stderr, "\told_tuple size: {}", old_tuple.GetLength());
            fmt::println(stderr, "\ttuple size: {}", tuple.GetLength());
            fmt::println(stderr, "\tulink: {}", ulink.has_value() ? "has_value" : "nullopt");
            fmt::println(stderr, "\ttuple_content_equal: {}", IsTupleContentEqual(old_tuple, tuple));
            fmt::println(stderr, "========================");
          }
          return pass;
        }
      );
      if (success_write) {
        txn_->AppendWriteSet(table_info_->oid_, r);
        //update index
        auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
        auto *txn = exec_ctx_->GetTransaction();
        for (auto &index_info : indexes) {
          index_info->index_->DeleteEntry(
            base_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()), 
            r, 
            txn
          );
        }
        ++deleted;
      } else {
        txn_->SetTainted();
        std::ostringstream error_msg;
        fmt::print(error_msg, "txn{} encountered a write write conflict and is marked as tained", txn_->GetTransactionIdHumanReadable());
        throw ExecutionException(error_msg.str());
      }
    }
  }
  *tuple = Tuple({ValueFactory::GetIntegerValue(deleted)}, &plan_->OutputSchema());
  produced_ = true;
  return true;
}

}  // namespace bustub
