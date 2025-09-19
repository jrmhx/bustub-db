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
#include "common/exception.h"
#include "common/macros.h"
#include "common/rid.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
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
  table_info_ = catalog->GetTable(plan->GetTableOid());
  BUSTUB_ASSERT(table_info_ != nullptr, "invalid table");
  txn_ = exec_ctx_->GetTransaction();
  txn_mgr_ = exec_ctx_->GetTransactionManager();
  child_executor_ = std::move(child_executor);
}

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
    TupleMeta meta{txn_->GetTransactionTempTs(), false};
    auto rid_opt = table_info_->table_->InsertTuple(meta, t);
    if (rid_opt != std::nullopt) {
      txn_->AppendWriteSet(table_info_->oid_, rid_opt.value());
      
      try {
        auto undo_log = GenerateNewUndoLog(&table_info_->schema_, nullptr, &t, txn_->GetTransactionId(), UndoLink{});
        auto undo_link = txn_->AppendUndoLog(undo_log);
        
        auto check = [](std::optional<UndoLink> current_link) -> bool {
          return !current_link.has_value();  // should be no existing undo link for new insert
        };
        
        bool success = txn_mgr_->UpdateUndoLink(rid_opt.value(), undo_link, std::move(check));
        if (!success) {
          txn_->SetTainted();
          throw ExecutionException("Failed to update undo link for insert - concurrent modification detected");
        }
        
        // update index
        auto indices = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
        for (const auto &index_info : indices) {
          bool index_success = index_info->index_->InsertEntry(
            t.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
            rid_opt.value(), txn_);
          if (!index_success) {
            txn_->SetTainted();
            throw ExecutionException("Failed to insert into index - possible duplicate key or constraint violation");
          }
        }
        ++inserted;
      } catch (const Exception &e) {
        txn_->SetTainted();
        throw;
      }
    } else {
      txn_->SetTainted();
      throw ExecutionException("Failed to allocate tuple into table heap");
    }
  }
  *tuple = Tuple({ValueFactory::GetIntegerValue(inserted)}, &plan_->OutputSchema());
  produced_ = true;
  return true;
}

}  // namespace bustub
