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

#include "execution/executors/insert_executor.h"
#include <memory>
#include <optional>
#include <vector>
#include "common/exception.h"
#include "common/macros.h"
#include "common/rid.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "fmt/ostream.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

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
    std::vector<RID> existing_pk_rid;
    auto indices = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    for (const auto &index_info : indices) {
      if (index_info->is_primary_key_) {
        auto key = t.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        index_info->index_->ScanKey(key, &existing_pk_rid, txn_);
      }
    }
    if (existing_pk_rid.empty()) {
      TupleMeta meta{txn_->GetTransactionTempTs(), false};
      auto rid = table_info_->table_->InsertTuple(meta, t);
      if (rid != std::nullopt) {
        txn_->AppendWriteSet(table_info_->oid_, rid.value());
        // update index
        for (const auto &index_info : indices) {
          auto key = t.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
          auto index_update_success = index_info->index_->InsertEntry(key, rid.value(), txn_);
          if (!index_update_success) {
            txn_->SetTainted();
            std::ostringstream error_msg;
            fmt::print(error_msg, "index update failure! txn{} failed to insert tuple {}",
                       txn_->GetTransactionIdHumanReadable(), t.ToString(&table_info_->schema_));
            throw ExecutionException(error_msg.str());
          }
        }
        ++inserted;
      } else {
        txn_->SetTainted();
        std::ostringstream error_msg;
        fmt::print(error_msg, "allocation failure! txn{} failed to insert tuple {}",
                   txn_->GetTransactionIdHumanReadable(), t.ToString(&table_info_->schema_));
        throw ExecutionException(error_msg.str());
      }
    } else {
      const auto [base_meta, base_tuple, base_ulink] =
          GetTupleAndUndoLink(txn_mgr_, table_info_->table_.get(), existing_pk_rid.at(0));
      if (base_meta.is_deleted_) {
        // fall back to update the old deleted tuple
        // check write write conflict
        if (IsWriteWriteConflict(txn_, base_meta)) {
          txn_->SetTainted();
          std::ostringstream error_msg;
          fmt::print(error_msg, "txn{} encountered a write write conflict and is marked as tained",
                     txn_->GetTransactionIdHumanReadable());
          throw ExecutionException(error_msg.str());
        }
        auto updated_meta = TupleMeta{txn_->GetTransactionTempTs(), false};
        auto updated_ulink = base_ulink;
        if (base_meta.ts_ <= txn_->GetReadTs()) {
          auto ulog = GenerateNewUndoLog(&table_info_->schema_, &base_tuple, &t, base_meta.ts_,
                                         base_ulink.value_or(UndoLink{}));
          updated_ulink = txn_->AppendUndoLog(ulog);

        } else if (base_meta.ts_ == txn_->GetTransactionTempTs()) {
          if (base_ulink.has_value()) {  // its not a newly inserted tuple
            auto base_ulog = txn_mgr_->GetUndoLog(base_ulink.value());
            auto ulog = GenerateUpdatedUndoLog(&table_info_->schema_, &base_tuple, &t, base_ulog);
            txn_->ModifyUndoLog(base_ulink->prev_log_idx_, ulog);
          }
        }
        const auto old_meta = base_meta;
        const auto old_tuple = base_tuple;
        bool success_write = false;
        success_write =
            UpdateTupleAndUndoLink(txn_mgr_, r, updated_ulink, table_info_->table_.get(), txn_, updated_meta, t,
                                   [old_meta, old_tuple, r](const TupleMeta &meta, const Tuple &tuple, const RID rid,
                                                            const std::optional<UndoLink> ulink) {
                                     return (r == rid && old_meta == meta && IsTupleContentEqual(old_tuple, tuple));
                                   });
        if (success_write) {
          txn_->AppendWriteSet(table_info_->oid_, r);
          // update index
          for (const auto &index_info : indices) {
            index_info->index_->DeleteEntry(base_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_,
                                                                    index_info->index_->GetKeyAttrs()),
                                            r, txn_);
            auto success_index_update = index_info->index_->InsertEntry(
                t.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()), r,
                txn_);
            if (!success_index_update) {
              txn_->SetTainted();
              std::ostringstream error_msg;
              fmt::print(error_msg, "txn{} failed to update index and is marked as tained",
                         txn_->GetTransactionIdHumanReadable());
              throw ExecutionException(error_msg.str());
            }
          }
          ++inserted;
        }
      } else {
        txn_->SetTainted();
        std::ostringstream error_msg;
        fmt::print(error_msg, "duplicated pk! txn{} failed to insert tuple {}", txn_->GetTransactionIdHumanReadable(),
                   t.ToString(&table_info_->schema_));
        throw ExecutionException(error_msg.str());
      }
    }
  }
  *tuple = Tuple({ValueFactory::GetIntegerValue(inserted)}, &plan_->OutputSchema());
  produced_ = true;
  return true;
}

}  // namespace bustub
