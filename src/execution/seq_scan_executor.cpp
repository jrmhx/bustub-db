//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <memory>
#include <optional>
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/table_iterator.h"

namespace bustub {

/**
 * Construct a new SeqScanExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The sequential scan plan to be executed
 */
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  txn_manager_ = exec_ctx->GetTransactionManager();
  txn_ = exec_ctx->GetTransaction();
  auto *catalog = exec_ctx->GetCatalog();
  BUSTUB_ASSERT(catalog != nullptr, "Invalid Catalog");
  table_info_ = catalog->GetTable(plan_->GetTableOid());
  BUSTUB_ASSERT(table_info_ != nullptr, "Invalid Table Info!");
  table_schema_ = &table_info_->schema_;
  table_iter_.reset(nullptr);
}

void SeqScanExecutor::Init() {
  table_iter_ = std::make_unique<TableIterator>(table_info_->table_->MakeIterator());
}

/**
 * Yield the next tuple from the sequential scan.
 * @param[out] tuple The next tuple produced by the scan
 * @param[out] rid The next tuple RID produced by the scan
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (table_iter_ == nullptr) {
    return false;
  }

  while (!table_iter_->IsEnd()) {
    *rid = table_iter_->GetRID();
    ++(*table_iter_);
    // see if we need to retive the prev version of this tuple
    auto [meta, t, undo_link] = GetTupleAndUndoLink(txn_manager_, table_info_->table_.get(), *rid);
    auto undo_logs = CollectUndoLogs(*rid, meta, t, undo_link, txn_, txn_manager_);
    if (undo_logs == std::nullopt) {
      continue;
    }

    auto tuple_opt = ReconstructTuple(table_schema_, t, meta, undo_logs.value());

    if (tuple_opt == std::nullopt) {
      continue;
    }

    if (plan_->filter_predicate_ != nullptr) {
      auto val = plan_->filter_predicate_->Evaluate(&tuple_opt.value(), plan_->OutputSchema());
      if (!val.IsNull() && val.GetAs<bool>()) {
        *tuple = tuple_opt.value();
        return true;
      }
    } else {
      *tuple = tuple_opt.value();
      return true;
    }
  }
  return false;
}

}  // namespace bustub
