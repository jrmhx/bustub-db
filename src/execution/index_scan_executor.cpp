//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/index_scan_executor.h"
#include <memory>
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "storage/index/b_plus_tree_index.h"

namespace bustub {

/**
 * Creates a new index scan executor.
 * @param exec_ctx the executor context
 * @param plan the index scan plan to be executed
 */
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx) {
  BUSTUB_ASSERT(plan != nullptr, "invalid plan");
  plan_ = plan;
  auto *catalog = exec_ctx->GetCatalog();
  BUSTUB_ASSERT(catalog != nullptr, "invalid catalog");
  txn_mgr_ = exec_ctx->GetTransactionManager();
  txn_ = exec_ctx->GetTransaction();
  table_info_ = catalog->GetTable(plan_->table_oid_);
  BUSTUB_ASSERT(table_info_ != nullptr, "invalid table_info");
}

void IndexScanExecutor::Init() {
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  tree_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info->index_.get());
  // init idx iterator state for full scan
  if (tree_ != nullptr && plan_->pred_keys_.empty()) {
    idx_iter_ = tree_->GetBeginIterator();
  } else {  // point lookup
    // evaluate pred_keys_ expressions to get actual values
    std::vector<Value> key_values;
    key_values.reserve(plan_->pred_keys_.size());

    for (const auto &expr : plan_->pred_keys_) {
      // these should be constant values for point lookup
      Value value = expr->Evaluate(nullptr, GetOutputSchema());
      key_values.push_back(value);
    }

    auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());

    Tuple key_tuple(key_values, &index_info->key_schema_);
    pt_lkup_res_.clear();
    index_info->index_->ScanKey(key_tuple, &pt_lkup_res_, exec_ctx_->GetTransaction());
    pt_lkup_iter_ = pt_lkup_res_.begin();
  }
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (tree_ == nullptr) {
    return false;
  }

  if (!plan_->pred_keys_.empty()) {
    return HandlePointLookup(tuple, rid);
  } else {  // NOLINT
    return HandleFullScan(tuple, rid);
  }
}

auto IndexScanExecutor::HandlePointLookup(Tuple *tuple, RID *rid) -> bool {
  while (pt_lkup_iter_ != pt_lkup_res_.end()) {
    auto r = *pt_lkup_iter_;
    ++pt_lkup_iter_;

    if (r.GetPageId() == INVALID_PAGE_ID) {
      continue;
    }
    const auto [meta, t, undo_link] = GetTupleAndUndoLink(txn_mgr_, table_info_->table_.get(), r);

    auto undo_logs = CollectUndoLogs(r, meta, t, undo_link, txn_, txn_mgr_);
    if (undo_logs == std::nullopt) {
      continue;
    }

    auto tuple_opt = ReconstructTuple(&table_info_->schema_, t, meta, undo_logs.value());

    if (tuple_opt == std::nullopt) {
      continue;
    }

    if (plan_->filter_predicate_ != nullptr) {
      auto result = plan_->filter_predicate_->Evaluate(&tuple_opt.value(), GetOutputSchema());
      if (!result.GetAs<bool>()) {
        continue;
      }
    }
    *tuple = tuple_opt.value();
    *rid = r;
    return true;
  }
  return false;
}

auto IndexScanExecutor::HandleFullScan(Tuple *tuple, RID *rid) -> bool {
  if (tree_ == nullptr) {
    return false;
  }

  while (!idx_iter_.IsEnd()) {
    const auto r = (*idx_iter_).second;
    ++idx_iter_;

    if (r.GetPageId() == INVALID_PAGE_ID) {
      continue;
    }

    const auto [meta, t, undo_link] = GetTupleAndUndoLink(txn_mgr_, table_info_->table_.get(), r);

    auto undo_logs = CollectUndoLogs(r, meta, t, undo_link, txn_, txn_mgr_);
    if (undo_logs == std::nullopt) {
      continue;
    }

    auto tuple_opt = ReconstructTuple(&table_info_->schema_, t, meta, undo_logs.value());

    if (tuple_opt == std::nullopt) {
      continue;
    }

    if (plan_->filter_predicate_ != nullptr) {
      auto result = plan_->filter_predicate_->Evaluate(&tuple_opt.value(), GetOutputSchema());
      if (!result.GetAs<bool>()) {
        continue;
      }
    }
    *tuple = tuple_opt.value();
    *rid = r;
    return true;
  }
  return false;
}

}  // namespace bustub
