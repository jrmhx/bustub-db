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
#include "common/macros.h"
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
  auto *catalog = exec_ctx->GetCatalog();
  BUSTUB_ASSERT(catalog != nullptr, "Invalid Catalog");
  auto table_info = catalog->GetTable(plan_->GetTableOid());
  BUSTUB_ASSERT(table_info != nullptr, "Invalid Table Info!");
  table_iter_.reset(nullptr);
}

/** Initialize the sequential scan */
void SeqScanExecutor::Init() { 
  // init the table iterator
  auto table_oid = plan_->GetTableOid();
  auto *catalog = exec_ctx_->GetCatalog();
  BUSTUB_ASSERT(catalog != nullptr, "Invalid Catalog");
  auto table_info = catalog->GetTable(table_oid);
  BUSTUB_ASSERT(table_info != nullptr, "Invalid Table Info!");
  table_iter_ = std::make_unique<TableIterator>(table_info->table_->MakeIterator());
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

  while(!table_iter_->IsEnd()) {
    auto [meta, t] = table_iter_->GetTuple();
    *rid = table_iter_->GetRID();
    ++(*table_iter_);
    // skip deleted tuples
    if (meta.is_deleted_) {
      continue;
    }

    // apply filter predicate
    if (plan_->filter_predicate_ != nullptr) {
      auto val = plan_->filter_predicate_->Evaluate(&t, plan_->OutputSchema());
      if (!val.IsNull() && val.GetAs<bool>() == true) {
        *tuple = t;
        return true;
      }
    } else {
      *tuple = t;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
