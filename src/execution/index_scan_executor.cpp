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
#include "common/macros.h"
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
  auto * catalog = exec_ctx->GetCatalog();
  BUSTUB_ASSERT(catalog != nullptr, "invalid catalog");
  table_info_ = catalog->GetTable(plan_->table_oid_).get();
  BUSTUB_ASSERT(table_info_ != nullptr, "invalid table_info");
}

void IndexScanExecutor::Init() {  
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  tree_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info->index_.get());
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  if (tree_ == nullptr) {
    return false;
  }
  
  // check if we have predicate keys for point lookup
  if (!plan_->pred_keys_.empty()) {
    return HandlePointLookup(tuple, rid);
  } else {
    // full index scan
    return HandleFullScan(tuple, rid);
  }
}

auto IndexScanExecutor::HandlePointLookup(Tuple *tuple, RID *rid) -> bool {
  // evaluate pred_keys_ expressions to get actual values
  std::vector<Value> key_values;
  key_values.reserve(plan_->pred_keys_.size());
  
  for (const auto &expr : plan_->pred_keys_) {
    // evaluate the expression (these should be constant values for point lookup)
    Value value = expr->Evaluate(nullptr, GetOutputSchema());
    key_values.push_back(value);
  }
  
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  
  Tuple key_tuple(key_values, &index_info->key_schema_);
  
  // use the index's ScanKey method for efficient point lookup
  std::vector<RID> result_rids;
  index_info->index_->ScanKey(key_tuple, &result_rids, exec_ctx_->GetTransaction());
  
  for (const auto &result_rid : result_rids) {
    const auto [meta, t] = table_info_->table_->GetTuple(result_rid);
    if (!meta.is_deleted_) {
      if (plan_->filter_predicate_ != nullptr) {
        auto result = plan_->filter_predicate_->Evaluate(&t, GetOutputSchema());
        if (!result.GetAs<bool>()) {
          continue;
        }
      }
      *tuple = t;
      *rid = result_rid;
      return true;
    }
  }
  
  return false;
}

auto IndexScanExecutor::HandleFullScan(Tuple *tuple, RID *rid) -> bool {
  // Iterate through all index entries
  auto iter = tree_->GetBeginIterator();
  auto end_iter = tree_->GetEndIterator();
  
  for (; iter != end_iter; ++iter) {
    auto [key, r] = *iter;
    *rid = r;
    
    auto tuple_meta = table_info_->table_->GetTupleMeta(*rid);
    if (!tuple_meta.is_deleted_) {
      const auto [meta, t] = table_info_->table_->GetTuple(*rid);
      *tuple = t;
      
      if (plan_->filter_predicate_ != nullptr) {
        auto result = plan_->filter_predicate_->Evaluate(&t, GetOutputSchema());
        if (!result.GetAs<bool>()) {
          continue;  // Skip this tuple if predicate fails
        }
      }
      return true;  // Found a matching tuple
    }
  }
  
  return false;  // No more tuples
}

}  // namespace bustub
