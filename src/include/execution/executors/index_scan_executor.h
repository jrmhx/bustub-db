//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.h
//
// Identification: src/include/execution/executors/index_scan_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <deque>
#include <memory>
#include <vector>
#include "catalog/catalog.h"
#include "common/rid.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/index_scan_plan.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * IndexScanExecutor executes an index scan over a table.
 */

class IndexScanExecutor : public AbstractExecutor {
 public:
  IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan);

  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

 private:
  /** The index scan plan node to be executed. */
  const IndexScanPlanNode *plan_;

  const TableInfo *table_info_;

  BPlusTreeIndexForTwoIntegerColumn * tree_{nullptr};
  
  /** Iterator state for full index scans */
  BPlusTreeIndexIteratorForTwoIntegerColumn idx_iter_;

  std::vector<RID> pt_lkup_res_;

  std::vector<RID>::iterator pt_lkup_iter_;
  
  /** Helper method for point lookup using pred_keys_ */
  auto HandlePointLookup(Tuple *tuple, RID *rid) -> bool;
  
  /** Helper method for full index scan */
  auto HandleFullScan(Tuple *tuple, RID *rid) -> bool;
};
}  // namespace bustub
