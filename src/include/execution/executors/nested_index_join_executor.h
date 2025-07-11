//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.h
//
// Identification: src/include/execution/executors/nested_index_join_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/nested_index_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * NestedIndexJoinExecutor executes index join operations.
 */
class NestedIndexJoinExecutor : public AbstractExecutor {
 public:
  NestedIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                          std::unique_ptr<AbstractExecutor> &&child_executor);

  /** @return The output schema for the nested index join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

 private:
  /** The nested index join plan node. */
  const NestedIndexJoinPlanNode *plan_;

  /** The child executor (outer table). */
  std::unique_ptr<AbstractExecutor> child_executor_;

  /** Current outer tuple. */
  Tuple outer_tuple_;

  /** Current outer tuple RID. */
  RID outer_rid_;

  /** Whether we have a valid outer tuple. */
  bool outer_tuple_ready_{false};

  /** Whether the current outer tuple has been matched (for LEFT JOIN). */
  bool outer_matched_{false};

  /** RIDs of inner tuples matching the current outer tuple. */
  std::vector<RID> inner_rids_;

  /** Current index in inner_rids_ vector. */
  size_t inner_rid_idx_{0};

  /** Helper method to find inner matches for the current outer tuple. */
  void FindInnerMatches();
};

/** for the old codebase type name */
using NestIndexJoinExecutor = NestedIndexJoinExecutor;

}  // namespace bustub
