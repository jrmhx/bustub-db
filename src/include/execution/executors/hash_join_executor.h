//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

/**
 * HashJoinExecutor executes a hash JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** Hash function for join keys */
  struct JoinKeyHash {
    auto operator()(const std::vector<Value> &key) const -> std::size_t;
  };

  /** Equality comparison for join keys */
  struct JoinKeyEqual {
    auto operator()(const std::vector<Value> &lhs, const std::vector<Value> &rhs) const -> bool;
  };

  /** Hash table type for storing left tuples */
  using HashTableType = std::unordered_map<std::vector<Value>, std::vector<Tuple>, JoinKeyHash, JoinKeyEqual>;

  /** Helper function to extract join keys from a tuple */
  auto MakeJoinKey(const Tuple *tuple, const Schema &schema, const std::vector<AbstractExpressionRef> &key_expressions)
      -> std::vector<Value>;

  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  /** Left child executor */
  std::unique_ptr<AbstractExecutor> left_child_;

  /** Right child executor */
  std::unique_ptr<AbstractExecutor> right_child_;

  /** Hash table built from left child tuples */
  HashTableType hash_table_;

  /** Current right tuple being processed */
  Tuple right_tuple_;

  /** Current right tuple RID */
  RID right_rid_;

  /** Iterator for matched left tuples for current right tuple */
  std::vector<Tuple>::const_iterator left_matches_it_;

  /** End iterator for matched left tuples for current right tuple */
  std::vector<Tuple>::const_iterator left_matches_end_;

  /** Whether current right tuple has been matched (for LEFT JOIN) */
  bool right_tuple_matched_;

  /** Whether we have a valid right tuple ready */
  bool right_tuple_ready_;

  /** For LEFT JOIN: set of left tuples that have been matched */
  std::unordered_set<const Tuple *> matched_left_tuples_;

  /** For LEFT JOIN: iterator for unmatched left tuples */
  HashTableType::const_iterator unmatched_left_it_;

  /** For LEFT JOIN: iterator within unmatched left tuple vector */
  std::vector<Tuple>::const_iterator unmatched_left_tuple_it_;

  /** For LEFT JOIN: whether we're processing unmatched left tuples */
  bool processing_unmatched_left_;
};

}  // namespace bustub
