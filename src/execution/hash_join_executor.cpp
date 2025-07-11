//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "common/util/hash_util.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new HashJoinExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The HashJoin join plan to be executed
 * @param left_child The child executor that produces tuples for the left side of join
 * @param right_child The child executor that produces tuples for the right side of join
 */
HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)),
      right_tuple_matched_(false),
      right_tuple_ready_(false),
      processing_unmatched_left_(false) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

/** Initialize the join */
void HashJoinExecutor::Init() {
  // init child executors
  left_child_->Init();
  right_child_->Init();

  // clear the hash table and other state
  hash_table_.clear();
  matched_left_tuples_.clear();
  right_tuple_matched_ = false;
  right_tuple_ready_ = false;
  processing_unmatched_left_ = false;

  // build hash table from left child
  Tuple left_tuple;
  RID left_rid;
  while (left_child_->Next(&left_tuple, &left_rid)) {
    auto join_key = MakeJoinKey(&left_tuple, left_child_->GetOutputSchema(), plan_->LeftJoinKeyExpressions());
    hash_table_[join_key].push_back(left_tuple);
  }

  // get first right tuple
  right_tuple_ready_ = right_child_->Next(&right_tuple_, &right_rid_);
  if (right_tuple_ready_) {
    auto right_key = MakeJoinKey(&right_tuple_, right_child_->GetOutputSchema(), plan_->RightJoinKeyExpressions());
    auto it = hash_table_.find(right_key);
    if (it != hash_table_.end()) {
      left_matches_it_ = it->second.begin();
      left_matches_end_ = it->second.end();
      right_tuple_matched_ = (left_matches_it_ != left_matches_end_);
    } else {
      left_matches_it_ = left_matches_end_;
      right_tuple_matched_ = false;
    }
  }

  // init unmatched left tuple processing for LEFT JOIN
  if (plan_->GetJoinType() == JoinType::LEFT) {
    unmatched_left_it_ = hash_table_.begin();
  }
}

/**
 * Yield the next tuple from the join.
 * @param[out] tuple The next tuple produced by the join.
 * @param[out] rid The next tuple RID, not used by hash join.
 * @return `true` if a tuple was produced, `false` if there are no more tuples.
 */
auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // phase 1: process matched tuples (INNER JOIN and LEFT JOIN)
  while (right_tuple_ready_) {
    // if we have matches for current right tuple
    if (left_matches_it_ != left_matches_end_) {
      const Tuple &left_tuple = *left_matches_it_;

      // create output tuple by concatenating left and right tuples
      std::vector<Value> output_values;

      // add all columns from left tuple
      for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i++) {
        output_values.push_back(left_tuple.GetValue(&left_child_->GetOutputSchema(), i));
      }

      // add all columns from right tuple
      for (uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); i++) {
        output_values.push_back(right_tuple_.GetValue(&right_child_->GetOutputSchema(), i));
      }

      *tuple = Tuple(output_values, &GetOutputSchema());
      *rid = RID{};

      // mark this left tuple as matched for LEFT JOIN
      if (plan_->GetJoinType() == JoinType::LEFT) {
        matched_left_tuples_.insert(&left_tuple);
      }

      // move to next left match
      ++left_matches_it_;
      return true;
    }

    // handle LEFT JOIN - emit right tuple with NULL values for left side if no match
    if (plan_->GetJoinType() == JoinType::LEFT && !right_tuple_matched_) {
      // for LEFT JOIN, we should NOT emit unmatched right tuples
      // skip this right tuple and move to the next one
      right_tuple_ready_ = right_child_->Next(&right_tuple_, &right_rid_);
      if (right_tuple_ready_) {
        auto right_key = MakeJoinKey(&right_tuple_, right_child_->GetOutputSchema(), plan_->RightJoinKeyExpressions());
        auto it = hash_table_.find(right_key);
        if (it != hash_table_.end()) {
          left_matches_it_ = it->second.begin();
          left_matches_end_ = it->second.end();
          right_tuple_matched_ = (left_matches_it_ != left_matches_end_);
        } else {
          left_matches_it_ = left_matches_end_;
          right_tuple_matched_ = false;
        }
      }

      // continue to next iteration to process the new right tuple
      continue;
    }

    // no more matches for current right tuple, get next right tuple
    right_tuple_ready_ = right_child_->Next(&right_tuple_, &right_rid_);
    if (right_tuple_ready_) {
      auto right_key = MakeJoinKey(&right_tuple_, right_child_->GetOutputSchema(), plan_->RightJoinKeyExpressions());
      auto it = hash_table_.find(right_key);
      if (it != hash_table_.end()) {
        left_matches_it_ = it->second.begin();
        left_matches_end_ = it->second.end();
        right_tuple_matched_ = (left_matches_it_ != left_matches_end_);
      } else {
        left_matches_it_ = left_matches_end_;
        right_tuple_matched_ = false;
      }
    }
  }

  // phase 2: for LEFT JOIN, emit unmatched left tuples with NULL values for right side
  if (plan_->GetJoinType() == JoinType::LEFT && !processing_unmatched_left_) {
    processing_unmatched_left_ = true;
    unmatched_left_it_ = hash_table_.begin();
    if (unmatched_left_it_ != hash_table_.end()) {
      unmatched_left_tuple_it_ = unmatched_left_it_->second.begin();
    }
  }

  if (processing_unmatched_left_) {
    while (unmatched_left_it_ != hash_table_.end()) {
      while (unmatched_left_tuple_it_ != unmatched_left_it_->second.end()) {
        const Tuple &left_tuple = *unmatched_left_tuple_it_;

        // check if this left tuple was matched
        if (matched_left_tuples_.find(&left_tuple) == matched_left_tuples_.end()) {
          // this left tuple was not matched, emit it with NULL values for right side
          std::vector<Value> output_values;

          // add all columns from left tuple
          for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i++) {
            output_values.push_back(left_tuple.GetValue(&left_child_->GetOutputSchema(), i));
          }

          // add NULL values for right tuple columns
          for (uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); i++) {
            const auto &column = right_child_->GetOutputSchema().GetColumn(i);
            output_values.push_back(ValueFactory::GetNullValueByType(column.GetType()));
          }

          *tuple = Tuple(output_values, &GetOutputSchema());
          *rid = RID{};

          // move to next left tuple
          ++unmatched_left_tuple_it_;
          return true;
        }

        ++unmatched_left_tuple_it_;
      }

      // move to next bucket in hash table
      ++unmatched_left_it_;
      if (unmatched_left_it_ != hash_table_.end()) {
        unmatched_left_tuple_it_ = unmatched_left_it_->second.begin();
      }
    }
  }

  return false;
}

/** helper function to extract join keys from a tuple */
auto HashJoinExecutor::MakeJoinKey(const Tuple *tuple, const Schema &schema,
                                   const std::vector<AbstractExpressionRef> &key_expressions) -> std::vector<Value> {
  std::vector<Value> keys;
  keys.reserve(key_expressions.size());

  for (const auto &expr : key_expressions) {
    keys.push_back(expr->Evaluate(tuple, schema));
  }

  return keys;
}

/** hash function for join keys */
auto HashJoinExecutor::JoinKeyHash::operator()(const std::vector<Value> &key) const -> std::size_t {
  std::size_t hash = 0;
  for (const auto &value : key) {
    if (!value.IsNull()) {
      hash = HashUtil::CombineHashes(hash, HashUtil::HashValue(&value));
    }
  }
  return hash;
}

/** equality comparison for join keys */
auto HashJoinExecutor::JoinKeyEqual::operator()(const std::vector<Value> &lhs, const std::vector<Value> &rhs) const
    -> bool {
  if (lhs.size() != rhs.size()) {
    return false;
  }

  for (size_t i = 0; i < lhs.size(); i++) {
    if (lhs[i].CompareEquals(rhs[i]) != CmpBool::CmpTrue) {
      return false;
    }
  }

  return true;
}

}  // namespace bustub
