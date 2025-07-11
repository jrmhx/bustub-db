//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Creates a new nested index join executor.
 * @param exec_ctx the context that the nested index join should be performed in
 * @param plan the nested index join plan to be executed
 * @param child_executor the outer table
 */
NestedIndexJoinExecutor::NestedIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                                 std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedIndexJoinExecutor::Init() {
  child_executor_->Init();

  // Get the first outer tuple
  outer_tuple_ready_ = child_executor_->Next(&outer_tuple_, &outer_rid_);
  outer_matched_ = false;

  // Find matches for the first outer tuple
  if (outer_tuple_ready_) {
    FindInnerMatches();
  }
}

void NestedIndexJoinExecutor::FindInnerMatches() {
  // Clear previous matches
  inner_rids_.clear();
  inner_rid_idx_ = 0;

  // Extract the join key from the outer tuple using the key predicate
  Value join_key = plan_->KeyPredicate()->Evaluate(&outer_tuple_, child_executor_->GetOutputSchema());

  // Use the index to find matching inner tuples
  auto catalog = exec_ctx_->GetCatalog();
  auto index_info = catalog->GetIndex(plan_->GetIndexOid());

  if (index_info != nullptr) {
    // Create a tuple with the join key for index lookup
    std::vector<Value> key_values{join_key};
    Tuple key_tuple(key_values, &index_info->key_schema_);

    // Scan the index for matching RIDs
    index_info->index_->ScanKey(key_tuple, &inner_rids_, exec_ctx_->GetTransaction());
  }
}

auto NestedIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (outer_tuple_ready_) {
    // If we have remaining inner tuples to process for current outer tuple
    if (inner_rid_idx_ < inner_rids_.size()) {
      // Get the next inner tuple from our cached RIDs
      auto catalog = exec_ctx_->GetCatalog();
      auto table_info = catalog->GetTable(plan_->GetInnerTableOid());

      if (table_info != nullptr) {
        auto inner_rid = inner_rids_[inner_rid_idx_++];

        // Get the inner tuple from the table
        auto tuple_meta = table_info->table_->GetTupleMeta(inner_rid);
        if (!tuple_meta.is_deleted_) {
          auto [meta, inner_tuple] = table_info->table_->GetTuple(inner_rid);

          // Create the output tuple by combining outer and inner tuples
          std::vector<Value> output_values;

          // Add all columns from outer tuple
          for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
            output_values.push_back(outer_tuple_.GetValue(&child_executor_->GetOutputSchema(), i));
          }

          // Add all columns from inner tuple
          for (uint32_t i = 0; i < plan_->InnerTableSchema().GetColumnCount(); i++) {
            output_values.push_back(inner_tuple.GetValue(&plan_->InnerTableSchema(), i));
          }

          *tuple = Tuple(output_values, &GetOutputSchema());
          *rid = RID{};
          outer_matched_ = true;
          return true;
        }
        // If the inner tuple was deleted, continue to next inner tuple
        continue;
      }
    }

    // No more inner tuples to process for current outer tuple
    // Handle LEFT JOIN - emit outer tuple with NULL values if no matches were found
    if (plan_->GetJoinType() == JoinType::LEFT && !outer_matched_) {
      std::vector<Value> output_values;

      // Add all columns from outer tuple
      for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
        output_values.push_back(outer_tuple_.GetValue(&child_executor_->GetOutputSchema(), i));
      }

      // Add NULL values for inner tuple columns
      for (uint32_t i = 0; i < plan_->InnerTableSchema().GetColumnCount(); i++) {
        const auto &col = plan_->InnerTableSchema().GetColumn(i);
        output_values.push_back(ValueFactory::GetNullValueByType(col.GetType()));
      }

      *tuple = Tuple(output_values, &GetOutputSchema());
      *rid = RID{};

      // Move to next outer tuple and find matches
      outer_tuple_ready_ = child_executor_->Next(&outer_tuple_, &outer_rid_);
      outer_matched_ = false;
      if (outer_tuple_ready_) {
        FindInnerMatches();
      }

      return true;
    }

    // Move to next outer tuple (for INNER JOIN or when LEFT JOIN had matches)
    outer_tuple_ready_ = child_executor_->Next(&outer_tuple_, &outer_rid_);
    outer_matched_ = false;
    if (outer_tuple_ready_) {
      FindInnerMatches();
    }
  }

  return false;
}

}  // namespace bustub
