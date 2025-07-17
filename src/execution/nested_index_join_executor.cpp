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

  outer_tuple_ready_ = child_executor_->Next(&outer_tuple_, &outer_rid_);
  outer_matched_ = false;

  if (outer_tuple_ready_) {
    FindInnerMatches();
  }
}

void NestedIndexJoinExecutor::FindInnerMatches() {
  inner_rids_.clear();
  inner_rid_idx_ = 0;

  Value join_key = plan_->KeyPredicate()->Evaluate(&outer_tuple_, child_executor_->GetOutputSchema());

  auto catalog = exec_ctx_->GetCatalog();
  auto index_info = catalog->GetIndex(plan_->GetIndexOid());

  if (index_info != nullptr) {
    std::vector<Value> key_values{join_key};
    Tuple key_tuple(key_values, &index_info->key_schema_);
    index_info->index_->ScanKey(key_tuple, &inner_rids_, exec_ctx_->GetTransaction());
  }
}

auto NestedIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (outer_tuple_ready_) {
    if (inner_rid_idx_ < inner_rids_.size()) {
      auto catalog = exec_ctx_->GetCatalog();
      auto table_info = catalog->GetTable(plan_->GetInnerTableOid());

      if (table_info != nullptr) {
        auto inner_rid = inner_rids_[inner_rid_idx_++];
        auto tuple_meta = table_info->table_->GetTupleMeta(inner_rid);
        if (!tuple_meta.is_deleted_) {
          auto [meta, inner_tuple] = table_info->table_->GetTuple(inner_rid);

          std::vector<Value> output_values;

          for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
            output_values.push_back(outer_tuple_.GetValue(&child_executor_->GetOutputSchema(), i));
          }
          for (uint32_t i = 0; i < plan_->InnerTableSchema().GetColumnCount(); i++) {
            output_values.push_back(inner_tuple.GetValue(&plan_->InnerTableSchema(), i));
          }

          *tuple = Tuple(output_values, &GetOutputSchema());
          *rid = RID{};
          outer_matched_ = true;
          return true;
        }
        continue;
      }
    }

    // Handle LEFT JOIN - emit outer tuple with NULL values if no matches were found
    if (plan_->GetJoinType() == JoinType::LEFT && !outer_matched_) {
      std::vector<Value> output_values;

      for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
        output_values.push_back(outer_tuple_.GetValue(&child_executor_->GetOutputSchema(), i));
      }

      for (uint32_t i = 0; i < plan_->InnerTableSchema().GetColumnCount(); i++) {
        const auto &col = plan_->InnerTableSchema().GetColumn(i);
        output_values.push_back(ValueFactory::GetNullValueByType(col.GetType()));
      }

      *tuple = Tuple(output_values, &GetOutputSchema());
      *rid = RID{};

      outer_tuple_ready_ = child_executor_->Next(&outer_tuple_, &outer_rid_);
      outer_matched_ = false;
      if (outer_tuple_ready_) {
        FindInnerMatches();
      }

      return true;
    }

    outer_tuple_ready_ = child_executor_->Next(&outer_tuple_, &outer_rid_);
    outer_matched_ = false;
    if (outer_tuple_ready_) {
      FindInnerMatches();
    }
  }

  return false;
}

}  // namespace bustub
