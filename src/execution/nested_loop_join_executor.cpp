//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new NestedLoopJoinExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The nested loop join plan to be executed
 * @param left_executor The child executor that produces tuple for the left side of join
 * @param right_executor The child executor that produces tuple for the right side of join
 */
NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  plan_ = plan;
  left_executor_ = std::move(left_executor);
  right_executor_ = std::move(right_executor);
}

/** Initialize the join */
void NestedLoopJoinExecutor::Init() { 
  left_executor_->Init();
  right_executor_->Init();
  
  // get the first left tuple
  left_tuple_ready_ = left_executor_->Next(&left_tuple_, &left_rid_);
  left_matched_ = false;
}

/**
 * Yield the next tuple from the join.
 * @param[out] tuple The next tuple produced by the join
 * @param[out] rid The next tuple RID produced, not used by nested loop join.
 * @return `true` if a tuple was produced, `false` if there are no more tuples.
 */
auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  while (left_tuple_ready_) {
    Tuple right_tuple;
    RID right_rid;
    
    // try to get the next right tuple
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      // evaluate the join predicate
      Value predicate_result = plan_->Predicate()->EvaluateJoin(
          &left_tuple_, left_executor_->GetOutputSchema(),
          &right_tuple, right_executor_->GetOutputSchema());
      
      if (!predicate_result.IsNull() && predicate_result.GetAs<bool>()) {
        // has a match
        std::vector<Value> output_values;
        
        for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
          output_values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        
        for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
          output_values.push_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
        }
        
        *tuple = Tuple(output_values, &GetOutputSchema());
        *rid = RID{};
        left_matched_ = true;
        return true;
      }
    }
    
    // no more right tuples for current left tuple
    // handle LEFT JOIN - emit left tuple with NULL values for right side if no match
    if (plan_->GetJoinType() == JoinType::LEFT && !left_matched_) {
      std::vector<Value> output_values;
      
      // add all columns from left tuple
      for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
        output_values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
      }
      
      // add NULL values for right tuple columns
      for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
        const auto &col = right_executor_->GetOutputSchema().GetColumn(i);
        output_values.push_back(ValueFactory::GetNullValueByType(col.GetType()));
      }
      
      *tuple = Tuple(output_values, &GetOutputSchema());
      *rid = RID{};
      
      // move to next left tuple
      left_tuple_ready_ = left_executor_->Next(&left_tuple_, &left_rid_);
      left_matched_ = false;
      
      // reset right executor for next left tuple
      right_executor_->Init();
      
      return true;
    }
    
    // move to next left tuple
    left_tuple_ready_ = left_executor_->Next(&left_tuple_, &left_rid_);
    left_matched_ = false;
    
    // reset right executor for next left tuple
    right_executor_->Init();
  }
  
  return false;
}

}  // namespace bustub
