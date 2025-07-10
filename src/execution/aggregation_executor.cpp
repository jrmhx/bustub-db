//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

/**
 * Construct a new AggregationExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The insert plan to be executed
 * @param child_executor The child executor from which inserted tuples are pulled (may be `nullptr`)
 */
AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor): 
    AbstractExecutor(exec_ctx), 
    plan_(plan),
    child_executor_(std::move(child_executor)),
    aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
    aht_iterator_(aht_.Begin()) {}

/** Initialize the aggregation */
void AggregationExecutor::Init() { 
  child_executor_->Init();
  aht_.Clear();

  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    AggregateKey agg_key = MakeAggregateKey(&tuple);
    AggregateValue agg_val = MakeAggregateValue(&tuple);
    aht_.InsertCombine(agg_key, agg_val);
  }

  // if no tuples were processed and there are no GROUP BY clauses,
  // we still need to return one row with initial aggregate values
  if (aht_.Begin() == aht_.End() && plan_->GetGroupBys().empty()) {
    // create a single group with empty key and initial aggregate values
    AggregateKey empty_key{{}}; // empty group by key
    aht_.InsertInitial(empty_key);
  }

  aht_iterator_ = aht_.Begin();
}

/**
 * Yield the next tuple from the aggregation.
 * @param[out] tuple The next tuple produced by the aggregation
 * @param[out] rid The next tuple RID produced by the aggregation
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  if (aht_iterator_ == aht_.End()) {
    return false;
  }

  const AggregateKey &agg_key = aht_iterator_.Key();
  const AggregateValue &agg_val = aht_iterator_.Val();

  std::vector<Value> output_values;
  
  for (const auto &group_by_val : agg_key.group_bys_) {
    output_values.push_back(group_by_val);
  }
  
  for (const auto &agg_result : agg_val.aggregates_) {
    output_values.push_back(agg_result);
  }

  *tuple = Tuple(output_values, &GetOutputSchema());
  *rid = RID{};
  
  ++aht_iterator_;
  return true;
}

/** Do not use or remove this function, otherwise you will get zero points. */
auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
