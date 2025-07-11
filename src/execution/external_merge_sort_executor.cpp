//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.cpp
//
// Identification: src/execution/external_merge_sort_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/external_merge_sort_executor.h"
#include <algorithm>
#include <vector>
#include "execution/execution_common.h"
#include "execution/plans/sort_plan.h"

namespace bustub {

template <size_t K>
ExternalMergeSortExecutor<K>::ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                                                        std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), cmp_(plan->GetOrderBy()), child_executor_(std::move(child_executor)) {}

/** Initialize the external merge sort */
template <size_t K>
void ExternalMergeSortExecutor<K>::Init() {
  child_executor_->Init();

  if (!initialized_) {
    // collect all tuples from child
    std::vector<SortEntry> sort_entries;
    Tuple tuple;
    RID rid;

    while (child_executor_->Next(&tuple, &rid)) {
      auto sort_key = GenerateSortKey(tuple, plan_->GetOrderBy(), child_executor_->GetOutputSchema());
      sort_entries.emplace_back(sort_key, tuple);
    }

    // sort all tuples using the comparator
    std::sort(sort_entries.begin(), sort_entries.end(), cmp_);

    // extract sorted tuples
    sorted_tuples_.clear();
    sorted_tuples_.reserve(sort_entries.size());
    for (const auto &entry : sort_entries) {
      sorted_tuples_.push_back(entry.second);
    }

    initialized_ = true;
  }

  current_index_ = 0;
}

/**
 * Yield the next tuple from the external merge sort.
 * @param[out] tuple The next tuple produced by the external merge sort.
 * @param[out] rid The next tuple RID produced by the external merge sort.
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
template <size_t K>
auto ExternalMergeSortExecutor<K>::Next(Tuple *tuple, RID *rid) -> bool {
  if (current_index_ >= sorted_tuples_.size()) {
    return false;
  }

  *tuple = sorted_tuples_[current_index_];
  *rid = RID{};  // external sort doesn't preserve original RIDs
  current_index_++;

  return true;
}

template class ExternalMergeSortExecutor<2>;

}  // namespace bustub
