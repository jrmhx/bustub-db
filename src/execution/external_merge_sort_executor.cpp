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
#include <cstddef>
#include <queue>
#include <vector>
#include "execution/execution_common.h"
#include "execution/plans/sort_plan.h"

namespace bustub {

// SortPage Implementation
void SortPage::Init() {
  auto *header = GetHeader();
  header->tuple_count_ = 0;
  header->next_tuple_offset_ = sizeof(PageHeader);
}

auto SortPage::InsertTuple(const Tuple &tuple) -> bool {
  if (!CanInsertTuple(tuple)) {
    return false;
  }

  auto *header = GetHeader();
  auto tuple_size = tuple.GetLength();

  // Store tuple size first, then tuple data
  memcpy(data_ + header->next_tuple_offset_, &tuple_size, sizeof(uint32_t));
  memcpy(data_ + header->next_tuple_offset_ + sizeof(uint32_t), tuple.GetData(), tuple_size);

  header->next_tuple_offset_ += sizeof(uint32_t) + tuple_size;
  header->tuple_count_++;

  return true;
}

auto SortPage::GetTuple(size_t index, const Schema &schema) const -> Tuple {
  auto *header = GetHeader();
  if (index >= header->tuple_count_) {
    return Tuple{};
  }

  size_t offset = sizeof(PageHeader);
  for (size_t i = 0; i < index; i++) {
    uint32_t tuple_size;
    memcpy(&tuple_size, data_ + offset, sizeof(uint32_t));
    offset += sizeof(uint32_t) + tuple_size;
  }

  uint32_t tuple_size;
  memcpy(&tuple_size, data_ + offset, sizeof(uint32_t));

  // create a new tuple from the stored data
  return Tuple{RID{}, data_ + offset + sizeof(uint32_t), tuple_size};
}

auto SortPage::GetTupleCount() const -> size_t { return GetHeader()->tuple_count_; }

auto SortPage::CanInsertTuple(const Tuple &tuple) -> bool {
  auto *header = GetHeader();
  size_t required_space = sizeof(uint32_t) + tuple.GetLength();
  return header->next_tuple_offset_ + required_space <= BUSTUB_PAGE_SIZE;
}

// MergeSortRun::Iterator Implementation
void MergeSortRun::Iterator::LoadCurrentPage() const {
  if (run_ == nullptr || page_idx_ >= run_->pages_.size()) {
    return;
  }

  if (!page_guard_.has_value() || page_guard_->GetPageId() != run_->pages_[page_idx_]) {
    page_guard_ = run_->bpm_->ReadPage(run_->pages_[page_idx_]);
  }
}

auto MergeSortRun::Iterator::operator++() -> Iterator & {
  if (run_ == nullptr || page_idx_ >= run_->pages_.size()) {
    return *this;
  }

  LoadCurrentPage();
  if (!page_guard_.has_value()) {
    return *this;
  }

  auto *sort_page = page_guard_->template As<SortPage>();
  tuple_idx_++;

  if (tuple_idx_ >= sort_page->GetTupleCount()) {
    tuple_idx_ = 0;
    page_idx_++;
    page_guard_.reset();  // release current page
  }

  return *this;
}

auto MergeSortRun::Iterator::operator*() -> Tuple {
  if (run_ == nullptr || page_idx_ >= run_->pages_.size()) {
    return Tuple{};
  }

  LoadCurrentPage();
  if (!page_guard_.has_value()) {
    return Tuple{};
  }

  auto *sort_page = page_guard_->template As<SortPage>();
  return sort_page->GetTuple(tuple_idx_, run_->schema_);
}

auto MergeSortRun::Iterator::operator==(const Iterator &other) const -> bool {
  return run_ == other.run_ && page_idx_ == other.page_idx_ && tuple_idx_ == other.tuple_idx_;
}

auto MergeSortRun::Iterator::operator!=(const Iterator &other) const -> bool { return !(*this == other); }

// ExternalMergeSortExecutor Implementation
template <size_t K>
ExternalMergeSortExecutor<K>::ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                                                        std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), cmp_(plan->GetOrderBy()), child_executor_(std::move(child_executor)) {}

template <size_t K>
void ExternalMergeSortExecutor<K>::Init() {
  child_executor_->Init();

  if (!initialized_) {
    // 1 Generate sorted runs
    GenerateSortedRuns();

    // 2 Merge sorted runs
    while (sorted_runs_.size() > 1) {
      MergeRuns();
    }

    // Initialize iterator for final merged run
    if (!sorted_runs_.empty()) {
      current_iterator_ = sorted_runs_[0].Begin();
    }

    initialized_ = true;
  } else {
    // Reset iterator to beginning
    if (!sorted_runs_.empty()) {
      current_iterator_ = sorted_runs_[0].Begin();
    }
  }
}

template <size_t K>
auto ExternalMergeSortExecutor<K>::Next(Tuple *tuple, RID *rid) -> bool {
  if (sorted_runs_.empty() || current_iterator_ == sorted_runs_[0].End()) {
    return false;
  }

  *tuple = *current_iterator_;
  *rid = RID{};  // external sort doesn't preserve original RIDs
  ++current_iterator_;

  return true;
}

template <size_t K>
void ExternalMergeSortExecutor<K>::GenerateSortedRuns() {
  const auto &schema = child_executor_->GetOutputSchema();
  std::vector<SortEntry> current_run;
  Tuple tuple;
  RID rid;

  // buffer to accumulate tuples for each run
  // use a conservative estimate: assume each tuple is ~100 bytes average
  const size_t MAX_TUPLES_PER_RUN = (BUFFER_POOL_SIZE * BUSTUB_PAGE_SIZE) / 200; // NOLINT

  while (child_executor_->Next(&tuple, &rid)) {
    auto sort_key = GenerateSortKey(tuple, plan_->GetOrderBy(), schema);
    current_run.emplace_back(sort_key, tuple);

    // when we have enough tuples for a run, sort and write to disk
    if (current_run.size() >= MAX_TUPLES_PER_RUN) {
      // sort the current run
      std::sort(current_run.begin(), current_run.end(), cmp_);

      // create pages for this run
      std::vector<page_id_t> pages;
      CreateSortPages(current_run, pages, schema);

      // create MergeSortRun and add to sorted_runs_
      sorted_runs_.emplace_back(std::move(pages), exec_ctx_->GetBufferPoolManager(), schema);

      current_run.clear();
    }
  }

  // handle the last run if it has any tuples
  if (!current_run.empty()) {
    std::sort(current_run.begin(), current_run.end(), cmp_);

    std::vector<page_id_t> pages;
    CreateSortPages(current_run, pages, schema);

    sorted_runs_.emplace_back(std::move(pages), exec_ctx_->GetBufferPoolManager(), schema);
  }
}

template <size_t K>
void ExternalMergeSortExecutor<K>::CreateSortPages(const std::vector<SortEntry> &entries, std::vector<page_id_t> &pages,
                                                   const Schema &schema) {
  if (entries.empty()) {
    return;
  }

  auto *bpm = exec_ctx_->GetBufferPoolManager();
  page_id_t current_page_id = bpm->NewPage();
  auto page_guard = bpm->WritePage(current_page_id);
  auto *sort_page = page_guard.AsMut<SortPage>();
  sort_page->Init();

  for (const auto &entry : entries) {
    if (!sort_page->CanInsertTuple(entry.second)) {
      // current page is full, create a new page
      page_guard.Drop();  // write current page to disk
      pages.push_back(current_page_id);

      current_page_id = bpm->NewPage();
      page_guard = bpm->WritePage(current_page_id);
      sort_page = page_guard.AsMut<SortPage>();
      sort_page->Init();
    }

    sort_page->InsertTuple(entry.second);
  }

  // don't forget the last page
  page_guard.Drop();
  pages.push_back(current_page_id);
}

template <size_t K>
void ExternalMergeSortExecutor<K>::MergeRuns() {
  if (sorted_runs_.size() <= 1) {
    return;
  }

  std::vector<MergeSortRun> new_runs;
  const auto &schema = child_executor_->GetOutputSchema();

  // process runs in groups of K
  for (size_t i = 0; i < sorted_runs_.size(); i += K) {
    size_t end_idx = std::min(i + K, sorted_runs_.size());

    // collect the runs to merge
    std::vector<MergeSortRun::Iterator> iterators;
    for (size_t j = i; j < end_idx; j++) {
      iterators.push_back(sorted_runs_[j].Begin());
    }

    // merge these runs
    std::vector<SortEntry> merged_tuples;

    // priority queue for K-way merge - use indices instead of copying iterators
    using IndexPair = std::pair<size_t, size_t>;  // (run_index, local_iterator_index)
    auto cmp_func = [this, &iterators](const IndexPair &a, const IndexPair &b) {
      auto tuple_a = *iterators[a.second];
      auto tuple_b = *iterators[b.second];
      auto key_a = GenerateSortKey(tuple_a, plan_->GetOrderBy(), child_executor_->GetOutputSchema());
      auto key_b = GenerateSortKey(tuple_b, plan_->GetOrderBy(), child_executor_->GetOutputSchema());
      // invert the comparison for priority queue (max heap -> min heap)
      return !cmp_(SortEntry{key_a, tuple_a}, SortEntry{key_b, tuple_b});
    };

    std::priority_queue<IndexPair, std::vector<IndexPair>, decltype(cmp_func)> pq(cmp_func);

    // init priority queue
    for (size_t j = 0; j < iterators.size(); j++) {
      if (iterators[j] != sorted_runs_[i + j].End()) {
        pq.emplace(i + j, j);
      }
    }

    // perform K-way merge
    while (!pq.empty()) {
      auto [run_global_idx, local_idx] = pq.top();
      pq.pop();

      auto tuple = *iterators[local_idx];
      auto sort_key = GenerateSortKey(tuple, plan_->GetOrderBy(), schema);
      merged_tuples.emplace_back(sort_key, tuple);

      ++iterators[local_idx];
      if (iterators[local_idx] != sorted_runs_[run_global_idx].End()) {
        pq.emplace(run_global_idx, local_idx);
      }
    }

    // create new run from merged tuples
    std::vector<page_id_t> pages;
    CreateSortPages(merged_tuples, pages, schema);
    new_runs.emplace_back(std::move(pages), exec_ctx_->GetBufferPoolManager(), schema);
  }

  // replace old runs with new merged runs
  sorted_runs_ = std::move(new_runs);
}

template class ExternalMergeSortExecutor<2>;

}  // namespace bustub
