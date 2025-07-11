//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.h
//
// Identification: src/include/execution/executors/external_merge_sort_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <utility>
#include <vector>
#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "execution/execution_common.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/sort_plan.h"
#include "storage/page/page_guard.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * Page to hold the intermediate data for external merge sort.
 *
 * Only fixed-length data will be supported in Spring 2025.
 */
class SortPage {
 public:
  void Init();

  /** Add a tuple to this sort page */
  auto InsertTuple(const Tuple &tuple) -> bool;

  /** Get tuple at the given index */
  auto GetTuple(size_t index, const Schema &schema) const -> Tuple;

  /** Get the number of tuples in this page */
  auto GetTupleCount() const -> size_t;

  /** Check if this page can hold another tuple */
  auto CanInsertTuple(const Tuple &tuple) -> bool;

  /** Get the underlying page data */
  auto GetData() -> char * { return data_; }

 private:
  /** Page header containing metadata */
  struct PageHeader {
    size_t tuple_count_{0};
    size_t next_tuple_offset_{sizeof(PageHeader)};
  };

  /** Raw page data */
  char data_[BUSTUB_PAGE_SIZE];

  /** Get the page header */
  auto GetHeader() -> PageHeader * { return reinterpret_cast<PageHeader *>(data_); }
  auto GetHeader() const -> const PageHeader * { return reinterpret_cast<const PageHeader *>(data_); }
};

/**
 * A data structure that holds the sorted tuples as a run during external merge sort.
 * Tuples might be stored in multiple pages, and tuples are ordered both within one page
 * and across pages.
 */
class MergeSortRun {
 public:
  MergeSortRun(std::vector<page_id_t> pages, BufferPoolManager *bpm, Schema schema)
      : pages_(std::move(pages)), bpm_(bpm), schema_(std::move(schema)) {}

  // Move constructor
  MergeSortRun(MergeSortRun&& other) noexcept 
      : pages_(std::move(other.pages_)), bpm_(other.bpm_), schema_(std::move(other.schema_)) {
    other.bpm_ = nullptr;  // prevent double deletion
  }

  // Move assignment operator
  MergeSortRun& operator=(MergeSortRun&& other) noexcept {
    if (this != &other) {
      // Clean up current pages before moving
      DeletePages();
      pages_ = std::move(other.pages_);
      bpm_ = other.bpm_;
      schema_ = std::move(other.schema_);
      other.bpm_ = nullptr;  // prevent double deletion
    }
    return *this;
  }

  // Destructor to clean up pages
  ~MergeSortRun() {
    DeletePages();
  }

  // Delete copy constructor and copy assignment to prevent issues
  MergeSortRun(const MergeSortRun&) = delete;
  MergeSortRun& operator=(const MergeSortRun&) = delete;

  auto GetPageCount() -> size_t { return pages_.size(); }

  auto GetSchema() const -> const Schema & { return schema_; }

 private:
  // Helper method to delete all pages in this run
  void DeletePages() {
    if (bpm_ != nullptr) {
      for (auto page_id : pages_) {
        bpm_->DeletePage(page_id);
      }
    }
  }

 public:

  /** Iterator for iterating on the sorted tuples in one run. */
  class Iterator {
    friend class MergeSortRun;

   public:
    Iterator() = default;

    auto operator++() -> Iterator &;

    auto operator*() -> Tuple;

    auto operator==(const Iterator &other) const -> bool;

    auto operator!=(const Iterator &other) const -> bool;

   private:
    explicit Iterator(const MergeSortRun *run, size_t page_idx = 0, size_t tuple_idx = 0)
        : run_(run), page_idx_(page_idx), tuple_idx_(tuple_idx) {}

    /** the sorted run that the iterator is iterating on. */
    const MergeSortRun *run_{nullptr};

    /** current page index in the run */
    size_t page_idx_{0};

    /** current tuple index within the current page */
    size_t tuple_idx_{0};

    /** page guard for the current page */
    mutable std::optional<ReadPageGuard> page_guard_;

    void LoadCurrentPage() const;
  };

  auto Begin() -> Iterator { return Iterator{this, 0, 0}; }

  auto End() -> Iterator { return Iterator{this, pages_.size(), 0}; }

 private:
  /** the page IDs of the sort pages that store the sorted tuples. */
  std::vector<page_id_t> pages_;
  /**
   * the buffer pool manager used to read sort pages. The buffer pool manager is responsible for
   * deleting the sort pages when they are no longer needed.
   */
  BufferPoolManager *bpm_{nullptr};

  Schema schema_;
};

/**
 * ExternalMergeSortExecutor executes an external merge sort.
 *
 * In Spring 2025, only 2-way external merge sort is required.
 */
template <size_t K>
class ExternalMergeSortExecutor : public AbstractExecutor {
 public:
  ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                            std::unique_ptr<AbstractExecutor> &&child_executor);

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the external merge sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;

  /** Compares tuples based on the order-bys */
  TupleComparator cmp_;

  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  /** Sorted runs created during external merge sort */
  std::vector<MergeSortRun> sorted_runs_;

  /** Current iterator position in the final merged run */
  MergeSortRun::Iterator current_iterator_;

  /** Whether initialization is complete */
  bool initialized_{false};

  /** Helper method to generate sorted runs from input data */
  void GenerateSortedRuns();

  /** Helper method to merge K sorted runs into fewer runs */
  void MergeRuns();

  /** Helper method to create sort pages from sorted entries */
  void CreateSortPages(const std::vector<SortEntry> &entries, std::vector<page_id_t> &pages, const Schema &schema);
};

}  // namespace bustub
