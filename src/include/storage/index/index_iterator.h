//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.h
//
// Identification: src/include/storage/index/index_iterator.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include <utility>
#include "buffer/buffer_pool_manager.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  ~IndexIterator();  // NOLINT
  IndexIterator(BufferPoolManager *bpm, ReadPageGuard rpg, int pos);

  auto IsEnd() -> bool;

  auto operator*() -> std::pair<const KeyType &, const ValueType &>;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    if (is_valid_ != itr.is_valid_) {
      return false;
    }

    if (!is_valid_) {
      return true;
    }

    return curr_leaf_page_ == itr.curr_leaf_page_ && curr_pos_ == itr.curr_pos_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool {
    if (is_valid_ != itr.is_valid_) {
      return true;
    }

    if (!is_valid_) {
      return false;
    }

    return !(curr_leaf_page_ == itr.curr_leaf_page_ && curr_pos_ == itr.curr_pos_);
  }

 private:
  // add your own private member variables here

  BufferPoolManager *bpm_{nullptr};
  ReadPageGuard rpg_;
  const B_PLUS_TREE_LEAF_PAGE_TYPE *curr_leaf_page_{nullptr};
  int curr_pos_{0};
  bool is_valid_{false};
};

}  // namespace bustub
