//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.cpp
//
// Identification: src/storage/index/index_iterator.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * index_iterator.cpp
 */
#include <cassert>
#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/macros.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/page_guard.h"

#include "storage/index/index_iterator.h"

namespace bustub {

/**
 * @note you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, ReadPageGuard rpg, int pos) 
: bpm_(bpm), rpg_(std::move(rpg)), curr_pos_(pos) {
  if(bpm_ != nullptr) {
    if (rpg_.IsValid()) {
      curr_leaf_page_ = rpg_.As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
      is_valid_ = true;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { 
  return !is_valid_ || curr_leaf_page_->GetSize() == 0;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> std::pair<const KeyType &, const ValueType &> {
  BUSTUB_ASSERT(!IsEnd(), "iterator is end");
  return {curr_leaf_page_->KeyAt(curr_pos_), curr_leaf_page_->ValueAt(curr_pos_)};
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & { 
  BUSTUB_ASSERT(!IsEnd(), "iterator end!");
  if (curr_pos_ == curr_leaf_page_->GetSize() - 1) {
    if (curr_leaf_page_->GetNextPageId() == INVALID_PAGE_ID){
      rpg_.Drop();
      is_valid_ = false;
    } else {
      rpg_ = bpm_->ReadPage(curr_leaf_page_->GetNextPageId());
      if (rpg_.IsValid()) {
        curr_leaf_page_ = rpg_.As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
        curr_pos_ = 0;
        is_valid_ = true;
      } else {
        is_valid_ = false;
      }
    }
  } else {
    ++curr_pos_;
  }

  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
