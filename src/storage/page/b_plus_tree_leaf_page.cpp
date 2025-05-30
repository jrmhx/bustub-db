//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_leaf_page.cpp
//
// Identification: src/storage/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstring>
#include <sstream>

#include "common/config.h"
#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * @brief Init method after creating a new leaf page
 *
 * After creating a new leaf page from buffer pool, must call initialize method to set default values,
 * including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size.
 *
 * @param max_size Max size of the leaf node
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetMaxSize(max_size);
  SetSize(0);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index" (a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  if (index < 0 && index >= GetSize()) {
    return {};
  }
  return key_array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  if (index < 0 && index >= GetSize()) {
    return {};
  }
  return rid_array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertAt(int index, const KeyType &key, const ValueType &value) -> bool {
  if (GetSize() >= GetMaxSize() || index < 0 || index > GetSize() || index >= GetMaxSize()) {
    return false;
  }
  auto size = GetSize();
  if (index != size) {
    // shift the array if insertion pos is in the middle
    std::memmove(
      reinterpret_cast<void*>(&key_array_[index + 1]),
      reinterpret_cast<void*>(&key_array_[index]),
      (size - index) * sizeof(KeyType)
    );
    std::memmove(
      reinterpret_cast<void*>(&rid_array_[index + 1]),
      reinterpret_cast<void*>(&rid_array_[index]),
      (size - index) * sizeof(ValueType)
    );
  }
  key_array_[index] = key;
  rid_array_[index] = value;
  ChangeSizeBy(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::DeleteAt(int index) -> bool {
  if(index < 0 || index >= GetSize()) {
    return false;
  }
  auto size = GetSize();
  std::memmove(
      reinterpret_cast<void*>(&key_array_[index]),
      reinterpret_cast<void*>(&key_array_[index + 1]),
      (size - index - 1) * sizeof(KeyType)
    );
  std::memmove(
    reinterpret_cast<void*>(&rid_array_[index]),
    reinterpret_cast<void*>(&rid_array_[index + 1]),
    (size - index - 1) * sizeof(ValueType)
  );

  ChangeSizeBy(-1);

  return true;
}

/**
 * @brief Binary search, duplicate elements(actually non-duplicated but doesnt matter), right-most inser point
 * similar to std::upperbound
 * 
 * @param target target key to search
 * @param cmp comparator for key type
 * @return return the first index of key in a leaf that is greater than given key. return size if not found.
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyUpperBound(const KeyType &target, const KeyComparator &cmp) const -> int {
  auto size = GetSize();
  int left = 0;
  int right = size;

  while (left < right) {
    int mid = left + (right - left) / 2;
    auto key = KeyAt(mid);
    if (cmp(key, target) > 0) {
      right = mid;
    } else {
      left = mid + 1;
    }
  }

  return left;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetKeyArrayPtr(int offset) -> void* {
  if (offset >= 0 && offset < GetMaxSize()) {
    return reinterpret_cast<void*>(&key_array_[offset]);
  }
  return nullptr;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetRidArrayPtr(int offset) -> void* {
  if (offset >= 0 && offset < GetMaxSize()) {
    return reinterpret_cast<void*>(&rid_array_[offset]);
  }
  return nullptr;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
