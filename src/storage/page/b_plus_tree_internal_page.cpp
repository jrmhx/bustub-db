//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_internal_page.cpp
//
// Identification: src/storage/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstring>
#include <iostream>
#include <sstream>

#include "common/config.h"
#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * @brief Init method after creating a new internal page.
 *
 * Writes the necessary header information to a newly created page,
 * including set page type, set current size, set page id, set parent id and set max page size,
 * must be called after the creation of a new page to make a valid BPlusTreeInternalPage.
 *
 * @param max_size Maximal size of the page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetMaxSize(max_size);
  SetSize(0);  // the size is for the number of kids page
  // set all page_id_array_[i] to INVALID_PAGE_ID
  // std::memset(page_id_array_, 0xFF, max_size * sizeof(page_id_t));
}

/**
 * @brief Helper method to get/set the key associated with input "index"(a.k.a
 * array offset).
 *
 * @param index The index of the key to get. Index must be non-zero.
 * @return Key at index
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  if (index < 0 && index >= GetSize()) {
    // TODO(jrmhx): this is less safe, consider refactor these into a std::optional<KeyType>
    return {};
  }
  return key_array_[index];
}

/**
 * @brief Set key at the specified index.
 *
 * @param index The index of the key to set. Index must be non-zero.
 * @param key The new value for key
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  if (index < 0 && index >= GetSize()) {
    return;
  }
  key_array_[index] = key;
}

/**
 * @brief Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 *
 * @param index The index of the value to get.
 * @return Value at index
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  if (index < 0 && index >= GetSize()) {
    return ValueType{};
  }
  return page_id_array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  int size = GetSize();
  for (int i = 0; i < size; i++) {
    if (page_id_array_[i] == value) {
      return i;
    }
  }
  return -1;
}

// the page_index is [0, size-1];
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int page_index, const ValueType &value) -> void {
  if (page_index < 0 && page_index >= GetSize()) {
    return;
  }

  page_id_array_[page_index] = value;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertAt(int index, const KeyType &key, const ValueType &value) -> bool {
  if (GetSize() >= GetMaxSize() || index < 0 || index > GetSize() || index >= GetMaxSize()) {
    // when insert kv pair into a internal, the index should be in the valid key index range [1, size]
    return false;
  }
  auto size = GetSize();
  if (index != size) {
    // shift the array if insertion pos is in the middle
    std::memmove(reinterpret_cast<void *>(&key_array_[index + 1]), reinterpret_cast<void *>(&key_array_[index]),
                 (size - index) * sizeof(KeyType));
    std::memmove(reinterpret_cast<void *>(&page_id_array_[index + 1]), reinterpret_cast<void *>(&page_id_array_[index]),
                 (size - index) * sizeof(ValueType));
  }
  key_array_[index] = key;
  page_id_array_[index] = value;
  ChangeSizeBy(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::DeleteAt(int index) -> bool {
  if (index < 0 || index >= GetSize()) {
    return false;
  }
  auto size = GetSize();
  std::memmove(reinterpret_cast<void *>(&key_array_[index]), reinterpret_cast<void *>(&key_array_[index + 1]),
               (size - index - 1) * sizeof(KeyType));
  std::memmove(reinterpret_cast<void *>(&page_id_array_[index]), reinterpret_cast<void *>(&page_id_array_[index + 1]),
               (size - index - 1) * sizeof(ValueType));

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
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyUpperBound(const KeyType &target, const KeyComparator &cmp) const -> int {
  auto size = GetSize();
  int left = 1;
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
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetKeyArrayPtr(int offset) -> void * {
  if (offset >= 0 && offset < GetMaxSize()) {
    // the valid key idx in internal node start from 1
    return reinterpret_cast<void *>(&key_array_[offset]);
  }
  return nullptr;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetPageIdArrayPtr(int offset) -> void * {
  if (offset >= 0 && offset < GetMaxSize()) {
    return reinterpret_cast<void *>(&page_id_array_[offset]);
  }
  return nullptr;
}

// valuetype for internalNode should be page_id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
