//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree.cpp
//
// Identification: src/storage/index/b_plus_tree.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
// B Plus Tree Invariants:
// 1. every internal node (expect the root) has [floor(n/2), n] children; interal node with k kids must has k-1 keys
// 2. leaf node has [floor(L/2), L] keys/kids L is the order for leaf (can be different than internal's order n)
// 3. keys in each node is sorted in ascending order
// 4. in internal nodes each keys seperates the ranges of its child pointers
// 5. all leaves at the same level
// 6. leaves are linked
// 7. the root must have at least 2 kids (unless tree is empty or only has one node)
// 8. for any keys, the path from root to it should be unique
// a non root internal node is always generated from splition of another internal node (might be root)
// so the internal node's size is at least ceil(max_internal_kids_size / 2)
//===----------------------------------------------------------------------===//

#include "storage/index/b_plus_tree.h"
#include <cstring>
#include <functional>
#include <optional>
#include <utility>
#include "common/config.h"
#include "common/macros.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree_debug.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"
#include "type/value.h"

namespace bustub {

// The header_page is only a dummy page (only for storing root_page_id) and provide entry point of a tree
// the root_page is the actual tree top it has b_plus_tree_page structure and can be internal or leaf node
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/**
 * @brief Helper function to decide whether current b+tree is empty
 * There are 2 case when a tree is empty:
 * 1. header's root_page_id is invaild
 * 2. there is a root_page but it is a leaf with size==0
 * @return Returns true if this B+ tree has no keys and values.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {  
  auto header_rpg = bpm_->ReadPage(header_page_id_);
  auto header_pg = header_rpg.template As<BPlusTreeHeaderPage>();
  if(header_pg->root_page_id_ == INVALID_PAGE_ID) {
    return true;
  }

  auto root_rpg = bpm_->ReadPage(header_pg->root_page_id_);
  auto root_pg = root_rpg.template As<BPlusTreePage>();

  return root_pg->IsLeafPage() && root_pg->GetSize() == 0;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/**
 * @brief Return the only value that associated with input key
 *
 * This method is used for point query
 *
 * @param key input key
 * @param[out] result vector that stores the only value that associated with input key, if the value exists
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  // Declaration of context instance. Using the Context is not necessary but advised.
  if (IsEmpty()) {
    return false;
  }
  Context ctx;
  auto header_rpg = bpm_->ReadPage(header_page_id_);
  auto header = header_rpg.template As<BPlusTreeHeaderPage>();

  std::function<bool(page_id_t)> search = [&](page_id_t page_id) -> bool {
    if (page_id == INVALID_PAGE_ID) {
      return false;
    }
    auto rpg = bpm_->ReadPage(page_id);
    
    auto page = rpg.As<BPlusTreePage>();
    if (page->IsLeafPage() && page->GetSize() == 0) {
      return false;
    }

    if (page->IsLeafPage()) {
      const B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_page = rpg.template As<B_PLUS_TREE_LEAF_PAGE_TYPE>();

      auto first_greater_key_index = leaf_page->KeyUpperBound(key, comparator_);
      if (first_greater_key_index <= 0 || first_greater_key_index > leaf_page->GetSize()) {
        return false;
      } else {
        auto possible_rid_index = first_greater_key_index - 1;
        if (comparator_(leaf_page->KeyAt(possible_rid_index), key) == 0) {
          result->push_back(leaf_page->ValueAt(possible_rid_index));
        }
        return comparator_(leaf_page->KeyAt(possible_rid_index), key) == 0;
      }
    }

    const BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* internal_page = rpg.As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    auto first_greater_key_index = internal_page->KeyUpperBound(key, comparator_);
    auto possible_pid_index = first_greater_key_index - 1;
    if (possible_pid_index < 0 || possible_pid_index >= internal_page->GetSize()) {
      return false;
    }
    auto kid_pid = internal_page->ValueAt(possible_pid_index);
    // add current page's read lock into read_set_ when searching its kids
    ctx.read_set_.push_back(std::move(rpg));
    return search(kid_pid);
  };

  return search(header->root_page_id_);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/**
 * @brief Insert constant key & value pair into b+ tree
 *
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 *
 * @param key the key to insert
 * @param value the value associated with key
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  Context ctx;
  ctx.header_page_ = std::make_optional(bpm_->WritePage(header_page_id_));
  auto header_p = ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>();

  if (header_p->root_page_id_ == INVALID_PAGE_ID) {
    auto root_pid = bpm_->NewPage();
    BUSTUB_ASSERT(root_pid != INVALID_PAGE_ID, "unable to get a new page");
    auto root_pg = bpm_->WritePage(root_pid);
    B_PLUS_TREE_LEAF_PAGE_TYPE* root_p = root_pg.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    root_p->Init(leaf_max_size_);
    root_p->InsertAt(0, key, value);
    header_p->root_page_id_ = root_pg.GetPageId();
    return true;
  }

  ctx.root_page_id_ = header_p->root_page_id_;
  ctx.write_set_.push_back(bpm_->WritePage(header_p->root_page_id_));
  auto possible_insert_index = getUpperBoundLocation(ctx, key, comparator_);
  
  if (!possible_insert_index.has_value()) {
    ctx.header_page_ = std::nullopt;
    return false;
  }
  insertAndSplit(ctx, possible_insert_index.value(), key, value, comparator_);
  ctx.header_page_ = std::nullopt;
  return true;
}

/**
 * @brief helper function for Insert
 * 
 * @param[out] ctx the ctx's write_set_.back() is the leaf page to insert
 * @return std::optional<int> the index to insert in the leaf page nullopt if there's no leaf found
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::getUpperBoundLocation(Context &ctx, const KeyType &key, const KeyComparator &cmp) -> std::optional<int> {
  while(!ctx.write_set_.empty()) {
    auto &wpg = ctx.write_set_.back();
    auto curr_page = wpg.AsMut<BPlusTreePage>();
    if (curr_page->IsLeafPage()) {
      auto leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(curr_page);
      auto first_greater_key_index = leaf->KeyUpperBound(key, cmp);
      return std::make_optional(first_greater_key_index);
    } else {
      auto page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(curr_page);
      auto first_greater_key_index = page->KeyUpperBound(key, cmp);
      auto possible_pid_index = first_greater_key_index - 1;
      if (possible_pid_index >= 0 && possible_pid_index < page->GetSize()) {
        auto kid_page_id = page->ValueAt(possible_pid_index);
        ctx.write_set_.push_back(bpm_->WritePage(kid_page_id));
      } else {
        return std::nullopt;
      }
    }
  }
  return std::nullopt;
}

/**
 * @brief helper function for Insert reculsive insert kv and split if necessary
 * 
 * @param ctx context ctx's write_set_.back() is the page to insert
 * @param index the index to insert in that page
 * @param key 
 * @param value 
 * @return true successful
 * @return false failed
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::insertAndSplit(Context &ctx, int index, const KeyType &key, const ValueType &value, const KeyComparator &cmp) -> bool{
  if(ctx.write_set_.empty()) {
    return false;
  }
  
  bool is_root = false;
  bool is_leaf = false;

  auto wpg = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  auto curr_pid = wpg.GetPageId();
  auto curr_page = wpg.AsMut<BPlusTreePage>();
  is_root = curr_pid == ctx.root_page_id_;
  is_leaf = curr_page->IsLeafPage();

  // Helper for root creation
  auto create_new_root = [&](const KeyType &old_key, const KeyType &split_key, page_id_t left_pid, page_id_t right_pid) {
    auto new_root_pid = bpm_->NewPage();
    BUSTUB_ASSERT(new_root_pid != INVALID_PAGE_ID, "unable to get a new page");
    auto new_root_wpg = bpm_->WritePage(new_root_pid);
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *new_root = new_root_wpg.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    new_root->Init(internal_max_size_);
    new_root->InsertAt(0, old_key, left_pid);
    new_root->InsertAt(1, split_key, right_pid);
    auto &header_wpg = ctx.header_page_.value();
    auto header = header_wpg.AsMut<BPlusTreeHeaderPage>();
    header->root_page_id_ = new_root_pid;
    ctx.root_page_id_ = new_root_pid;
    return true;
  };

  if (is_leaf) {
    auto leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(curr_page);
    if (leaf->GetSize() >= leaf->GetMaxSize()) {
      // need to split
      auto new_pid = bpm_->NewPage();
      BUSTUB_ASSERT(new_pid != INVALID_PAGE_ID, "unable to get a new page");
      auto new_wpg = bpm_->WritePage(new_pid);
      B_PLUS_TREE_LEAF_PAGE_TYPE* new_leaf = new_wpg.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
      new_leaf->Init(leaf_max_size_);
      new_leaf->SetNextPageId(leaf->GetNextPageId());
      leaf->SetNextPageId(new_pid);
      // [0, split_index-1] [split_index, max_size - 1]
      auto split_index = leaf->GetMinSize();
      auto move_size = leaf->GetMaxSize() - split_index;

      std::memmove(
        new_leaf->GetKeyArrayPtr(0),
        leaf->GetKeyArrayPtr(split_index),
        move_size * sizeof(KeyType)
      );
      std::memmove(
        new_leaf->GetRidArrayPtr(0),
        leaf->GetRidArrayPtr(split_index),
        move_size * sizeof(RID)
      );

      leaf->SetSize(split_index);
      new_leaf->SetSize(move_size);

      if (index > split_index) {
        // the new kv pair should be inserted in the new_leaf
        index -= split_index;
        new_leaf->InsertAt(index, key, value);
      } else {
        leaf->InsertAt(index, key, value);
      }
      auto old_key = leaf->KeyAt(0);
      auto new_key = new_leaf->KeyAt(0);
      auto old_pid = curr_pid;
      if (is_root) {
        // we need a new root_page
        return create_new_root(old_key, new_key, old_pid, new_pid);
      } else {
        auto &parent_wpg = ctx.write_set_.back();
        BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* parent = parent_wpg.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
        auto old_index = parent->ValueIndex(old_pid);
        parent->SetKeyAt(old_index, old_key);
        auto first_greater_key_index = parent->KeyUpperBound(new_key, cmp);
        return insertAndSplit(ctx, first_greater_key_index, new_key, ValueType(new_pid, 0), cmp);
      }
    } else { 
      // dont need to split;
      return leaf->InsertAt(index, key, value);
    }
  } else {
    // is internal
    auto internal = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(curr_page);
    if (internal->GetSize() >= internal->GetMaxSize()){
      // need to split internal
      auto new_pid = bpm_->NewPage();
      BUSTUB_ASSERT(new_pid != INVALID_PAGE_ID, "unable to get a new page");
      auto new_wpg = bpm_->WritePage(new_pid);
      BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* new_internal = new_wpg.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
      new_internal->Init(internal_max_size_);
      // page_id_size: [0, split_index-1] [split_index, max_size - 1]
      auto split_index = new_internal->GetMinSize();
      auto move_size = new_internal->GetMaxSize() - split_index;

      std::memmove(
        new_internal->GetKeyArrayPtr(0),
        internal->GetKeyArrayPtr(split_index),
        move_size * sizeof(KeyType)
      );
      std::memmove(
        new_internal->GetPageIdArrayPtr(0),
        internal->GetPageIdArrayPtr(split_index),
        move_size * sizeof(page_id_t)
      );

      internal->SetSize(split_index);
      new_internal->SetSize(move_size);

      if (index > split_index) {
        // the new kv pair should be inserted in the new_leaf
        index -= split_index;
        new_internal->InsertAt(index, key, value.GetPageId());
      } else {
        internal->InsertAt(index, key, value.GetPageId());
      }
      auto old_key = internal->KeyAt(0);
      auto new_key = new_internal->KeyAt(0);
      auto old_pid = curr_pid;
      if (is_root) {
        // we need a new root_page
        return create_new_root(old_key, new_key, old_pid, new_pid);
      } else {
        auto &parent_wpg = ctx.write_set_.back();
        BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* parent = parent_wpg.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
        auto old_index = parent->ValueIndex(old_pid);
        parent->SetKeyAt(old_index, old_key);
        auto first_greater_key_index = parent->KeyUpperBound(new_key, cmp);
        return insertAndSplit(ctx, first_greater_key_index, new_key, ValueType(new_pid, 0), cmp);
      }
    } else {
      return internal->InsertAt(index, key, value.GetPageId());
    }
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/**
 * @brief Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 *
 * @param key input key
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  // Declaration of context instance.
  if (IsEmpty()) {
    return;
  }
  Context ctx;
  ctx.header_page_ = std::make_optional(bpm_->WritePage(header_page_id_));
  auto header_p = ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header_p->root_page_id_;
  ctx.write_set_.push_back(bpm_->WritePage(header_p->root_page_id_));
  auto upper_bound_key_index = getUpperBoundLocation(ctx, key, comparator_);
  
  if (!upper_bound_key_index.has_value()) {
    ctx.header_page_ = std::nullopt;
    return;
  }

  auto possible_key_index = upper_bound_key_index.value() - 1;
  auto possible_key = ctx.write_set_.back().As<B_PLUS_TREE_LEAF_PAGE_TYPE>()->KeyAt(possible_key_index);
  if (comparator_(possible_key, key) != 0) {
    ctx.header_page_ = std::nullopt;
    return;
  }
  // recursively remove and balance
  removeAndBalance(ctx, possible_key_index);
  ctx.header_page_ = std::nullopt;
  return;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::removeAndBalance(Context &ctx, int index) -> void {
  if(ctx.write_set_.empty()) {
    return;
  }

  auto curr_wpg = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  auto curr_pid = curr_wpg.GetPageId();
  auto curr_page = curr_wpg.As<BPlusTreePage>();
  std::optional<int> curr_page_index_in_parent = std::nullopt;

  auto find_curr_sibling = [&](const page_id_t page_id) {
    std::optional<WritePageGuard> left = std::nullopt;
    std::optional<WritePageGuard> right = std::nullopt;

    if (!ctx.write_set_.empty()) {
      auto &parent_wpg = ctx.write_set_.back();
      const BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* parent = parent_wpg.As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
      auto kid_index = parent->ValueIndex(page_id);
      curr_page_index_in_parent = std::make_optional(kid_index);
      auto left_index = kid_index - 1;
      auto right_index = kid_index + 1;
      if (left_index >= 0 && left_index < parent->GetSize()) {
        auto left_pid = parent->ValueAt(left_index);
        left = bpm_->CheckedWritePage(left_pid);
      }
      if (right_index >= 0 && right_index < parent->GetSize()) {
        auto right_pid = parent->ValueAt(right_index);
        right = bpm_->CheckedWritePage(right_pid);
      }
    }

    return std::make_pair(std::move(left), std::move(right));
  };

  if (curr_page->IsLeafPage()) {
    B_PLUS_TREE_LEAF_PAGE_TYPE* curr_leaf = curr_wpg.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    curr_leaf->DeleteAt(index);
    if (curr_pid == ctx.header_page_.value().GetPageId()) {
      return;
    }
    if (curr_leaf->GetSize() < curr_leaf->GetMinSize()){
      auto siblings = find_curr_sibling(curr_pid);
      auto left_opt = std::move(siblings.first);
      auto right_opt = std::move(siblings.second);
      if (left_opt.has_value()) {
        auto left_wpg = std::move(left_opt.value());
        B_PLUS_TREE_LEAF_PAGE_TYPE* left = left_wpg.template AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
        if (left->GetSize() > left->GetMinSize()) {
          // redistribute from left to curr
          auto re_key = left->KeyAt(left->GetSize()-1);
          auto re_value = left->ValueAt(left->GetSize()-1);
          left->DeleteAt(left->GetSize()-1);
          curr_leaf->InsertAt(0, re_key, re_value);
          auto &parent_wpg = ctx.write_set_.back();
          BUSTUB_ASSERT(curr_page_index_in_parent.has_value(), "invalid index");
          BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* parent = parent_wpg.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
          parent->SetKeyAt(curr_page_index_in_parent.value(), re_key);
          return;
        } else if (left->GetSize() == left->GetMinSize()) {
          // merge the current page into the left
          std::memmove(
            left->GetKeyArrayPtr(left->GetSize()),
            curr_leaf->GetKeyArrayPtr(0),
            curr_leaf->GetSize() * sizeof(KeyType)
          );

          std::memmove(
            left->GetRidArrayPtr(left->GetSize()),
            curr_leaf->GetRidArrayPtr(0),
            curr_leaf->GetSize() * sizeof(ValueType)
          );
          left->ChangeSizeBy(curr_leaf->GetSize());
          left->SetNextPageId(curr_leaf->GetNextPageId());
          bpm_->DeletePage(curr_pid);
          BUSTUB_ASSERT(curr_page_index_in_parent.has_value(), "invalid index");
          return removeAndBalance(ctx, curr_page_index_in_parent.value());
        }
      }

      if (right_opt.has_value()) {
        auto right_wpg = std::move(right_opt.value());
        B_PLUS_TREE_LEAF_PAGE_TYPE* right = right_wpg.template AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
        if (right->GetSize() > right->GetMinSize()) {
          // redistribute from right to curr
          auto re_key = right->KeyAt(0);
          auto re_value = right->ValueAt(0);
          right->DeleteAt(0);
          curr_leaf->InsertAt(curr_leaf->GetSize(), re_key, re_value);
          auto &parent_wpg = ctx.write_set_.back();
          BUSTUB_ASSERT(curr_page_index_in_parent.has_value(), "invalid index");
          BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* parent = parent_wpg.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
          parent->SetKeyAt(curr_page_index_in_parent.value()+1, right->KeyAt(0));
          return;
        } else if (right->GetSize() == right->GetMinSize()) {
          // merge the current right into the curr
          std::memmove(
            curr_leaf->GetKeyArrayPtr(curr_leaf->GetSize()),
            right->GetKeyArrayPtr(0),
            right->GetSize() * sizeof(KeyType)
          );

          std::memmove(
            curr_leaf->GetRidArrayPtr(curr_leaf->GetSize()),
            right->GetRidArrayPtr(0),
            right->GetSize() * sizeof(KeyType)
          );
          curr_leaf->ChangeSizeBy(right->GetSize());
          curr_leaf->SetNextPageId(right->GetNextPageId());
          auto &parent_wpg = ctx.write_set_.back();
          BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* parent = parent_wpg.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
          BUSTUB_ASSERT(curr_page_index_in_parent.has_value(), "invalid index");
          bpm_->DeletePage(parent->ValueAt(curr_page_index_in_parent.value() + 1));
          return removeAndBalance(ctx, curr_page_index_in_parent.value());
        }
      }
    }
  } else {
    // internal node
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* curr_internal = curr_wpg.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    
    curr_internal->DeleteAt(index);
    if (curr_pid == ctx.header_page_.value().GetPageId()) {
      if (curr_internal->GetSize() == 1) {
        auto &header_wpg = ctx.header_page_.value();
        auto header = header_wpg.AsMut<BPlusTreeHeaderPage>();
        header->root_page_id_ = curr_internal->ValueAt(0);
        return;
      }
    }
    if (curr_internal->GetSize() < curr_internal->GetMinSize()){
      auto siblings = find_curr_sibling(curr_pid);
      auto left_opt = std::move(siblings.first);
      auto right_opt = std::move(siblings.second);
      if (left_opt.has_value()) {
        auto left_wpg = std::move(left_opt.value());
        BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* left = left_wpg.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
        if (left->GetSize() > left->GetMinSize()) {
          // redistribute from left to curr
          auto re_key = left->KeyAt(left->GetSize()-1);
          auto re_value = left->ValueAt(left->GetSize()-1);
          left->DeleteAt(left->GetSize()-1);
          curr_internal->InsertAt(0, re_key, re_value);
          auto &parent_wpg = ctx.write_set_.back();
          BUSTUB_ASSERT(curr_page_index_in_parent.has_value(), "invalid index");
          BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* parent = parent_wpg.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
          parent->SetKeyAt(curr_page_index_in_parent.value(), re_key);
          return;
        } else if (left->GetSize() == left->GetMinSize()) {
          // merge the current page into the left
          std::memmove(
            left->GetKeyArrayPtr(left->GetSize()),
            curr_internal->GetKeyArrayPtr(0),
            curr_internal->GetSize() * sizeof(KeyType)
          );

          std::memmove(
            left->GetPageIdArrayPtr(left->GetSize()),
            curr_internal->GetPageIdArrayPtr(0),
            curr_internal->GetSize() * sizeof(page_id_t)
          );
          left->ChangeSizeBy(curr_internal->GetSize());
          bpm_->DeletePage(curr_pid);
          BUSTUB_ASSERT(curr_page_index_in_parent.has_value(), "invalid index");
          return removeAndBalance(ctx, curr_page_index_in_parent.value());
        }
      }

      if (right_opt.has_value()) {
        auto right_wpg = std::move(right_opt.value());
        BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* right = right_wpg.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
        if (right->GetSize() > right->GetMinSize()) {
          // redistribute from right to curr
          auto re_key = right->KeyAt(0);
          auto re_value = right->ValueAt(0);
          right->DeleteAt(0);
          curr_internal->InsertAt(curr_internal->GetSize(), re_key, re_value);
          auto &parent_wpg = ctx.write_set_.back();
          BUSTUB_ASSERT(curr_page_index_in_parent.has_value(), "invalid index");
          BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* parent = parent_wpg.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
          parent->SetKeyAt(curr_page_index_in_parent.value()+1, right->KeyAt(0));
          return;
        } else if (right->GetSize() == right->GetMinSize()) {
          // merge the current right into the curr
          std::memmove(
            curr_internal->GetKeyArrayPtr(curr_internal->GetSize()),
            right->GetKeyArrayPtr(0),
            right->GetSize() * sizeof(KeyType)
          );

          std::memmove(
            curr_internal->GetPageIdArrayPtr(curr_internal->GetSize()),
            right->GetPageIdArrayPtr(0),
            right->GetSize() * sizeof(KeyType)
          );
          curr_internal->ChangeSizeBy(right->GetSize());
          auto &parent_wpg = ctx.write_set_.back();
          BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* parent = parent_wpg.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
          BUSTUB_ASSERT(curr_page_index_in_parent.has_value(), "invalid index");
          bpm_->DeletePage(parent->ValueAt(curr_page_index_in_parent.value() + 1));
          return removeAndBalance(ctx, curr_page_index_in_parent.value());
        }
      }
    }
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/**
 * @brief Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 *
 * You may want to implement this while implementing Task #3.
 *
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { 
  auto rpg_opt = getLeftMostLeafPageGuard();
  if (!rpg_opt.has_value()) {
    return INDEXITERATOR_TYPE();
  }

  return {bpm_, std::move(rpg_opt.value()), 0};
}

/**
 * @brief Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {  
  auto page_info = getLeftPageGuardByKey(key);

  if (!page_info.has_value()) {
    return INDEXITERATOR_TYPE();
  }

  return {bpm_, std::move(page_info.value().first), page_info.value().second};
}

/**
 * @brief Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 *
 * You may want to implement this while implementing Task #3.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { 
  auto header_rpg = bpm_->ReadPage(header_page_id_);
  auto header = header_rpg.template As<BPlusTreeHeaderPage>();
  return header->root_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::getLeftMostLeafPageGuard() -> std::optional<ReadPageGuard> {
  if (IsEmpty()) {
    return std::nullopt;
  }

  Context ctx;
  ctx.read_set_.push_back(bpm_->ReadPage(header_page_id_));
  auto header = ctx.read_set_.back().As<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header->root_page_id_;
  ctx.read_set_.push_back(bpm_->ReadPage(ctx.root_page_id_));
  auto curr_page = ctx.read_set_.back().As<BPlusTreePage>();

  while (!curr_page->IsLeafPage()) {
    auto curr_interal = reinterpret_cast<const BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(curr_page);
    BUSTUB_ASSERT(curr_interal->GetSize() >= 2, "internal node must has at least 2 kids");
    auto kid_pid = curr_interal->ValueAt(0);
    auto curr_rpg = bpm_->ReadPage(kid_pid);
    if (!curr_rpg.IsValid()) {
      return std::nullopt;
    }
    ctx.read_set_.push_back(std::move(curr_rpg));
    curr_page = ctx.read_set_.back().template As<BPlusTreePage>();
  }

  if(curr_page->IsLeafPage()) {
    return std::make_optional(std::move(ctx.read_set_.back()));
  }

  return std::nullopt;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::getLeftPageGuardByKey(const KeyType &key) -> std::optional<std::pair<ReadPageGuard, int>>{
  if (IsEmpty()) {
    return std::nullopt;
  }

  Context ctx;
  ctx.read_set_.push_back(bpm_->ReadPage(header_page_id_));
  auto header = ctx.read_set_.back().As<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header->root_page_id_;
  ctx.read_set_.push_back(bpm_->ReadPage(ctx.root_page_id_));
  auto curr_page = ctx.read_set_.back().As<BPlusTreePage>();

  while (!curr_page->IsLeafPage()) {
    auto curr_interal = reinterpret_cast<const BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(curr_page);
    BUSTUB_ASSERT(curr_interal->GetSize() >= 2, "internal node must has at least 2 kids");
    auto possible_key_index = curr_interal->KeyUpperBound(key, comparator_) - 1;
    auto kid_pid = curr_interal->ValueAt(possible_key_index);
    auto curr_rpg = bpm_->ReadPage(kid_pid);
    if (!curr_rpg.IsValid()) {
      return std::nullopt;
    }
    ctx.read_set_.push_back(std::move(curr_rpg));
    curr_page = ctx.read_set_.back().template As<BPlusTreePage>();
  }

  if(curr_page->IsLeafPage()) {
    auto curr_leaf = reinterpret_cast<const B_PLUS_TREE_LEAF_PAGE_TYPE*>(curr_page);
    auto first_greater_key_index = curr_leaf->KeyUpperBound(key, comparator_);
    auto possible_key_index = first_greater_key_index - 1;
    if (comparator_(curr_leaf->KeyAt(possible_key_index), key) == 0) {
      return std::make_optional(std::make_pair(std::move(ctx.read_set_.back()), possible_key_index));
    }
  }

  return std::nullopt;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
