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
#include "common/config.h"
#include "common/macros.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree_debug.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
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
  auto header_pg = header_rpg.As<BPlusTreeHeaderPage>();
  if(header_pg->root_page_id_ == INVALID_PAGE_ID) {
    return true;
  }

  auto root_rpg = bpm_->ReadPage(header_pg->root_page_id_);
  auto root_pg = root_rpg.As<BPlusTreePage>();

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
  auto header = header_rpg.As<BPlusTreeHeaderPage>();

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

    const B_PLUS_TREE_INTERNAL_PAGE_TYPE* internal_page = rpg.As<B_PLUS_TREE_INTERNAL_PAGE_TYPE>();
    auto first_greater_key_index = internal_page->KeyUpperBound(key, comparator_);
    auto possible_pid_index = first_greater_key_index - 1;
    if (possible_pid_index < 0 || possible_pid_index >= internal_page->GetSize()) {
      return false;
    }
    auto v = internal_page->ValueAt(possible_pid_index);
    auto kid_pid = v.GetPageId();
    // add current page's read lock into read_set_ when searching its kids
    ctx.read_set_.push_back(rpg);
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
  auto possible_insert_index = getInsertLocation(ctx, key, comparator_);
  
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
auto BPLUSTREE_TYPE::getInsertLocation(Context &ctx, const KeyType &key, const KeyComparator &cmp) -> std::optional<int> {
  while(!ctx.write_set_.empty()) {
    auto &wpg = ctx.write_set_.back();
    auto curr_page = wpg.AsMut<BPlusTreePage>();
    if (curr_page->IsLeafPage()) {
      auto leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(curr_page);
      auto first_greater_key_index = leaf->KeyUpperBound(key, cmp);
      return std::make_optional(first_greater_key_index);
    } else {
      auto page = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(curr_page);
      auto first_greater_key_index = page->KeyUpperBound(key, cmp);
      auto possible_pid_index = first_greater_key_index - 1;
      if (possible_pid_index >= 0 && possible_pid_index < page->GetSize()) {
        auto kid_page_id = page->ValueAt(possible_pid_index);
        auto kid_wpg = bpm_->WritePage(kid_page_id.GetPageId());
        ctx.write_set_.push_back(kid_wpg);
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
    B_PLUS_TREE_INTERNAL_PAGE_TYPE *new_root = new_root_wpg.AsMut<B_PLUS_TREE_INTERNAL_PAGE_TYPE>();
    new_root->Init(internal_max_size_);
    new_root->InsertAt(0, old_key, ValueType(left_pid, 0));
    new_root->InsertAt(1, split_key, ValueType(right_pid, 0));
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
        move_size
      );
      std::memmove(
        new_leaf->GetRidArrayPtr(0),
        leaf->GetRidArrayPtr(split_index),
        move_size
      );

      leaf->SetSize(split_index);
      new_leaf->SetSize(move_size);

      if (index >= split_index) {
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
        B_PLUS_TREE_INTERNAL_PAGE_TYPE* parent = parent_wpg.AsMut<B_PLUS_TREE_INTERNAL_PAGE_TYPE>();
        auto first_greater_key_index = parent->KeyUpperBound(new_key, cmp);
        return insertAndSplit(ctx, first_greater_key_index, new_key, ValueType(new_pid, 0), cmp);
      }
    } else { 
      // dont need to split;
      return leaf->InsertAt(index, key, value);
    }
  } else {
    // is internal
    auto internal = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(curr_page);
    if (internal->GetSize() >= internal->GetMaxSize()){
      // need to split internal
      auto new_pid = bpm_->NewPage();
      BUSTUB_ASSERT(new_pid != INVALID_PAGE_ID, "unable to get a new page");
      auto new_wpg = bpm_->WritePage(new_pid);
      B_PLUS_TREE_INTERNAL_PAGE_TYPE* new_internal = new_wpg.AsMut<B_PLUS_TREE_INTERNAL_PAGE_TYPE>();
      new_internal->Init(internal_max_size_);
      // page_id_size: [0, split_index-1] [split_index, max_size - 1]
      auto split_index = new_internal->GetMinSize();
      auto move_size = new_internal->GetMaxSize() - split_index;

      std::memmove(
        new_internal->GetKeyArrayPtr(0),
        internal->GetKeyArrayPtr(split_index),
        move_size
      );
      std::memmove(
        new_internal->GetPageIdArrayPtr(0),
        internal->GetPageIdArrayPtr(split_index),
        move_size
      );

      internal->SetSize(split_index);
      new_internal->SetSize(move_size);

      if (index >= split_index) {
        // the new kv pair should be inserted in the new_leaf
        index -= split_index;
        new_internal->InsertAt(index, key, value);
      } else {
        internal->InsertAt(index, key, value);
      }
      auto old_key = internal->KeyAt(0);
      auto new_key = new_internal->KeyAt(0);
      auto old_pid = curr_pid;
      if (is_root) {
        // we need a new root_page
        return create_new_root(old_key, new_key, old_pid, new_pid);
      } else {
        auto &parent_wpg = ctx.write_set_.back();
        B_PLUS_TREE_INTERNAL_PAGE_TYPE* parent = parent_wpg.AsMut<B_PLUS_TREE_INTERNAL_PAGE_TYPE>();
        auto first_greater_key_index = parent->KeyUpperBound(new_key, cmp);
        return insertAndSplit(ctx, first_greater_key_index, new_key, ValueType(new_pid, 0), cmp);
      }
    } else {
      return internal->InsertAt(index, key, value);
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
  auto possible_insert_index = getInsertLocation(ctx, key, comparator_);
  
  if (!possible_insert_index.has_value()) {
    ctx.header_page_ = std::nullopt;
    return;
  }

  auto possible_key_index = possible_insert_index.value() - 1;
  auto possible_key = ctx.write_set_.back().As<B_PLUS_TREE_LEAF_PAGE_TYPE>()->KeyAt(possible_key_index);
  if (comparator_(possible_key, key) != 0) {
    return;
  }
  // TODO recrusively remove
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
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { UNIMPLEMENTED("TODO(P2): Add implementation."); }

/**
 * @brief Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { UNIMPLEMENTED("TODO(P2): Add implementation."); }

/**
 * @brief Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { UNIMPLEMENTED("TODO(P2): Add implementation."); }

/**
 * @return Page id of the root of this tree
 *
 * You may want to implement this while implementing Task #3.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { UNIMPLEMENTED("TODO(P2): Add implementation."); }

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
