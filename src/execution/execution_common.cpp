//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// execution_common.cpp
//
// Identification: src/execution/execution_common.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/execution_common.h"
#include <sys/types.h>
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <numeric>
#include <optional>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "fmt/base.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

TupleComparator::TupleComparator(std::vector<OrderBy> order_bys) : order_bys_(std::move(order_bys)) {}

auto TupleComparator::operator()(const SortEntry &entry_a, const SortEntry &entry_b) const -> bool {
  const auto &key_a = entry_a.first;
  const auto &key_b = entry_b.first;

  // compare each sort key in order
  for (size_t i = 0; i < order_bys_.size() && i < key_a.size() && i < key_b.size(); i++) {
    const auto &value_a = key_a[i];
    const auto &value_b = key_b[i];
    const auto &order_by = order_bys_[i];

    auto cmp_result = value_a.CompareEquals(value_b);

    if (cmp_result == CmpBool::CmpTrue) {
      continue;  // values are equal, check next sort key
    }

    // values are not equal, determine order
    auto less_than = value_a.CompareLessThan(value_b);

    if (order_by.first == OrderByType::ASC || order_by.first == OrderByType::DEFAULT) {
      return less_than == CmpBool::CmpTrue;
    } else {  // NOLINT DESC
      return less_than == CmpBool::CmpFalse;
    }
  }

  return false;  // All keys are equal
}

/**
 * Generate sort key for a tuple based on the order by expressions.
 */
auto GenerateSortKey(const Tuple &tuple, const std::vector<OrderBy> &order_bys, const Schema &schema) -> SortKey {
  SortKey sort_key;
  sort_key.reserve(order_bys.size());

  for (const auto &order_by : order_bys) {
    auto value = order_by.second->Evaluate(&tuple, schema);
    sort_key.push_back(value);
  }

  return sort_key;
}

/**
 * Above are all you need for P3.
 * You can ignore the remaining part of this file until P4.
 */

/**
 * @brief Reconstruct a tuple by applying the provided undo logs from the base tuple. All logs in the undo_logs are
 * applied regardless of the timestamp
 *
 * @param schema The schema of the base tuple and the returned tuple.
 * @param base_tuple The base tuple to start the reconstruction from.
 * @param base_meta The metadata of the base tuple.
 * @param undo_logs The list of undo logs to apply during the reconstruction, the front is applied first.
 * @return An optional tuple that represents the reconstructed tuple. If the tuple is deleted as the result, returns
 * std::nullopt.
 */
auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  bool is_deleted = base_meta.is_deleted_;
  std::vector<Value> values;
  values.reserve(schema->GetColumnCount());
  for(uint32_t i = 0; i < schema->GetColumnCount(); ++i) {
    values.emplace_back(base_tuple.GetValue(schema, i));
  }

  for (const auto &undo_log : undo_logs) {
    if (undo_log.is_deleted_) {
      is_deleted = true;
      continue;
    }
    is_deleted = false;

    std::vector<uint32_t> modified_indices;
    std::for_each(
      undo_log.modified_fields_.begin(),
      undo_log.modified_fields_.end(),
      [&modified_indices, idx=uint32_t{0}] (bool is_modified) mutable {
        if (is_modified) {
          modified_indices.push_back(idx);
        }
        ++idx;
      }
    );
    auto partical_schema = std::make_unique<Schema>(Schema::CopySchema(schema, modified_indices));
    std::for_each(
      undo_log.modified_fields_.begin(),
      undo_log.modified_fields_.end(),
      [&, base_idx=uint32_t{0}, update_idx=uint32_t{0}](bool is_modified) mutable {
        if (is_modified) {
          values.at(base_idx) = undo_log.tuple_.GetValue(partical_schema.get(), update_idx);
          ++update_idx;
        }
        ++base_idx;
      }
    );
  }
  
  return is_deleted ? std::nullopt : std::make_optional<Tuple>(values, schema);
}

/**
 * @brief Collects the undo logs sufficient to reconstruct the tuple w.r.t. the txn.
 *
 * @param rid The RID of the tuple.
 * @param base_meta The metadata of the base tuple.
 * @param base_tuple The base tuple.
 * @param undo_link The undo link to the latest undo log.
 * @param txn The transaction.
 * @param txn_mgr The transaction manager.
 * @return An optional vector of undo logs to pass to ReconstructTuple(). std::nullopt if the tuple did not exist at the
 * time.
 */
auto CollectUndoLogs(RID rid, const TupleMeta &base_meta, const Tuple &base_tuple, std::optional<UndoLink> undo_link,
                     Transaction *txn, TransactionManager *txn_mgr) -> std::optional<std::vector<UndoLog>> {
  // tuple is updated by an uncommitted txn
  if (base_meta.ts_ >= TXN_START_ID) {
    // tuple is updated by current txn
    if (base_meta.ts_ == txn->GetTransactionTempTs()) {
      return base_meta.is_deleted_ ? std::nullopt : std::make_optional<std::vector<UndoLog>>({});
    }

    // by other tuple
    std::vector<UndoLog> udlgs;
    while (undo_link.has_value() && undo_link->IsValid()) {
      auto udlg = txn_mgr->GetUndoLog(undo_link.value());
      undo_link = std::make_optional(udlg.prev_version_);
      udlgs.push_back(udlg);
      if (udlg.ts_ <= txn->GetReadTs() ) {
        break;
      }
    }

    if (udlgs.empty() || udlgs.back().is_deleted_ || udlgs.back().ts_ > txn->GetReadTs()) {
      return std::nullopt;
    }

    return std::make_optional(udlgs);

  } else { // tuple is updated by a committed txn
    if (base_meta.ts_ > txn->GetReadTs()) {
      std::vector<UndoLog> udlgs;
      while (undo_link.has_value() && undo_link->IsValid()) {
        auto udlg = txn_mgr->GetUndoLog(undo_link.value());
        udlgs.push_back(udlg);
        undo_link = std::make_optional(udlg.prev_version_);
        
        if (udlg.ts_ <= txn->GetReadTs()) {
          break;
        }
      }
      
      if (udlgs.empty() || udlgs.back().ts_ > txn->GetReadTs()) {
        return std::nullopt;
      }
      return std::make_optional(udlgs);
    } else {
      return std::make_optional<std::vector<UndoLog>>({});
    }
  }
}

/**
 * @brief Generates a new undo log as the transaction tries to modify this tuple at the first time.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from the table heap. nullptr if the tuple is
 * deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a deletion.
 * @param ts The timestamp of the base tuple.
 * @param prev_version The undo link to the latest undo log of this tuple.
 * @return The generated undo log.
 */
auto GenerateNewUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple, timestamp_t ts,
                        UndoLink prev_version) -> UndoLog {
  if (base_tuple==nullptr && target_tuple == nullptr) {
    throw Exception("Cannot generate undo_log for 2 nullptr");
  }
  UndoLog undo_log;
  undo_log.ts_ = ts;
  undo_log.prev_version_ = prev_version;
  if (target_tuple==nullptr) {
    undo_log.modified_fields_ = std::vector<bool>(schema->GetColumnCount(), true);
    undo_log.is_deleted_ = false;
    undo_log.tuple_ = *base_tuple;
  } else if (base_tuple == nullptr) {
    undo_log.is_deleted_ = true;
  } else {
    undo_log.modified_fields_ = std::vector<bool>(schema->GetColumnCount(), false);
    undo_log.is_deleted_ = false;
    std::vector<uint32_t> attrs;
    std::vector<Value> values;
    for (uint32_t i = 0; i < schema->GetColumnCount(); ++i) {
      if (!base_tuple->GetValue(schema, i).CompareExactlyEquals(target_tuple->GetValue(schema, i))){
        undo_log.modified_fields_.at(i) = true;
        attrs.push_back(i);
        values.push_back(base_tuple->GetValue(schema, i));
      }
    }
    if (values.empty()) {
      throw Exception("cannot add undo log to unchanged tuples");
    }
    auto mask_schema = schema->CopySchema(schema, attrs);
    undo_log.tuple_ = Tuple(values, &mask_schema);
  }
  
  return undo_log;
}

/**
 * @brief Generate the updated undo log to replace the old one, whereas the tuple is already modified by this txn once.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from the table heap. nullptr if the tuple is
 * deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a deletion.
 * @param log The original undo log.
 * @return The updated undo log.
 */
auto GenerateUpdatedUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple,
                            const UndoLog &log) -> UndoLog {
  if (base_tuple==nullptr && target_tuple == nullptr) {
    throw Exception("Cannot generate undo_log for 2 nullptr");
  }
  UndoLog undo_log;
  undo_log.is_deleted_ = log.is_deleted_;
  undo_log.ts_ = log.ts_;
  undo_log.prev_version_ = log.prev_version_;
  undo_log.modified_fields_ = log.modified_fields_;
  std::vector<Value> values;
  std::vector<uint32_t> attrs;

  if (!log.is_deleted_) {
    auto origin = ReconstructTuple(schema, *base_tuple, {.ts_=0, .is_deleted_=false}, {log});
    values.reserve(schema->GetColumnCount());
    attrs.reserve(schema->GetColumnCount());
    for (uint32_t i = 0; i < schema->GetColumnCount(); ++i) {
      if (undo_log.modified_fields_.at(i)) {
        values.push_back(origin->GetValue(schema, i));
        attrs.push_back(i);
      } else if (target_tuple == nullptr) {
        undo_log.modified_fields_.at(i) = true;
        values.push_back(origin->GetValue(schema, i));
        attrs.push_back(i);
      } else if (!target_tuple->GetValue(schema, i).CompareExactlyEquals(origin->GetValue(schema, i))) {
        undo_log.modified_fields_.at(i) = true;
        values.push_back(origin->GetValue(schema, i));
        attrs.push_back(i);
      }
    }
    auto partical_schema = Schema::CopySchema(schema, attrs);
    undo_log.tuple_ = Tuple(values, &partical_schema);
  }
  return undo_log;
}

/**
 * @brief check if the txn's write operation on this tuple is conflicted with others
 *
 * @param txn The pointer of the txn that will conduct a write op.
 * @param base_meta the meta of the tuple in the table heap
 */
auto IsWriteWriteConflict(Transaction * txn, const TupleMeta & base_meta) -> bool {
  if (txn != nullptr && (base_meta.ts_ == txn->GetTransactionTempTs() || (base_meta.ts_ <= txn->GetReadTs()))) {
    return false;
  }

  return true;
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);
  fmt::println(stderr, "TXN_START_ID: {}", TXN_START_ID);

  auto it = table_heap->MakeIterator();

  while (!it.IsEnd()) {
    const auto rid = it.GetRID();
    const auto [meta, tuple] = it.GetTuple();
    
    std::string ts_str;
    if (meta.ts_ >= TXN_START_ID) {
      ts_str = fmt::format("txn{}", meta.ts_ ^ TXN_START_ID);
    } else {
      ts_str = std::to_string(meta.ts_);
    }
    
    const auto *schema = &table_info->schema_;
    std::string tuple_str = tuple.ToString(schema);

    if (meta.is_deleted_) {
      fmt::println(stderr, "RID={}/{} ts={} <del marker> tuple={}", 
                   rid.GetPageId(), rid.GetSlotNum(), ts_str, tuple_str);
    } else {
      fmt::println(stderr, "RID={}/{} ts={} tuple={}", 
                   rid.GetPageId(), rid.GetSlotNum(), ts_str, tuple_str);
    }
    
    auto undo_link = txn_mgr->GetUndoLink(rid);
    while (undo_link.has_value() && undo_link->IsValid()) {
      auto undo_log = txn_mgr->GetUndoLog(undo_link.value());
      
      std::string undo_txn_str;
      undo_txn_str = fmt::format("txn{}@{}", undo_link->prev_txn_ ^ TXN_START_ID, undo_link->prev_log_idx_);
      std::string undo_tuple_str = undo_log.is_deleted_ ? "<del>" : "(";
      if (!undo_log.is_deleted_) {
        std::vector<uint32_t> modified_indices;
        for (uint32_t i = 0; i < undo_log.modified_fields_.size(); ++i) {
          if (undo_log.modified_fields_[i]) {
            modified_indices.push_back(i);
          }
        }
        
        auto partial_schema = std::make_unique<Schema>(Schema::CopySchema(schema, modified_indices));
        uint32_t partial_idx = 0;
        
        for (uint32_t i = 0; i < schema->GetColumnCount(); ++i) {
          if (i > 0) undo_tuple_str += ", ";
          if (undo_log.modified_fields_[i]) {
            auto value = undo_log.tuple_.GetValue(partial_schema.get(), partial_idx++);
            if (value.IsNull()) {
              undo_tuple_str += "<NULL>";
            } else {
              undo_tuple_str += value.ToString();
            }
          } else {
            undo_tuple_str += "_";  // unchanged field
          }
        }
        undo_tuple_str += ")";
      }
      auto is_temp_ts = undo_log.ts_ >= TXN_START_ID;
      auto ts = is_temp_ts ? undo_log.ts_ ^ TXN_START_ID : undo_log.ts_;

      fmt::println(stderr, "\t{} {} ts={}{}", undo_txn_str, undo_tuple_str, is_temp_ts ? "txn" : "", ts);
      undo_link = undo_log.prev_version_;
    }
    
    ++it;
  }

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}

}  // namespace bustub
