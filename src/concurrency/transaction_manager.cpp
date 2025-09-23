//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <cstdio>
#include <functional>
#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include "catalog/catalog.h"
#include "common/config.h"
#include "common/exception.h"
#include "concurrency/transaction.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * Begins a new transaction.
 * @param isolation_level an optional isolation level of the transaction.
 * @return an initialized transaction
 */
auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // txn_id start from 1 but the last_commit_ts start from 0
  txn_ref->read_ts_.store(last_commit_ts_.load());

  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

/** @brief Verify if a txn satisfies serializability. We will not test this function and you can change / remove it as
 * you want. */
auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

/**
 * Commits a transaction.
 * @param txn the transaction to commit, the txn will be managed by the txn manager so no need to delete it by
 * yourself
 */
auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  txn->commit_ts_.store(last_commit_ts_.load() + 1);

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // the below code of commit update the base tuple's meta.ts_ to commit ts
  // it only work with all write executor using TableHeap::UpdateTuple() with a correct check() passed to provide txn
  // level write write conflict.
  const auto &ws = txn->GetWriteSets();
  for (const auto &[table_oid, rids] : ws) {
    const auto table_info = this->catalog_->GetTable(table_oid);
    for (const auto rid : rids) {
      auto meta = table_info->table_->GetTupleMeta(rid);
      meta.ts_ = txn->GetCommitTs();
      table_info->table_->UpdateTupleMeta(meta, rid);  // this func acquire table_heap page lock
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->GetCommitTs());
  running_txns_.RemoveTxn(txn->read_ts_);
  last_commit_ts_.fetch_add(1);
  return true;
}

/**
 * Aborts a transaction
 * @param txn the transaction to abort, the txn will be managed by the txn manager so no need to delete it by yourself
 */
void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!
  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

/** @brief Stop-the-world garbage collection. Will be called only when all transactions are not accessing the table
 * heap. */
void TransactionManager::GarbageCollection() {
  std::unique_lock<std::shared_mutex> lock(txn_map_mutex_);
  auto GetUndoLogOptionalUnsafe = [&](UndoLink &link) -> std::optional<UndoLog> { // NOLINT
    auto iter = txn_map_.find(link.prev_txn_);
    if (iter == txn_map_.end()) {
      return std::nullopt;
    }
    auto txn = iter->second;
    return txn->GetUndoLog(link.prev_log_idx_);
  };
  auto watermark = GetWatermark();
  const auto UndoLinkHash = [](const UndoLink &link) { // NOLINT
    return link.prev_txn_ ^ (static_cast<int64_t>(link.prev_log_idx_) << 32);
  };
  std::unordered_set<UndoLink, decltype(UndoLinkHash)> reachable(0, UndoLinkHash);

  auto table_names = catalog_->GetTableNames();
  for (const auto &table_name : table_names) {
    auto table_info = catalog_->GetTable(table_name);
    if (table_info == nullptr) {
      continue;
    }

    auto table_heap = table_info->table_.get();
    auto it = table_heap->MakeIterator();

    while (!it.IsEnd()) {
      const auto rid = it.GetRID();
      const auto [meta, tuple, undo_link] = GetTupleAndUndoLink(this, table_heap, rid);
      // fmt::println(stderr, "\tDEBUG: rid{}/{}, tuple_ts={}, tuple_is_delete={}", rid.GetPageId(), rid.GetSlotNum(),
      // meta.ts_, meta.is_deleted_ ? 1 : 0);
      if (meta.ts_ <= watermark) {
        ++it;
        continue;
      }
      auto curr_link = undo_link;
      while (curr_link.has_value() && curr_link->IsValid()) {
        auto log = GetUndoLogOptionalUnsafe(curr_link.value());
        if (!log.has_value()) {
          break;
        }
        if (log->ts_ > watermark) {
          reachable.insert(curr_link.value());
          curr_link = log->prev_version_;
        } else {
          reachable.insert(curr_link.value());
          break;
        }
      }

      ++it;
    }
  }

  for (auto it = txn_map_.begin(); it != txn_map_.end();) {
    if (it->second->GetTransactionState() == TransactionState::COMMITTED ||
        it->second->GetTransactionState() == TransactionState::ABORTED) {
      auto can_gc = true;

      for (int i = 0; i < static_cast<int>(it->second->undo_logs_.size()); ++i) {
        UndoLink target_link{it->first, i};
        if (reachable.find(target_link) != reachable.end()) {
          can_gc = false;
          break;
        }
      }

      if (can_gc) {
        fmt::println(stderr, "\tDEBUG: watermark={}, garbage collected txn{}", watermark, it->first ^ TXN_START_ID);
        it = txn_map_.erase(it);
      } else {
        ++it;
      }
    } else {
      ++it;
    }
  }
}

}  // namespace bustub
