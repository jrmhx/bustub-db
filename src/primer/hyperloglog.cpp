//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hyperloglog.cpp
//
// Identification: src/primer/hyperloglog.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/hyperloglog.h"
#include <cmath>
#include <cstdint>
#include <mutex>
#include <shared_mutex>
#include <vector>

namespace bustub {

/** @brief Parameterized constructor. */
template <typename KeyType>
HyperLogLog<KeyType>::HyperLogLog(int16_t n_bits) : cardinality_(0) {
  if (n_bits < 0) {
    n_bits = 0;
  }
  n_bits_ = static_cast<size_t>(n_bits);
  auto n_buckets = static_cast<size_t>(std::pow(2, n_bits));
  buckets_ = std::vector<uint64_t>(n_buckets, 0);
}

/**
 * @brief Function that computes binary.
 *
 * @param[in] hash
 * @returns binary of a given hash
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY> {
  /** @TODO(student) Implement this function! */
  return {hash};
}

/**
 * @brief Function that computes leading zeros.
 *
 * @param[in] bset - binary values of a given bitset
 * @returns leading zeros of given binary set
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::PositionOfLeftmostOne(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t {
  /** @TODO(student) Implement this function! */
  size_t count = 0;

  for (int64_t i = BITSET_CAPACITY - 1 - n_bits_; i >= 0; i--) {
    count++;
    if (static_cast<int>(bset[i]) != 0) {
      break;
    }
  }

  return static_cast<uint64_t>(count);
}

/**
 * @brief Adds a value into the HyperLogLog.
 *
 * @param[in] val - value that's added into hyperloglog
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  std::unique_lock<std::shared_mutex> lock(smtx_);
  auto hash_bits = ComputeBinary(CalculateHash(val));
  size_t bucket = 0;
  auto offset = static_cast<size_t>(BITSET_CAPACITY - n_bits_);

  if (sizeof(bucket) * 8 > offset) {
    bucket = hash_bits.to_ulong() >> offset;
  }

  auto p = PositionOfLeftmostOne(hash_bits);

  buckets_[bucket] = std::max(buckets_[bucket], p);
}

/**
 * @brief Function that computes cardinality.
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  std::unique_lock<std::shared_mutex> lock(smtx_);
  double factor = 0;
  for (const auto &val : buckets_) {
    factor += std::pow(2, -static_cast<double>(val));
  }

  cardinality_ = static_cast<size_t>(CONSTANT * buckets_.size() * buckets_.size() / factor);
}

template class HyperLogLog<int64_t>;
template class HyperLogLog<std::string>;

}  // namespace bustub
