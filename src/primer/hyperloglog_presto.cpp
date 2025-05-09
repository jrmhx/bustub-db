//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hyperloglog_presto.cpp
//
// Identification: src/primer/hyperloglog_presto.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/hyperloglog_presto.h"
#include <bitset>
#include <cstdint>
#include <mutex>
#include <shared_mutex>

namespace bustub {

/** @brief Parameterized constructor. */
template <typename KeyType>
HyperLogLogPresto<KeyType>::HyperLogLogPresto(int16_t n_leading_bits) : cardinality_(0) {
  if (n_leading_bits < 0) {
    n_leading_bits = 0;
  }

  leading_bits_ = static_cast<uint16_t>(n_leading_bits);
  dense_bucket_ = std::vector<std::bitset<DENSE_BUCKET_SIZE>>(static_cast<int>(std::pow(2, leading_bits_)),
                                                              std::bitset<DENSE_BUCKET_SIZE>(0));
}

/** @brief Element is added for HLL calculation. */
template <typename KeyType>
auto HyperLogLogPresto<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  auto hash_bits = std::bitset<BITS_SIZE>(CalculateHash(val));
  uint16_t bucket_idx = 0;
  std::unique_lock<std::shared_mutex> lock(smtx_);
  if (leading_bits_ > 0) {
    uint64_t flag = 1;
    flag <<= leading_bits_;
    flag -= 1;
    flag <<= (BITS_SIZE - leading_bits_);
    auto mask = std::bitset<BITS_SIZE>(flag);
    bucket_idx = static_cast<uint64_t>(((mask & hash_bits) >> (BITS_SIZE - leading_bits_)).to_ullong());
  }

  uint16_t count = 0;

  for (int i = 0; i < BITS_SIZE - leading_bits_; i++) {
    if (static_cast<int>(hash_bits[i]) == 0) {
      count++;
    } else {
      break;
    }
  }

  if (count > GetBucketValue(bucket_idx)) {
    dense_bucket_[bucket_idx] = std::bitset<DENSE_BUCKET_SIZE>(count);
    overflow_bucket_[bucket_idx] = std::bitset<OVERFLOW_BUCKET_SIZE>(count >> DENSE_BUCKET_SIZE);
  }
}

/** @brief Function to compute cardinality. */
template <typename T>
auto HyperLogLogPresto<T>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  std::unique_lock<std::shared_mutex> lock(smtx_);
  double factor = 0;
  for (uint64_t i = 0; i < dense_bucket_.size(); i++) {
    factor += std::pow(2, -static_cast<double>(GetBucketValue(i)));
  }

  cardinality_ = static_cast<uint64_t>(CONSTANT * dense_bucket_.size() * dense_bucket_.size() / factor);
}

template <typename T>
inline auto HyperLogLogPresto<T>::GetBucketValue(uint16_t idx) -> uint64_t {
  std::bitset<OVERFLOW_BUCKET_SIZE> of_bits = overflow_bucket_[idx];
  auto of_value = of_bits.to_ullong();
  auto bk_bits = std::bitset<TOTAL_BUCKET_SIZE>(of_value);
  bk_bits <<= DENSE_BUCKET_SIZE;
  auto bk_value = bk_bits.to_ullong();
  return static_cast<uint64_t>(bk_value | dense_bucket_[idx].to_ullong());
}

template class HyperLogLogPresto<int64_t>;
template class HyperLogLogPresto<std::string>;
}  // namespace bustub
