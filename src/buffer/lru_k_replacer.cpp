//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <climits>
#include <deque>
#include <mutex>
#include <optional>
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"


namespace bustub {
LRUKNode::LRUKNode(size_t k, frame_id_t fid, size_t access_timestamp): k_(k), fid_(fid) {
  BUSTUB_ASSERT(k > 0, "k should be a positive value");
  history_ = std::deque<size_t>(k_, 0);
  this->history_[k-1] = access_timestamp;
}

void LRUKNode::Access(const size_t current_timestamp){
  this->history_.push_back(current_timestamp);
  this->history_.pop_front();
}

auto LRUKNode::GetKRecentAccessTime(const size_t current_timestamp) const -> size_t {
  BUSTUB_ASSERT(!this->history_.empty(), "history_ should never be empty");
  return this->history_.front();
}
auto LRUKNode::GetLastAccessTime() const -> size_t{
  BUSTUB_ASSERT(!this->history_.empty(), "history_ should never be empty");
  return this->history_.back();
}
auto LRUKNode::IsEvictable() const -> bool{
  return this->is_evictable_;
}
void LRUKNode::SetIsEvictable(bool is_evictable){
  this->is_evictable_ = is_evictable;
}

auto LRUKNode::GetFrameId() const -> frame_id_t {
  return this->fid_;
}

// smaller lexicographical order of history_ means higher piority when selecting eviction victim
auto LRUKNode::operator<(const LRUKNode& other) const -> bool {
  return this->history_ < other.history_;
}

auto LRUKNode::operator==(const LRUKNode& other) const -> bool {
  return this->fid_ == other.fid_;
}

/**
 * @brief a new LRUKReplacer.
 * @param num_frames the maximum number of frames the LRUReplacer will be required to store
 */
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

/**
 *
 * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
 * that are marked as 'evictable' are candidates for eviction.
 *
 * A frame with less than k historical references is given +inf as its backward k-distance.
 * If multiple frames have inf backward k-distance, then evict frame whose oldest timestamp
 * is furthest in the past.
 *
 * Successful eviction of a frame should decrement the size of replacer and remove the frame's
 * access history.
 *
 * @return true if a frame is evicted successfully, false if no frames can be evicted.
 */
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> { 
  std::lock_guard guard(latch_);
  std::optional<frame_id_t> vict_fid;

  for (const auto& [fid, node] : node_store_){
    if (node.IsEvictable()) {
      if (vict_fid.has_value()) {
        if (node < node_store_.at(vict_fid.value())) {
          vict_fid = node.GetFrameId();
        }
      } else {
        vict_fid = node.GetFrameId();
      }
    }
  }

  if (vict_fid.has_value()) {
    const auto it = node_store_.find(vict_fid.value());
    if (it != node_store_.end()) {
      node_store_.erase(it);
      curr_size_ --;
    }
    return vict_fid;
  } else {
    return std::nullopt;
  }
}

/**
 *
 * @brief Record the event that the given frame id is accessed at current timestamp.
 * Create a new entry for access history if frame id has not been seen before.
 *
 * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
 * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
 *
 * @param frame_id id of frame that received a new access.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard guard(latch_);
  bool isFrameIdIvalid = static_cast<size_t>(frame_id) >= replacer_size_;
  BUSTUB_ASSERT(!isFrameIdIvalid, "frame_id is invalid!");

  current_timestamp_ ++;

  const auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    node_store_.emplace(frame_id, LRUKNode(k_, frame_id, current_timestamp_));
  } else {
    auto& node = it->second;
    node.Access(current_timestamp_);
  }
}

/**
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard guard(this->latch_);
  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    throw bustub::Exception("invalid frame_id");
  }

  auto it = node_store_.find(frame_id);
  if (it != node_store_.end()){
    bool prev = it->second.IsEvictable();
    it->second.SetIsEvictable(set_evictable);

    if (prev) {
      if (!set_evictable) curr_size_ --;
    } else {
      if (set_evictable) curr_size_ ++;
    }
  }
}

/**
 *
 * @brief Remove an evictable frame from replacer, along with its access history.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * with largest backward k-distance. This function removes specified frame id,
 * no matter what its backward k-distance is.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard guard(latch_);
  auto it = node_store_.find(frame_id);

  if (it != node_store_.end()) {
    if (it->second.IsEvictable()) {
      node_store_.erase(it);
      curr_size_ --;
    } else {
      throw bustub::Exception("cannot remove a non-evictable frame!");
    }
  }
}

/**
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t { 
  std::lock_guard guard(latch_);
  return curr_size_;
}

}  // namespace bustub
