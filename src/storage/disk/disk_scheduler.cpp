//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include <optional>
#include "common/exception.h"
#include "common/macros.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    // join will wait for the thread to finish.
    background_thread_->join();
  }
}

/**
 *
 * @brief Schedules a request for the DiskManager to execute.
 *
 * @param r The request to be scheduled.
 */
void DiskScheduler::Schedule(DiskRequest r) {
  auto req = std::make_optional(std::move(r));
  // auto future = r.callback_.get_future();
  if (req.has_value()) request_queue_.Put(std::move(req));
}

/**
 *
 * @brief Background worker thread function that processes scheduled requests.
 *
 * The background thread needs to process requests while the DiskScheduler exists, i.e., this function should not
 * return until ~DiskScheduler() is called. At that point you need to make sure that the function does return.
 */
void DiskScheduler::StartWorkerThread() {
  while (true) {
    auto request = request_queue_.Get(); // will wait til fullfilled
    if (request.has_value()) {
      if (disk_manager_ != nullptr) {
        if (request->is_write_) {
          disk_manager_->WritePage(request->page_id_, request->data_);
        } else {
          disk_manager_->ReadPage(request->page_id_, request->data_);
        }
        request->callback_.set_value(true);
      } else {
        throw bustub::Exception("Disk Manager Resource is not found!!");
      }
    } else {
      return;
    }
  }
}

}  // namespace bustub
