#include "block_manager.h"

namespace minitfs {

BlockManager::BlockManager() {}

void BlockManager::register_datanode(const DataNodeInfo& info) {
    std::lock_guard<std::mutex> lk(mu_);
    datanodes_[info.id] = info;
}

DataNodeInfo* BlockManager::select_datanode() {
    std::lock_guard<std::mutex> lk(mu_);
    DataNodeInfo* best = nullptr;
    for (auto& [id, node] : datanodes_) {
        if (node.is_alive()) {
            if (!best || node.available_cap > best->available_cap) {
                best = &node;
            }
        }
    }
    return best;
}

uint64_t BlockManager::alloc_file_id() {
    return next_file_id_.fetch_add(1);
}

uint64_t BlockManager::alloc_block_id() {
    return next_block_id_.fetch_add(1);
}

void BlockManager::commit_file(uint64_t file_id, const FileLocation& loc) {
    std::lock_guard<std::mutex> lk(mu_);
    file_map_[file_id] = loc;
}

bool BlockManager::get_file_location(uint64_t file_id, FileLocation& loc) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = file_map_.find(file_id);
    if (it == file_map_.end()) return false;
    loc = it->second;
    return true;
}

bool BlockManager::remove_file(uint64_t file_id, FileLocation& loc) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = file_map_.find(file_id);
    if (it == file_map_.end()) return false;
    loc = it->second;
    file_map_.erase(it);
    return true;
}

DataNodeInfo* BlockManager::get_datanode(const std::string& id) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = datanodes_.find(id);
    if (it == datanodes_.end()) return nullptr;
    return &it->second;
}

} // namespace minitfs
