#include "block_manager.h"
#include <iostream>
#include <sstream>

namespace minitfs {

BlockManager::BlockManager(const std::string& wal_path) : wal_path_(wal_path) {
    if (!wal_path_.empty()) {
        replay_wal();
        wal_file_.open(wal_path_, std::ios::app);
    }
}

BlockManager::~BlockManager() {
    stop_dead_detector();
}

void BlockManager::register_datanode(const DataNodeInfo& raw) {
    std::lock_guard<std::mutex> lk(mu_);
    auto& node = datanodes_[raw.id];

    // 保留动态权重，其余全量更新
    double prev_cw = node.current_weight;
    node = raw;
    node.last_heartbeat  = std::chrono::steady_clock::now();
    node.current_weight  = prev_cw;
    node.dead_notified   = false; // 节点重新上线，重置通知标志

    // 综合评分：容量 60% + 连接数 40%
    // available_cap 单位 bytes，归一化到 GB
    double cap_score  = static_cast<double>(node.available_cap) / (1024.0 * 1024 * 1024);
    double conn_score = 1.0 / (1.0 + node.active_connections);
    node.load_score   = cap_score * 0.6 + conn_score * 0.4;
}

// 平滑加权轮询（Nginx smooth weighted round-robin）
// 每次调用：① 所有节点 current_weight += load_score
//           ② 选 current_weight 最大的节点
//           ③ 被选中节点 current_weight -= 所有节点 load_score 之和
std::vector<DataNodeInfo*> BlockManager::select_datanodes(int n) {
    std::lock_guard<std::mutex> lk(mu_);

    std::vector<DataNodeInfo*> alive;
    for (auto& [id, node] : datanodes_) {
        if (node.is_alive()) alive.push_back(&node);
    }

    int pick = std::min(n, static_cast<int>(alive.size()));
    std::vector<DataNodeInfo*> result;
    result.reserve(pick);

    // 计算所有存活节点的权重总和
    double total_weight = 0.0;
    for (auto* node : alive) total_weight += node->load_score;

    for (int i = 0; i < pick; ++i) {
        // 每个节点 current_weight += load_score
        for (auto* node : alive) node->current_weight += node->load_score;

        // 选 current_weight 最大的（且不在已选列表中）
        DataNodeInfo* best = nullptr;
        for (auto* node : alive) {
            bool already_picked = false;
            for (auto* r : result) {
                if (r->id == node->id) { already_picked = true; break; }
            }
            if (!already_picked && (!best || node->current_weight > best->current_weight))
                best = node;
        }
        if (!best) break;

        // 被选中节点 current_weight -= 总权重
        best->current_weight -= total_weight;
        result.push_back(best);
    }
    return result;
}

uint64_t BlockManager::alloc_file_id()  { return next_file_id_.fetch_add(1); }
uint64_t BlockManager::alloc_block_id() { return next_block_id_.fetch_add(1); }

void BlockManager::commit_file(uint64_t file_id, const FileLocation& loc) {
    std::lock_guard<std::mutex> lk(mu_);
    file_map_[file_id] = loc;
    wal_append_commit(file_id, loc);
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
    wal_append_remove(file_id);
    return true;
}

DataNodeInfo* BlockManager::get_datanode(const std::string& id) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = datanodes_.find(id);
    if (it == datanodes_.end()) return nullptr;
    return &it->second;
}

void BlockManager::start_dead_detector(
        std::function<void(const std::string&)> on_dead) {
    detector_running_ = true;
    detector_thread_ = std::thread([this, on_dead]() {
        while (detector_running_) {
            std::this_thread::sleep_for(std::chrono::seconds(10));

            std::vector<std::string> dead_ids;
            {
                std::lock_guard<std::mutex> lk(mu_);
                for (auto& [id, node] : datanodes_) {
                    if (!node.is_alive() && !node.dead_notified) {
                        node.dead_notified = true;
                        dead_ids.push_back(id);
                    }
                }
            }
            // 在锁外触发回调，避免回调内再加锁导致死锁
            for (const auto& id : dead_ids) {
                std::cerr << "[NameServer] DataNode " << id << " is dead!\n";
                on_dead(id);
            }
        }
    });
}

void BlockManager::stop_dead_detector() {
    detector_running_ = false;
    if (detector_thread_.joinable()) detector_thread_.join();
}

void BlockManager::wal_append_commit(uint64_t file_id, const FileLocation& loc) {
    if (!wal_file_.is_open()) return;
    wal_file_ << "COMMIT " << file_id << " " << loc.block_id
              << " " << loc.replicas.size();
    for (const auto& r : loc.replicas) {
        wal_file_ << " " << r.datanode_id << " " << r.offset << " " << r.size;
    }
    wal_file_ << "\n";
    wal_file_.flush();
}

void BlockManager::wal_append_remove(uint64_t file_id) {
    if (!wal_file_.is_open()) return;
    wal_file_ << "REMOVE " << file_id << "\n";
    wal_file_.flush();
}

void BlockManager::replay_wal() {
    std::ifstream in(wal_path_);
    if (!in.is_open()) return;

    std::string line;
    uint64_t max_file_id = 0, max_block_id = 0;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        std::istringstream ss(line);
        std::string op;
        ss >> op;
        if (op == "COMMIT") {
            uint64_t file_id, block_id;
            size_t replica_count;
            ss >> file_id >> block_id >> replica_count;
            FileLocation loc;
            loc.block_id = block_id;
            for (size_t i = 0; i < replica_count; ++i) {
                ReplicaLocation r;
                ss >> r.datanode_id >> r.offset >> r.size;
                loc.replicas.push_back(r);
            }
            file_map_[file_id] = loc;
            if (file_id >= max_file_id) max_file_id = file_id + 1;
            if (block_id >= max_block_id) max_block_id = block_id + 1;
        } else if (op == "REMOVE") {
            uint64_t file_id;
            ss >> file_id;
            file_map_.erase(file_id);
        }
    }
    if (max_file_id > next_file_id_.load())   next_file_id_.store(max_file_id);
    if (max_block_id > next_block_id_.load()) next_block_id_.store(max_block_id);
    std::cout << "[NameServer] WAL replayed: " << file_map_.size() << " files restored\n";
}

} // namespace minitfs
