#pragma once
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <chrono>
#include <vector>
#include <atomic>
#include <thread>
#include <functional>
#include <algorithm>
#include <fstream>

namespace minitfs {

struct DataNodeInfo {
    std::string id;
    std::string ip;
    int32_t     port;
    int64_t     available_cap;
    int32_t     block_count;
    int32_t     active_connections{0};
    double      current_weight{0.0};  // 平滑加权轮询的动态权重
    double      load_score{0.0};      // 综合评分（静态权重）
    std::chrono::steady_clock::time_point last_heartbeat;
    bool        dead_notified{false}; // 是否已触发过宕机回调

    bool is_alive() const {
        auto now  = std::chrono::steady_clock::now();
        auto diff = std::chrono::duration_cast<std::chrono::seconds>(
                        now - last_heartbeat).count();
        return diff < 30;
    }
};

// 单个副本的位置信息
struct ReplicaLocation {
    std::string datanode_id;
    int64_t     offset{0};
    int64_t     size{0};
};

struct FileLocation {
    uint64_t                    block_id{0};
    std::vector<ReplicaLocation> replicas;  // 所有副本
};

class BlockManager {
public:
    explicit BlockManager(const std::string& wal_path = "");
    ~BlockManager();

    // 注册或更新 DataNode（同时重新计算 load_score）
    void register_datanode(const DataNodeInfo& info);

    // 平滑加权轮询：选出 n 个不同的存活节点
    std::vector<DataNodeInfo*> select_datanodes(int n);

    uint64_t alloc_file_id();
    uint64_t alloc_block_id();

    void commit_file(uint64_t file_id, const FileLocation& loc);
    bool get_file_location(uint64_t file_id, FileLocation& loc);
    bool remove_file(uint64_t file_id, FileLocation& loc);
    DataNodeInfo* get_datanode(const std::string& id);

    // 对账 BlockReport：移除 datanode_id 上已不存在的副本
    void reconcile_block_report(const std::string& datanode_id,
                                 const std::vector<uint64_t>& block_ids);

    // 副本管理（用于 rebalance）
    void add_replica(uint64_t file_id, const ReplicaLocation& replica);
    void remove_replica(uint64_t file_id, const std::string& datanode_id);

    // 获取所有文件元数据（用于 rebalance 计算）
    std::unordered_map<uint64_t, FileLocation> get_all_files();

    // 获取所有存活节点（用于 rebalance 计算）
    std::vector<DataNodeInfo> get_alive_datanodes();

    // 启动/停止心跳超时检测后台线程
    // on_dead: 节点宕机时的回调，参数为宕机节点 id
    void start_dead_detector(std::function<void(const std::string&)> on_dead);
    void stop_dead_detector();

private:
    std::mutex mu_;
    std::unordered_map<std::string, DataNodeInfo> datanodes_;
    std::unordered_map<uint64_t, FileLocation>    file_map_;
    std::atomic<uint64_t> next_file_id_{1};
    std::atomic<uint64_t> next_block_id_{1};

    std::thread           detector_thread_;
    std::atomic<bool>     detector_running_{false};

    std::string   wal_path_;
    std::ofstream wal_file_;

    void replay_wal();
    void wal_append_commit(uint64_t file_id, const FileLocation& loc);
    void wal_append_remove(uint64_t file_id);
};

} // namespace minitfs
