#pragma once
#include <string>
#include <unordered_map>
#include <mutex>
#include <chrono>
#include <vector>
#include <atomic>

namespace minitfs {

struct DataNodeInfo {
    std::string id;
    std::string ip;
    int32_t     port;
    int64_t     available_cap;
    int32_t     block_count;
    std::chrono::steady_clock::time_point last_heartbeat;

    bool is_alive() const {
        auto now = std::chrono::steady_clock::now();
        auto diff = std::chrono::duration_cast<std::chrono::seconds>(now - last_heartbeat).count();
        return diff < 30; // 30秒超时
    }
};

struct FileLocation {
    uint64_t block_id;
    std::string datanode_id;
    int64_t  offset;
    int64_t  size;
};

class BlockManager {
public:
    BlockManager();

    // 注册或更新 DataNode
    void register_datanode(const DataNodeInfo& info);

    // 选择最优 DataNode（可用容量最大）
    DataNodeInfo* select_datanode();

    // 分配新的 file_id 和 block_id
    uint64_t alloc_file_id();
    uint64_t alloc_block_id();

    // 提交文件位置映射
    void commit_file(uint64_t file_id, const FileLocation& loc);

    // 查询文件位置
    bool get_file_location(uint64_t file_id, FileLocation& loc);

    // 删除文件映射
    bool remove_file(uint64_t file_id, FileLocation& loc);

    // 获取 DataNode 信息
    DataNodeInfo* get_datanode(const std::string& id);

private:
    std::mutex                                    mu_;
    std::unordered_map<std::string, DataNodeInfo> datanodes_;
    std::unordered_map<uint64_t, FileLocation>    file_map_;
    std::atomic<uint64_t>                         next_file_id_{1};
    std::atomic<uint64_t>                         next_block_id_{1};
};

} // namespace minitfs
