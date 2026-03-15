#pragma once
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <queue>
#include "block_manager.h"

namespace minitfs {

struct MigrateTask {
    uint64_t file_id;
    uint64_t block_id;
    std::string src_dn_id;
    std::string dst_dn_id;
    int64_t offset;
    int64_t size;
};

class RebalanceManager {
public:
    RebalanceManager(BlockManager* bm) : block_mgr_(bm) {}
    ~RebalanceManager();

    void start();
    void stop();

    // 新节点加入时通知
    void notify_new_node(const std::string& dn_id);

    // 手动触发 rebalance，返回生成的任务数
    int trigger();

private:
    void worker_thread();
    std::vector<MigrateTask> compute_tasks();
    bool execute_task(const MigrateTask& task);

    BlockManager* block_mgr_;

    std::atomic<bool> running_{false};
    std::thread worker_;

    std::mutex mu_;
    std::condition_variable cv_;
    std::queue<MigrateTask> task_queue_;
};

} // namespace minitfs
