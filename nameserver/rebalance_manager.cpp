#include "rebalance_manager.h"
#include "datanode.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <algorithm>
#include <unordered_map>

namespace minitfs {

RebalanceManager::~RebalanceManager() {
    stop();
}

void RebalanceManager::start() {
    if (running_.exchange(true)) return;
    worker_ = std::thread(&RebalanceManager::worker_thread, this);
}

void RebalanceManager::stop() {
    if (!running_.exchange(false)) return;
    cv_.notify_all();
    if (worker_.joinable()) worker_.join();
}

void RebalanceManager::notify_new_node(const std::string& dn_id) {
    std::cout << "[RebalanceManager] New node detected: " << dn_id << ", triggering rebalance\n";
    trigger();
}

int RebalanceManager::trigger() {
    auto tasks = compute_tasks();
    if (tasks.empty()) {
        std::cout << "[RebalanceManager] No rebalance needed\n";
        return 0;
    }

    std::lock_guard<std::mutex> lock(mu_);
    for (auto& t : tasks) {
        task_queue_.push(std::move(t));
    }
    cv_.notify_all();

    std::cout << "[RebalanceManager] Generated " << tasks.size() << " migrate tasks\n";
    return static_cast<int>(tasks.size());
}

void RebalanceManager::worker_thread() {
    while (running_) {
        MigrateTask task;
        {
            std::unique_lock<std::mutex> lock(mu_);
            cv_.wait(lock, [this] { return !task_queue_.empty() || !running_; });
            if (!running_) break;
            if (task_queue_.empty()) continue;

            task = task_queue_.front();
            task_queue_.pop();
        }

        std::cout << "[RebalanceManager] Executing task: file=" << task.file_id
                  << " from " << task.src_dn_id << " to " << task.dst_dn_id << "\n";

        if (execute_task(task)) {
            std::cout << "[RebalanceManager] Task succeeded\n";
        } else {
            std::cerr << "[RebalanceManager] Task failed\n";
        }
    }
}

std::vector<MigrateTask> RebalanceManager::compute_tasks() {
    std::vector<MigrateTask> tasks;

    // 1. 获取所有存活节点
    auto all_nodes = block_mgr_->get_alive_datanodes();
    if (all_nodes.size() <= 1) return tasks;

    // 2. 统计每个节点的副本数
    std::unordered_map<std::string, int> node_replica_count;
    std::unordered_map<std::string, std::vector<std::pair<uint64_t, ReplicaLocation>>> node_replicas;

    for (const auto& dn : all_nodes) {
        node_replica_count[dn.id] = 0;
    }

    // 遍历所有文件，统计副本分布
    auto file_map = block_mgr_->get_all_files();
    int total_replicas = 0;
    for (const auto& [file_id, loc] : file_map) {
        for (const auto& replica : loc.replicas) {
            if (node_replica_count.find(replica.datanode_id) != node_replica_count.end()) {
                node_replica_count[replica.datanode_id]++;
                node_replicas[replica.datanode_id].push_back({file_id, replica});
                total_replicas++;
            }
        }
    }

    if (total_replicas == 0) return tasks;

    // 3. 计算目标副本数
    int target_per_node = total_replicas / all_nodes.size();
    if (target_per_node == 0) target_per_node = 1;

    // 4. 找出供体（超标）和受体（不足）
    std::vector<std::string> donors, receivers;
    for (const auto& [dn_id, count] : node_replica_count) {
        if (count > target_per_node) {
            donors.push_back(dn_id);
        } else if (count < target_per_node) {
            receivers.push_back(dn_id);
        }
    }

    if (donors.empty() || receivers.empty()) return tasks;

    // 5. 生成迁移任务
    size_t recv_idx = 0;
    for (const auto& donor_id : donors) {
        int excess = node_replica_count[donor_id] - target_per_node;
        auto& replicas = node_replicas[donor_id];

        for (int i = 0; i < excess && !replicas.empty() && recv_idx < receivers.size(); ++i) {
            auto [file_id, replica] = replicas.back();
            replicas.pop_back();

            MigrateTask task;
            task.file_id = file_id;
            task.block_id = file_map.at(file_id).block_id;
            task.src_dn_id = donor_id;
            task.dst_dn_id = receivers[recv_idx];
            task.offset = replica.offset;
            task.size = replica.size;

            tasks.push_back(task);

            // 更新计数，轮转受体
            node_replica_count[donor_id]--;
            node_replica_count[receivers[recv_idx]]++;
            if (node_replica_count[receivers[recv_idx]] >= target_per_node) {
                recv_idx++;
            }
        }
    }

    return tasks;
}

bool RebalanceManager::execute_task(const MigrateTask& task) {
    // 1. 获取源和目标节点信息
    auto src_dn = block_mgr_->get_datanode(task.src_dn_id);
    auto dst_dn = block_mgr_->get_datanode(task.dst_dn_id);

    if (!src_dn || !dst_dn) {
        std::cerr << "[RebalanceManager] Source or dest node not found\n";
        return false;
    }

    // 2. 调用目标节点的 CopyBlock
    std::string dst_addr = dst_dn->ip + ":" + std::to_string(dst_dn->port);
    auto channel = grpc::CreateChannel(dst_addr, grpc::InsecureChannelCredentials());
    auto stub = DataNodeService::NewStub(channel);

    CopyBlockRequest req;
    req.set_block_id(task.block_id);
    req.set_file_id(task.file_id);
    req.set_source_datanode_ip(src_dn->ip);
    req.set_source_datanode_port(src_dn->port);
    req.set_source_offset(task.offset);
    req.set_source_size(task.size);

    CopyBlockResponse resp;
    grpc::ClientContext ctx;
    auto status = stub->CopyBlock(&ctx, req, &resp);

    if (!status.ok() || resp.status() != 0) {
        std::cerr << "[RebalanceManager] CopyBlock failed: " << resp.message() << "\n";
        return false;
    }

    // 3. 更新 NameServer 元数据：添加新副本
    ReplicaLocation new_replica;
    new_replica.datanode_id = task.dst_dn_id;
    new_replica.offset = resp.offset();
    new_replica.size = resp.size();

    block_mgr_->add_replica(task.file_id, new_replica);

    // 4. 删除旧副本（可选：可以延迟删除，确保新副本稳定后再删）
    // 这里为了简化，立即删除
    block_mgr_->remove_replica(task.file_id, task.src_dn_id);

    // 5. 通知源节点删除 block（可选）
    std::string src_addr = src_dn->ip + ":" + std::to_string(src_dn->port);
    auto src_channel = grpc::CreateChannel(src_addr, grpc::InsecureChannelCredentials());
    auto src_stub = DataNodeService::NewStub(src_channel);

    DeleteBlockRequest del_req;
    del_req.set_block_id(task.block_id);
    del_req.set_file_id(task.file_id);

    DeleteBlockResponse del_resp;
    grpc::ClientContext del_ctx;
    src_stub->DeleteBlock(&del_ctx, del_req, &del_resp);

    return true;
}

} // namespace minitfs

