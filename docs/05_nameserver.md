# NameServer 模块详解

NameServer 是整个系统的"大脑"，负责元数据管理和节点调度。它不存储任何文件数据，只维护"文件在哪里"的映射关系。

---

## 一、模块结构

```
nameserver/
├── block_manager.h/cpp     # 核心数据结构和算法（含 WAL 持久化）
├── rebalance_manager.h/cpp # 动态扩容 rebalance 调度
├── name_server.h/cpp       # gRPC 服务实现（薄层，委托给 BlockManager）
└── main.cpp                # 启动入口（支持 WAL 路径参数）
```

---

## 二、BlockManager — 核心数据管理

**文件：** `nameserver/block_manager.h` / `block_manager.cpp`

### 数据结构

```cpp
class BlockManager {
    std::mutex mu_;  // 保护以下所有数据结构

    // DataNode 注册表：id → DataNodeInfo
    std::unordered_map<std::string, DataNodeInfo> datanodes_;

    // 文件映射表：file_id → FileLocation
    std::unordered_map<uint64_t, FileLocation> file_map_;

    // 全局 ID 分配器（原子操作，线程安全）
    std::atomic<uint64_t> next_file_id_{1};
    std::atomic<uint64_t> next_block_id_{1};

    // 宕机检测后台线程
    std::thread       detector_thread_;
    std::atomic<bool> detector_running_{false};

    // WAL 持久化
    std::string   wal_path_;
    std::ofstream wal_file_;
};
```

**公开方法一览：**

| 方法 | 说明 |
|------|------|
| `register_datanode` | 注册/更新 DataNode，重新计算 `load_score` |
| `select_datanodes(n)` | 平滑加权轮询，选出 n 个存活节点 |
| `alloc_file_id / alloc_block_id` | 原子自增，分配全局唯一 ID |
| `commit_file` | 写入 `file_map_` + WAL |
| `get_file_location` | 查询文件副本位置 |
| `remove_file` | 删除映射 + WAL |
| `get_datanode` | 按 ID 查询节点信息 |
| `reconcile_block_report` | BlockReport 对账，移除失效副本 |
| `add_replica` | 向指定文件添加一个副本（rebalance 迁移成功后调用） |
| `remove_replica` | 移除指定文件在某节点上的副本（rebalance 迁移完成后调用） |
| `get_all_files` | 返回 file_map_ 快照（供 rebalance 计算用） |
| `get_alive_datanodes` | 返回所有存活节点列表（供 rebalance 计算用） |
| `start_dead_detector` | 启动宕机检测后台线程 |

### DataNodeInfo — 节点信息

```cpp
struct DataNodeInfo {
    std::string id;
    std::string ip;
    int32_t     port;
    int64_t     available_cap;       // 可用容量（字节，由 statvfs 获取）
    int32_t     block_count;
    int32_t     active_connections;
    double      current_weight{0.0}; // 平滑加权轮询的动态权重（每次选节点后更新）
    double      load_score{0.0};     // 综合评分（静态权重，心跳时计算）
    std::chrono::steady_clock::time_point last_heartbeat;
    bool        dead_notified{false};

    bool is_alive() const {
        // 距上次心跳超过 30 秒则认为宕机
        return duration_cast<seconds>(now - last_heartbeat).count() < 30;
    }
};
```

### FileLocation — 文件位置

```cpp
struct ReplicaLocation {
    std::string datanode_id;
    int64_t     offset{0};
    int64_t     size{0};
};

struct FileLocation {
    uint64_t                     block_id{0};
    std::vector<ReplicaLocation> replicas;  // 所有副本
};
```

---

## 三、负载均衡：平滑加权轮询

**算法来源：** Nginx 的 smooth weighted round-robin

**核心思想：** 每次选节点时，所有节点的 `current_weight` 都加上自己的 `load_score`，然后选 `current_weight` 最大的节点，被选中的节点 `current_weight` 减去所有节点 `load_score` 之和。

**load_score 计算（心跳时更新）：**

```cpp
double cap_score  = available_cap / (1024.0 * 1024 * 1024);  // 容量（GB）
double conn_score = 1.0 / (1.0 + active_connections);         // 连接数越少越好
load_score = cap_score * 0.6 + conn_score * 0.4;
// 容量权重 60%，连接数权重 40%
```

**select_datanodes(n) 的执行过程：**

```
假设有 3 个节点，load_score 分别为 [5, 1, 3]，需要选 2 个：

初始 current_weight = [0, 0, 0]

第 1 轮：
  current_weight += load_score → [5, 1, 3]
  选最大的 dn1（current_weight=5）
  dn1.current_weight -= 总权重(9) → [-4, 1, 3]

第 2 轮：
  current_weight += load_score → [1, 2, 6]
  选最大的 dn3（current_weight=6，且不在已选列表中）
  dn3.current_weight -= 9 → [1, 2, -3]

结果：选出 [dn1, dn3]
```

---

## 四、WAL 持久化

NameServer 通过 **Write-Ahead Log（WAL）** 将元数据持久化到磁盘，进程重启后自动回放恢复。

### WAL 格式

```
COMMIT <file_id> <block_id> <replica_count> <dn_id> <offset> <size> [...]
REMOVE <file_id>
```

示例：
```
COMMIT 1 1 2 dn1 0 1024 dn2 0 1024
COMMIT 2 2 1 dn1 1024 512
REMOVE 1
```

### 写入时机

```cpp
void BlockManager::commit_file(uint64_t file_id, const FileLocation& loc) {
    std::lock_guard<std::mutex> lk(mu_);
    file_map_[file_id] = loc;
    wal_append_commit(file_id, loc);  // 先写内存，再追加 WAL
}

bool BlockManager::remove_file(uint64_t file_id, FileLocation& loc) {
    std::lock_guard<std::mutex> lk(mu_);
    file_map_.erase(file_id);
    wal_append_remove(file_id);       // 先改内存，再追加 WAL
    return true;
}
```

每次写入后立即 `flush()`，确保数据落盘。

### 启动时回放

```cpp
BlockManager::BlockManager(const std::string& wal_path) : wal_path_(wal_path) {
    if (!wal_path_.empty()) {
        replay_wal();                          // 先回放已有 WAL
        wal_file_.open(wal_path_, ios::app);   // 再以追加模式打开
    }
}

void BlockManager::replay_wal() {
    ifstream in(wal_path_);
    uint64_t max_file_id = 0, max_block_id = 0;
    while (getline(in, line)) {
        if (op == "COMMIT") {
            // 解析并恢复 file_map_[file_id]
            // 更新 max_file_id, max_block_id
        } else if (op == "REMOVE") {
            file_map_.erase(file_id);
        }
    }
    // 恢复 ID 计数器，避免重启后分配重复 ID
    next_file_id_.store(max_file_id);
    next_block_id_.store(max_block_id);
    cout << "[NameServer] WAL replayed: " << file_map_.size() << " files restored\n";
}
```

**WAL 的局限性：** 当前实现是追加写，不做压缩（compaction）。长期运行后 WAL 文件会持续增大，生产系统需要定期做 checkpoint（将当前 `file_map_` 快照写入新文件，清空旧 WAL）。

---

## 五、宕机检测

```cpp
void BlockManager::start_dead_detector(
        std::function<void(const std::string&)> on_dead) {
    detector_thread_ = std::thread([this, on_dead]() {
        while (detector_running_) {
            sleep(10 秒);
            // 在锁内收集宕机节点 ID
            vector<string> dead_ids;
            {
                lock_guard lk(mu_);
                for (auto& [id, node] : datanodes_) {
                    if (!node.is_alive() && !node.dead_notified) {
                        node.dead_notified = true;  // 避免重复触发
                        dead_ids.push_back(id);
                    }
                }
            }
            // 在锁外触发回调（避免回调内加锁导致死锁）
            for (auto& id : dead_ids) on_dead(id);
        }
    });
}
```

**dead_notified 标志的作用：** 防止同一个宕机节点被反复触发回调。当节点重新上线（心跳恢复）时，`register_datanode` 会将 `dead_notified` 重置为 `false`。

---

## 六、reconcile_block_report — BlockReport 对账

DataNode 重启后，其数据目录可能已被清空，但 NameServer 的 `file_map_` 中仍保留着指向该节点的副本记录。如果不清理，下载时会尝试从一个实际上没有数据的节点读取，导致静默失败。

`reconcile_block_report` 解决这个问题：

```cpp
void BlockManager::reconcile_block_report(const std::string& datanode_id,
                                           const std::vector<uint64_t>& block_ids) {
    // 将上报的 block_ids 转为 unordered_set，O(1) 查找
    unordered_set<uint64_t> reported(block_ids.begin(), block_ids.end());

    lock_guard lk(mu_);
    for (auto& [file_id, loc] : file_map_) {
        auto& replicas = loc.replicas;
        // erase-remove 惯用法：移除属于该 DataNode 且不在上报列表中的副本
        auto it = replicas.begin();
        while (it != replicas.end()) {
            if (it->datanode_id == datanode_id &&
                reported.find(loc.block_id) == reported.end()) {
                cerr << "[NameServer] BlockReport: removed stale replica"
                     << " file=" << file_id
                     << " block=" << loc.block_id
                     << " datanode=" << datanode_id << "\n";
                it = replicas.erase(it);
            } else {
                ++it;
            }
        }
    }
}
```

**对账逻辑：**

```
DataNode 上报：block_ids = [1, 3, 5]   ← 实际持有的 block

NameServer file_map_ 中：
  file_id=10 → block_id=1, replicas=[dn1, dn2]   ← dn1 上报了 block_id=1，保留
  file_id=11 → block_id=2, replicas=[dn1]         ← dn1 没有上报 block_id=2，移除 dn1 副本
  file_id=12 → block_id=3, replicas=[dn1, dn2]   ← dn1 上报了 block_id=3，保留

结果：file_id=11 的 dn1 副本被移除，后续下载不会再尝试从 dn1 读取
```

---

## 七、NameServerServiceImpl — gRPC 服务实现

**文件：** `nameserver/name_server.h` / `name_server.cpp`

这是一个薄层，每个 RPC 方法都直接委托给 `BlockManager`。

### AllocateBlock

```cpp
grpc::Status AllocateBlock(...) {
    int replica_num = req->replica_num() > 0 ? req->replica_num() : 1;
    auto nodes = mgr_.select_datanodes(replica_num);  // 平滑加权轮询
    if (nodes.empty()) { resp->set_status(-1); return OK; }

    uint64_t file_id  = mgr_.alloc_file_id();
    uint64_t block_id = mgr_.alloc_block_id();

    for (auto* dn : nodes) {
        // 填充 BlockLocation（此时 offset/size 为 0，写完才知道）
        loc->set_datanode_ip(dn->ip);
        loc->set_datanode_port(dn->port);
        ...
    }
}
```

### GetBlockLocation

```cpp
grpc::Status GetBlockLocation(...) {
    FileLocation loc;
    mgr_.get_file_location(file_id, loc);

    for (const auto& replica : loc.replicas) {
        DataNodeInfo* dn = mgr_.get_datanode(replica.datanode_id);
        if (!dn || !dn->is_alive()) continue;  // 过滤宕机节点
        // 填充 BlockLocation（含 offset/size）
    }
}
```

### CommitFile

```cpp
grpc::Status CommitFile(...) {
    FileLocation loc;
    loc.block_id = req->block_id();
    for (const auto& l : req->locations()) {
        ReplicaLocation r;
        r.datanode_id = l.datanode_id();
        r.offset      = l.offset();
        r.size        = l.size();
        loc.replicas.push_back(r);
    }
    mgr_.commit_file(file_id, loc);  // 写入 file_map_ + WAL
}
```

### DeleteFile

```cpp
grpc::Status DeleteFile(...) {
    FileLocation loc;
    mgr_.remove_file(file_id, loc);  // 从 file_map_ 删除 + WAL 追加 REMOVE
}
```

### BlockReport

```cpp
grpc::Status BlockReport(...) {
    vector<uint64_t> ids(req->block_ids().begin(), req->block_ids().end());
    mgr_.reconcile_block_report(req->datanode_id(), ids);
}
```

DataNode 重启后上报自己实际持有的 block 列表，NameServer 调用 `reconcile_block_report` 对账，移除已不存在的副本。

### Heartbeat

```cpp
grpc::Status Heartbeat(...) {
    bool is_new = (mgr_.get_datanode(req->datanode_id()) == nullptr);
    mgr_.register_datanode(info);
    if (is_new) rebalance_mgr_.notify_new_node(req->datanode_id());
}
```

首次心跳时检测到新节点，自动触发 rebalance。

### TriggerRebalance

```cpp
grpc::Status TriggerRebalance(...) {
    int count = rebalance_mgr_.trigger();
    resp->set_task_count(count);
}
```

运维手动触发，返回生成的迁移任务数。

---

## 八、main.cpp — 启动流程

```cpp
int main(int argc, char** argv) {
    std::string addr     = "0.0.0.0:50051";
    std::string wal_path = "";  // 可选，通过命令行参数指定

    // 支持: ./nameserver [listen_addr] [wal_path]
    if (argc >= 2) addr     = argv[1];
    if (argc >= 3) wal_path = argv[2];

    minitfs::BlockManager mgr(wal_path);  // 传入 WAL 路径

    mgr.start_dead_detector([](const std::string& dead_id) {
        std::cerr << "[NameServer] ALERT: DataNode [" << dead_id << "] heartbeat timeout\n";
    });

    minitfs::NameServerServiceImpl service(mgr);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    auto server = builder.BuildAndStart();
    server->Wait();
}
```

**启用 WAL 持久化：**
```bash
# 不持久化（重启后数据丢失）
./build/nameserver

# 启用 WAL（重启后自动恢复）
./build/nameserver 0.0.0.0:50051 ./nameserver.wal
```

---

## 九、RebalanceManager — 动态扩容调度

**文件：** `nameserver/rebalance_manager.h` / `rebalance_manager.cpp`

### 数据结构

```cpp
struct MigrateTask {
    uint64_t    file_id;
    uint64_t    block_id;
    std::string src_dn_id;   // 供体节点
    std::string dst_dn_id;   // 受体节点
    int64_t     offset;
    int64_t     size;
};

class RebalanceManager {
    BlockManager* block_mgr_;
    std::queue<MigrateTask> task_queue_;
    std::thread worker_;     // 后台 worker 线程
};
```

### 触发方式

1. **自动触发**：`NameServerServiceImpl::Heartbeat` 检测到新节点 ID 时调用 `notify_new_node()`
2. **手动触发**：`TriggerRebalance` RPC → `trigger()`

### 任务计算逻辑（compute_tasks）

```
1. 获取所有存活节点
2. 统计每个节点在 file_map_ 中的副本数
3. 计算目标副本数 = total_replicas / alive_node_count
4. 副本数 > target 的节点 → 供体（donor）
   副本数 < target 的节点 → 受体（receiver）
5. 从供体取出多余副本，生成迁移任务到受体
```

### 执行流程（execute_task）

```
1. 调用 dst_dn.CopyBlock(src_ip, src_port, block_id, file_id, offset, size)
2. dst_dn 从 src_dn 拉取数据，返回 new_offset/crc32
3. block_mgr_.add_replica(file_id, {dst_dn, new_offset, size})  → WAL
4. 调用 src_dn.DeleteBlock(block_id, file_id)
5. block_mgr_.remove_replica(file_id, src_dn_id)                → WAL
```

### WAL 扩展

rebalance 操作同样持久化到 WAL，格式：

```
ADD_REPLICA  <file_id> <datanode_id> <offset> <size>
DEL_REPLICA  <file_id> <datanode_id>
```

NameServer 重启后 `replay_wal()` 会正确回放这两条记录，保证副本状态一致。
