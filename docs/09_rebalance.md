# 动态扩容与 Rebalance 详解

---

## 一、问题背景

mini-tfs 支持运行时动态加入新 DataNode。新节点加入后：

- 新上传的文件会通过平滑加权轮询自动分配到新节点 ✅
- 已有文件的副本**不会自动迁移**，导致新节点长期空载 ❌

Rebalance 模块解决第二个问题：检测到新节点后，自动将部分已有副本迁移过去，使各节点数据量趋于均衡。

---

## 二、整体架构

```
NameServer
├── BlockManager          ← 元数据存储，提供 add_replica / remove_replica
└── RebalanceManager      ← 调度层
    ├── compute_tasks()   ← 计算迁移任务
    ├── task_queue_       ← 任务队列
    └── worker_thread()   ← 后台执行线程

DataNode
└── CopyBlock RPC         ← 从源节点拉取数据并写入本地
```

---

## 三、触发方式

### 自动触发（推荐）

`NameServerServiceImpl::Heartbeat` 中检测新节点：

```cpp
bool is_new = (mgr_.get_datanode(req->datanode_id()) == nullptr);
mgr_.register_datanode(info);
if (is_new) rebalance_mgr_.notify_new_node(req->datanode_id());
```

新节点首次心跳（约 10 秒内）后立即触发。

### 手动触发

```bash
grpc_cli call localhost:50051 TriggerRebalance ""
```

适用于运维场景，例如手动扩容后想立即均衡，或调整节点权重后重新平衡。

---

## 四、任务计算策略

```
total_replicas = 所有文件的副本总数
target = total_replicas / alive_node_count

node_replica_count[dn_id] = 该节点在 file_map_ 中出现的副本次数

供体（donor）：node_replica_count > target
受体（receiver）：node_replica_count < target

迁移任务：从供体取出多余副本 → 分配给受体
```

**示例：**

```
3 个节点，10 个副本，target = 3

dn1: 5 个副本 → 供体，需迁出 2 个
dn2: 4 个副本 → 供体，需迁出 1 个
dn3: 1 个副本 → 受体，需迁入 2 个（新节点）

生成任务：
  file_id=3: dn1 → dn3
  file_id=7: dn1 → dn3
  file_id=9: dn2 → dn3
```

---

## 五、迁移执行流程

```
RebalanceManager                dst_dn              src_dn
       │                           │                   │
       │─ CopyBlock ───────────────►│                   │
       │  (block_id, file_id,       │                   │
       │   src_ip, src_port,        │─ ReadBlock ──────►│
       │   offset, size)            │◄─ stream data ────│
       │                           │  边收边写本地       │
       │◄─ CopyBlockResp ──────────│                   │
       │  (new_offset, crc32)       │                   │
       │                           │                   │
       │ add_replica(dst_dn)        │                   │
       │ → file_map_ + WAL          │                   │
       │                           │                   │
       │─ DeleteBlock ─────────────────────────────────►│
       │                           │                   │
       │ remove_replica(src_dn)     │                   │
       │ → file_map_ + WAL          │                   │
```

---

## 六、一致性保证

| 阶段 | src_dn | dst_dn | 读请求 |
|------|--------|--------|--------|
| CopyBlock 进行中 | 有副本 | 无副本 | 正常，读 src |
| add_replica 后 | 有副本 | 有副本 | 正常，可读任意一个 |
| remove_replica 后 | 无副本 | 有副本 | 正常，读 dst |

任何阶段失败都不会丢数据，最坏情况是留下一个多余副本（下次触发 rebalance 时会被清理）。

---

## 七、WAL 持久化

rebalance 操作同样写入 WAL，NameServer 重启后可正确回放：

```
ADD_REPLICA  <file_id> <datanode_id> <offset> <size>
DEL_REPLICA  <file_id> <datanode_id>
```

`replay_wal()` 中对应处理：

```cpp
} else if (op == "ADD_REPLICA") {
    // 向 file_map_[file_id].replicas 追加副本
} else if (op == "DEL_REPLICA") {
    // 从 file_map_[file_id].replicas 移除指定节点的副本
}
```

---

## 八、当前限制

- 迁移任务在内存中排队，NameServer 重启后未完成的任务会丢失（需重新触发）
- 每次只迁移到均值，不做进一步的精细化均衡
- 没有迁移速率限制，大量副本迁移时可能对集群 I/O 造成压力
