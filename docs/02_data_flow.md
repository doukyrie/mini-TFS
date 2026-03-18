# 完整数据流

本文用时序图详细描述三种操作的完整数据流。

---

## 一、上传文件（upload）

```
Client          NameServer              DataNode-1        DataNode-2
  │                  │                      │                  │
  │─AllocateBlock──►│                      │                  │
  │  (file_size,     │                      │                  │
  │   replica_num=2) │                      │                  │
  │                  │ 分配 file_id/block_id │                  │
  │                  │ 平滑加权轮询选2个节点  │                  │
  │◄─AllocateBlock──│                      │                  │
  │  (file_id,       │                      │                  │
  │   block_id,      │                      │                  │
  │   locations[dn1, dn2])                  │                  │
  │                  │                      │                  │
  │─WriteBlock(stream)──────────────────►  │                  │  ← 并行
  │─WriteBlock(stream)────────────────────────────────────►   │  ← 并行
  │  (分块流式传输，每次最多 1MB)            │                  │
  │                  │                      │                  │
  │◄─WriteBlockResp─────────────────────── │                  │
  │  (offset, size)  │                      │                  │
  │◄─WriteBlockResp──────────────────────────────────────── │ │
  │                  │                      │                  │
  │─CommitFile──────►│                      │                  │
  │  (file_id,       │                      │                  │
  │   block_id,      │ 写入 file_map_:       │                  │
  │   locations 含   │ file_id → {block_id, │                  │
  │   offset/size)   │  replicas[dn1,dn2]}  │                  │
  │◄─CommitFileResp─│                      │                  │
  │                  │                      │                  │
  返回 file_id 给用户
```

**关键步骤说明：**

1. `AllocateBlock`：NameServer 分配全局唯一的 `file_id` 和 `block_id`，并用平滑加权轮询选出 N 个 DataNode
2. 并行写入：Client 用 `std::async` 同时向所有副本节点发起 gRPC 流式写入
3. 多数派成功：只要 `成功副本数 >= N/2+1` 就认为上传成功
4. `CommitFile`：将实际写成功的副本信息（含 offset/size）提交给 NameServer 持久化

---

## 二、下载文件（download）

```
Client          NameServer              DataNode（随机选一个）
  │                  │                      │
  │─GetBlockLocation►│                      │
  │  (file_id)       │                      │
  │                  │ 查 file_map_          │
  │                  │ 过滤存活副本          │
  │◄─GetBlockLocation│                      │
  │  (locations[dn1, dn2, ...])             │
  │                  │                      │
  │ 随机选一个副本    │                      │
  │─ReadBlock(stream)──────────────────────►│
  │  (block_id,      │                      │
  │   file_id,       │                      │ 查索引哈希表
  │   offset, size)  │                      │ 找到 offset/size
  │                  │                      │ 分块流式返回
  │◄─ReadBlockResp(stream)─────────────────│
  │  (data chunks)   │                      │
  │                  │                      │
  写入本地文件
```

**关键步骤说明：**

1. `GetBlockLocation`：NameServer 返回所有**存活**副本的位置（会过滤掉心跳超时的节点）
2. 随机选副本：Client 用随机数选一个副本，实现简单的读负载均衡
3. `ReadBlock`：DataNode 从索引中查到 `file_id` 对应的 `offset/size`，然后从主块文件分块流式读取返回

---

## 三、删除文件（delete）

```
Client          NameServer              DataNode-1        DataNode-2
  │                  │                      │                  │
  │─GetBlockLocation►│                      │                  │
  │  (file_id)       │                      │                  │
  │◄─GetBlockLocation│                      │                  │
  │  (locations)     │                      │                  │
  │                  │                      │                  │
  │─DeleteBlock──────────────────────────► │                  │
  │  (block_id, file_id)                   │ 从索引哈希表删除  │
  │─DeleteBlock────────────────────────────────────────────►  │
  │                  │                      │                  │
  │─DeleteFile──────►│                      │                  │
  │  (file_id)       │ 从 file_map_ 删除    │                  │
  │◄─DeleteFileResp─│                      │                  │
```

**关键步骤说明：**

1. 先查副本位置，再逐一通知每个 DataNode 删除索引记录（注意：主块文件数据不会立即回收，只是索引被删除，空间通过后续 compact 回收）
2. 最后通知 NameServer 删除 `file_map_` 中的映射

---

## 四、DataNode 心跳与 BlockReport 流程

```
DataNode                    NameServer
  │                              │
  │─Heartbeat(每10秒)───────────►│
  │  (id, ip, port,              │ register_datanode()
  │   available_cap,             │ 更新 last_heartbeat
  │   block_count,               │ 重新计算 load_score
  │   active_connections)        │
  │◄─HeartbeatResp──────────────│
  │                              │
  │  首次心跳成功后立即发送：      │
  │─BlockReport────────────────►│
  │  (datanode_id,               │ reconcile_block_report()
  │   block_ids[1,3,5,...])      │ 遍历 file_map_，移除该节点
  │◄─BlockReportResp────────────│ 上未上报的 block 对应的副本记录
```

**BlockReport 对账的意义：**

DataNode 重启或数据目录被清空后，NameServer 的 `file_map_` 中仍保留着指向该节点的副本记录。DataNode 启动时上报自己实际持有的 block 列表，NameServer 对比后移除已不存在的副本，避免后续下载时尝试从空节点读取而静默失败。

**宕机检测：**

NameServer 有一个后台线程（`dead_detector`），每 10 秒扫描一次所有 DataNode，若某节点的 `last_heartbeat` 超过 30 秒未更新，则触发宕机回调（当前实现为打印告警日志，预留了副本修复的扩展点）。

---

## 五、Rebalance 副本迁移流程

新节点加入后，NameServer 自动触发 rebalance，将部分已有副本迁移到新节点。

```
NameServer              dst_dn（新节点）          src_dn（供体节点）
    │                        │                        │
    │ 计算迁移任务             │                        │
    │ (file_id, block_id,    │                        │
    │  src→dst, offset, size)│                        │
    │                        │                        │
    │─CopyBlock─────────────►│                        │
    │  (block_id, file_id,   │                        │
    │   src_ip, src_port,    │                        │
    │   offset, size)        │                        │
    │                        │─ReadBlock─────────────►│
    │                        │  (block_id, file_id)   │ 查索引，读数据
    │                        │◄─stream data───────────│
    │                        │  边收边写本地            │
    │◄─CopyBlockResp─────────│                        │
    │  (new_offset, crc32)   │                        │
    │                        │                        │
    │ add_replica(dst_dn)    │                        │
    │ → file_map_ + WAL      │                        │
    │                        │                        │
    │─DeleteBlock────────────────────────────────────►│
    │  (block_id, file_id)   │                        │ 删除索引记录
    │                        │                        │
    │ remove_replica(src_dn) │                        │
    │ → file_map_ + WAL      │                        │
```

**一致性保证：**

| 阶段 | src_dn | dst_dn | 读请求 |
|------|--------|--------|--------|
| CopyBlock 进行中 | 有副本 | 无副本 | 正常，读 src |
| add_replica 后 | 有副本 | 有副本 | 正常，可读任意一个 |
| remove_replica 后 | 无副本 | 有副本 | 正常，读 dst |

任何阶段失败都不会丢数据，最坏情况是留下一个多余副本。

---

## 五、数据在磁盘上的布局

```
DataNode 数据目录（如 ./data_dn1/）
├── mainblock/
│   ├── 1          ← block_id=1 的主块文件（64MB 预分配）
│   ├── 2          ← block_id=2 的主块文件
│   └── ...
└── index/
    ├── 1          ← block_id=1 的索引文件（mmap）
    ├── 2
    └── ...
```

**主块文件内部布局（以 block_id=1 为例）：**

```
offset=0    [file_id=3 的数据，size=1024]
offset=1024 [file_id=7 的数据，size=2048]
offset=3072 [file_id=9 的数据，size=512]
...
```

**索引文件内部布局：**

```
[IndexHeader]          ← 固定大小头部，含 BlockInfo、bucket_size、data_file_offset 等
[bucket[0]]            ← 哈希桶数组，每个桶存第一个 MetaInfo 的偏移
[bucket[1]]
...
[bucket[999]]
[MetaInfo: file_id=3, offset=0,    size=1024, next=0]
[MetaInfo: file_id=7, offset=1024, size=2048, next=0]
[MetaInfo: file_id=9, offset=3072, size=512,  next=0]
...
```
