# Proto 接口定义详解

mini-tfs 定义了两个 proto 文件，分别描述 NameServer 和 DataNode 的 gRPC 服务接口。

---

## 一、nameserver.proto

**包名：** `minitfs`

### 公共类型：BlockLocation

```protobuf
message BlockLocation {
    uint64 block_id      = 1;  // 块 ID
    string datanode_id   = 2;  // DataNode 的唯一标识（如 "dn1"）
    string datanode_ip   = 3;  // DataNode 的 IP
    int32  datanode_port = 4;  // DataNode 的端口
    int64  offset        = 5;  // 文件数据在主块文件中的偏移
    int64  size          = 6;  // 文件数据的大小
}
```

`BlockLocation` 是贯穿整个系统的核心数据结构，描述"某个文件的某个副本存在哪个 DataNode 的哪个位置"。

---

### 文件操作接口

#### AllocateBlock — 申请块（上传前调用）

```protobuf
message AllocateBlockRequest {
    int64 file_size   = 1;  // 文件大小（字节）
    int32 replica_num = 2;  // 副本数，默认 1
}
message AllocateBlockResponse {
    int32                  status    = 1;  // 0=成功，-1=失败
    string                 message   = 2;
    uint64                 file_id   = 3;  // 分配的全局唯一文件 ID
    uint64                 block_id  = 4;  // 分配的块 ID
    repeated BlockLocation locations = 5;  // 目标 DataNode 列表（副本数个）
}
```

Client 上传前调用此接口，NameServer 返回 `file_id`、`block_id` 以及要写入的 DataNode 列表。注意此时 `locations` 中的 `offset/size` 为空，写完后才知道。

#### GetBlockLocation — 查询块位置（下载/删除前调用）

```protobuf
message GetBlockLocationRequest {
    uint64 file_id = 1;
}
message GetBlockLocationResponse {
    int32                  status    = 1;
    string                 message   = 2;
    repeated BlockLocation locations = 3;  // 所有存活副本
}
```

NameServer 只返回**存活**副本（心跳未超时的节点），已宕机节点的副本会被过滤掉。

#### CommitFile — 提交文件（上传成功后调用）

```protobuf
message CommitFileRequest {
    uint64                 file_id   = 1;
    uint64                 block_id  = 2;
    repeated BlockLocation locations = 3;  // 实际写成功的副本列表（含 offset/size）
}
message CommitFileResponse {
    int32  status  = 1;
    string message = 2;
}
```

Client 写完数据后，将实际写成功的副本信息（包含 `offset` 和 `size`）提交给 NameServer，NameServer 将其存入 `file_map_`。

#### DeleteFile — 删除文件元数据

```protobuf
message DeleteFileRequest  { uint64 file_id = 1; }
message DeleteFileResponse { int32 status = 1; string message = 2; }
```

从 NameServer 的 `file_map_` 中删除该文件的映射记录。

---

### DataNode 管理接口

#### Heartbeat — 心跳

```protobuf
message HeartbeatRequest {
    string datanode_id        = 1;  // 节点唯一 ID
    string ip                 = 2;
    int32  port               = 3;
    int64  available_cap      = 4;  // 可用容量（字节）
    int32  block_count        = 5;  // 当前管理的块数量
    int32  active_connections = 6;  // 当前活跃连接数（用于负载均衡）
}
message HeartbeatResponse { int32 status = 1; string message = 2; }
```

DataNode 每 10 秒发送一次心跳，NameServer 据此更新节点状态和负载评分。

#### BlockReport — 块报告

```protobuf
message BlockReportRequest {
    string          datanode_id = 1;
    repeated uint64 block_ids   = 2;  // 该节点上所有块的 ID 列表
}
message BlockReportResponse { int32 status = 1; string message = 2; }
```

DataNode 启动时上报自己持有的所有块，NameServer 对账后移除失效副本。

#### TriggerRebalance — 手动触发 rebalance

```protobuf
message TriggerRebalanceRequest {}
message TriggerRebalanceResponse {
    int32  status     = 1;
    string message    = 2;
    int32  task_count = 3;  // 生成的迁移任务数
}
```

运维接口，手动触发副本均衡。新节点加入时 NameServer 也会自动调用。

---

### NameServerService 服务定义

```protobuf
service NameServerService {
    rpc AllocateBlock    (AllocateBlockRequest)    returns (AllocateBlockResponse);
    rpc GetBlockLocation (GetBlockLocationRequest) returns (GetBlockLocationResponse);
    rpc CommitFile       (CommitFileRequest)       returns (CommitFileResponse);
    rpc DeleteFile       (DeleteFileRequest)       returns (DeleteFileResponse);
    rpc Heartbeat        (HeartbeatRequest)        returns (HeartbeatResponse);
    rpc BlockReport      (BlockReportRequest)      returns (BlockReportResponse);
    rpc TriggerRebalance (TriggerRebalanceRequest) returns (TriggerRebalanceResponse);
}
```

全部是普通 Unary RPC（一请求一响应）。

---

## 二、datanode.proto

**包名：** `minitfs`

### WriteBlock — 写块（客户端流式）

```protobuf
message WriteBlockRequest {
    uint64 block_id = 1;
    uint64 file_id  = 2;
    bytes  data     = 3;  // 本次传输的数据片段
    bool   is_last  = 4;  // 是否是最后一片
}
message WriteBlockResponse {
    int32  status  = 1;
    string message = 2;
    int64  offset  = 3;  // 数据写入主块文件的起始偏移
    int64  size    = 4;  // 数据总大小
}
```

使用**客户端流式 RPC**（`stream WriteBlockRequest`）：Client 将文件数据分成最多 1MB 的片段，逐片发送给 DataNode，DataNode 收完所有片段后一次性写入主块文件并返回 `offset/size`。

### ReadBlock — 读块（服务端流式）

```protobuf
message ReadBlockRequest {
    uint64 block_id = 1;
    uint64 file_id  = 2;
    int64  offset   = 3;
    int64  size     = 4;
}
message ReadBlockResponse {
    int32  status  = 1;
    string message = 2;
    bytes  data    = 3;  // 本次返回的数据片段
}
```

使用**服务端流式 RPC**（`stream ReadBlockResponse`）：DataNode 将数据分成最多 1MB 的片段逐片返回给 Client。

### DeleteBlock — 删除块（普通 RPC）

```protobuf
message DeleteBlockRequest  { uint64 block_id = 1; uint64 file_id = 2; }
message DeleteBlockResponse { int32 status = 1; string message = 2; }
```

从索引哈希表中删除 `file_id` 对应的 `MetaInfo`，主块文件中的数据不立即清除。

### CopyBlock — 副本迁移（普通 RPC）

```protobuf
message CopyBlockRequest {
    uint64 block_id             = 1;
    uint64 file_id              = 2;
    string source_datanode_ip   = 3;  // 源节点 IP
    int32  source_datanode_port = 4;  // 源节点端口
    int64  source_offset        = 5;  // 源节点上的数据偏移
    int64  source_size          = 6;  // 数据大小
}
message CopyBlockResponse {
    int32  status  = 1;
    string message = 2;
    int64  offset  = 3;   // 写入本地后的偏移
    int64  size    = 4;
    uint32 crc32   = 5;
}
```

由 NameServer 的 RebalanceManager 调用，目标节点收到请求后主动从源节点拉取数据（调用源节点的 ReadBlock），边收边写到本地，完成后返回新的 offset/crc32 给 NameServer 更新元数据。

### DataNodeService 服务定义

```protobuf
service DataNodeService {
    rpc WriteBlock  (stream WriteBlockRequest) returns (WriteBlockResponse);
    rpc ReadBlock   (ReadBlockRequest)         returns (stream ReadBlockResponse);
    rpc DeleteBlock (DeleteBlockRequest)       returns (DeleteBlockResponse);
    rpc CopyBlock   (CopyBlockRequest)         returns (CopyBlockResponse);
}
```

WriteBlock 是客户端流式，ReadBlock 是服务端流式，DeleteBlock 和 CopyBlock 是普通 Unary RPC。
