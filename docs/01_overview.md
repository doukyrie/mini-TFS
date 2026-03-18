# mini-tfs 整体架构概览

## 项目简介

mini-tfs 是一个参考 TFS（Taobao File System）思想实现的**分布式文件存储系统**，使用 C++17 + gRPC 构建。它将文件存储拆分为三个独立进程：**NameServer（元数据服务）**、**DataNode（数据节点）**、**Client（客户端）**，三者通过 gRPC 通信。

---

## 整体架构图

```
┌──────────────────────────────────────────────────────────────────┐
│                           Client                                 │
│  upload（流式，CRC32）/ download（CRC32 验证）/ delete            │
└────────────┬──────────────────────────────┬──────────────────────┘
             │ gRPC (nameserver.proto)       │ gRPC (datanode.proto)
             ▼                              ▼
┌────────────────────────┐      ┌──────────────────────────────────┐
│      NameServer        │      │           DataNode(s)             │
│  :50051                │◄─────│  心跳 / BlockReport（对账）       │
│                        │      │  :50052, :50053, ...              │
│  BlockManager          │      │                                  │
│  ├─ DataNode 注册表    │      │  BlockContext (per block_id)      │
│  ├─ file→block 映射    │      │  ├─ std::mutex mu（独立锁）       │
│  ├─ WAL 持久化         │      │  ├─ IndexHandle (mmap 索引)       │
│  ├─ 平滑加权轮询 LB    │      │  └─ FileOperation（主块文件）     │
│  └─ RebalanceManager  │      │                                  │
└────────────────────────┘      └──────────────────────────────────┘
```

---

## 三层角色职责

### NameServer（元数据服务）
- 维护所有 DataNode 的注册信息（IP、端口、真实可用容量、心跳时间）
- 维护 `file_id → block_id + 副本位置列表` 的映射
- 为上传请求分配 `file_id`、`block_id`，并用**平滑加权轮询**选出目标 DataNode
- 接收 DataNode 的心跳，检测宕机节点；**首次心跳时自动触发 rebalance**
- **通过 WAL 日志持久化元数据**，重启后自动回放恢复（支持 ADD_REPLICA / DEL_REPLICA）
- 内置 **RebalanceManager**，新节点加入时自动计算迁移任务，也支持手动触发
- 不存储任何文件数据，只管"文件在哪里"

### DataNode（数据节点）
- 实际存储文件数据，可以启动多个实例
- 每个 DataNode 管理若干个 **Block（块）**，每个 Block 对应：
  - 一个**主块文件**（64MB 预分配的大文件，顺序写入多个小文件的数据）
  - 一个**索引文件**（mmap 映射到内存，哈希表结构，记录每个 file_id 的 offset/size/crc32）
- 写入时**边收边写**（每个 chunk 直接 pwrite 落盘，不在内存中累积），同时滚动计算 **CRC32 校验值**；读取时验证数据完整性
- 使用**每 block 独立锁**，不同 block 的读写可真正并发；per-block 锁在读第一个 chunk 之前就持有，防止并发写同一 block 的 offset 竞争
- 定期向 NameServer 发送心跳，上报**真实磁盘可用容量**（statvfs）
- 启动后首次心跳成功时发送 **BlockReport**，上报实际持有的 block 列表，NameServer 据此移除失效副本
- 支持 **CopyBlock** RPC：从指定源节点流式拉取数据并写入本地，供 rebalance 迁移使用

### Client（客户端）
- 提供 `upload` / `download` / `delete` 三个命令
- 上传：**流式读取**本地文件（1MB 分块），先问 NameServer 要节点列表，再**并行**写入所有副本，最后提交给 NameServer
- 下载：先问 NameServer 要副本位置，随机选一个副本读取，**验证 CRC32** 确保数据完整
- 删除：先通知所有副本 DataNode 删除，再通知 NameServer 删除映射

---

## 技术栈

| 技术 | 用途 |
|------|------|
| gRPC + Protobuf | 进程间通信，定义服务接口 |
| C++17 | 主要开发语言 |
| mmap（内存映射） | 索引文件的高性能读写 |
| pread64/pwrite64 | 主块文件的随机读写 |
| zlib CRC32 | 端到端数据完整性校验 |
| statvfs | 获取真实磁盘可用容量 |
| WAL（Write-Ahead Log） | NameServer 元数据持久化（含副本迁移记录） |
| std::thread | 心跳线程、宕机检测线程、rebalance worker 线程 |
| std::future/async | 客户端并行写多副本 |
| per-block mutex | DataNode 细粒度并发控制 |
| RebalanceManager | 动态扩容时自动均衡副本分布 |
| CMake | 构建系统 |

---

## 目录结构

```
mini-tfs/
├── proto/
│   ├── nameserver.proto    # NameServer 服务接口定义
│   └── datanode.proto      # DataNode 服务接口定义（含 crc32 字段）
├── generated/              # protoc 手动生成的 pb.cc/pb.h（共 8 个文件）
├── nameserver/
│   ├── block_manager.h/cpp     # DataNode 注册表 + 文件映射表 + WAL 持久化
│   ├── rebalance_manager.h/cpp # 动态扩容 rebalance 调度
│   ├── name_server.h/cpp       # gRPC 服务实现
│   └── main.cpp                # 支持 WAL 路径参数
├── datanode/
│   ├── data_node.h/cpp     # gRPC 服务实现 + 存储操作 + CRC32 + 细粒度锁 + CopyBlock
│   └── main.cpp
├── client/
│   ├── tfs_client.h/cpp    # 流式上传、CRC32 下载验证
│   └── main.cpp
├── storage/                # 底层存储库（独立于 gRPC）
│   ├── common.h            # 公共数据结构（BlockInfo, MetaInfo 含 crc32 等）
│   ├── file_op.h/cpp       # 基础文件操作（pread/pwrite 封装）
│   ├── mmap_file.h/cpp     # mmap 内存映射封装
│   ├── mmap_file_op.h/cpp  # mmap + 文件操作组合
│   └── index_handle.h/cpp  # 索引文件管理（哈希表）
├── docs/                   # 文档
├── README.md
└── CMakeLists.txt
```

---

## 文档导航

- [02_data_flow.md](./02_data_flow.md) — 完整数据流（上传/下载/删除时序）
- [03_proto.md](./03_proto.md) — Proto 接口定义详解
- [04_storage.md](./04_storage.md) — Storage 底层存储模块详解
- [05_nameserver.md](./05_nameserver.md) — NameServer 模块详解
- [06_datanode.md](./06_datanode.md) — DataNode 模块详解
- [07_client.md](./07_client.md) — Client 模块详解
- [08_build.md](./08_build.md) — 构建与运行指南
- [09_rebalance.md](./09_rebalance.md) — 动态扩容与 Rebalance 详解
