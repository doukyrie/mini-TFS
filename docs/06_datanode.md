# DataNode 模块详解

DataNode 是实际存储文件数据的节点，可以启动多个实例。它同时扮演两个角色：gRPC 服务端（接受 Client 的读写请求）和 gRPC 客户端（向 NameServer 发送心跳）。

---

## 一、模块结构

```
datanode/
├── data_node.h/cpp   # gRPC 服务实现 + 存储操作
└── main.cpp          # 启动入口
```

DataNode 依赖 `libstorage.a`（Storage 层静态库）和 `libproto_gen.a`（gRPC 生成代码），以及 `zlib`（CRC32 校验）。

---

## 二、BlockContext — 块上下文

```cpp
struct BlockContext {
    std::mutex mu;   // 每个 block 独立的锁（细粒度并发控制）
    std::unique_ptr<qiniu::largefile::IndexHandle>   index;
    std::unique_ptr<qiniu::largefile::FileOperation> main_block;
    std::string main_block_path;
    uint32_t    block_id;
};
```

每个 `block_id` 对应一个 `BlockContext`，包含：
- `mu`：**每个 block 独立的互斥锁**，不同 block 的读写可真正并发
- `index`：管理 `base_path/index/block_id` 文件（mmap 索引，哈希表）
- `main_block`：管理 `base_path/mainblock/block_id` 文件（64MB 预分配，顺序写入）

---

## 三、DataNodeServiceImpl — 核心类

### 成员变量

```cpp
class DataNodeServiceImpl {
    std::string base_path_;
    std::string self_id_;
    std::string self_ip_;
    int32_t     self_port_;

    std::mutex mu_;  // 只保护 blocks_ map 的增删
    std::unordered_map<uint64_t, std::shared_ptr<BlockContext>> blocks_;

    std::shared_ptr<grpc::Channel>       ns_channel_;
    std::unique_ptr<NameServerService::Stub> ns_stub_;

    std::atomic<bool>    running_{true};
    std::thread          heartbeat_thread_;
    std::atomic<int32_t> active_connections_{0};

    static const MMapOption kMMapOpt;       // {1MB, 4KB, 4KB}
    static const int32_t kBucketSize = 1000;
    static const int32_t kMainBlockSz = 64MB;
};
```

**两级锁设计：**
- `mu_`（全局锁）：只在 `get_or_create_block` 时持有，保护 `blocks_` map 的增删
- `bctx->mu`（per-block 锁）：保护单个 block 的读写操作

这样不同 block 的操作可以真正并发，只有同一 block 的操作才会互斥。

---

## 四、get_or_create_block — 懒加载块上下文

```cpp
std::shared_ptr<BlockContext> get_or_create_block(uint64_t block_id) {
    // 调用前需持有 mu_
    auto it = blocks_.find(block_id);
    if (it != blocks_.end()) return it->second;

    auto ctx = make_shared<BlockContext>();
    ctx->main_block_path = base_path_ + "/mainblock/" + block_id;
    ctx->main_block = make_unique<FileOperation>(path, O_RDWR|O_LARGEFILE|O_CREAT);
    ctx->main_block->ftruncate_file(64MB);

    ctx->index = make_unique<IndexHandle>(base_path_, block_id);
    int ret = ctx->index->load(block_id, 1000, kMMapOpt);
    if (ret < 0) {
        ctx->index->create(block_id, 1000, kMMapOpt);
    }

    blocks_[block_id] = ctx;
    return ctx;
}
```

返回 `shared_ptr<BlockContext>`，调用方在持有 `shared_ptr` 的情况下可以安全地在 `mu_` 释放后继续操作 block。

---

## 五、WriteBlock — 边收边写（含 CRC32）

写入流程的核心约束：**必须在写第一个字节之前就持有 per-block 锁**，否则两个并发写同一 block 会拿到相同的 `start_offset`，导致数据覆盖。

```cpp
grpc::Status WriteBlock(ServerContext*, ServerReader<WriteBlockRequest>* reader,
                        WriteBlockResponse* resp) {
    ++active_connections_;  // RAII Guard 在函数退出时自动 --

    // 1. 读第一个 chunk，提取 block_id / file_id
    WriteBlockRequest req;
    reader->Read(&req);
    uint64_t block_id = req.block_id();
    uint64_t file_id  = req.file_id();
    vector<char> first_data(req.data().begin(), req.data().end());

    // 2. 全局锁内 get_or_create_block，拿到 shared_ptr 后立即释放全局锁
    shared_ptr<BlockContext> bctx;
    { lock_guard lk(mu_); bctx = get_or_create_block(block_id); }

    // 3. 持有 per-block 锁，覆盖整个写入过程（防止并发写同一 block 的 offset 竞争）
    lock_guard blk_lk(bctx->mu);

    // 4. 从索引头读取 start_offset（此时已持锁，值是稳定的）
    int32_t start_offset  = bctx->index->get_index_header()->data_file_offset;
    int32_t total_written = 0;
    uLong   running_crc   = crc32(0L, Z_NULL, 0);

    // 5. 写第一个 chunk
    bctx->main_block->pwrite_file(first_data.data(), first_data.size(), start_offset);
    running_crc = crc32(running_crc, first_data.data(), first_data.size());
    total_written += first_data.size();

    // 6. 继续读取剩余 chunk，每个 chunk 直接 pwrite 到磁盘（不在内存中累积）
    while (reader->Read(&req)) {
        const auto& d = req.data();
        bctx->main_block->pwrite_file(d.data(), d.size(), start_offset + total_written);
        running_crc = crc32(running_crc, d.data(), d.size());
        total_written += d.size();
    }

    uint32_t crc = static_cast<uint32_t>(running_crc);

    // 7. 写索引 MetaInfo（含最终 crc32）
    MetaInfo meta;
    meta.set_file_id(file_id);
    meta.set_offset(start_offset);
    meta.set_size(total_written);
    meta.set_crc32(crc);
    bctx->index->write_segment_meta(meta.get_key(), meta);

    // 8. 更新索引头部，刷盘
    bctx->index->set_index_header_offset(total_written);
    bctx->index->update_block_info(C_OPER_INSERT, total_written);
    bctx->index->flush();

    resp->set_offset(start_offset);
    resp->set_size(total_written);
    resp->set_crc32(crc);
}
```

**边收边写流程：**

```
Client 流式发送数据片段
    ↓
DataNode 读第一个 chunk → 提取 block_id/file_id
    ↓
get_or_create_block（全局锁，短暂持有）
    ↓
持有 per-block 锁
    ↓
从索引头读 start_offset（稳定值）
    ↓
pwrite(first_chunk, start_offset)    ← 第一个 chunk 直接落盘
    ↓
循环 reader->Read：
    pwrite(chunk, start_offset + written)  ← 每个 chunk 直接落盘，不在内存累积
    running_crc = crc32(running_crc, chunk)
    ↓
hash_insert(file_id, {start_offset, total_written, crc32})  ← 写索引
    ↓
data_file_offset += total_written    ← 更新下次写入位置
    ↓
msync(index_file)                    ← 索引刷盘
```

**与旧实现的区别：** 旧实现将所有 chunk 拼接到 `vector<char> buf` 后才写盘，大文件场景下内存占用 = 文件大小。新实现每个 chunk 收到即写盘，内存占用始终只有一个 chunk（≤1MB）。

---

## 六、ReadBlock — 读取数据（含 CRC32 验证）

```cpp
grpc::Status ReadBlock(ServerContext*, const ReadBlockRequest* req,
                       ServerWriter<ReadBlockResponse>* writer) {
    // 获取 block 上下文
    shared_ptr<BlockContext> bctx;
    { lock_guard lk(mu_); bctx = get_or_create_block(req->block_id()); }
    lock_guard blk_lk(bctx->mu);

    // 从索引获取 offset/size/stored_crc
    MetaInfo meta;
    bctx->index->read_segment_meta(req->file_id(), meta);
    int32_t offset = meta.get_offset();
    int32_t size   = meta.get_size();
    uint32_t stored_crc = meta.get_crc32();

    // 分块流式返回，每次最多 1MB，同时累积计算 CRC32
    uLong running_crc = crc32(0L, Z_NULL, 0);
    while (sent < size) {
        // pread + 累积 CRC32
        running_crc = crc32(running_crc, buf.data(), to_read);
        // 最后一个 chunk：验证 CRC32
        if (sent >= size) {
            if (stored_crc != 0 && running_crc != stored_crc) {
                // 数据损坏，返回错误
                resp.set_status(-1);
                resp.set_message("CRC32 mismatch");
            }
            resp.set_crc32(stored_crc);  // 携带 CRC32 给 Client 二次验证
        }
        writer->Write(resp);
    }
}
```

**DataNode 侧的 CRC32 验证：** 读取时重新计算数据的 CRC32，与索引中存储的值比对。如果不一致，说明主块文件数据已损坏，立即返回错误。

---

## 七、DeleteBlock — 删除数据

```cpp
grpc::Status DeleteBlock(ServerContext*, const DeleteBlockRequest* req,
                         DeleteBlockResponse* resp) {
    shared_ptr<BlockContext> bctx;
    { lock_guard lk(mu_); bctx = get_or_create_block(req->block_id()); }
    lock_guard blk_lk(bctx->mu);

    bctx->index->delete_segment_meta(req->file_id());
    bctx->index->flush();
}
```

删除只是从索引中移除记录，主块文件中的数据字节不会被清除（空间回收通过后台 compact 完成，当前未实现）。

---

## 八、心跳与 BlockReport

### 心跳（每 10 秒）

```cpp
void start_heartbeat() {
    heartbeat_thread_ = thread([this]() {
        bool reported = false;
        while (running_) {
            HeartbeatRequest req;
            req.set_datanode_id(self_id_);
            req.set_ip(self_ip_);
            req.set_port(self_port_);

            // 使用 statvfs 获取真实磁盘可用容量
            struct statvfs vfs_stat;
            int64_t avail_cap = 1024LL * 1024 * 1024; // fallback: 1GB
            if (statvfs(base_path_.c_str(), &vfs_stat) == 0) {
                avail_cap = (int64_t)vfs_stat.f_bavail * vfs_stat.f_frsize;
            }
            req.set_available_cap(avail_cap);
            req.set_active_connections(active_connections_.load());
            { lock_guard lk(mu_); req.set_block_count(blocks_.size()); }

            grpc::ClientContext ctx;
            ctx.set_deadline(now + 5秒);
            auto st = ns_stub_->Heartbeat(&ctx, req, &resp);

            // 首次心跳成功后立即发送 BlockReport
            if (st.ok() && !reported) {
                reported = true;
                send_block_report();
            }

            sleep(10秒);
        }
    });
}
```

**statvfs 的作用：** 通过系统调用获取 `base_path_` 所在文件系统的真实可用块数（`f_bavail`）和块大小（`f_frsize`），计算出真实可用字节数。NameServer 据此计算 `cap_score`，容量越大的节点被选中的概率越高。

### BlockReport（启动后发送一次）

```cpp
void DataNodeServiceImpl::send_block_report() {
    BlockReportRequest req;
    req.set_datanode_id(self_id_);

    // 扫描 base_path_/index/ 目录，收集所有 block_id
    std::string index_dir = base_path_ + "/index";
    DIR* dir = opendir(index_dir.c_str());
    if (dir) {
        struct dirent* entry;
        while ((entry = readdir(dir)) != nullptr) {
            if (entry->d_name[0] == '.') continue;
            uint64_t bid = std::stoull(entry->d_name);
            req.add_block_ids(bid);
        }
        closedir(dir);
    }

    ns_stub_->BlockReport(&ctx, req, &resp);
}
```

**触发时机：** DataNode 启动后，首次心跳成功时调用一次（`bool reported` 标志保证只执行一次）。

**作用：** DataNode 重启或数据目录被清空后，NameServer 通过对账发现哪些副本已不存在，并将其从 `file_map_` 中移除，避免后续下载时静默失败。

---

## 九、CopyBlock — 副本迁移接收端

由 NameServer 的 RebalanceManager 调用，目标节点主动从源节点拉取数据。

```cpp
grpc::Status CopyBlock(ServerContext*, const CopyBlockRequest* req,
                       CopyBlockResponse* resp) {
    // 1. 建立到源节点的 gRPC 连接
    auto src_stub = DataNodeService::NewStub(
        CreateChannel(src_ip + ":" + src_port, InsecureChannelCredentials()));

    // 2. 调用源节点 ReadBlock 流式读取
    ReadBlockRequest read_req;
    read_req.set_block_id(req->block_id());
    read_req.set_file_id(req->file_id());
    read_req.set_offset(req->source_offset());
    read_req.set_size(req->source_size());
    auto reader = src_stub->ReadBlock(&ctx, read_req);

    // 3. 获取本地 block 上下文，持有 per-block 锁
    auto bctx = get_or_create_block(req->block_id());
    lock_guard blk_lk(bctx->mu);

    int32_t start_offset = bctx->index->get_index_header()->data_file_offset;

    // 4. 边收边写（与 WriteBlock 完全一致）
    while (reader->Read(&chunk)) {
        bctx->main_block->pwrite_file(chunk.data(), chunk.size(),
                                       start_offset + total_written);
        running_crc = crc32(running_crc, chunk.data(), chunk.size());
        total_written += chunk.size();
    }

    // 5. 写索引、更新头部、刷盘
    bctx->index->write_segment_meta(meta.get_key(), meta);
    bctx->index->set_index_header_offset(total_written);
    bctx->index->update_block_info(C_OPER_INSERT, total_written);
    bctx->index->flush();

    resp->set_offset(start_offset);
    resp->set_size(total_written);
    resp->set_crc32(final_crc);
}
```

**关键点：** CopyBlock 复用了 WriteBlock 的全部写入逻辑（per-block 锁、pwrite、索引更新），保证并发安全和数据一致性。

---

## 十、main.cpp — 启动流程

```
用法: ./datanode <listen_addr> <ns_addr> <self_id> <self_ip> <data_dir>
示例: ./datanode 0.0.0.0:50052 localhost:50051 dn1 127.0.0.1 ./data_dn1
```

```cpp
int main(...) {
    DataNodeServiceImpl service(data_dir, ns_addr, self_id, self_ip, self_port);
    service.start_heartbeat();  // 启动心跳后台线程

    grpc::ServerBuilder builder;
    builder.AddListeningPort(listen_addr, InsecureServerCredentials());
    builder.RegisterService(&service);

    auto server = builder.BuildAndStart();
    server->Wait();
}
```
