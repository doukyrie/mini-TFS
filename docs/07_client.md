# Client 模块详解

Client 是用户与分布式文件系统交互的入口，提供命令行工具，封装了上传、下载、删除三个操作的完整流程。

---

## 一、模块结构

```
client/
├── tfs_client.h/cpp   # TfsClient 类，封装三个操作
└── main.cpp           # 命令行入口
```

---

## 二、TfsClient 类

```cpp
class TfsClient {
public:
    explicit TfsClient(const std::string& ns_addr);

    int64_t upload_file(const std::string& local_path, int replica_num = 1);
    int     download_file(uint64_t file_id, const std::string& local_path);
    int     delete_file(uint64_t file_id);

private:
    std::unique_ptr<NameServerService::Stub> ns_stub_;
    // DataNode 的 stub 在每次操作时临时创建
};
```

Client 只持有一个到 NameServer 的长连接，DataNode 的连接在每次操作时按需创建（因为 DataNode 地址是动态的，由 NameServer 返回）。

---

## 三、upload_file — 流式上传文件

### 完整流程

```cpp
int64_t upload_file(const string& local_path, int replica_num) {
    // 1. 只获取文件大小，不读内容到内存
    ifstream ifs(local_path, ios::binary | ios::ate);
    int64_t file_size = ifs.tellg();
    ifs.close();

    // 2. 向 NameServer 申请块
    AllocateBlockRequest req;
    req.set_file_size(file_size);
    req.set_replica_num(replica_num);
    AllocateBlockResponse resp;
    ns_stub_->AllocateBlock(&ctx, req, &resp);
    // resp 包含 file_id, block_id, locations[dn1, dn2, ...]

    // 3. 并行写入所有副本
    vector<future<WriteBlockResponse>> futures;
    for (int i = 0; i < n; ++i) {
        futures.push_back(async(launch::async,
            write_to_node, locations[i], file_id, local_path, file_size));
    }

    // 4. 收集结果，多数派成功才算成功
    int majority = n / 2 + 1;
    CommitFileRequest commit_req;
    int success = 0;
    for (int i = 0; i < n; ++i) {
        auto resp = futures[i].get();
        if (resp.status() == 0) {
            // 将成功副本的 offset/size 加入 commit_req
            ++success;
        }
    }
    if (success < majority) return -1;

    // 5. 提交给 NameServer
    ns_stub_->CommitFile(&ctx2, commit_req, &commit_resp);
    return file_id;
}
```

### write_to_node — 向单个 DataNode 流式写入

```cpp
static WriteBlockResponse write_to_node(
        const BlockLocation& loc, uint64_t file_id,
        const string& local_path, int64_t file_size) {

    // 临时创建到该 DataNode 的连接
    string addr = loc.datanode_ip() + ":" + to_string(loc.datanode_port());
    auto stub = DataNodeService::NewStub(CreateChannel(addr, ...));

    WriteBlockResponse resp;
    auto writer = stub->WriteBlock(&ctx, &resp);

    // 流式读取本地文件，每次最多 1MB，直接发送
    ifstream ifs(local_path, ios::binary);
    const int32_t chunk = 1024 * 1024;
    int64_t sent = 0;
    vector<char> buf(chunk);
    while (sent < file_size) {
        int64_t to_send = min(chunk, file_size - sent);
        ifs.read(buf.data(), to_send);
        int64_t actually_read = ifs.gcount();
        WriteBlockRequest req;
        req.set_block_id(loc.block_id());
        req.set_file_id(file_id);
        req.set_data(buf.data(), actually_read);
        req.set_is_last(sent + actually_read >= file_size);
        writer->Write(req);
        sent += actually_read;
    }
    writer->WritesDone();
    writer->Finish();
    return resp;
}
```

**流式上传的优势：** 不需要将整个文件读入内存，每次只读 1MB 缓冲区，适合上传大文件。每个副本的 `write_to_node` 在独立线程中各自打开文件并流式读取，互不干扰。

**并行写入的实现：** 使用 `std::async(launch::async, ...)` 为每个副本启动一个线程，所有线程同时向各自的 DataNode 发起 gRPC 流式写入，然后用 `future::get()` 等待所有结果。

**多数派（Majority）策略：** 只要 `成功副本数 >= N/2 + 1` 就认为上传成功。例如 3 副本时，2 个成功即可。

---

## 四、download_file — 下载文件（含 CRC32 验证）

```cpp
int download_file(uint64_t file_id, const string& local_path) {
    // 1. 查询所有存活副本
    GetBlockLocationRequest req;
    req.set_file_id(file_id);
    GetBlockLocationResponse resp;
    ns_stub_->GetBlockLocation(&ctx, req, &resp);

    // 2. 随机选一个副本（简单负载均衡）
    mt19937 rng(random_device{}());
    int idx = uniform_int_distribution<int>(0, n-1)(rng);
    const auto& loc = resp.locations(idx);

    // 3. 向选中的 DataNode 发起流式读取
    auto reader = dn_stub->ReadBlock(&ctx, read_req);

    // 4. 接收流式数据，同时累积计算 CRC32
    ofstream ofs(local_path, ios::binary);
    uLong running_crc = crc32(0L, Z_NULL, 0);
    uint32_t server_crc = 0;
    while (reader->Read(&chunk)) {
        if (chunk.status() != 0) { /* 错误处理 */ }
        running_crc = crc32(running_crc, chunk.data().data(), chunk.data().size());
        if (chunk.crc32() != 0) server_crc = chunk.crc32();  // 最后一个 chunk 携带 CRC32
        ofs.write(chunk.data().data(), chunk.data().size());
    }

    // 5. 验证 CRC32
    if (server_crc != 0 && running_crc != server_crc) {
        cerr << "[Client] CRC32 mismatch! data corrupted.\n";
        return -1;
    }
}
```

**端到端 CRC32 验证：** DataNode 在写入时计算并存储 CRC32，读取时在最后一个 chunk 中携带该值。Client 在接收所有数据后重新计算 CRC32 并与服务端值比对，确保数据在传输过程中没有损坏。

**随机选副本的好处：** 多个 Client 同时下载同一文件时，请求会分散到不同副本节点，避免单节点成为热点。

---

## 五、delete_file — 删除文件

```cpp
int delete_file(uint64_t file_id) {
    // 1. 查询所有副本位置
    ns_stub_->GetBlockLocation(&ctx, req, &resp);

    // 2. 逐一通知每个副本节点删除
    for (const auto& loc : resp.locations()) {
        auto dn_stub = DataNodeService::NewStub(CreateChannel(addr, ...));
        dn_stub->DeleteBlock(&ctx, del_req, &del_resp);
    }

    // 3. 通知 NameServer 删除元数据
    ns_stub_->DeleteFile(&ctx, ns_del, &ns_resp);
}
```

---

## 六、main.cpp — 命令行接口

```
用法:
  ./client upload   <ns_addr> <local_file> [replicas=1]
  ./client download <ns_addr> <file_id> <local_file>
  ./client delete   <ns_addr> <file_id>

示例:
  ./client upload   localhost:50051 /tmp/test.txt 2
  ./client download localhost:50051 1 /tmp/output.txt
  ./client delete   localhost:50051 1
```

上传成功后会打印 `file_id`，用户需要保存这个 ID 用于后续的下载和删除操作。
