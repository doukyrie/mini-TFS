# 构建与运行指南

---

## 一、CMakeLists.txt 解析

项目使用 CMake 构建，整体分为四个编译目标：

```
proto_gen (静态库)
    ↑ 依赖
storage (静态库)
    ↑ 依赖
nameserver (可执行)   datanode (可执行)   client (可执行)
```

### Proto 代码生成

```cmake
foreach(PROTO nameserver datanode)
    add_custom_command(
        OUTPUT ${PROTO}.pb.cc ${PROTO}.pb.h ${PROTO}.grpc.pb.cc ${PROTO}.grpc.pb.h
        COMMAND protoc
            --proto_path=${PROTO_DIR}
            --cpp_out=${PROTO_OUT}
            --grpc_out=${PROTO_OUT}
            --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
            ${PROTO_DIR}/${PROTO}.proto
    )
endforeach()
```

每次构建时，CMake 会检查 `.proto` 文件是否有变化，有变化则重新生成 `.pb.cc` / `.grpc.pb.cc` 等文件，输出到 `build/proto_gen/` 目录。

### 编译目标依赖关系

| 目标 | 源文件 | 依赖库 |
|------|--------|--------|
| `proto_gen` | 生成的 pb.cc 文件 | gRPC::grpc++, protobuf |
| `storage` | storage/*.cpp | 无外部依赖 |
| `nameserver` | nameserver/*.cpp | proto_gen |
| `datanode` | datanode/*.cpp | proto_gen, storage, zlib |
| `client` | client/*.cpp | proto_gen, zlib |

---

## 二、依赖安装

项目依赖 gRPC 和 Protobuf，CMake 会从 `$HOME/.local` 查找：

```bash
# 如果已安装到 ~/.local，直接构建即可
# 如果未安装，参考 gRPC 官方文档从源码编译安装：
# https://grpc.io/docs/languages/cpp/quickstart/
```

---

## 三、构建步骤

```bash
cd /home/kyrie/develop/cppprojects/grpc_projects/mini-tfs

mkdir -p build && cd build
cmake ..
make -j$(nproc)
```

构建成功后，`build/` 目录下会生成三个可执行文件：
- `build/nameserver`
- `build/datanode`
- `build/client`

---

## 四、运行示例

### 启动 NameServer

```bash
# 默认监听 0.0.0.0:50051，WAL 写入 ./nameserver.wal
./build/nameserver

# 指定监听地址和 WAL 路径
./build/nameserver 0.0.0.0:50051 ./nameserver.wal
```

输出：
```
[NameServer] WAL replayed: 0 files restored   ← 首次启动
[NameServer] listening on 0.0.0.0:50051
```

重启后：
```
[NameServer] WAL replayed: 3 files restored   ← 自动恢复之前上传的文件
[NameServer] listening on 0.0.0.0:50051
```

### 启动 DataNode（可启动多个）

```bash
# 用法: ./datanode <listen_addr> <ns_addr> <self_id> <self_ip> <data_dir>

# 启动第一个 DataNode
./build/datanode 0.0.0.0:50052 localhost:50051 dn1 127.0.0.1 ./data_dn1

# 启动第二个 DataNode（新终端）
./build/datanode 0.0.0.0:50053 localhost:50051 dn2 127.0.0.1 ./data_dn2
```

输出：
```
[DataNode] dn1 listening on 0.0.0.0:50052
[DataNode] dn2 listening on 0.0.0.0:50053
```

DataNode 启动后会立即开始向 NameServer 发送心跳，NameServer 日志会显示：
```
[NameServer] AllocateBlock: ...  ← 有上传请求时
```

### 使用 Client

```bash
# 上传文件（2 副本）
./build/client upload localhost:50051 /tmp/test.txt 2
# 输出: file_id: 1

# 下载文件
./build/client download localhost:50051 1 /tmp/output.txt
# 输出: [Client] download success: file_id=1 from dn1 -> /tmp/output.txt

# 删除文件
./build/client delete localhost:50051 1
# 输出: [Client] delete success: file_id=1
```

---

## 五、完整启动脚本示例

```bash
#!/bin/bash
cd /home/kyrie/develop/cppprojects/grpc_projects/mini-tfs

# 清理旧数据
rm -rf data_dn1 data_dn2

# 启动 NameServer（后台，启用 WAL 持久化）
./build/nameserver 0.0.0.0:50051 ./nameserver.wal &
NS_PID=$!
sleep 1

# 启动两个 DataNode（后台）
./build/datanode 0.0.0.0:50052 localhost:50051 dn1 127.0.0.1 ./data_dn1 &
DN1_PID=$!
./build/datanode 0.0.0.0:50053 localhost:50051 dn2 127.0.0.1 ./data_dn2 &
DN2_PID=$!
sleep 2  # 等待心跳注册

# 上传测试
echo "hello mini-tfs" > /tmp/test.txt
FID=$(./build/client upload localhost:50051 /tmp/test.txt 2 | grep file_id | awk '{print $2}')
echo "Uploaded file_id: $FID"

# 下载验证
./build/client download localhost:50051 $FID /tmp/output.txt
diff /tmp/test.txt /tmp/output.txt && echo "Download OK"

# 删除
./build/client delete localhost:50051 $FID

# 清理进程
kill $NS_PID $DN1_PID $DN2_PID
```

---

## 六、动态扩容示例

在已有集群基础上新增一个 DataNode，rebalance 会自动触发：

```bash
# 假设已有 dn1、dn2 在运行，现在加入 dn3
./build/datanode 0.0.0.0:50054 localhost:50051 dn3 127.0.0.1 ./data_dn3 &

# dn3 首次心跳成功后，NameServer 日志会出现：
# [RebalanceManager] New node detected: dn3, triggering rebalance
# [RebalanceManager] Generated N migrate tasks
# [RebalanceManager] Executing task: file=X from dn1 to dn3
# [DataNode] CopyBlock: block=1 file=X from=127.0.0.1:50052 size=...
# [RebalanceManager] Task succeeded
```

也可以手动触发（例如扩容后想立即均衡）：

```bash
grpc_cli call localhost:50051 TriggerRebalance ""
# 返回: task_count: N
```

---

## 七、常见问题

**Q: 上传失败，提示 "no available datanode"**

A: DataNode 还没有向 NameServer 注册。等待至少 10 秒（第一次心跳）后再重试。

**Q: 下载失败，提示 "all replicas unavailable"**

A: 所有副本节点都已宕机（心跳超时 30 秒）。检查 DataNode 进程是否还在运行。

如果 DataNode 刚重启且数据目录被清空，NameServer 日志中应出现：
```
[NameServer] BlockReport: removed stale replica file=X block=Y datanode=dn1
```
这说明 BlockReport 对账已生效，该文件的副本已被标记为不可用，下载会返回 "all replicas unavailable" 而非静默失败。

**Q: 重启 NameServer 后，之前上传的文件找不到了**

A: 确认启动时指定了 WAL 路径（第二个参数）。默认 WAL 路径是 `./nameserver.wal`，如果工作目录不同可能找不到。启动日志中会显示 `WAL replayed: N files restored`，N > 0 说明恢复成功。

**Q: 如何查看 DataNode 上存储了哪些数据？**

A: 查看 DataNode 的数据目录：
```bash
ls -la ./data_dn1/mainblock/   # 主块文件
ls -la ./data_dn1/index/       # 索引文件
```
