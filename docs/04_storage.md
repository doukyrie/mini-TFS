# Storage 底层存储模块详解

Storage 是整个系统的最底层，完全独立于 gRPC，负责将文件数据高效地读写到磁盘。它被编译为静态库 `libstorage.a`，供 DataNode 链接使用。

---

## 一、核心数据结构（common.h）

### BlockInfo — 块统计信息

```cpp
struct BlockInfo {
    uint32_t block_id;       // 块 ID
    int32_t  version;        // 版本号，每次写/删都 +1
    int32_t  file_count;     // 当前存活文件数
    int32_t  current_size;   // 当前存活文件数据总大小
    int32_t  del_file_count; // 已删除文件数
    int32_t  del_size;       // 已删除文件数据总大小
    uint32_t seq_no;         // 下一个可分配的文件序号
};
```

存储在索引文件头部，记录这个块的整体统计信息。

### MetaInfo — 文件索引条目

```cpp
struct MetaInfo {
private:
    uint64_t file_id;       // 文件 ID（哈希表的 key）
    struct {
        int32_t offset;     // 文件数据在主块文件中的偏移
        int32_t size;       // 文件数据大小
    } location;
    int32_t next_meta;      // 哈希链表中下一个节点的偏移（0 表示链表尾）
};
```

每个文件在索引中对应一个 `MetaInfo`，通过 `file_id % bucket_size` 定位到哈希桶，再通过 `next_meta` 链表解决哈希冲突。

### MMapOption — 内存映射配置

```cpp
struct MMapOption {
    int32_t max_mmap_size;    // 最大映射大小
    int32_t first_mmap_size;  // 首次映射大小
    int32_t per_mmap_size;    // 每次扩展大小
};
```

DataNode 中使用的配置：`{1MB, 4KB, 4KB}`，索引文件通常很小，4KB 足够初始映射。

### IndexHeader — 索引文件头部

```cpp
struct IndexHeader {
    BlockInfo block_info;       // 块统计信息
    int32_t   bucket_size;      // 哈希桶数量（固定为 1000）
    int32_t   data_file_offset; // 主块文件已写入的字节数（下次写入的起始偏移）
    int32_t   index_file_size;  // 索引文件已使用的字节数（下次写 MetaInfo 的位置）
    int32_t   free_head_offset; // 空闲链表头（被删除的 MetaInfo 可重用）
};
```

索引文件的内存布局：

```
[IndexHeader (固定大小)]
[bucket[0]] [bucket[1]] ... [bucket[999]]  ← 1000 个 int32_t，每个存链表头偏移
[MetaInfo] [MetaInfo] [MetaInfo] ...       ← 动态增长
```

---

## 二、FileOperation — 基础文件操作

**文件：** `storage/file_op.h` / `file_op.cpp`

封装了 Linux 系统调用，提供带重试机制的文件读写。

### 核心设计：重试机制

```cpp
// pread_file 的核心循环（pwrite_file 类似）
while (left > 0) {
    if (++i >= max_disk) break;  // 最多重试 5 次，防止死循环
    int fd = check_file();       // 如果 fd 无效则重新打开文件
    read_len = pread64(fd, p_temp, left, read_offset);
    if (read_len < 0) {
        if (errno == EINTR || errno == EAGAIN) continue;  // 临时中断，重试
        if (errno == EBADF) { this->fd = -1; continue; } // fd 损坏，重新打开
        return -errno;
    }
    left -= read_len;
    read_offset += read_len;
    p_temp += read_len;
}
```

**为什么需要重试？** `pread64` 不保证一次读完所有数据（内核可能只返回部分），所以需要循环直到读完 `size` 字节。`EINTR`（信号中断）和 `EAGAIN`（资源暂时不可用）是正常的临时错误，应该重试。

### check_file()

```cpp
int check_file() {
    if (fd < 0) {
        fd = ::open(file_name, open_flag, open_mode);
    }
    return fd;
}
```

懒加载：文件描述符无效时自动重新打开，避免调用方每次都要手动 `open_file()`。

### 主要接口

| 方法 | 说明 |
|------|------|
| `pread_file(buf, size, offset)` | 从指定偏移读 size 字节，带重试 |
| `pwrite_file(buf, size, offset)` | 向指定偏移写 size 字节，带重试 |
| `ftruncate_file(size)` | 预分配文件大小（DataNode 用来预分配 64MB 主块文件） |
| `flush_file()` | `fsync`，强制刷盘 |
| `get_file_size()` | `fstat` 获取文件大小 |

---

## 三、MMapFile — 内存映射封装

**文件：** `storage/mmap_file.h` / `mmap_file.cpp`

封装了 `mmap` / `munmap` / `msync` 系统调用。

### 核心：mmap_file()

```cpp
bool mmap_file(const bool write = false) {
    // 1. 如果文件比 first_mmap_size 小，先扩展文件到 first_mmap_size
    // 2. 调用 mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0)
    // 3. 将映射地址存入 data 指针
}
```

映射后，对 `data` 指针的读写会直接反映到文件（`MAP_SHARED`），内核负责在合适时机将脏页写回磁盘。

### remmap_file() — 扩展映射

当索引文件需要增长时（写入新的 `MetaInfo`），调用 `remmap_file()` 先 `munmap` 再重新 `mmap` 更大的区域。

---

## 四、MMapFileOperation — mmap + 文件操作组合

**文件：** `storage/mmap_file_op.h` / `mmap_file_op.cpp`

继承自 `FileOperation`，重写了 `pread_file` 和 `pwrite_file`：

```
如果已经 mmap：
    直接用 memcpy 从/到内存映射区域读写（极快）
如果未 mmap 或偏移超出映射范围：
    退化为父类的 pread64/pwrite64 系统调用
```

这是索引文件高性能的关键：索引文件被 mmap 到内存后，读写索引就是纯内存操作，不需要系统调用。

---

## 五、IndexHandle — 索引文件管理

**文件：** `storage/index_handle.h` / `index_handle.cpp`

这是 Storage 层最核心的类，管理一个块的索引文件（哈希表）。

### 初始化流程

```
IndexHandle(base_path, block_id)
    → 构造 MMapFileOperation，路径为 base_path/index/block_id

create(block_id, bucket_size=1000, mmap_option)
    → 写入 IndexHeader + 1000 个空桶
    → flush 到磁盘
    → mmap 到内存

load(block_id, bucket_size, mmap_option)
    → mmap 到内存
    → 校验 block_id 和 bucket_size 是否匹配
```

DataNode 的策略是先尝试 `load`，失败（文件不存在）则 `create`。

### 哈希表操作

**写入（write_segment_meta）：**

```
1. hash_find(file_id) → 确认不存在（避免重复写）
2. hash_insert(file_id, meta)
   a. 计算 slot = file_id % 1000
   b. 若有空闲节点（free_head_offset != 0），复用它
      否则在 index_file_size 处追加新节点
   c. 将 MetaInfo 写入索引文件
   d. 更新链表指针（前置节点的 next_meta 或桶的头指针）
```

**读取（read_segment_meta）：**

```
1. hash_find(file_id) → 找到 current_offset
2. pread_file(current_offset) → 读出 MetaInfo
```

**删除（delete_segment_meta）：**

```
1. hash_find(file_id) → 找到 current_offset 和 previous_offset
2. 从链表中摘除该节点（修改前置节点的 next_meta 或桶头指针）
3. 将被删节点加入空闲链表（free_head_offset，头插法）
4. update_block_info(C_OPER_DELETE, size)
```

**hash_find 的链表遍历：**

```cpp
int32_t slot = file_id % bucket_size;
int32_t pos  = bucket_slot[slot];  // 桶头
while (pos != 0) {
    读出 pos 处的 MetaInfo
    if (MetaInfo.file_id == key) → 找到，返回 current_offset
    previous_offset = pos;
    pos = MetaInfo.next_meta;      // 沿链表继续
}
// pos == 0 → 未找到
```

### set_index_header_offset 和 update_block_info

```cpp
// 写入文件后调用，更新主块文件的写入位置
void set_index_header_offset(int32_t size) {
    header->data_file_offset += size;  // 下次写入从这里开始
}

// 更新块统计信息
int update_block_info(OperType type, int32_t size) {
    if (INSERT) { version++; file_count++; seq_no++; current_size += size; }
    if (DELETE) { version++; file_count--; current_size -= size;
                  del_file_count++; del_size += size; }
}
```
