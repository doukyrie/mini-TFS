# mini-tfs

A simplified distributed file system inspired by TFS (Taobao File System), built with C++17 and gRPC.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                        Client                           │
│  upload / download / delete  (streaming, CRC32 verify)  │
└────────────────────┬────────────────────────────────────┘
                     │ gRPC
          ┌──────────▼──────────┐
          │     NameServer      │
          │  BlockManager       │
          │  - file → block map │
          │  - DataNode registry│
          │  - WAL persistence  │
          │  - smooth WRR LB    │
          └──┬──────────────┬───┘
             │ heartbeat    │ gRPC
    ┌────────▼───┐   ┌──────▼─────┐
    │  DataNode1 │   │  DataNode2 │  ...
    │  IndexHandle│  │  IndexHandle│
    │  FileOp    │   │  FileOp    │
    │  per-block │   │  per-block │
    │  mutex     │   │  mutex     │
    └────────────┘   └────────────┘
```

## Modules

| Module | Description |
|--------|-------------|
| `nameserver/` | Manages file metadata, block allocation, DataNode registry, WAL persistence |
| `datanode/` | Stores actual file data using IndexHandle + FileOperation (from `storage/`) |
| `client/` | Uploads/downloads/deletes files via NameServer + DataNode gRPC calls |
| `storage/` | Low-level block storage: mmap-based index, pread/pwrite file operations |
| `proto/` | gRPC service definitions for NameServer and DataNode |

## Key Features

- **WAL persistence**: NameServer metadata survives restarts via append-only WAL log
- **Per-block locking**: DataNode uses per-block mutexes instead of a global lock for better concurrency
- **Real disk capacity**: DataNode reports actual available disk space via `statvfs` for accurate load balancing
- **CRC32 integrity**: Data checksums computed on write, verified on read end-to-end
- **Streaming upload**: Client reads files in 1 MB chunks — no full-file memory load
- **Smooth WRR**: NameServer selects DataNodes using Nginx-style smooth weighted round-robin

## Build

```bash
# Prerequisites: gRPC + protobuf installed (e.g. under ~/.local), zlib-dev
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

## Run

```bash
# Terminal 1 — NameServer (WAL at ./ns.wal)
./nameserver 0.0.0.0:50051 ./ns.wal

# Terminal 2 — DataNode
./datanode 0.0.0.0:50052 localhost:50051 dn1 127.0.0.1 ./data_dn1

# Upload / download / verify
echo "hello mini-tfs" > /tmp/test.txt
FID=$(./client upload localhost:50051 /tmp/test.txt 1 | grep file_id | awk '{print $2}')
./client download localhost:50051 $FID /tmp/out.txt
diff /tmp/test.txt /tmp/out.txt && echo "OK"

# WAL persistence test: restart NameServer, file still accessible
kill <nameserver_pid>
./nameserver 0.0.0.0:50051 ./ns.wal
./client download localhost:50051 $FID /tmp/out2.txt
diff /tmp/test.txt /tmp/out2.txt && echo "WAL OK"
```

## Known Limitations

- Single block per file (no large-file splitting across multiple blocks)
- No automatic replica repair when a DataNode dies
- No TLS / authentication
- WAL is never compacted (grows unbounded)
