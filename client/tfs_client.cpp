#include "tfs_client.h"
#include <fstream>
#include <iostream>
#include <future>
#include <random>
#include <zlib.h>

namespace minitfs {

TfsClient::TfsClient(const std::string& ns_addr) {
    auto channel = grpc::CreateChannel(ns_addr, grpc::InsecureChannelCredentials());
    ns_stub_ = NameServerService::NewStub(channel);
}

// 向单个 DataNode 写入数据，流式读取本地文件，返回 WriteBlockResponse
static WriteBlockResponse write_to_node(
        const BlockLocation& loc,
        uint64_t file_id,
        const std::string& local_path,
        int64_t file_size) {

    std::string addr = loc.datanode_ip() + ":" + std::to_string(loc.datanode_port());
    auto stub = DataNodeService::NewStub(
        grpc::CreateChannel(addr, grpc::InsecureChannelCredentials()));

    WriteBlockResponse resp;
    grpc::ClientContext ctx;
    auto writer = stub->WriteBlock(&ctx, &resp);

    std::ifstream ifs(local_path, std::ios::binary);
    if (!ifs) {
        resp.set_status(-1);
        resp.set_message("cannot open local file");
        return resp;
    }

    const int32_t chunk = 1024 * 1024;
    int64_t sent = 0;
    std::vector<char> buf(chunk);
    while (sent < file_size) {
        int64_t to_send = std::min(static_cast<int64_t>(chunk), file_size - sent);
        ifs.read(buf.data(), to_send);
        int64_t actually_read = ifs.gcount();
        if (actually_read <= 0) break;
        WriteBlockRequest req;
        req.set_block_id(loc.block_id());
        req.set_file_id(file_id);
        req.set_data(buf.data(), actually_read);
        req.set_is_last(sent + actually_read >= file_size);
        writer->Write(req);
        sent += actually_read;
    }
    writer->WritesDone();
    auto st = writer->Finish();
    if (!st.ok()) {
        resp.set_status(-1);
        resp.set_message(st.error_message());
    }
    return resp;
}

int64_t TfsClient::upload_file(const std::string& local_path, int replica_num) {
    // 只获取文件大小，不读内容
    std::ifstream ifs(local_path, std::ios::binary | std::ios::ate);
    if (!ifs) {
        std::cerr << "[Client] cannot open file: " << local_path << "\n";
        return -1;
    }
    int64_t file_size = ifs.tellg();
    ifs.close();

    // 1. 向 NameServer 申请副本节点列表
    AllocateBlockRequest alloc_req;
    alloc_req.set_file_size(file_size);
    alloc_req.set_replica_num(replica_num);
    AllocateBlockResponse alloc_resp;
    {
        grpc::ClientContext ctx;
        auto st = ns_stub_->AllocateBlock(&ctx, alloc_req, &alloc_resp);
        if (!st.ok() || alloc_resp.status() != 0) {
            std::cerr << "[Client] AllocateBlock failed: " << alloc_resp.message() << "\n";
            return -1;
        }
    }

    uint64_t file_id  = alloc_resp.file_id();
    uint64_t block_id = alloc_resp.block_id();
    int n = alloc_resp.locations_size();
    if (n == 0) {
        std::cerr << "[Client] no datanode allocated\n";
        return -1;
    }

    // 2. 并行写入所有副本
    std::vector<std::future<WriteBlockResponse>> futures;
    for (int i = 0; i < n; ++i) {
        const BlockLocation& loc = alloc_resp.locations(i);
        futures.push_back(std::async(std::launch::async,
            write_to_node, loc, file_id, local_path, file_size));
    }

    // 3. 收集写入结果，至少 majority 成功才算成功
    int majority = n / 2 + 1;
    CommitFileRequest commit_req;
    commit_req.set_file_id(file_id);
    commit_req.set_block_id(block_id);

    int success = 0;
    for (int i = 0; i < n; ++i) {
        auto resp = futures[i].get();
        if (resp.status() == 0) {
            auto* cloc = commit_req.add_locations();
            cloc->set_block_id(block_id);
            cloc->set_datanode_id(alloc_resp.locations(i).datanode_id());
            cloc->set_offset(resp.offset());
            cloc->set_size(resp.size());
            ++success;
        } else {
            std::cerr << "[Client] replica write failed on "
                      << alloc_resp.locations(i).datanode_id()
                      << ": " << resp.message() << "\n";
        }
    }

    if (success < majority) {
        std::cerr << "[Client] upload failed: only " << success
                  << "/" << n << " replicas written\n";
        return -1;
    }

    // 4. 提交成功副本到 NameServer
    CommitFileResponse commit_resp;
    grpc::ClientContext ctx2;
    ns_stub_->CommitFile(&ctx2, commit_req, &commit_resp);

    std::cout << "[Client] upload success: file_id=" << file_id
              << " size=" << file_size
              << " replicas=" << success << "/" << n << "\n";
    return static_cast<int64_t>(file_id);
}

int TfsClient::download_file(uint64_t file_id, const std::string& local_path) {
    // 1. 查询所有存活副本
    GetBlockLocationRequest loc_req;
    loc_req.set_file_id(file_id);
    GetBlockLocationResponse loc_resp;
    {
        grpc::ClientContext ctx;
        auto st = ns_stub_->GetBlockLocation(&ctx, loc_req, &loc_resp);
        if (!st.ok() || loc_resp.status() != 0) {
            std::cerr << "[Client] GetBlockLocation failed: " << loc_resp.message() << "\n";
            return -1;
        }
    }

    // 2. 随机选一个副本读取（简单负载均衡）
    int n = loc_resp.locations_size();
    std::mt19937 rng(std::random_device{}());
    int idx = std::uniform_int_distribution<int>(0, n - 1)(rng);
    const auto& loc = loc_resp.locations(idx);

    std::string dn_addr = loc.datanode_ip() + ":" + std::to_string(loc.datanode_port());
    auto dn_stub = DataNodeService::NewStub(
        grpc::CreateChannel(dn_addr, grpc::InsecureChannelCredentials()));

    ReadBlockRequest read_req;
    read_req.set_block_id(loc.block_id());
    read_req.set_file_id(file_id);
    read_req.set_offset(loc.offset());
    read_req.set_size(loc.size());

    grpc::ClientContext ctx;
    auto reader = dn_stub->ReadBlock(&ctx, read_req);

    std::ofstream ofs(local_path, std::ios::binary);
    ReadBlockResponse chunk;
    uLong running_crc = crc32(0L, Z_NULL, 0);
    uint32_t server_crc = 0;
    while (reader->Read(&chunk)) {
        if (chunk.status() != 0) {
            std::cerr << "[Client] ReadBlock error: " << chunk.message() << "\n";
            return -1;
        }
        running_crc = crc32(running_crc,
            reinterpret_cast<const Bytef*>(chunk.data().data()), chunk.data().size());
        if (chunk.crc32() != 0) server_crc = chunk.crc32();
        ofs.write(chunk.data().data(), chunk.data().size());
    }
    if (server_crc != 0 && static_cast<uint32_t>(running_crc) != server_crc) {
        std::cerr << "[Client] CRC32 mismatch! data corrupted.\n";
        return -1;
    }

    std::cout << "[Client] download success: file_id=" << file_id
              << " from " << loc.datanode_id()
              << " -> " << local_path << "\n";
    return 0;
}

int TfsClient::delete_file(uint64_t file_id) {
    // 1. 查询所有副本位置
    GetBlockLocationRequest loc_req;
    loc_req.set_file_id(file_id);
    GetBlockLocationResponse loc_resp;
    {
        grpc::ClientContext ctx;
        ns_stub_->GetBlockLocation(&ctx, loc_req, &loc_resp);
        if (loc_resp.status() != 0) {
            std::cerr << "[Client] file not found\n";
            return -1;
        }
    }

    // 2. 通知所有副本节点删除
    for (const auto& loc : loc_resp.locations()) {
        std::string addr = loc.datanode_ip() + ":" + std::to_string(loc.datanode_port());
        auto dn_stub = DataNodeService::NewStub(
            grpc::CreateChannel(addr, grpc::InsecureChannelCredentials()));
        DeleteBlockRequest del_req;
        del_req.set_block_id(loc.block_id());
        del_req.set_file_id(file_id);
        DeleteBlockResponse del_resp;
        grpc::ClientContext ctx;
        dn_stub->DeleteBlock(&ctx, del_req, &del_resp);
    }

    // 3. 通知 NameServer 删除映射
    DeleteFileRequest ns_del;
    ns_del.set_file_id(file_id);
    DeleteFileResponse ns_resp;
    grpc::ClientContext ctx;
    ns_stub_->DeleteFile(&ctx, ns_del, &ns_resp);

    std::cout << "[Client] delete success: file_id=" << file_id << "\n";
    return 0;
}

} // namespace minitfs
