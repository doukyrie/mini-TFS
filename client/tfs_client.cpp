#include "tfs_client.h"
#include <fstream>
#include <iostream>
#include <vector>

namespace minitfs {

TfsClient::TfsClient(const std::string& ns_addr) {
    auto channel = grpc::CreateChannel(ns_addr, grpc::InsecureChannelCredentials());
    ns_stub_ = NameServerService::NewStub(channel);
}

int64_t TfsClient::upload_file(const std::string& local_path) {
    // 读取本地文件
    std::ifstream ifs(local_path, std::ios::binary);
    if (!ifs) {
        std::cerr << "[Client] cannot open file: " << local_path << "\n";
        return -1;
    }
    std::vector<char> data((std::istreambuf_iterator<char>(ifs)),
                            std::istreambuf_iterator<char>());

    // 1. 向 NameServer 申请 block
    AllocateBlockRequest alloc_req;
    alloc_req.set_file_size(static_cast<int64_t>(data.size()));
    AllocateBlockResponse alloc_resp;
    {
        grpc::ClientContext ctx;
        auto st = ns_stub_->AllocateBlock(&ctx, alloc_req, &alloc_resp);
        if (!st.ok() || alloc_resp.status() != 0) {
            std::cerr << "[Client] AllocateBlock failed: " << alloc_resp.message() << "\n";
            return -1;
        }
    }

    const auto& loc = alloc_resp.location();
    uint64_t file_id  = alloc_resp.file_id();
    uint64_t block_id = loc.block_id();
    std::string dn_addr = loc.datanode_ip() + ":" + std::to_string(loc.datanode_port());

    // 2. 流式写入 DataNode
    auto dn_channel = grpc::CreateChannel(dn_addr, grpc::InsecureChannelCredentials());
    auto dn_stub    = DataNodeService::NewStub(dn_channel);

    WriteBlockResponse write_resp;
    grpc::ClientContext ctx;
    auto writer = dn_stub->WriteBlock(&ctx, &write_resp);

    const int32_t chunk = 1024 * 1024; // 1MB per chunk
    int32_t sent = 0;
    int32_t total = static_cast<int32_t>(data.size());
    while (sent < total) {
        int32_t to_send = std::min(chunk, total - sent);
        WriteBlockRequest req;
        req.set_block_id(block_id);
        req.set_file_id(file_id);
        req.set_data(data.data() + sent, to_send);
        req.set_is_last(sent + to_send >= total);
        writer->Write(req);
        sent += to_send;
    }
    writer->WritesDone();
    auto st = writer->Finish();

    if (!st.ok() || write_resp.status() != 0) {
        std::cerr << "[Client] WriteBlock failed: " << write_resp.message() << "\n";
        return -1;
    }

    // 3. 通知 NameServer 提交
    CommitFileRequest commit_req;
    commit_req.set_file_id(file_id);
    auto* cloc = commit_req.mutable_location();
    cloc->set_block_id(block_id);
    cloc->set_datanode_id(loc.datanode_id());
    cloc->set_offset(write_resp.offset());
    cloc->set_size(write_resp.size());

    CommitFileResponse commit_resp;
    grpc::ClientContext ctx2;
    ns_stub_->CommitFile(&ctx2, commit_req, &commit_resp);

    std::cout << "[Client] upload success: file_id=" << file_id
              << " size=" << data.size() << "\n";
    return static_cast<int64_t>(file_id);
}

int TfsClient::download_file(uint64_t file_id, const std::string& local_path) {
    // 1. 查询 NameServer 获取位置
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

    const auto& loc = loc_resp.location();
    std::string dn_addr = loc.datanode_ip() + ":" + std::to_string(loc.datanode_port());

    // 2. 从 DataNode 读取
    auto dn_channel = grpc::CreateChannel(dn_addr, grpc::InsecureChannelCredentials());
    auto dn_stub    = DataNodeService::NewStub(dn_channel);

    ReadBlockRequest read_req;
    read_req.set_block_id(loc.block_id());
    read_req.set_file_id(file_id);
    read_req.set_offset(loc.offset());
    read_req.set_size(loc.size());

    grpc::ClientContext ctx;
    auto reader = dn_stub->ReadBlock(&ctx, read_req);

    std::ofstream ofs(local_path, std::ios::binary);
    ReadBlockResponse chunk;
    while (reader->Read(&chunk)) {
        if (chunk.status() != 0) {
            std::cerr << "[Client] ReadBlock error: " << chunk.message() << "\n";
            return -1;
        }
        ofs.write(chunk.data().data(), chunk.data().size());
    }

    std::cout << "[Client] download success: file_id=" << file_id
              << " -> " << local_path << "\n";
    return 0;
}

int TfsClient::delete_file(uint64_t file_id) {
    // 1. 查询位置
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

    const auto& loc = loc_resp.location();
    std::string dn_addr = loc.datanode_ip() + ":" + std::to_string(loc.datanode_port());

    // 2. 通知 DataNode 删除
    auto dn_channel = grpc::CreateChannel(dn_addr, grpc::InsecureChannelCredentials());
    auto dn_stub    = DataNodeService::NewStub(dn_channel);

    DeleteBlockRequest del_req;
    del_req.set_block_id(loc.block_id());
    del_req.set_file_id(file_id);
    DeleteBlockResponse del_resp;
    {
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
