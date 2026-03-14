#include "data_node.h"
#include <iostream>
#include <sstream>
#include <thread>
#include <chrono>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <zlib.h>

namespace minitfs {

const qiniu::largefile::MMapOption DataNodeServiceImpl::kMMapOpt = {1024*1024, 4*1024, 4*1024};

DataNodeServiceImpl::DataNodeServiceImpl(
        const std::string& base_path,
        const std::string& ns_addr,
        const std::string& self_id,
        const std::string& self_ip,
        int32_t self_port)
    : base_path_(base_path), self_id_(self_id),
      self_ip_(self_ip), self_port_(self_port) {

    // 创建数据目录
    mkdir((base_path_ + "/mainblock").c_str(), 0755);
    mkdir((base_path_ + "/index").c_str(), 0755);

    // 连接 NameServer
    ns_channel_ = grpc::CreateChannel(ns_addr, grpc::InsecureChannelCredentials());
    ns_stub_    = NameServerService::NewStub(ns_channel_);
}

DataNodeServiceImpl::~DataNodeServiceImpl() {
    running_ = false;
    if (heartbeat_thread_.joinable()) heartbeat_thread_.join();
}

void DataNodeServiceImpl::start_heartbeat() {
    heartbeat_thread_ = std::thread([this]() {
        while (running_) {
            HeartbeatRequest req;
            req.set_datanode_id(self_id_);
            req.set_ip(self_ip_);
            req.set_port(self_port_);
            struct statvfs vfs_stat;
            int64_t avail_cap = 1024LL * 1024 * 1024; // fallback
            if (statvfs(base_path_.c_str(), &vfs_stat) == 0) {
                avail_cap = static_cast<int64_t>(vfs_stat.f_bavail) * static_cast<int64_t>(vfs_stat.f_frsize);
            }
            req.set_available_cap(avail_cap);
            req.set_active_connections(active_connections_.load());
            {
                std::lock_guard<std::mutex> lk(mu_);
                req.set_block_count(static_cast<int32_t>(blocks_.size()));
            }

            HeartbeatResponse resp;
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
            auto st = ns_stub_->Heartbeat(&ctx, req, &resp);
            if (!st.ok()) {
                std::cerr << "[DataNode] heartbeat failed: " << st.error_message() << "\n";
            }
            std::this_thread::sleep_for(std::chrono::seconds(10));
        }
    });
}

std::shared_ptr<BlockContext> DataNodeServiceImpl::get_or_create_block(uint64_t block_id) {
    // 调用前需持有 mu_
    auto it = blocks_.find(block_id);
    if (it != blocks_.end()) return it->second;

    auto ctx = std::make_shared<BlockContext>();
    ctx->block_id = static_cast<uint32_t>(block_id);

    // 主块文件路径
    std::ostringstream oss;
    oss << base_path_ << "/mainblock/" << block_id;
    ctx->main_block_path = oss.str();

    // 创建主块文件
    ctx->main_block = std::make_unique<qiniu::largefile::FileOperation>(
        ctx->main_block_path, O_RDWR | O_LARGEFILE | O_CREAT);
    ctx->main_block->ftruncate_file(kMainBlockSz);

    // 创建索引
    ctx->index = std::make_unique<qiniu::largefile::IndexHandle>(base_path_, ctx->block_id);

    // 尝试 load，失败则 create
    int ret = ctx->index->load(ctx->block_id, kBucketSize, kMMapOpt);
    if (ret < 0) {
        ret = ctx->index->create(ctx->block_id, kBucketSize, kMMapOpt);
        if (ret < 0) {
            std::cerr << "[DataNode] create index failed for block " << block_id << "\n";
            return nullptr;
        }
    }

    blocks_[block_id] = ctx;
    return ctx;
}

grpc::Status DataNodeServiceImpl::WriteBlock(
        grpc::ServerContext*,
        grpc::ServerReader<WriteBlockRequest>* reader,
        WriteBlockResponse* resp) {

    ++active_connections_;
    struct Guard { std::atomic<int32_t>& c; ~Guard(){ --c; } } g{active_connections_};

    WriteBlockRequest req;
    std::vector<char> buf;
    uint64_t block_id = 0, file_id = 0;

    while (reader->Read(&req)) {
        if (block_id == 0) {
            block_id = req.block_id();
            file_id  = req.file_id();
        }
        const auto& d = req.data();
        buf.insert(buf.end(), d.begin(), d.end());
    }

    if (block_id == 0 || buf.empty()) {
        resp->set_status(-1);
        resp->set_message("empty request");
        return grpc::Status::OK;
    }

    std::shared_ptr<BlockContext> bctx;
    {
        std::lock_guard<std::mutex> lk(mu_);
        bctx = get_or_create_block(block_id);
    }
    if (!bctx) {
        resp->set_status(-1);
        resp->set_message("failed to open block");
        return grpc::Status::OK;
    }

    std::lock_guard<std::mutex> blk_lk(bctx->mu);

    int32_t offset = bctx->index->get_index_header()->data_file_offset;
    int32_t size   = static_cast<int32_t>(buf.size());

    // 计算 CRC32
    uint32_t crc = static_cast<uint32_t>(
        crc32(0, reinterpret_cast<const Bytef*>(buf.data()), buf.size()));

    int ret = bctx->main_block->pwrite_file(buf.data(), size, offset);
    if (ret < 0) {
        resp->set_status(-1);
        resp->set_message("pwrite_file failed");
        return grpc::Status::OK;
    }

    // 写索引
    qiniu::largefile::MetaInfo meta;
    meta.set_file_id(file_id);
    meta.set_offset(offset);
    meta.set_size(size);
    meta.set_crc32(crc);

    ret = bctx->index->write_segment_meta(meta.get_key(), meta);
    if (ret < 0) {
        resp->set_status(-1);
        resp->set_message("write_segment_meta failed");
        return grpc::Status::OK;
    }

    bctx->index->set_index_header_offset(size);
    bctx->index->update_block_info(qiniu::largefile::C_OPER_INSERT, size);
    bctx->index->flush();

    resp->set_status(0);
    resp->set_message("ok");
    resp->set_offset(offset);
    resp->set_size(size);
    resp->set_crc32(crc);

    std::cout << "[DataNode] WriteBlock: block=" << block_id
              << " file=" << file_id
              << " offset=" << offset << " size=" << size << "\n";
    return grpc::Status::OK;
}

grpc::Status DataNodeServiceImpl::ReadBlock(
        grpc::ServerContext*,
        const ReadBlockRequest* req,
        grpc::ServerWriter<ReadBlockResponse>* writer) {

    ++active_connections_;
    struct Guard { std::atomic<int32_t>& c; ~Guard(){ --c; } } g{active_connections_};

    std::shared_ptr<BlockContext> bctx;
    {
        std::lock_guard<std::mutex> lk(mu_);
        bctx = get_or_create_block(req->block_id());
    }
    if (!bctx) {
        ReadBlockResponse resp;
        resp.set_status(-1);
        resp.set_message("block not found");
        writer->Write(resp);
        return grpc::Status::OK;
    }

    std::lock_guard<std::mutex> blk_lk(bctx->mu);

    // 从索引获取 offset/size
    qiniu::largefile::MetaInfo meta;
    int ret = bctx->index->read_segment_meta(req->file_id(), meta);
    if (ret < 0) {
        ReadBlockResponse resp;
        resp.set_status(-1);
        resp.set_message("file not found in index");
        writer->Write(resp);
        return grpc::Status::OK;
    }

    int32_t offset = meta.get_offset();
    int32_t size   = meta.get_size();
    uint32_t stored_crc = meta.get_crc32();

    // 分块流式返回，每次最多 1MB
    const int32_t chunk = 1024 * 1024;
    int32_t sent = 0;
    uLong running_crc = crc32(0L, Z_NULL, 0);
    while (sent < size) {
        int32_t to_read = std::min(chunk, size - sent);
        std::vector<char> buf(to_read);
        ret = bctx->main_block->pread_file(buf.data(), to_read, offset + sent);
        if (ret < 0) {
            ReadBlockResponse resp;
            resp.set_status(-1);
            resp.set_message("pread_file failed");
            writer->Write(resp);
            return grpc::Status::OK;
        }
        running_crc = crc32(running_crc, reinterpret_cast<const Bytef*>(buf.data()), to_read);
        ReadBlockResponse resp;
        resp.set_status(0);
        resp.set_data(buf.data(), to_read);
        sent += to_read;
        if (sent >= size) {
            // 最后一个 chunk：验证并携带 CRC32
            if (stored_crc != 0 && static_cast<uint32_t>(running_crc) != stored_crc) {
                std::cerr << "[DataNode] CRC32 mismatch on read: block=" << req->block_id()
                          << " file=" << req->file_id() << "\n";
                resp.set_status(-1);
                resp.set_message("CRC32 mismatch");
                writer->Write(resp);
                return grpc::Status::OK;
            }
            resp.set_crc32(stored_crc);
        }
        writer->Write(resp);
    }

    std::cout << "[DataNode] ReadBlock: block=" << req->block_id()
              << " file=" << req->file_id() << " size=" << size << "\n";
    return grpc::Status::OK;
}

grpc::Status DataNodeServiceImpl::DeleteBlock(
        grpc::ServerContext*,
        const DeleteBlockRequest* req,
        DeleteBlockResponse* resp) {

    std::shared_ptr<BlockContext> bctx;
    {
        std::lock_guard<std::mutex> lk(mu_);
        bctx = get_or_create_block(req->block_id());
    }
    if (!bctx) {
        resp->set_status(-1);
        resp->set_message("block not found");
        return grpc::Status::OK;
    }

    std::lock_guard<std::mutex> blk_lk(bctx->mu);

    int ret = bctx->index->delete_segment_meta(req->file_id());
    if (ret < 0) {
        resp->set_status(-1);
        resp->set_message("delete_segment_meta failed");
        return grpc::Status::OK;
    }

    bctx->index->flush();
    resp->set_status(0);
    resp->set_message("ok");
    std::cout << "[DataNode] DeleteBlock: block=" << req->block_id()
              << " file=" << req->file_id() << "\n";
    return grpc::Status::OK;
}

} // namespace minitfs
