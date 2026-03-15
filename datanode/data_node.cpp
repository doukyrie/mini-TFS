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

void DataNodeServiceImpl::send_block_report() {
    BlockReportRequest req;
    req.set_datanode_id(self_id_);

    // Scan base_path_/index/ for block id files
    std::string index_dir = base_path_ + "/index";
    DIR* dir = opendir(index_dir.c_str());
    if (dir) {
        struct dirent* entry;
        while ((entry = readdir(dir)) != nullptr) {
            if (entry->d_name[0] == '.') continue;
            try {
                uint64_t bid = std::stoull(entry->d_name);
                req.add_block_ids(bid);
            } catch (...) {}
        }
        closedir(dir);
    }

    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    BlockReportResponse resp;
    auto st = ns_stub_->BlockReport(&ctx, req, &resp);
    if (!st.ok()) {
        std::cerr << "[DataNode] BlockReport failed: " << st.error_message() << "\n";
    } else {
        std::cout << "[DataNode] BlockReport sent: " << req.block_ids_size() << " blocks\n";
    }
}

void DataNodeServiceImpl::start_heartbeat() {
    heartbeat_thread_ = std::thread([this]() {
        bool reported = false;
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
            } else if (!reported) {
                reported = true;
                send_block_report();
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

    // Step 1: Read first chunk to extract block_id / file_id
    WriteBlockRequest req;
    if (!reader->Read(&req)) {
        resp->set_status(-1);
        resp->set_message("empty request");
        return grpc::Status::OK;
    }
    uint64_t block_id = req.block_id();
    uint64_t file_id  = req.file_id();
    std::vector<char> first_data(req.data().begin(), req.data().end());

    if (block_id == 0) {
        resp->set_status(-1);
        resp->set_message("invalid block_id");
        return grpc::Status::OK;
    }

    // Step 2: Get/create block context under global lock, then release
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

    // Step 3: Hold per-block lock for the entire write to prevent offset races
    std::lock_guard<std::mutex> blk_lk(bctx->mu);

    // Step 4: Read start_offset from index header
    int32_t start_offset  = bctx->index->get_index_header()->data_file_offset;
    int32_t total_written = 0;
    uLong   running_crc   = crc32(0L, Z_NULL, 0);

    // Step 5: Write first chunk
    if (!first_data.empty()) {
        int ret = bctx->main_block->pwrite_file(
            first_data.data(), static_cast<int32_t>(first_data.size()),
            start_offset);
        if (ret < 0) {
            resp->set_status(-1);
            resp->set_message("pwrite_file failed");
            return grpc::Status::OK;
        }
        running_crc = crc32(running_crc,
            reinterpret_cast<const Bytef*>(first_data.data()), first_data.size());
        total_written += static_cast<int32_t>(first_data.size());
    }

    // Step 6: Stream remaining chunks directly to disk
    while (reader->Read(&req)) {
        const auto& d = req.data();
        if (d.empty()) continue;
        int ret = bctx->main_block->pwrite_file(
            d.data(), static_cast<int32_t>(d.size()),
            start_offset + total_written);
        if (ret < 0) {
            resp->set_status(-1);
            resp->set_message("pwrite_file failed");
            return grpc::Status::OK;
        }
        running_crc = crc32(running_crc,
            reinterpret_cast<const Bytef*>(d.data()), d.size());
        total_written += static_cast<int32_t>(d.size());
    }

    if (total_written == 0) {
        resp->set_status(-1);
        resp->set_message("no data written");
        return grpc::Status::OK;
    }

    uint32_t crc = static_cast<uint32_t>(running_crc);

    // Step 7: Write index MetaInfo with final CRC32
    qiniu::largefile::MetaInfo meta;
    meta.set_file_id(file_id);
    meta.set_offset(start_offset);
    meta.set_size(total_written);
    meta.set_crc32(crc);

    int ret = bctx->index->write_segment_meta(meta.get_key(), meta);
    if (ret < 0) {
        resp->set_status(-1);
        resp->set_message("write_segment_meta failed");
        return grpc::Status::OK;
    }

    // Step 8: Update index header and flush
    bctx->index->set_index_header_offset(total_written);
    bctx->index->update_block_info(qiniu::largefile::C_OPER_INSERT, total_written);
    bctx->index->flush();

    resp->set_status(0);
    resp->set_message("ok");
    resp->set_offset(start_offset);
    resp->set_size(total_written);
    resp->set_crc32(crc);

    std::cout << "[DataNode] WriteBlock: block=" << block_id
              << " file=" << file_id
              << " offset=" << start_offset << " size=" << total_written << "\n";
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

grpc::Status DataNodeServiceImpl::CopyBlock(
        grpc::ServerContext*,
        const CopyBlockRequest* req,
        CopyBlockResponse* resp) {

    // 1. 建立到源 DataNode 的连接
    std::string src_addr = req->source_datanode_ip() + ":" + std::to_string(req->source_datanode_port());
    auto src_channel = grpc::CreateChannel(src_addr, grpc::InsecureChannelCredentials());
    auto src_stub = DataNodeService::NewStub(src_channel);

    // 2. 调用源节点的 ReadBlock 流式读取
    ReadBlockRequest read_req;
    read_req.set_block_id(req->block_id());
    read_req.set_file_id(req->file_id());
    read_req.set_offset(req->source_offset());
    read_req.set_size(req->source_size());

    grpc::ClientContext read_ctx;
    auto reader = src_stub->ReadBlock(&read_ctx, read_req);

    // 3. 边收边写到本地（复用 WriteBlock 逻辑）
    std::shared_ptr<BlockContext> bctx;
    {
        std::lock_guard<std::mutex> lk(mu_);
        bctx = get_or_create_block(req->block_id());
    }
    if (!bctx) {
        resp->set_status(-1);
        resp->set_message("failed to create block context");
        return grpc::Status::OK;
    }

    std::lock_guard<std::mutex> blk_lk(bctx->mu);

    // 读取 start_offset（与 WriteBlock 完全一致）
    int32_t start_offset  = bctx->index->get_index_header()->data_file_offset;
    int32_t total_written = 0;
    uLong   running_crc   = crc32(0L, Z_NULL, 0);
    uint32_t final_crc    = 0;
    ReadBlockResponse chunk;

    while (reader->Read(&chunk)) {
        if (chunk.status() != 0) {
            resp->set_status(-1);
            resp->set_message("source read failed: " + chunk.message());
            return grpc::Status::OK;
        }

        const auto& d = chunk.data();
        if (d.empty()) continue;

        int ret = bctx->main_block->pwrite_file(
            d.data(), static_cast<int32_t>(d.size()),
            start_offset + total_written);
        if (ret < 0) {
            resp->set_status(-1);
            resp->set_message("pwrite_file failed");
            return grpc::Status::OK;
        }
        running_crc = crc32(running_crc,
            reinterpret_cast<const Bytef*>(d.data()), d.size());
        total_written += static_cast<int32_t>(d.size());
        if (chunk.crc32() != 0) final_crc = chunk.crc32();
    }

    auto st = reader->Finish();
    if (!st.ok()) {
        resp->set_status(-1);
        resp->set_message("source stream error: " + st.error_message());
        return grpc::Status::OK;
    }

    if (total_written == 0) {
        resp->set_status(-1);
        resp->set_message("no data received from source");
        return grpc::Status::OK;
    }

    // 6. 写索引 MetaInfo
    qiniu::largefile::MetaInfo meta;
    meta.set_file_id(req->file_id());
    meta.set_offset(start_offset);
    meta.set_size(total_written);
    meta.set_crc32(final_crc);

    int ret = bctx->index->write_segment_meta(meta.get_key(), meta);
    if (ret < 0) {
        resp->set_status(-1);
        resp->set_message("write_segment_meta failed");
        return grpc::Status::OK;
    }

    // 7. 更新索引头，刷盘
    bctx->index->set_index_header_offset(total_written);
    bctx->index->update_block_info(qiniu::largefile::C_OPER_INSERT, total_written);
    bctx->index->flush();

    resp->set_status(0);
    resp->set_message("ok");
    resp->set_offset(start_offset);
    resp->set_size(total_written);
    resp->set_crc32(final_crc);

    std::cout << "[DataNode] CopyBlock: block=" << req->block_id()
              << " file=" << req->file_id()
              << " from=" << src_addr
              << " size=" << total_written << "\n";
    return grpc::Status::OK;
}

} // namespace minitfs
