#include "name_server.h"
#include <iostream>

namespace minitfs {

grpc::Status NameServerServiceImpl::AllocateBlock(
        grpc::ServerContext*, const AllocateBlockRequest* req,
        AllocateBlockResponse* resp) {

    DataNodeInfo* dn = mgr_.select_datanode();
    if (!dn) {
        resp->set_status(-1);
        resp->set_message("no available datanode");
        return grpc::Status::OK;
    }

    uint64_t file_id  = mgr_.alloc_file_id();
    uint64_t block_id = mgr_.alloc_block_id();

    auto* loc = resp->mutable_location();
    loc->set_block_id(block_id);
    loc->set_datanode_id(dn->id);
    loc->set_datanode_ip(dn->ip);
    loc->set_datanode_port(dn->port);

    resp->set_file_id(file_id);
    resp->set_status(0);
    resp->set_message("ok");

    std::cout << "[NameServer] AllocateBlock: file_id=" << file_id
              << " block_id=" << block_id
              << " -> " << dn->ip << ":" << dn->port << "\n";
    return grpc::Status::OK;
}

grpc::Status NameServerServiceImpl::GetBlockLocation(
        grpc::ServerContext*, const GetBlockLocationRequest* req,
        GetBlockLocationResponse* resp) {

    FileLocation loc;
    if (!mgr_.get_file_location(req->file_id(), loc)) {
        resp->set_status(-1);
        resp->set_message("file not found");
        return grpc::Status::OK;
    }

    DataNodeInfo* dn = mgr_.get_datanode(loc.datanode_id);
    if (!dn || !dn->is_alive()) {
        resp->set_status(-1);
        resp->set_message("datanode unavailable");
        return grpc::Status::OK;
    }

    auto* l = resp->mutable_location();
    l->set_block_id(loc.block_id);
    l->set_datanode_id(loc.datanode_id);
    l->set_datanode_ip(dn->ip);
    l->set_datanode_port(dn->port);
    l->set_offset(loc.offset);
    l->set_size(loc.size);

    resp->set_status(0);
    resp->set_message("ok");
    return grpc::Status::OK;
}

grpc::Status NameServerServiceImpl::CommitFile(
        grpc::ServerContext*, const CommitFileRequest* req,
        CommitFileResponse* resp) {

    FileLocation loc;
    loc.block_id    = req->location().block_id();
    loc.datanode_id = req->location().datanode_id();
    loc.offset      = req->location().offset();
    loc.size        = req->location().size();

    mgr_.commit_file(req->file_id(), loc);

    resp->set_status(0);
    resp->set_message("ok");
    std::cout << "[NameServer] CommitFile: file_id=" << req->file_id()
              << " offset=" << loc.offset << " size=" << loc.size << "\n";
    return grpc::Status::OK;
}

grpc::Status NameServerServiceImpl::DeleteFile(
        grpc::ServerContext*, const DeleteFileRequest* req,
        DeleteFileResponse* resp) {

    FileLocation loc;
    if (!mgr_.remove_file(req->file_id(), loc)) {
        resp->set_status(-1);
        resp->set_message("file not found");
        return grpc::Status::OK;
    }

    resp->set_status(0);
    resp->set_message("ok");
    std::cout << "[NameServer] DeleteFile: file_id=" << req->file_id() << "\n";
    return grpc::Status::OK;
}

grpc::Status NameServerServiceImpl::Heartbeat(
        grpc::ServerContext*, const HeartbeatRequest* req,
        HeartbeatResponse* resp) {

    DataNodeInfo info;
    info.id             = req->datanode_id();
    info.ip             = req->ip();
    info.port           = req->port();
    info.available_cap  = req->available_cap();
    info.block_count    = req->block_count();
    info.last_heartbeat = std::chrono::steady_clock::now();

    mgr_.register_datanode(info);

    resp->set_status(0);
    resp->set_message("ok");
    return grpc::Status::OK;
}

grpc::Status NameServerServiceImpl::BlockReport(
        grpc::ServerContext*, const BlockReportRequest* req,
        BlockReportResponse* resp) {

    std::cout << "[NameServer] BlockReport from " << req->datanode_id()
              << ": " << req->block_ids_size() << " blocks\n";
    resp->set_status(0);
    resp->set_message("ok");
    return grpc::Status::OK;
}

} // namespace minitfs
