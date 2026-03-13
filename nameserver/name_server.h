#pragma once
#include <grpcpp/grpcpp.h>
#include "nameserver.grpc.pb.h"
#include "block_manager.h"

namespace minitfs {

class NameServerServiceImpl final : public NameServerService::Service {
public:
    explicit NameServerServiceImpl(BlockManager& mgr) : mgr_(mgr) {}

    grpc::Status AllocateBlock(grpc::ServerContext* ctx,
                               const AllocateBlockRequest* req,
                               AllocateBlockResponse* resp) override;

    grpc::Status GetBlockLocation(grpc::ServerContext* ctx,
                                  const GetBlockLocationRequest* req,
                                  GetBlockLocationResponse* resp) override;

    grpc::Status CommitFile(grpc::ServerContext* ctx,
                            const CommitFileRequest* req,
                            CommitFileResponse* resp) override;

    grpc::Status DeleteFile(grpc::ServerContext* ctx,
                            const DeleteFileRequest* req,
                            DeleteFileResponse* resp) override;

    grpc::Status Heartbeat(grpc::ServerContext* ctx,
                           const HeartbeatRequest* req,
                           HeartbeatResponse* resp) override;

    grpc::Status BlockReport(grpc::ServerContext* ctx,
                             const BlockReportRequest* req,
                             BlockReportResponse* resp) override;

private:
    BlockManager& mgr_;
};

} // namespace minitfs
