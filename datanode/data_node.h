#pragma once
#include <string>
#include <unordered_map>
#include <mutex>
#include <memory>
#include <grpcpp/grpcpp.h>
#include "datanode.grpc.pb.h"
#include "nameserver.grpc.pb.h"

// storage 层头文件
#include "../storage/common.h"
#include "../storage/file_op.h"
#include "../storage/index_handle.h"
#include <thread>
#include <atomic>
#include <dirent.h>

namespace minitfs {

// 每个 block 的上下文：持有 IndexHandle + 主块文件操作
struct BlockContext {
    std::mutex mu;   // 每个 block 独立的锁
    std::unique_ptr<qiniu::largefile::IndexHandle>   index;
    std::unique_ptr<qiniu::largefile::FileOperation> main_block;
    std::string main_block_path;
    uint32_t    block_id;
};

class DataNodeServiceImpl final : public DataNodeService::Service {
public:
    // base_path: 数据存储根目录（如 "./data"）
    // ns_addr:   NameServer 地址（用于心跳）
    // self_id/ip/port: 本节点信息
    DataNodeServiceImpl(const std::string& base_path,
                        const std::string& ns_addr,
                        const std::string& self_id,
                        const std::string& self_ip,
                        int32_t self_port);
    ~DataNodeServiceImpl();

    grpc::Status WriteBlock(grpc::ServerContext* ctx,
                            grpc::ServerReader<WriteBlockRequest>* reader,
                            WriteBlockResponse* resp) override;

    grpc::Status ReadBlock(grpc::ServerContext* ctx,
                           const ReadBlockRequest* req,
                           grpc::ServerWriter<ReadBlockResponse>* writer) override;

    grpc::Status DeleteBlock(grpc::ServerContext* ctx,
                             const DeleteBlockRequest* req,
                             DeleteBlockResponse* resp) override;

    // 启动心跳后台线程
    void start_heartbeat();

    // 向 NameServer 上报本节点持有的所有 block
    void send_block_report();

private:
    std::shared_ptr<BlockContext> get_or_create_block(uint64_t block_id);

    std::string base_path_;
    std::string self_id_;
    std::string self_ip_;
    int32_t     self_port_;

    std::mutex                                                      mu_;
    std::unordered_map<uint64_t, std::shared_ptr<BlockContext>>     blocks_;

    std::shared_ptr<grpc::Channel>                      ns_channel_;
    std::unique_ptr<NameServerService::Stub>            ns_stub_;

    std::atomic<bool>    running_{true};
    std::thread          heartbeat_thread_;
    std::atomic<int32_t> active_connections_{0}; // 当前活跃写/读请求数

    static const qiniu::largefile::MMapOption kMMapOpt;
    static const int32_t kBucketSize  = 1000;
    static const int32_t kMainBlockSz = 1024 * 1024 * 64; // 64MB
};

} // namespace minitfs
