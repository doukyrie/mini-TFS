#include <iostream>
#include <string>
#include <grpcpp/grpcpp.h>
#include "name_server.h"
#include "block_manager.h"

int main(int argc, char** argv) {
    std::string addr = "0.0.0.0:50051";
    if (argc > 1) addr = argv[1];

    std::string wal_path = "./nameserver.wal";
    if (argc > 2) wal_path = argv[2];

    minitfs::BlockManager mgr(wal_path);

    // 启动心跳超时检测，节点宕机时打印日志（可在此扩展副本修复逻辑）
    mgr.start_dead_detector([](const std::string& dead_id) {
        std::cerr << "[NameServer] ALERT: DataNode [" << dead_id
                  << "] heartbeat timeout, marked as dead.\n";
        // TODO: 触发副本修复
    });

    minitfs::NameServerServiceImpl service(mgr);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    auto server = builder.BuildAndStart();
    std::cout << "[NameServer] listening on " << addr << "\n";
    server->Wait();
    return 0;
}
