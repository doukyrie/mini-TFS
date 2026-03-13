#include <iostream>
#include <string>
#include <grpcpp/grpcpp.h>
#include "data_node.h"

int main(int argc, char** argv) {
    // 用法: ./datanode <listen_addr> <ns_addr> <self_id> <self_ip> <data_dir>
    // 示例: ./datanode 0.0.0.0:50052 localhost:50051 dn1 127.0.0.1 ./data_dn1
    if (argc < 6) {
        std::cerr << "Usage: " << argv[0]
                  << " <listen_addr> <ns_addr> <self_id> <self_ip> <data_dir>\n";
        return 1;
    }

    std::string listen_addr = argv[1];
    std::string ns_addr     = argv[2];
    std::string self_id     = argv[3];
    std::string self_ip     = argv[4];
    std::string data_dir    = argv[5];

    // 解析端口
    int32_t self_port = 50052;
    auto pos = listen_addr.rfind(':');
    if (pos != std::string::npos) {
        self_port = std::stoi(listen_addr.substr(pos + 1));
    }

    minitfs::DataNodeServiceImpl service(data_dir, ns_addr, self_id, self_ip, self_port);
    service.start_heartbeat();

    grpc::ServerBuilder builder;
    builder.AddListeningPort(listen_addr, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    auto server = builder.BuildAndStart();
    std::cout << "[DataNode] " << self_id << " listening on " << listen_addr << "\n";
    server->Wait();
    return 0;
}
