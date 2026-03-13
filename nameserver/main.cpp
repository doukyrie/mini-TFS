#include <iostream>
#include <string>
#include <grpcpp/grpcpp.h>
#include "name_server.h"
#include "block_manager.h"

int main(int argc, char** argv) {
    std::string addr = "0.0.0.0:50051";
    if (argc > 1) addr = argv[1];

    minitfs::BlockManager mgr;
    minitfs::NameServerServiceImpl service(mgr);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    auto server = builder.BuildAndStart();
    std::cout << "[NameServer] listening on " << addr << "\n";
    server->Wait();
    return 0;
}
