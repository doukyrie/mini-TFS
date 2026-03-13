#include <iostream>
#include <string>
#include "tfs_client.h"

static void print_usage(const char* prog) {
    std::cout << "Usage:\n"
              << "  " << prog << " upload   <ns_addr> <local_file>\n"
              << "  " << prog << " download <ns_addr> <file_id> <local_file>\n"
              << "  " << prog << " delete   <ns_addr> <file_id>\n";
}

int main(int argc, char** argv) {
    if (argc < 3) { print_usage(argv[0]); return 1; }

    std::string cmd     = argv[1];
    std::string ns_addr = argv[2];
    minitfs::TfsClient client(ns_addr);

    if (cmd == "upload") {
        if (argc < 4) { print_usage(argv[0]); return 1; }
        int64_t fid = client.upload_file(argv[3]);
        if (fid < 0) return 1;
        std::cout << "file_id: " << fid << "\n";

    } else if (cmd == "download") {
        if (argc < 5) { print_usage(argv[0]); return 1; }
        uint64_t fid = std::stoull(argv[3]);
        return client.download_file(fid, argv[4]);

    } else if (cmd == "delete") {
        if (argc < 4) { print_usage(argv[0]); return 1; }
        uint64_t fid = std::stoull(argv[3]);
        return client.delete_file(fid);

    } else {
        print_usage(argv[0]);
        return 1;
    }
    return 0;
}
