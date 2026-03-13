#pragma once
#include <string>
#include <vector>
#include <grpcpp/grpcpp.h>
#include "nameserver.grpc.pb.h"
#include "datanode.grpc.pb.h"

namespace minitfs {

class TfsClient {
public:
    explicit TfsClient(const std::string& ns_addr);

    // 上传文件，返回 file_id（>0 成功，<0 失败）
    int64_t upload_file(const std::string& local_path);

    // 下载文件到 local_path，返回 0 成功
    int download_file(uint64_t file_id, const std::string& local_path);

    // 删除文件，返回 0 成功
    int delete_file(uint64_t file_id);

private:
    std::unique_ptr<NameServerService::Stub> ns_stub_;
};

} // namespace minitfs
