cmake_minimum_required(VERSION 3.16)
project(mini-tfs CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ---- 支持 $HOME/.local 安装的 gRPC/protobuf ----
list(APPEND CMAKE_PREFIX_PATH "$ENV{HOME}/.local")

# ---- 查找依赖 ----
find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(ZLIB REQUIRED)

# ---- Proto 预生成文件（手动运行 protoc 生成到 generated/）----
set(PROTO_OUT "${CMAKE_SOURCE_DIR}/generated")

add_library(proto_gen STATIC
    ${PROTO_OUT}/nameserver.pb.cc
    ${PROTO_OUT}/nameserver.grpc.pb.cc
    ${PROTO_OUT}/datanode.pb.cc
    ${PROTO_OUT}/datanode.grpc.pb.cc
)
target_include_directories(proto_gen PUBLIC ${PROTO_OUT})
target_link_libraries(proto_gen PUBLIC
    gRPC::grpc++
    protobuf::libprotobuf
)

# ---- storage 静态库 ----
add_library(storage STATIC
    storage/file_op.cpp
    storage/mmap_file.cpp
    storage/mmap_file_op.cpp
    storage/index_handle.cpp
)
target_include_directories(storage PUBLIC storage)

# ---- nameserver ----
add_executable(nameserver
    nameserver/main.cpp
    nameserver/name_server.cpp
    nameserver/block_manager.cpp
    nameserver/rebalance_manager.cpp
)
target_include_directories(nameserver PRIVATE nameserver)
target_link_libraries(nameserver PRIVATE proto_gen)

# ---- datanode ----
add_executable(datanode
    datanode/main.cpp
    datanode/data_node.cpp
)
target_include_directories(datanode PRIVATE datanode)
target_link_libraries(datanode PRIVATE proto_gen storage ZLIB::ZLIB)

# ---- client ----
add_executable(client
    client/main.cpp
    client/tfs_client.cpp
)
target_include_directories(client PRIVATE client)
target_link_libraries(client PRIVATE proto_gen ZLIB::ZLIB)
