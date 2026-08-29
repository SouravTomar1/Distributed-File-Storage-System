cmake_minimum_required(VERSION 3.20)

project(DistributedFileStorage)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)


# ============================================================
# Dependencies
# ============================================================

find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)


# ============================================================
# Common generated protobuf sources
# ============================================================

set(PROTO_SOURCES
    proto/storage.pb.cc
    proto/storage.grpc.pb.cc
)


# ============================================================
# Storage Node
# ============================================================

add_executable(StorageNode

    storage/StorageNode.cpp

    ${PROTO_SOURCES}
)

target_include_directories(StorageNode
    PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/proto
)

target_link_libraries(StorageNode
    PRIVATE
    gRPC::grpc++
    protobuf::libprotobuf
)


# ============================================================
# Client
# ============================================================

add_executable(Client

    client/Client.cpp

    ${PROTO_SOURCES}
)

target_include_directories(Client
    PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/proto
)

target_link_libraries(Client
    PRIVATE
    gRPC::grpc++
    protobuf::libprotobuf
)


# ============================================================
# Master Server
# ============================================================

add_executable(Master

    master/Master.cpp

    ${PROTO_SOURCES}
)

target_include_directories(Master
    PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/proto
)

target_link_libraries(Master
    PRIVATE
    gRPC::grpc++
    protobuf::libprotobuf
)