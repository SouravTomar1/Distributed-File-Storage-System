cmake_minimum_required(VERSION 3.20)

project(DistributedFileStorage)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)


# ============================================================
# Dependencies
# ============================================================

find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(PkgConfig REQUIRED)

pkg_check_modules(PQXX REQUIRED libpqxx)


# ============================================================
# Protocol Buffer
# ============================================================

set(PROTO_FILE
    ${CMAKE_CURRENT_SOURCE_DIR}/proto/storage.proto
)

set(PROTO_DIR
    ${CMAKE_CURRENT_SOURCE_DIR}/proto
)

set(GENERATED_PROTO_SRCS
    ${PROTO_DIR}/storage.pb.cc
    ${PROTO_DIR}/storage.grpc.pb.cc
)

set(GENERATED_PROTO_HDRS
    ${PROTO_DIR}/storage.pb.h
    ${PROTO_DIR}/storage.grpc.pb.h
)


add_custom_command(
    OUTPUT
        ${GENERATED_PROTO_SRCS}
        ${GENERATED_PROTO_HDRS}

    COMMAND
        protobuf::protoc

    ARGS
        --proto_path=${PROTO_DIR}
        --cpp_out=${PROTO_DIR}
        --grpc_out=${PROTO_DIR}
        --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
        ${PROTO_FILE}

    DEPENDS
        ${PROTO_FILE}

    COMMENT
        "Generating C++ protobuf and gRPC files..."
)


set(PROTO_SOURCES
    ${GENERATED_PROTO_SRCS}
)


# ============================================================
# Storage Node
# ============================================================

add_executable(StorageNode
    storage/StorageNode.cpp
    network/TcpServer.cpp
    ${PROTO_SOURCES}
)


target_include_directories(StorageNode
    PRIVATE
        ${PROTO_DIR}
        ${CMAKE_SOURCE_DIR}
)


target_link_libraries(StorageNode
    PRIVATE
        gRPC::grpc++
        protobuf::libprotobuf
        ws2_32
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
        ${PROTO_DIR}
)


target_link_libraries(Client
    PRIVATE
        gRPC::grpc++
        protobuf::libprotobuf
)


# ============================================================
# Master
# ============================================================

add_executable(Master
    master/Master.cpp
    ${PROTO_SOURCES}
)


target_include_directories(Master
    PRIVATE
        ${PROTO_DIR}
        ${PQXX_INCLUDE_DIRS}
)


target_link_directories(Master
    PRIVATE
        ${PQXX_LIBRARY_DIRS}
)


target_link_libraries(Master
    PRIVATE
        gRPC::grpc++
        protobuf::libprotobuf
        ${PQXX_LIBRARIES}
)