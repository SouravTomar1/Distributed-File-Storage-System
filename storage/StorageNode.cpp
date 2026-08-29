#include <iostream>
#include <fstream>
#include <filesystem>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>

#include "storage.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using storage::StorageService;
using storage::UploadRequest;
using storage::UploadResponse;
using storage::DownloadRequest;
using storage::DownloadResponse;

using storage::MasterService;
using storage::RegisterNodeRequest;
using storage::RegisterNodeResponse;


// ============================================================
// Storage Service Implementation
// ============================================================

class StorageServiceImpl final
    : public StorageService::Service {

public:

    Status UploadFile(
        ServerContext* context,
        const UploadRequest* request,
        UploadResponse* response) override {

        try {

            std::string directory =
                "storage_data/" + request->file_id();

            std::filesystem::create_directories(
                directory
            );

            std::string chunkPath =
                directory +
                "/chunk_" +
                std::to_string(request->chunk_id());

            std::ofstream file(
                chunkPath,
                std::ios::binary
            );

            if (!file) {

                response->set_success(false);

                response->set_message(
                    "Failed to create chunk file"
                );

                return Status::OK;
            }

            file.write(
                request->data().data(),
                static_cast<std::streamsize>(
                    request->data().size()
                )
            );

            file.close();

            response->set_success(true);

            response->set_message(
                "Chunk uploaded successfully"
            );

            std::cout
                << "Uploaded chunk "
                << request->chunk_id()
                << "/"
                << request->total_chunks()
                << " for file "
                << request->filename()
                << " ("
                << request->data().size()
                << " bytes)"
                << std::endl;

            return Status::OK;

        }
        catch (const std::exception& e) {

            response->set_success(false);

            response->set_message(
                std::string("Storage error: ") +
                e.what()
            );

            return Status::OK;
        }
    }


    // ========================================================
    // Download
    // ========================================================

    Status DownloadFile(
        ServerContext* context,
        const DownloadRequest* request,
        DownloadResponse* response) override {

        response->set_success(false);

        response->set_message(
            "Chunk-based download not implemented yet"
        );

        return Status::OK;
    }
};


// ============================================================
// Register Storage Node with Master
// ============================================================

bool RegisterWithMaster(
    int port,
    const std::string& nodeId) {

    const std::string masterAddress =
        "localhost:50050";

    std::cout
        << "Connecting to Master at "
        << masterAddress
        << "..."
        << std::endl;

    auto channel =
        grpc::CreateChannel(
            masterAddress,
            grpc::InsecureChannelCredentials()
        );

    std::unique_ptr<MasterService::Stub> stub =
        MasterService::NewStub(channel);

    RegisterNodeRequest request;

    request.set_node_id(nodeId);
    request.set_address("localhost");
    request.set_port(port);

    RegisterNodeResponse response;

    ClientContext context;

    Status status =
        stub->RegisterNode(
            &context,
            request,
            &response
        );

    if (!status.ok()) {

        std::cerr
            << "Failed to register with Master: "
            << status.error_message()
            << std::endl;

        return false;
    }

    if (!response.success()) {

        std::cerr
            << "Master rejected registration: "
            << response.message()
            << std::endl;

        return false;
    }

    std::cout
        << "Successfully registered "
        << nodeId
        << " with Master."
        << std::endl;

    return true;
}


// ============================================================
// Run Storage Server
// ============================================================

void RunServer(
    int port,
    const std::string& nodeId) {

    std::string serverAddress =
        "0.0.0.0:" +
        std::to_string(port);

    StorageServiceImpl service;

    ServerBuilder builder;

    builder.SetMaxReceiveMessageSize(
        16 * 1024 * 1024
    );

    builder.SetMaxSendMessageSize(
        16 * 1024 * 1024
    );

    builder.AddListeningPort(
        serverAddress,
        grpc::InsecureServerCredentials()
    );

    builder.RegisterService(&service);

    std::unique_ptr<Server> server =
        builder.BuildAndStart();

    if (!server) {

        std::cerr
            << "Failed to start server on "
            << serverAddress
            << std::endl;

        return;
    }

    std::cout
        << "========================================"
        << std::endl;

    std::cout
        << "      DISTRIBUTED FILE STORAGE"
        << std::endl;

    std::cout
        << "========================================"
        << std::endl;

    std::cout
        << "Storage Node: "
        << nodeId
        << std::endl;

    std::cout
        << "Storage Node running on "
        << serverAddress
        << std::endl;

    std::cout
        << "========================================"
        << std::endl;

    server->Wait();
}


// ============================================================
// Main
// ============================================================

int main(
    int argc,
    char* argv[]) {

    int port = 50051;

    if (argc >= 2) {

        try {

            port = std::stoi(argv[1]);

        }
        catch (...) {

            std::cerr
                << "Invalid port."
                << std::endl;

            return 1;
        }
    }


    // --------------------------------------------------------
    // Determine Node ID from port
    // --------------------------------------------------------

    std::string nodeId;

    if (port == 50051) {

        nodeId = "node-1";

    }
    else if (port == 50052) {

        nodeId = "node-2";

    }
    else if (port == 50053) {

        nodeId = "node-3";

    }
    else {

        nodeId =
            "node-" +
            std::to_string(port);
    }


    // --------------------------------------------------------
    // Register with Master
    // --------------------------------------------------------

    RegisterWithMaster(
        port,
        nodeId
    );


    // --------------------------------------------------------
    // Start Storage Server
    // --------------------------------------------------------

    RunServer(
        port,
        nodeId
    );

    return 0;
}