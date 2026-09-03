#include <iostream>
#include <fstream>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <atomic>
#include <chrono>

#include <grpcpp/grpcpp.h>

#include "storage.grpc.pb.h"
#include "network/TcpServer.h"


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

using storage::HeartbeatRequest;
using storage::HeartbeatResponse;


// ============================================================
// Storage Service Implementation
// ============================================================

class StorageServiceImpl final
    : public StorageService::Service {

public:

    // ========================================================
    // Constructor
    // ========================================================

    explicit StorageServiceImpl(
        const std::string& nodeId)
        : nodeId_(nodeId) {
    }


    // ========================================================
    // Upload
    // ========================================================

    Status UploadFile(
        ServerContext* context,
        const UploadRequest* request,
        UploadResponse* response) override {

        try {

            // ------------------------------------------------
            // Create directory for this file
            // ------------------------------------------------

            std::string directory =
                "storage_data/" +
                nodeId_ +
                "/" +
                request->file_id();

            std::filesystem::create_directories(
                directory
            );


            // ------------------------------------------------
            // Create chunk filename
            // ------------------------------------------------

            std::string chunkPath =
                directory +
                "/chunk_" +
                std::to_string(request->chunk_id());


            // ------------------------------------------------
            // Open chunk file
            // ------------------------------------------------

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


            // ------------------------------------------------
            // Write chunk data
            // ------------------------------------------------

            file.write(
                request->data().data(),
                static_cast<std::streamsize>(
                    request->data().size()
                )
            );


            if (!file) {

                response->set_success(false);

                response->set_message(
                    "Failed to write chunk data"
                );

                return Status::OK;
            }


            file.close();


            // ------------------------------------------------
            // Send successful response
            // ------------------------------------------------

            response->set_success(true);

            response->set_message(
                "Chunk uploaded successfully"
            );


            // ------------------------------------------------
            // Logging
            // ------------------------------------------------

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

        try {

            // ------------------------------------------------
            // Build chunk path
            // ------------------------------------------------

            std::string chunkPath =
                "storage_data/" +
                nodeId_ +
                "/" +
                request->file_id() +
                "/chunk_" +
                std::to_string(request->chunk_id());


            std::cout
                << "[Download] Request for file "
                << request->file_id()
                << ", chunk "
                << request->chunk_id()
                << std::endl;


            // ------------------------------------------------
            // Open chunk file
            // ------------------------------------------------

            std::ifstream file(
                chunkPath,
                std::ios::binary
            );


            if (!file) {

                response->set_success(false);

                response->set_message(
                    "Chunk not found on this storage node."
                );

                std::cerr
                    << "[Download] Chunk not found: "
                    << chunkPath
                    << std::endl;

                return Status::OK;
            }


            // ------------------------------------------------
            // Determine chunk size
            // ------------------------------------------------

            file.seekg(
                0,
                std::ios::end
            );


            std::streamsize fileSize =
                file.tellg();


            file.seekg(
                0,
                std::ios::beg
            );


            if (fileSize < 0) {

                response->set_success(false);

                response->set_message(
                    "Failed to determine chunk size."
                );

                return Status::OK;
            }


            // ------------------------------------------------
            // Read chunk data
            // ------------------------------------------------

            std::string data;


            data.resize(
                static_cast<std::size_t>(
                    fileSize
                )
            );


            if (fileSize > 0) {

                file.read(
                    data.data(),
                    fileSize
                );


                if (!file) {

                    response->set_success(false);

                    response->set_message(
                        "Failed to read chunk data."
                    );

                    return Status::OK;
                }
            }


            file.close();


            // ------------------------------------------------
            // Send chunk data
            // ------------------------------------------------

            response->set_success(true);

            response->set_message(
                "Chunk downloaded successfully."
            );

            response->set_data(
                data
            );


            // ------------------------------------------------
            // Logging
            // ------------------------------------------------

            std::cout
                << "[Download] Successfully sent chunk "
                << request->chunk_id()
                << " ("
                << data.size()
                << " bytes)"
                << std::endl;


            return Status::OK;

        }
        catch (const std::exception& e) {

            response->set_success(false);

            response->set_message(
                std::string("Storage download error: ") +
                e.what()
            );

            return Status::OK;
        }
    }


private:

    // ========================================================
    // Node-specific storage identifier
    // ========================================================

    std::string nodeId_;
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


    // --------------------------------------------------------
    // Create channel
    // --------------------------------------------------------

    auto channel =
        grpc::CreateChannel(
            masterAddress,
            grpc::InsecureChannelCredentials()
        );


    // --------------------------------------------------------
    // Create Master stub
    // --------------------------------------------------------

    std::unique_ptr<MasterService::Stub> stub =
        MasterService::NewStub(channel);


    // --------------------------------------------------------
    // Prepare registration request
    // --------------------------------------------------------

    RegisterNodeRequest request;

    request.set_node_id(nodeId);

    request.set_address("localhost");

    request.set_port(port);


    // --------------------------------------------------------
    // Prepare response
    // --------------------------------------------------------

    RegisterNodeResponse response;

    ClientContext context;


    // --------------------------------------------------------
    // Send registration RPC
    // --------------------------------------------------------

    Status status =
        stub->RegisterNode(
            &context,
            request,
            &response
        );


    // --------------------------------------------------------
    // Check gRPC status
    // --------------------------------------------------------

    if (!status.ok()) {

        std::cerr
            << "Failed to register with Master: "
            << status.error_message()
            << std::endl;

        return false;
    }


    // --------------------------------------------------------
    // Check Master response
    // --------------------------------------------------------

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
// Heartbeat Loop
// ============================================================

void HeartbeatLoop(
    int port,
    const std::string& nodeId,
    std::atomic<bool>& running) {

    const std::string masterAddress =
        "localhost:50050";


    // --------------------------------------------------------
    // Create channel to Master
    // --------------------------------------------------------

    auto channel =
        grpc::CreateChannel(
            masterAddress,
            grpc::InsecureChannelCredentials()
        );


    // --------------------------------------------------------
    // Create Master stub
    // --------------------------------------------------------

    std::unique_ptr<MasterService::Stub> stub =
        MasterService::NewStub(channel);


    std::cout
        << "Heartbeat service started for "
        << nodeId
        << std::endl;


    // --------------------------------------------------------
    // Continuously send heartbeats
    // --------------------------------------------------------

    while (running) {

        HeartbeatRequest request;

        request.set_node_id(nodeId);

        request.set_address("localhost");

        request.set_port(port);


        HeartbeatResponse response;

        ClientContext context;


        // ----------------------------------------------------
        // Send heartbeat
        // ----------------------------------------------------

        Status status =
            stub->Heartbeat(
                &context,
                request,
                &response
            );


        // ----------------------------------------------------
        // Check result
        // ----------------------------------------------------

        if (!status.ok()) {

            std::cerr
                << "[HEARTBEAT] "
                << nodeId
                << " failed: "
                << status.error_message()
                << std::endl;

        }
        else if (!response.success()) {

            std::cerr
                << "[HEARTBEAT] "
                << nodeId
                << " rejected by Master: "
                << response.message()
                << std::endl;

        }
        else {

            std::cout
                << "[HEARTBEAT] "
                << nodeId
                << " -> Master OK"
                << std::endl;
        }


        // ----------------------------------------------------
        // Wait before next heartbeat
        // ----------------------------------------------------

        for (int i = 0; i < 5 && running; ++i) {

            std::this_thread::sleep_for(
                std::chrono::seconds(1)
            );
        }
    }


    std::cout
        << "Heartbeat service stopped for "
        << nodeId
        << std::endl;
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


    // --------------------------------------------------------
    // Create storage service for this node
    // --------------------------------------------------------

    StorageServiceImpl service(nodeId);

    ServerBuilder builder;


    // --------------------------------------------------------
    // gRPC message limits
    // --------------------------------------------------------

    builder.SetMaxReceiveMessageSize(
        16 * 1024 * 1024
    );

    builder.SetMaxSendMessageSize(
        16 * 1024 * 1024
    );


    // --------------------------------------------------------
    // Listen on storage node port
    // --------------------------------------------------------

    builder.AddListeningPort(
        serverAddress,
        grpc::InsecureServerCredentials()
    );


    // --------------------------------------------------------
    // Register service
    // --------------------------------------------------------

    builder.RegisterService(
        &service
    );


    // --------------------------------------------------------
    // Start gRPC server
    // --------------------------------------------------------

    std::unique_ptr<Server> server =
        builder.BuildAndStart();


    if (!server) {

        std::cerr
            << "Failed to start server on "
            << serverAddress
            << std::endl;

        return;
    }


    // --------------------------------------------------------
    // Start TCP server
    // --------------------------------------------------------

    // gRPC:
    //   50051 -> node-1
    //
    // TCP:
    //   60051 -> node-1
    //
    // Therefore:
    //   TCP port = gRPC port + 10000

    int tcpPort =
        port + 10000;


    TcpServer tcpServer(
        tcpPort
    );


    if (!tcpServer.Start()) {

        std::cerr
            << "Failed to start TCP server on port "
            << tcpPort
            << std::endl;

        server->Shutdown();

        return;
    }


    // --------------------------------------------------------
    // Start TCP server thread
    // --------------------------------------------------------

    std::thread tcpThread(
        [&tcpServer]() {

            tcpServer.Run();

        }
    );


    // --------------------------------------------------------
    // Display server information
    // --------------------------------------------------------

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
        << "gRPC Server: "
        << serverAddress
        << std::endl;

    std::cout
        << "TCP Server:  "
        << "0.0.0.0:"
        << tcpPort
        << std::endl;

    std::cout
        << "========================================"
        << std::endl;


    // --------------------------------------------------------
    // Keep gRPC server running
    // --------------------------------------------------------

    server->Wait();


    // --------------------------------------------------------
    // Stop TCP server
    // --------------------------------------------------------

    tcpServer.Stop();


    // --------------------------------------------------------
    // Wait for TCP thread
    // --------------------------------------------------------

    if (tcpThread.joinable()) {

        tcpThread.join();
    }
}


// ============================================================
// Main
// ============================================================

int main(
    int argc,
    char* argv[]) {

    // --------------------------------------------------------
    // Default port
    // --------------------------------------------------------

    int port = 50051;


    // --------------------------------------------------------
    // Read port from command line
    // --------------------------------------------------------

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
    // Determine Node ID
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

    if (!RegisterWithMaster(
            port,
            nodeId)) {

        std::cerr
            << "Storage node could not register "
            << "with Master."
            << std::endl;

        return 1;
    }


    // --------------------------------------------------------
    // Start heartbeat thread
    // --------------------------------------------------------

    std::atomic<bool> running(true);


    std::thread heartbeatThread(
        HeartbeatLoop,
        port,
        nodeId,
        std::ref(running)
    );


    // --------------------------------------------------------
    // Start Storage Server
    // --------------------------------------------------------

    RunServer(
        port,
        nodeId
    );


    // --------------------------------------------------------
    // Stop heartbeat
    // --------------------------------------------------------

    running = false;


    // --------------------------------------------------------
    // Wait for heartbeat thread
    // --------------------------------------------------------

    if (heartbeatThread.joinable()) {

        heartbeatThread.join();
    }


    return 0;
}