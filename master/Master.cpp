
#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <mutex>
#include <thread>
#include <chrono>
#include <atomic>
#include <algorithm>
#include <cstdlib>

#include <grpcpp/grpcpp.h>
#include <pqxx/pqxx>

#include "storage.grpc.pb.h"


using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using storage::MasterService;

using storage::RegisterNodeRequest;
using storage::RegisterNodeResponse;

using storage::HeartbeatRequest;
using storage::HeartbeatResponse;

using storage::NodeHealthRequest;
using storage::NodeHealthResponse;

using storage::GetNodesRequest;
using storage::GetNodesResponse;

using storage::RecordChunkRequest;
using storage::RecordChunkResponse;

using storage::GetFileRequest;
using storage::GetFileResponse;

using storage::FileChunkLocation;


// ============================================================
// CONFIGURATION
// ============================================================

constexpr int HEARTBEAT_TIMEOUT_SECONDS = 15;

constexpr int HEALTH_CHECK_INTERVAL_SECONDS = 5;


// ============================================================
// STORAGE NODE STRUCTURE
// ============================================================

struct Node {

    std::string nodeId;

    std::string address;

    int port;

    bool healthy;

    std::chrono::steady_clock::time_point lastHeartbeat;
};


// ============================================================
// MASTER SERVICE
// ============================================================

class MasterServiceImpl final
    : public MasterService::Service {

public:

    MasterServiceImpl()
        : running_(true),
          dbReady_(false) {

        std::cout
            << "\nInitializing Master Server..."
            << std::endl;


        // ----------------------------------------------------
        // Initial Storage Nodes
        // ----------------------------------------------------

        AddInitialNode(
            "node-1",
            "localhost",
            50051
        );

        AddInitialNode(
            "node-2",
            "localhost",
            50052
        );

        AddInitialNode(
            "node-3",
            "localhost",
            50053
        );


        // ----------------------------------------------------
        // PostgreSQL
        // ----------------------------------------------------

        InitializeDatabase();


        // ----------------------------------------------------
        // Heartbeat Monitoring
        // ----------------------------------------------------

        healthMonitorThread_ =
            std::thread(
                &MasterServiceImpl::HealthMonitor,
                this
            );
    }


    ~MasterServiceImpl() override {

        running_ = false;


        if (healthMonitorThread_.joinable()) {

            healthMonitorThread_.join();
        }


        if (dbConnection_) {

            try {

                dbConnection_->close();

            }
            catch (...) {

                // Ignore shutdown errors
            }
        }
    }


    // ========================================================
    // DATABASE INITIALIZATION
    // ========================================================

    void InitializeDatabase() {

        const char* password =
            std::getenv("DFSS_DB_PASSWORD");


        if (password == nullptr) {

            std::cerr
                << "[PostgreSQL] ERROR: "
                << "DFSS_DB_PASSWORD is not set."
                << std::endl;

            return;
        }


        try {

            std::string connectionString =
                "host=127.0.0.1 "
                "port=5432 "
                "dbname=dfss "
                "user=postgres "
                "password=";

            connectionString += password;


            dbConnection_ =
                std::make_unique<pqxx::connection>(
                    connectionString
                );


            if (dbConnection_->is_open()) {

                dbReady_ = true;

                std::cout
                    << "[PostgreSQL] Connected successfully."
                    << std::endl;
            }
            else {

                std::cerr
                    << "[PostgreSQL] "
                    << "Connection failed."
                    << std::endl;
            }

        }
        catch (const std::exception& e) {

            std::cerr
                << "[PostgreSQL] Connection error: "
                << e.what()
                << std::endl;
        }
    }


    // ========================================================
    // REGISTER NODE
    // ========================================================

    Status RegisterNode(
        ServerContext* context,
        const RegisterNodeRequest* request,
        RegisterNodeResponse* response
    ) override {

        std::lock_guard<std::mutex> lock(
            mutex_
        );


        const std::string nodeId =
            request->node_id();

        const std::string address =
            request->address();

        const int port =
            request->port();


        auto now =
            std::chrono::steady_clock::now();


        auto existingNode =
            std::find_if(
                nodes_.begin(),
                nodes_.end(),
                [&](const Node& node) {

                    return node.nodeId == nodeId;
                }
            );


        // ----------------------------------------------------
        // Existing Node
        // ----------------------------------------------------

        if (existingNode != nodes_.end()) {

            existingNode->address = address;

            existingNode->port = port;

            existingNode->healthy = true;

            existingNode->lastHeartbeat = now;


            std::cout
                << "[Master] Node re-registered: "
                << nodeId
                << " -> "
                << address
                << ":"
                << port
                << std::endl;
        }


        // ----------------------------------------------------
        // New Node
        // ----------------------------------------------------

        else {

            Node newNode;

            newNode.nodeId = nodeId;

            newNode.address = address;

            newNode.port = port;

            newNode.healthy = true;

            newNode.lastHeartbeat = now;


            nodes_.push_back(
                newNode
            );


            std::cout
                << "[Master] New node registered: "
                << nodeId
                << " -> "
                << address
                << ":"
                << port
                << std::endl;
        }


        response->set_success(true);

        response->set_message(
            "Node registered successfully."
        );


        return Status::OK;
    }


    // ========================================================
    // HEARTBEAT
    // ========================================================

    Status Heartbeat(
        ServerContext* context,
        const HeartbeatRequest* request,
        HeartbeatResponse* response
    ) override {

        std::lock_guard<std::mutex> lock(
            mutex_
        );


        const std::string nodeId =
            request->node_id();


        auto nodeIt =
            std::find_if(
                nodes_.begin(),
                nodes_.end(),
                [&](const Node& node) {

                    return node.nodeId == nodeId;
                }
            );


        if (nodeIt == nodes_.end()) {

            response->set_success(false);

            response->set_message(
                "Node not registered."
            );

            return Status::OK;
        }


        nodeIt->address =
            request->address();

        nodeIt->port =
            request->port();

        nodeIt->healthy = true;

        nodeIt->lastHeartbeat =
            std::chrono::steady_clock::now();


        response->set_success(true);

        response->set_message(
            "Heartbeat received."
        );


        return Status::OK;
    }


    // ========================================================
    // CHECK NODE HEALTH
    // ========================================================

    Status CheckNodeHealth(
        ServerContext* context,
        const NodeHealthRequest* request,
        NodeHealthResponse* response
    ) override {

        std::lock_guard<std::mutex> lock(
            mutex_
        );


        const std::string nodeId =
            request->node_id();


        auto nodeIt =
            std::find_if(
                nodes_.begin(),
                nodes_.end(),
                [&](const Node& node) {

                    return node.nodeId == nodeId;
                }
            );


        if (nodeIt == nodes_.end()) {

            response->set_healthy(false);

            response->set_message(
                "Node not found."
            );

            return Status::OK;
        }


        response->set_healthy(
            nodeIt->healthy
        );


        if (nodeIt->healthy) {

            response->set_message(
                "Node is healthy."
            );
        }
        else {

            response->set_message(
                "Node is unhealthy."
            );
        }


        return Status::OK;
    }


    // ========================================================
    // GET STORAGE NODES
    // ========================================================

    Status GetNodes(
        ServerContext* context,
        const GetNodesRequest* request,
        GetNodesResponse* response
    ) override {

        std::lock_guard<std::mutex> lock(
            mutex_
        );


        for (const auto& node : nodes_) {

            auto* nodeInfo =
                response->add_nodes();


            nodeInfo->set_node_id(
                node.nodeId
            );

            nodeInfo->set_address(
                node.address
            );

            nodeInfo->set_port(
                node.port
            );

            nodeInfo->set_healthy(
                node.healthy
            );
        }


        return Status::OK;
    }


    // ========================================================
    // RECORD CHUNK
    // ========================================================

    Status RecordChunk(
        ServerContext* context,
        const RecordChunkRequest* request,
        RecordChunkResponse* response
    ) override {

        const std::string fileId =
            request->file_id();

        const int chunkId =
            request->chunk_id();

        const std::string nodeId =
            request->node_id();


        const std::string filename =
            request->filename();

        const long long fileSize =
            request->file_size();

        const int totalChunks =
            request->total_chunks();


        // ----------------------------------------------------
        // Validate Node
        // ----------------------------------------------------

        {

            std::lock_guard<std::mutex> lock(
                mutex_
            );


            auto nodeIt =
                std::find_if(
                    nodes_.begin(),
                    nodes_.end(),
                    [&](const Node& node) {

                        return node.nodeId == nodeId;
                    }
                );


            if (nodeIt == nodes_.end()) {

                response->set_success(false);

                response->set_message(
                    "Storage node does not exist."
                );

                return Status::OK;
            }
        }


        // ----------------------------------------------------
        // Check PostgreSQL
        // ----------------------------------------------------

        if (!dbReady_) {

            response->set_success(false);

            response->set_message(
                "PostgreSQL database unavailable."
            );

            return Status::OK;
        }


        // ----------------------------------------------------
        // Database Operation
        // ----------------------------------------------------

        std::lock_guard<std::mutex> dbLock(
            dbMutex_
        );


        try {

            pqxx::work transaction(
                *dbConnection_
            );


            // ------------------------------------------------
            // Store File Metadata
            // ------------------------------------------------

            if (
                !filename.empty() &&
                fileSize > 0 &&
                totalChunks > 0
            ) {

                transaction.exec_params(
                    "INSERT INTO files "
                    "(file_id, filename, file_size, total_chunks) "
                    "VALUES ($1, $2, $3, $4) "
                    "ON CONFLICT (file_id) DO UPDATE SET "
                    "filename = EXCLUDED.filename, "
                    "file_size = EXCLUDED.file_size, "
                    "total_chunks = EXCLUDED.total_chunks",

                    fileId,
                    filename,
                    fileSize,
                    totalChunks
                );
            }


            // ------------------------------------------------
            // Store Chunk Location
            // ------------------------------------------------

            transaction.exec_params(
                "INSERT INTO chunks "
                "(file_id, chunk_id, node_id) "
                "VALUES ($1, $2, $3) "
                "ON CONFLICT DO NOTHING",

                fileId,
                chunkId,
                nodeId
            );


            transaction.commit();


            std::cout
                << "[PostgreSQL] "
                << "File "
                << fileId
                << " | Chunk "
                << chunkId
                << " -> "
                << nodeId
                << std::endl;


            response->set_success(true);

            response->set_message(
                "Chunk metadata stored successfully."
            );
        }


        catch (const std::exception& e) {

            std::cerr
                << "[PostgreSQL] "
                << "Failed to store metadata: "
                << e.what()
                << std::endl;


            response->set_success(false);

            response->set_message(
                "Failed to store metadata."
            );
        }


        return Status::OK;
    }


    // ========================================================
    // GET FILE METADATA
    // ========================================================

    Status GetFile(
        ServerContext* context,
        const GetFileRequest* request,
        GetFileResponse* response
    ) override {

        const std::string fileId =
            request->file_id();


        // ----------------------------------------------------
        // Check PostgreSQL
        // ----------------------------------------------------

        if (!dbReady_) {

            response->set_success(false);

            response->set_message(
                "PostgreSQL database unavailable."
            );

            return Status::OK;
        }


        // ----------------------------------------------------
        // Database Operation
        // ----------------------------------------------------

        std::lock_guard<std::mutex> dbLock(
            dbMutex_
        );


        try {

            pqxx::work transaction(
                *dbConnection_
            );


            // ------------------------------------------------
            // Get File Metadata
            // ------------------------------------------------

            pqxx::result fileResult =
                transaction.exec_params(
                    "SELECT filename, file_size, total_chunks "
                    "FROM files "
                    "WHERE file_id = $1",
                    fileId
                );


            if (fileResult.empty()) {

                response->set_success(false);

                response->set_message(
                    "File not found."
                );

                return Status::OK;
            }


            const auto& fileRow =
                fileResult[0];


            response->set_filename(
                fileRow["filename"].as<std::string>()
            );


            response->set_file_size(
                fileRow["file_size"].as<long long>()
            );


            response->set_total_chunks(
                fileRow["total_chunks"].as<int>()
            );


            // ------------------------------------------------
            // Get Chunk Locations
            // ------------------------------------------------

            pqxx::result chunkResult =
                transaction.exec_params(
                    "SELECT chunk_id, node_id "
                    "FROM chunks "
                    "WHERE file_id = $1 "
                    "ORDER BY chunk_id, node_id",
                    fileId
                );


            for (const auto& row : chunkResult) {

                auto* location =
                    response->add_chunks();


                location->set_chunk_id(
                    row["chunk_id"].as<int>()
                );


                location->set_node_id(
                    row["node_id"].as<std::string>()
                );
            }


            transaction.commit();


            response->set_success(true);

            response->set_message(
                "File metadata retrieved successfully."
            );


            std::cout
                << "[Master] GetFile: "
                << fileId
                << " | Chunks: "
                << response->total_chunks()
                << " | Locations: "
                << response->chunks_size()
                << std::endl;


        }
        catch (const std::exception& e) {

            std::cerr
                << "[PostgreSQL] "
                << "Failed to retrieve file metadata: "
                << e.what()
                << std::endl;


            response->set_success(false);

            response->set_message(
                "Failed to retrieve file metadata."
            );
        }


        return Status::OK;
    }


    // ========================================================
    // DATABASE STATUS
    // ========================================================

    bool IsDatabaseReady() const {

        return dbReady_;
    }


private:


    // ========================================================
    // ADD INITIAL NODE
    // ========================================================

    void AddInitialNode(
        const std::string& nodeId,
        const std::string& address,
        int port
    ) {

        Node node;


        node.nodeId = nodeId;

        node.address = address;

        node.port = port;

        node.healthy = false;

        node.lastHeartbeat =
            std::chrono::steady_clock::now();


        nodes_.push_back(
            node
        );
    }


    // ========================================================
    // HEALTH MONITOR
    // ========================================================

    void HealthMonitor() {

        while (running_) {

            std::this_thread::sleep_for(
                std::chrono::seconds(
                    HEALTH_CHECK_INTERVAL_SECONDS
                )
            );


            std::lock_guard<std::mutex> lock(
                mutex_
            );


            auto now =
                std::chrono::steady_clock::now();


            for (auto& node : nodes_) {

                auto elapsed =
                    std::chrono::duration_cast<
                        std::chrono::seconds
                    >(
                        now -
                        node.lastHeartbeat
                    ).count();


                if (
                    node.healthy &&
                    elapsed >
                        HEARTBEAT_TIMEOUT_SECONDS
                ) {

                    node.healthy = false;


                    std::cout
                        << "\n===================================="
                        << std::endl;

                    std::cout
                        << "NODE FAILURE DETECTED"
                        << std::endl;

                    std::cout
                        << "Node: "
                        << node.nodeId
                        << std::endl;

                    std::cout
                        << "Last heartbeat: "
                        << elapsed
                        << " seconds ago"
                        << std::endl;

                    std::cout
                        << "Status: UNHEALTHY"
                        << std::endl;

                    std::cout
                        << "====================================\n"
                        << std::endl;
                }
            }
        }
    }


private:

    // ========================================================
    // STORAGE NODE STATE
    // ========================================================

    std::vector<Node> nodes_;

    std::mutex mutex_;


    // ========================================================
    // POSTGRESQL
    // ========================================================

    std::unique_ptr<pqxx::connection>
        dbConnection_;

    std::mutex dbMutex_;

    std::atomic<bool>
        dbReady_;


    // ========================================================
    // HEALTH MONITOR
    // ========================================================

    std::thread
        healthMonitorThread_;

    std::atomic<bool>
        running_;
};


// ============================================================
// RUN MASTER SERVER
// ============================================================

void RunServer(int port) {

    std::string serverAddress =
        "0.0.0.0:" +
        std::to_string(port);


    MasterServiceImpl service;


    ServerBuilder builder;


    builder.AddListeningPort(
        serverAddress,
        grpc::InsecureServerCredentials()
    );


    builder.RegisterService(
        &service
    );


    std::unique_ptr<Server> server(
        builder.BuildAndStart()
    );


    if (!server) {

        std::cerr
            << "Failed to start Master server."
            << std::endl;

        return;
    }


    std::cout
        << "\n===================================="
        << std::endl;

    std::cout
        << "       MASTER SERVER STARTED"
        << std::endl;

    std::cout
        << "===================================="
        << std::endl;

    std::cout
        << "Listening on: "
        << serverAddress
        << std::endl;

    std::cout
        << "Heartbeat timeout: "
        << HEARTBEAT_TIMEOUT_SECONDS
        << " seconds"
        << std::endl;

    std::cout
        << "Health check interval: "
        << HEALTH_CHECK_INTERVAL_SECONDS
        << " seconds"
        << std::endl;

    std::cout
        << "PostgreSQL: "
        << (
            service.IsDatabaseReady()
                ? "CONNECTED"
                : "NOT CONNECTED"
        )
        << std::endl;

    std::cout
        << "====================================\n"
        << std::endl;


    server->Wait();
}


// ============================================================
// MAIN
// ============================================================

int main(
    int argc,
    char** argv
) {

    int port = 50050;


    if (argc > 1) {

        port =
            std::stoi(
                argv[1]
            );
    }


    RunServer(port);


    return 0;
}
