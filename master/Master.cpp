#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <mutex>

#include <grpcpp/grpcpp.h>

#include "storage.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using storage::MasterService;
using storage::RegisterNodeRequest;
using storage::RegisterNodeResponse;
using storage::NodeHealthRequest;
using storage::NodeHealthResponse;
using storage::GetNodesRequest;
using storage::GetNodesResponse;
using storage::StorageNodeInfo;

struct Node {
    std::string nodeId;
    std::string address;
    int port;
    bool healthy;
};

class MasterServiceImpl final : public MasterService::Service {

public:

    MasterServiceImpl() {

        nodes_.push_back({
            "node-1",
            "localhost",
            50051,
            false
        });

        nodes_.push_back({
            "node-2",
            "localhost",
            50052,
            false
        });

        nodes_.push_back({
            "node-3",
            "localhost",
            50053,
            false
        });
    }

    Status RegisterNode(
        ServerContext* context,
        const RegisterNodeRequest* request,
        RegisterNodeResponse* response) override {

        std::lock_guard<std::mutex> lock(mutex_);

        std::cout << "\nNode registration request received"
                  << std::endl;

        std::cout << "Node ID: "
                  << request->node_id()
                  << std::endl;

        std::cout << "Address: "
                  << request->address()
                  << ":"
                  << request->port()
                  << std::endl;

        for (auto& node : nodes_) {

            if (node.nodeId == request->node_id()) {

                node.address = request->address();
                node.port = request->port();
                node.healthy = true;

                response->set_success(true);
                response->set_message(
                    "Node registered successfully"
                );

                return Status::OK;
            }
        }

        Node newNode;

        newNode.nodeId = request->node_id();
        newNode.address = request->address();
        newNode.port = request->port();
        newNode.healthy = true;

        nodes_.push_back(newNode);

        response->set_success(true);
        response->set_message(
            "New node registered successfully"
        );

        return Status::OK;
    }

    Status CheckNodeHealth(
        ServerContext* context,
        const NodeHealthRequest* request,
        NodeHealthResponse* response) override {

        std::lock_guard<std::mutex> lock(mutex_);

        for (auto& node : nodes_) {

            if (node.nodeId == request->node_id()) {

                std::string serverAddress =
                    node.address + ":" +
                    std::to_string(node.port);

                auto channel =
                    grpc::CreateChannel(
                        serverAddress,
                        grpc::InsecureChannelCredentials()
                    );

                auto state = channel->GetState(true);

                bool healthy =
                    state == GRPC_CHANNEL_READY ||
                    state == GRPC_CHANNEL_IDLE ||
                    state == GRPC_CHANNEL_CONNECTING;

                node.healthy = healthy;

                response->set_healthy(healthy);

                if (healthy) {

                    response->set_message(
                        "Storage node is reachable"
                    );

                } else {

                    response->set_message(
                        "Storage node is unreachable"
                    );
                }

                return Status::OK;
            }
        }

        response->set_healthy(false);

        response->set_message(
            "Node not found"
        );

        return Status::OK;
    }

    Status GetNodes(
        ServerContext* context,
        const GetNodesRequest* request,
        GetNodesResponse* response) override {

        std::lock_guard<std::mutex> lock(mutex_);

        for (const auto& node : nodes_) {

            StorageNodeInfo* info =
                response->add_nodes();

            info->set_node_id(node.nodeId);
            info->set_address(node.address);
            info->set_port(node.port);
            info->set_healthy(node.healthy);
        }

        return Status::OK;
    }

private:

    std::vector<Node> nodes_;
    std::mutex mutex_;
};

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

    builder.RegisterService(&service);

    std::unique_ptr<Server> server =
        builder.BuildAndStart();

    if (!server) {

        std::cerr
            << "Failed to start Master Server."
            << std::endl;

        return;
    }

    std::cout
        << "\n========================================"
        << std::endl;

    std::cout
        << "       DISTRIBUTED FILE STORAGE"
        << std::endl;

    std::cout
        << "          MASTER SERVER"
        << std::endl;

    std::cout
        << "========================================"
        << std::endl;

    std::cout
        << "Master running on "
        << serverAddress
        << std::endl;

    std::cout
        << "Storage Nodes:"
        << std::endl;

    std::cout
        << "  node-1 -> localhost:50051"
        << std::endl;

    std::cout
        << "  node-2 -> localhost:50052"
        << std::endl;

    std::cout
        << "  node-3 -> localhost:50053"
        << std::endl;

    std::cout
        << "========================================"
        << std::endl;

    server->Wait();
}

int main(int argc, char* argv[]) {

    int port = 50050;

    if (argc > 1) {

        try {

            port = std::stoi(argv[1]);

        } catch (...) {

            std::cerr
                << "Invalid port."
                << std::endl;

            return 1;
        }
    }

    RunServer(port);

    return 0;
}