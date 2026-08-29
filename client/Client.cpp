#include <iostream>
#include <fstream>
#include <memory>
#include <string>
#include <random>
#include <cstdint>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "storage.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using storage::StorageService;
using storage::UploadRequest;
using storage::UploadResponse;

using storage::MasterService;
using storage::GetNodesRequest;
using storage::GetNodesResponse;
using storage::StorageNodeInfo;


// ============================================================
// Configuration
// ============================================================

constexpr std::size_t CHUNK_SIZE =
    4 * 1024 * 1024;

constexpr int GRPC_MESSAGE_LIMIT =
    16 * 1024 * 1024;


// ============================================================
// Storage Client
// ============================================================

class StorageClient {

public:

    explicit StorageClient(
        std::shared_ptr<Channel> channel)
        : stub_(StorageService::NewStub(channel)) {}


    // ========================================================
    // Upload a single chunk
    // ========================================================

    bool UploadChunk(
        const std::string& fileId,
        const std::string& filename,
        int chunkId,
        int totalChunks,
        const std::string& data) {

        UploadRequest request;

        request.set_file_id(fileId);
        request.set_filename(filename);
        request.set_chunk_id(chunkId);
        request.set_total_chunks(totalChunks);
        request.set_data(data);


        UploadResponse response;

        ClientContext context;


        Status status =
            stub_->UploadFile(
                &context,
                request,
                &response
            );


        if (!status.ok()) {

            std::cerr
                << "Upload RPC failed: "
                << status.error_message()
                << std::endl;

            return false;
        }


        if (!response.success()) {

            std::cerr
                << "Chunk upload failed: "
                << response.message()
                << std::endl;

            return false;
        }


        std::cout
            << "Server response: "
            << response.message()
            << std::endl;


        return true;
    }


private:

    std::unique_ptr<StorageService::Stub> stub_;
};


// ============================================================
// Generate Unique File ID
// ============================================================

std::string GenerateFileId() {

    static std::random_device randomDevice;

    static std::mt19937_64 generator(
        randomDevice()
    );

    std::uniform_int_distribution<std::uint64_t>
        distribution;


    return std::to_string(
        distribution(generator)
    );
}


// ============================================================
// Get Available Nodes from Master
// ============================================================

bool GetStorageNodes(
    std::vector<StorageNodeInfo>& nodes) {

    std::cout
        << "Connecting to Master..."
        << std::endl;


    auto masterChannel =
        grpc::CreateChannel(
            "localhost:50050",
            grpc::InsecureChannelCredentials()
        );


    std::unique_ptr<MasterService::Stub> masterStub =
        MasterService::NewStub(masterChannel);


    GetNodesRequest request;

    GetNodesResponse response;

    ClientContext context;


    Status status =
        masterStub->GetNodes(
            &context,
            request,
            &response
        );


    if (!status.ok()) {

        std::cerr
            << "Failed to contact Master: "
            << status.error_message()
            << std::endl;

        return false;
    }


    for (const auto& node : response.nodes()) {

        if (node.healthy()) {

            nodes.push_back(node);
        }
    }


    if (nodes.empty()) {

        std::cerr
            << "No healthy storage nodes available."
            << std::endl;

        return false;
    }


    std::cout
        << "Master returned "
        << nodes.size()
        << " healthy storage node(s)."
        << std::endl;


    for (const auto& node : nodes) {

        std::cout
            << "  "
            << node.node_id()
            << " -> "
            << node.address()
            << ":"
            << node.port()
            << std::endl;
    }


    return true;
}


// ============================================================
// Create Storage Client for Node
// ============================================================

std::unique_ptr<StorageClient>
CreateStorageClient(
    const StorageNodeInfo& node) {

    std::string address =
        node.address() +
        ":" +
        std::to_string(node.port());


    grpc::ChannelArguments channelArgs;


    channelArgs.SetMaxReceiveMessageSize(
        GRPC_MESSAGE_LIMIT
    );


    channelArgs.SetMaxSendMessageSize(
        GRPC_MESSAGE_LIMIT
    );


    auto channel =
        grpc::CreateCustomChannel(
            address,
            grpc::InsecureChannelCredentials(),
            channelArgs
        );


    return std::make_unique<StorageClient>(
        channel
    );
}


// ============================================================
// Upload Complete File
// ============================================================

bool UploadFile(
    const std::string& filename) {


    // --------------------------------------------------------
    // Get nodes from Master
    // --------------------------------------------------------

    std::vector<StorageNodeInfo> nodes;


    if (!GetStorageNodes(nodes)) {

        return false;
    }


    // --------------------------------------------------------
    // Open file
    // --------------------------------------------------------

    std::ifstream file(
        filename,
        std::ios::binary
    );


    if (!file) {

        std::cerr
            << "Could not open file: "
            << filename
            << std::endl;

        return false;
    }


    // --------------------------------------------------------
    // Determine file size
    // --------------------------------------------------------

    file.seekg(
        0,
        std::ios::end
    );


    std::streamoff fileSize =
        file.tellg();


    file.seekg(
        0,
        std::ios::beg
    );


    if (fileSize < 0) {

        std::cerr
            << "Could not determine file size."
            << std::endl;

        return false;
    }


    // --------------------------------------------------------
    // Calculate total chunks
    // --------------------------------------------------------

    int totalChunks;


    if (fileSize == 0) {

        totalChunks = 1;

    } else {

        totalChunks =
            static_cast<int>(
                (
                    fileSize +
                    CHUNK_SIZE -
                    1
                ) / CHUNK_SIZE
            );
    }


    // --------------------------------------------------------
    // Generate File ID
    // --------------------------------------------------------

    std::string fileId =
        GenerateFileId();


    // --------------------------------------------------------
    // Display upload information
    // --------------------------------------------------------

    std::cout
        << "\n========================================"
        << std::endl;

    std::cout
        << "        DISTRIBUTED FILE STORAGE"
        << std::endl;

    std::cout
        << "========================================"
        << std::endl;


    std::cout
        << "File: "
        << filename
        << std::endl;


    std::cout
        << "File ID: "
        << fileId
        << std::endl;


    std::cout
        << "File size: "
        << fileSize
        << " bytes"
        << std::endl;


    std::cout
        << "Chunk size: "
        << CHUNK_SIZE
        << " bytes (4 MB)"
        << std::endl;


    std::cout
        << "Total chunks: "
        << totalChunks
        << std::endl;


    std::cout
        << "Available storage nodes: "
        << nodes.size()
        << std::endl;


    std::cout
        << "gRPC message limit: "
        << GRPC_MESSAGE_LIMIT
        << " bytes (16 MB)"
        << std::endl;


    std::cout
        << "========================================"
        << std::endl;


    // --------------------------------------------------------
    // Upload each chunk
    // --------------------------------------------------------

    for (
        int chunkId = 0;
        chunkId < totalChunks;
        ++chunkId
    ) {


        // ----------------------------------------------------
        // Calculate chunk size
        // ----------------------------------------------------

        std::size_t bytesToRead =
            CHUNK_SIZE;


        std::streamoff bytesAlreadyRead =
            static_cast<std::streamoff>(
                chunkId
            ) * CHUNK_SIZE;


        std::streamoff remainingBytes =
            fileSize -
            bytesAlreadyRead;


        if (remainingBytes < 0) {

            remainingBytes = 0;
        }


        if (
            remainingBytes <
            static_cast<std::streamoff>(
                CHUNK_SIZE
            )
        ) {

            bytesToRead =
                static_cast<std::size_t>(
                    remainingBytes
                );
        }


        // ----------------------------------------------------
        // Empty file
        // ----------------------------------------------------

        if (fileSize == 0) {

            bytesToRead = 0;
        }


        // ----------------------------------------------------
        // Allocate buffer
        // ----------------------------------------------------

        std::string buffer(
            bytesToRead,
            '\0'
        );


        // ----------------------------------------------------
        // Read chunk
        // ----------------------------------------------------

        if (bytesToRead > 0) {

            file.read(
                buffer.data(),
                static_cast<std::streamsize>(
                    bytesToRead
                )
            );


            std::streamsize bytesRead =
                file.gcount();


            if (
                bytesRead !=
                static_cast<std::streamsize>(
                    bytesToRead
                )
            ) {

                std::cerr
                    << "Failed to read chunk "
                    << chunkId
                    << std::endl;

                file.close();

                return false;
            }
        }


        // ----------------------------------------------------
        // Select storage node
        //
        // Round-robin distribution:
        //
        // chunk 0 -> node 0
        // chunk 1 -> node 1
        // chunk 2 -> node 2
        // chunk 3 -> node 0
        // ...
        // ----------------------------------------------------

        const StorageNodeInfo& node =
            nodes[
                chunkId % nodes.size()
            ];


        std::string nodeAddress =
            node.address() +
            ":" +
            std::to_string(node.port());


        // ----------------------------------------------------
        // Display chunk information
        // ----------------------------------------------------

        std::cout
            << "\nUploading chunk "
            << chunkId + 1
            << "/"
            << totalChunks
            << std::endl;


        std::cout
            << "Chunk ID: "
            << chunkId
            << std::endl;


        std::cout
            << "Chunk size: "
            << bytesToRead
            << " bytes"
            << std::endl;


        std::cout
            << "Target node: "
            << node.node_id()
            << " ("
            << nodeAddress
            << ")"
            << std::endl;


        // ----------------------------------------------------
        // Create client for selected node
        // ----------------------------------------------------

        auto client =
            CreateStorageClient(node);


        // ----------------------------------------------------
        // Upload chunk
        // ----------------------------------------------------

        bool success =
            client->UploadChunk(
                fileId,
                filename,
                chunkId,
                totalChunks,
                buffer
            );


        if (!success) {

            std::cerr
                << "\nFailed to upload chunk "
                << chunkId
                << " to "
                << node.node_id()
                << std::endl;

            file.close();

            return false;
        }


        std::cout
            << "Chunk "
            << chunkId
            << " uploaded successfully to "
            << node.node_id()
            << "."
            << std::endl;
    }


    // --------------------------------------------------------
    // Close file
    // --------------------------------------------------------

    file.close();


    // --------------------------------------------------------
    // Upload complete
    // --------------------------------------------------------

    std::cout
        << "\n========================================"
        << std::endl;


    std::cout
        << "Upload completed successfully!"
        << std::endl;


    std::cout
        << "File ID: "
        << fileId
        << std::endl;


    std::cout
        << "Chunks uploaded: "
        << totalChunks
        << std::endl;


    std::cout
        << "========================================"
        << std::endl;


    return true;
}


// ============================================================
// Main
// ============================================================

int main() {

    std::cout
        << "Connecting to Master..."
        << std::endl;


    std::string filename =
        "testfile.txt";


    if (
        !UploadFile(
            filename
        )
    ) {

        std::cerr
            << "\nUpload failed!"
            << std::endl;

        return 1;
    }


    std::cout
        << "\nFile uploaded successfully."
        << std::endl;


    return 0;
}