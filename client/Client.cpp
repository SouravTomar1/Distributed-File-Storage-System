#include <iostream>
#include <fstream>
#include <memory>
#include <string>
#include <random>
#include <cstdint>
#include <vector>
#include <unordered_map>
#include <filesystem>

#include <grpcpp/grpcpp.h>

#include "storage.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using storage::StorageService;
using storage::UploadRequest;
using storage::UploadResponse;
using storage::DownloadRequest;
using storage::DownloadResponse;

using storage::MasterService;
using storage::GetNodesRequest;
using storage::GetNodesResponse;
using storage::StorageNodeInfo;

using storage::RecordChunkRequest;
using storage::RecordChunkResponse;

using storage::GetFileRequest;
using storage::GetFileResponse;


// ============================================================
// Configuration
// ============================================================

constexpr std::size_t CHUNK_SIZE =
    4 * 1024 * 1024;       // 4 MB

constexpr int GRPC_MESSAGE_LIMIT =
    16 * 1024 * 1024;      // 16 MB

constexpr int REPLICATION_FACTOR =
    2;


// ============================================================
// Storage Client
// ============================================================

class StorageClient {

public:

    explicit StorageClient(
        std::shared_ptr<Channel> channel)
        : stub_(
            StorageService::NewStub(
                channel
            )
        ) {}


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

        request.set_file_id(
            fileId
        );

        request.set_filename(
            filename
        );

        request.set_chunk_id(
            chunkId
        );

        request.set_total_chunks(
            totalChunks
        );

        request.set_data(
            data
        );

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


    // ========================================================
    // Download a single chunk
    // ========================================================

    bool DownloadChunk(
        const std::string& fileId,
        const std::string& filename,
        int chunkId,
        std::string& data) {

        DownloadRequest request;

        request.set_file_id(
            fileId
        );

        request.set_filename(
            filename
        );

        request.set_chunk_id(
            chunkId
        );


        DownloadResponse response;

        ClientContext context;


        Status status =
            stub_->DownloadFile(
                &context,
                request,
                &response
            );


        if (!status.ok()) {

            std::cerr
                << "Download RPC failed: "
                << status.error_message()
                << std::endl;

            return false;
        }


        if (!response.success()) {

            std::cerr
                << "Chunk download failed: "
                << response.message()
                << std::endl;

            return false;
        }


        data =
            response.data();


        return true;
    }


private:

    std::unique_ptr<
        StorageService::Stub
    > stub_;
};


// ============================================================
// Generate Unique File ID
// ============================================================

std::string GenerateFileId() {

    static std::random_device randomDevice;

    static std::mt19937_64 generator(
        randomDevice()
    );

    std::uniform_int_distribution<
        std::uint64_t
    > distribution;

    return std::to_string(
        distribution(
            generator
        )
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


    std::unique_ptr<
        MasterService::Stub
    > masterStub =
        MasterService::NewStub(
            masterChannel
        );


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


    // --------------------------------------------------------
    // Only use healthy nodes
    // --------------------------------------------------------

    for (
        const auto& node :
        response.nodes()
    ) {

        if (node.healthy()) {

            nodes.push_back(
                node
            );
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


    for (
        const auto& node :
        nodes
    ) {

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
// Create Storage Client
// ============================================================

std::unique_ptr<
    StorageClient
>
CreateStorageClient(
    const StorageNodeInfo& node) {

    std::string address =
        node.address() +
        ":" +
        std::to_string(
            node.port()
        );


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


    return std::make_unique<
        StorageClient
    >(
        channel
    );
}


// ============================================================
// Record Chunk Location in Master
// ============================================================

bool RecordChunkLocation(
    const std::string& fileId,
    const std::string& filename,
    long long fileSize,
    int totalChunks,
    int chunkId,
    const StorageNodeInfo& node) {

    std::cout
        << "\nRecording metadata in Master..."
        << std::endl;


    auto masterChannel =
        grpc::CreateChannel(
            "localhost:50050",
            grpc::InsecureChannelCredentials()
        );


    std::unique_ptr<
        MasterService::Stub
    > masterStub =
        MasterService::NewStub(
            masterChannel
        );


    RecordChunkRequest request;


    request.set_file_id(
        fileId
    );


    request.set_chunk_id(
        chunkId
    );


    request.set_node_id(
        node.node_id()
    );


    request.set_filename(
        filename
    );


    request.set_file_size(
        fileSize
    );


    request.set_total_chunks(
        totalChunks
    );


    RecordChunkResponse response;


    ClientContext context;


    Status status =
        masterStub->RecordChunk(
            &context,
            request,
            &response
        );


    if (!status.ok()) {

        std::cerr
            << "Metadata RPC failed: "
            << status.error_message()
            << std::endl;

        return false;
    }


    if (!response.success()) {

        std::cerr
            << "Metadata recording failed: "
            << response.message()
            << std::endl;

        return false;
    }


    std::cout
        << "Metadata recorded successfully:"
        << std::endl;


    std::cout
        << "  File ID: "
        << fileId
        << std::endl;


    std::cout
        << "  Filename: "
        << filename
        << std::endl;


    std::cout
        << "  File size: "
        << fileSize
        << " bytes"
        << std::endl;


    std::cout
        << "  Total chunks: "
        << totalChunks
        << std::endl;


    std::cout
        << "  Chunk ID: "
        << chunkId
        << std::endl;


    std::cout
        << "  Node: "
        << node.node_id()
        << std::endl;


    return true;
}


// ============================================================
// Upload Complete File
// ============================================================

bool UploadFile(
    const std::string& filename,
    std::string& uploadedFileId) {

    // ========================================================
    // Get nodes from Master
    // ========================================================

    std::vector<
        StorageNodeInfo
    > nodes;


    if (!GetStorageNodes(nodes)) {

        return false;
    }


    // ========================================================
    // Check replication requirement
    // ========================================================

    if (
        static_cast<int>(
            nodes.size()
        ) <
        REPLICATION_FACTOR
    ) {

        std::cerr
            << "\nNot enough healthy storage nodes."
            << std::endl;


        std::cerr
            << "Replication factor: "
            << REPLICATION_FACTOR
            << std::endl;


        std::cerr
            << "Healthy nodes available: "
            << nodes.size()
            << std::endl;


        std::cerr
            << "At least "
            << REPLICATION_FACTOR
            << " healthy nodes are required."
            << std::endl;


        return false;
    }


    // ========================================================
    // Open file
    // ========================================================

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


    // ========================================================
    // Determine file size
    // ========================================================

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


    // ========================================================
    // Calculate total chunks
    // ========================================================

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
                ) /
                CHUNK_SIZE
            );
    }


    // ========================================================
    // Generate File ID
    // ========================================================

    std::string fileId =
        GenerateFileId();


    uploadedFileId =
        fileId;


    // ========================================================
    // Display upload information
    // ========================================================

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
        << "Replication factor: "
        << REPLICATION_FACTOR
        << std::endl;


    std::cout
        << "gRPC message limit: "
        << GRPC_MESSAGE_LIMIT
        << " bytes (16 MB)"
        << std::endl;


    std::cout
        << "PostgreSQL metadata: ENABLED"
        << std::endl;


    std::cout
        << "========================================"
        << std::endl;


    // ========================================================
    // Upload every chunk
    // ========================================================

    for (
        int chunkId = 0;
        chunkId < totalChunks;
        ++chunkId
    ) {

        // ====================================================
        // Calculate chunk size
        // ====================================================

        std::size_t bytesToRead =
            CHUNK_SIZE;


        std::streamoff bytesAlreadyRead =
            static_cast<
                std::streamoff
            >(
                chunkId
            ) *
            CHUNK_SIZE;


        std::streamoff remainingBytes =
            fileSize -
            bytesAlreadyRead;


        if (remainingBytes < 0) {

            remainingBytes = 0;
        }


        if (
            remainingBytes <
            static_cast<
                std::streamoff
            >(
                CHUNK_SIZE
            )
        ) {

            bytesToRead =
                static_cast<
                    std::size_t
                >(
                    remainingBytes
                );
        }


        if (fileSize == 0) {

            bytesToRead = 0;
        }


        // ====================================================
        // Allocate buffer
        // ====================================================

        std::string buffer(
            bytesToRead,
            '\0'
        );


        // ====================================================
        // Read chunk
        // ====================================================

        if (bytesToRead > 0) {

            file.read(
                buffer.data(),
                static_cast<
                    std::streamsize
                >(
                    bytesToRead
                )
            );


            std::streamsize bytesRead =
                file.gcount();


            if (
                bytesRead !=
                static_cast<
                    std::streamsize
                >(
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


        // ====================================================
        // Select primary node
        // ====================================================

        const StorageNodeInfo& primaryNode =
            nodes[
                chunkId %
                nodes.size()
            ];


        // ====================================================
        // Select replica node
        // ====================================================

        const StorageNodeInfo& replicaNode =
            nodes[
                (
                    chunkId + 1
                ) %
                nodes.size()
            ];


        std::string primaryAddress =
            primaryNode.address() +
            ":" +
            std::to_string(
                primaryNode.port()
            );


        std::string replicaAddress =
            replicaNode.address() +
            ":" +
            std::to_string(
                replicaNode.port()
            );


        // ====================================================
        // Display chunk information
        // ====================================================

        std::cout
            << "\n========================================"
            << std::endl;


        std::cout
            << "Uploading chunk "
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
            << "Primary node: "
            << primaryNode.node_id()
            << " ("
            << primaryAddress
            << ")"
            << std::endl;


        std::cout
            << "Replica node: "
            << replicaNode.node_id()
            << " ("
            << replicaAddress
            << ")"
            << std::endl;


        std::cout
            << "========================================"
            << std::endl;


        // ====================================================
        // Create primary client
        // ====================================================

        auto primaryClient =
            CreateStorageClient(
                primaryNode
            );


        // ====================================================
        // Upload primary copy
        // ====================================================

        std::cout
            << "\nUploading chunk "
            << chunkId
            << " to PRIMARY "
            << primaryNode.node_id()
            << "..."
            << std::endl;


        bool primarySuccess =
            primaryClient->UploadChunk(
                fileId,
                filename,
                chunkId,
                totalChunks,
                buffer
            );


        if (!primarySuccess) {

            std::cerr
                << "\nPrimary upload failed."
                << std::endl;


            file.close();


            return false;
        }


        std::cout
            << "Primary copy completed on "
            << primaryNode.node_id()
            << "."
            << std::endl;


        // ====================================================
        // Record PRIMARY metadata
        // ====================================================

        if (
            !RecordChunkLocation(
                fileId,
                filename,
                static_cast<long long>(
                    fileSize
                ),
                totalChunks,
                chunkId,
                primaryNode
            )
        ) {

            std::cerr
                << "\nFailed to record primary metadata."
                << std::endl;


            file.close();


            return false;
        }


        // ====================================================
        // Create replica client
        // ====================================================

        auto replicaClient =
            CreateStorageClient(
                replicaNode
            );


        // ====================================================
        // Upload replica copy
        // ====================================================

        std::cout
            << "\nUploading chunk "
            << chunkId
            << " to REPLICA "
            << replicaNode.node_id()
            << "..."
            << std::endl;


        bool replicaSuccess =
            replicaClient->UploadChunk(
                fileId,
                filename,
                chunkId,
                totalChunks,
                buffer
            );


        if (!replicaSuccess) {

            std::cerr
                << "\nReplica upload failed."
                << std::endl;


            file.close();


            return false;
        }


        std::cout
            << "Replica copy completed on "
            << replicaNode.node_id()
            << "."
            << std::endl;


        // ====================================================
        // Record REPLICA metadata
        // ====================================================

        if (
            !RecordChunkLocation(
                fileId,
                filename,
                static_cast<long long>(
                    fileSize
                ),
                totalChunks,
                chunkId,
                replicaNode
            )
        ) {

            std::cerr
                << "\nFailed to record replica metadata."
                << std::endl;


            file.close();


            return false;
        }


        // ====================================================
        // Chunk successfully replicated
        // ====================================================

        std::cout
            << "\nChunk "
            << chunkId
            << " successfully replicated:"
            << std::endl;


        std::cout
            << "  Copy 1 -> "
            << primaryNode.node_id()
            << std::endl;


        std::cout
            << "  Copy 2 -> "
            << replicaNode.node_id()
            << std::endl;
    }


    // ========================================================
    // Close file
    // ========================================================

    file.close();


    // ========================================================
    // Upload complete
    // ========================================================

    std::cout
        << "\n========================================"
        << std::endl;


    std::cout
        << "Upload completed successfully!"
        << std::endl;


    std::cout
        << "Replication completed successfully!"
        << std::endl;


    std::cout
        << "PostgreSQL metadata recorded successfully!"
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
        << "Copies per chunk: "
        << REPLICATION_FACTOR
        << std::endl;


    std::cout
        << "Total chunk copies: "
        << totalChunks *
           REPLICATION_FACTOR
        << std::endl;


    std::cout
        << "========================================"
        << std::endl;


    return true;
}


// ============================================================
// Get File Metadata from Master
// ============================================================

bool GetFileMetadata(
    const std::string& fileId,
    GetFileResponse& response) {

    std::cout
        << "\nConnecting to Master for file metadata..."
        << std::endl;


    auto masterChannel =
        grpc::CreateChannel(
            "localhost:50050",
            grpc::InsecureChannelCredentials()
        );


    std::unique_ptr<
        MasterService::Stub
    > masterStub =
        MasterService::NewStub(
            masterChannel
        );


    GetFileRequest request;


    request.set_file_id(
        fileId
    );


    ClientContext context;


    Status status =
        masterStub->GetFile(
            &context,
            request,
            &response
        );


    if (!status.ok()) {

        std::cerr
            << "GetFile RPC failed: "
            << status.error_message()
            << std::endl;

        return false;
    }


    if (!response.success()) {

        std::cerr
            << "Master could not find file: "
            << response.message()
            << std::endl;

        return false;
    }


    std::cout
        << "\nFile metadata retrieved successfully."
        << std::endl;


    std::cout
        << "  Filename: "
        << response.filename()
        << std::endl;


    std::cout
        << "  File size: "
        << response.file_size()
        << " bytes"
        << std::endl;


    std::cout
        << "  Total chunks: "
        << response.total_chunks()
        << std::endl;


    std::cout
        << "  Chunk locations: "
        << response.chunks_size()
        << std::endl;


    return true;
}


// ============================================================
// Download Complete File
// ============================================================

bool DownloadFile(
    const std::string& fileId,
    const std::string& outputFilename) {

    std::cout
        << "\n========================================"
        << std::endl;


    std::cout
        << "        DISTRIBUTED FILE DOWNLOAD"
        << std::endl;


    std::cout
        << "========================================"
        << std::endl;


    std::cout
        << "File ID: "
        << fileId
        << std::endl;


    // ========================================================
    // Step 1: Get file metadata from Master
    // ========================================================

    GetFileResponse fileMetadata;


    if (
        !GetFileMetadata(
            fileId,
            fileMetadata
        )
    ) {

        return false;
    }


    // ========================================================
    // Step 2: Get current node health
    // ========================================================

    std::vector<
        StorageNodeInfo
    > healthyNodes;


    if (
        !GetStorageNodes(
            healthyNodes
        )
    ) {

        return false;
    }


    // ========================================================
    // Build node lookup table
    // ========================================================

    std::unordered_map<
        std::string,
        StorageNodeInfo
    > nodeMap;


    for (
        const auto& node :
        healthyNodes
    ) {

        nodeMap[
            node.node_id()
        ] =
            node;
    }


    // ========================================================
    // Organize chunk replicas
    // ========================================================

    std::unordered_map<
        int,
        std::vector<std::string>
    > chunkLocations;


    for (
        const auto& chunk :
        fileMetadata.chunks()
    ) {

        chunkLocations[
            chunk.chunk_id()
        ].push_back(
            chunk.node_id()
        );
    }


    // ========================================================
    // Check every chunk has metadata
    // ========================================================

    int totalChunks =
        fileMetadata.total_chunks();


    for (
        int chunkId = 0;
        chunkId < totalChunks;
        ++chunkId
    ) {

        if (
            chunkLocations.find(
                chunkId
            ) ==
            chunkLocations.end()
        ) {

            std::cerr
                << "\nERROR: No storage location found for chunk "
                << chunkId
                << "."
                << std::endl;

            return false;
        }
    }


    // ========================================================
    // Open output file
    // ========================================================

    std::ofstream outputFile(
        outputFilename,
        std::ios::binary
    );


    if (!outputFile) {

        std::cerr
            << "Could not create output file: "
            << outputFilename
            << std::endl;

        return false;
    }


    // ========================================================
    // Download chunks in order
    // ========================================================

    for (
        int chunkId = 0;
        chunkId < totalChunks;
        ++chunkId
    ) {

        std::cout
            << "\n========================================"
            << std::endl;


        std::cout
            << "Downloading chunk "
            << chunkId + 1
            << "/"
            << totalChunks
            << std::endl;


        std::cout
            << "Chunk ID: "
            << chunkId
            << std::endl;


        bool chunkDownloaded =
            false;


        // ====================================================
        // Try every replica
        // ====================================================

        for (
            const std::string& nodeId :
            chunkLocations[chunkId]
        ) {

            // ------------------------------------------------
            // Check whether Master currently considers node
            // healthy.
            // ------------------------------------------------

            auto nodeIterator =
                nodeMap.find(
                    nodeId
                );


            if (
                nodeIterator ==
                nodeMap.end()
            ) {

                std::cout
                    << "Node "
                    << nodeId
                    << " is currently unhealthy."
                    << std::endl;


                std::cout
                    << "Trying another replica..."
                    << std::endl;


                continue;
            }


            const StorageNodeInfo& node =
                nodeIterator->second;


            std::cout
                << "\nTrying node: "
                << node.node_id()
                << " ("
                << node.address()
                << ":"
                << node.port()
                << ")"
                << std::endl;


            // ------------------------------------------------
            // Create storage client
            // ------------------------------------------------

            auto storageClient =
                CreateStorageClient(
                    node
                );


            // ------------------------------------------------
            // Download chunk
            // ------------------------------------------------

            std::string chunkData;


            bool success =
                storageClient->DownloadChunk(
                    fileId,
                    fileMetadata.filename(),
                    chunkId,
                    chunkData
                );


            if (success) {

                std::cout
                    << "Chunk "
                    << chunkId
                    << " downloaded successfully from "
                    << node.node_id()
                    << "."
                    << std::endl;


                std::cout
                    << "Chunk size received: "
                    << chunkData.size()
                    << " bytes"
                    << std::endl;


                // --------------------------------------------
                // Write chunk sequentially
                // --------------------------------------------

                outputFile.write(
                    chunkData.data(),
                    static_cast<
                        std::streamsize
                    >(
                        chunkData.size()
                    )
                );


                if (!outputFile) {

                    std::cerr
                        << "Failed to write chunk "
                        << chunkId
                        << " to output file."
                        << std::endl;


                    outputFile.close();


                    return false;
                }


                chunkDownloaded =
                    true;


                break;
            }


            // ------------------------------------------------
            // Failover
            // ------------------------------------------------

            std::cout
                << "\nNode "
                << node.node_id()
                << " failed to provide chunk "
                << chunkId
                << "."
                << std::endl;


            std::cout
                << "FAILOVER: Trying another replica..."
                << std::endl;
        }


        // ====================================================
        // No replica worked
        // ====================================================

        if (!chunkDownloaded) {

            std::cerr
                << "\n========================================"
                << std::endl;


            std::cerr
                << "DOWNLOAD FAILED"
                << std::endl;


            std::cerr
                << "No healthy replica could provide chunk "
                << chunkId
                << "."
                << std::endl;


            std::cerr
                << "========================================"
                << std::endl;


            outputFile.close();


            return false;
        }


        std::cout
            << "Chunk "
            << chunkId
            << " successfully reconstructed."
            << std::endl;
    }


    // ========================================================
    // Close output file
    // ========================================================

    outputFile.close();


    // ========================================================
    // Verify reconstructed file size
    // ========================================================

    std::uintmax_t downloadedSize =
        std::filesystem::file_size(
            outputFilename
        );


    std::cout
        << "\n========================================"
        << std::endl;


    std::cout
        << "DOWNLOAD COMPLETED SUCCESSFULLY!"
        << std::endl;


    std::cout
        << "========================================"
        << std::endl;


    std::cout
        << "Original file: "
        << fileMetadata.filename()
        << std::endl;


    std::cout
        << "Downloaded file: "
        << outputFilename
        << std::endl;


    std::cout
        << "Expected size: "
        << fileMetadata.file_size()
        << " bytes"
        << std::endl;


    std::cout
        << "Downloaded size: "
        << downloadedSize
        << " bytes"
        << std::endl;


    if (
        downloadedSize ==
        static_cast<std::uintmax_t>(
            fileMetadata.file_size()
        )
    ) {

        std::cout
            << "Size verification: PASSED"
            << std::endl;

    } else {

        std::cerr
            << "Size verification: FAILED"
            << std::endl;
    }


    std::cout
        << "Chunks reconstructed: "
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
        << "\n========================================"
        << std::endl;

    std::cout
        << "      DFSS FAILOVER TEST"
        << std::endl;

    std::cout
        << "========================================"
        << std::endl;

    // Existing file uploaded before Node-1 failure
    std::string fileId =
        "13422908813796369801";

    std::string downloadedFilename =
        "downloaded_testfile_failover.txt";

    std::cout
        << "\nStarting failover download..."
        << std::endl;

    std::cout
        << "File ID: "
        << fileId
        << std::endl;

    if (
        !DownloadFile(
            fileId,
            downloadedFilename
        )
    ) {

        std::cerr
            << "\nFailover download FAILED!"
            << std::endl;

        return 1;
    }

    std::cout
        << "\n========================================"
        << std::endl;

    std::cout
        << "   FAILOVER TEST COMPLETED"
        << std::endl;

    std::cout
        << "========================================"
        << std::endl;

    std::cout
        << "Node-1: FAILED"
        << std::endl;

    std::cout
        << "Node-2: AVAILABLE"
        << std::endl;

    std::cout
        << "Replica failover: SUCCESS"
        << std::endl;

    std::cout
        << "File download: SUCCESS"
        << std::endl;

    std::cout
        << "========================================"
        << std::endl;

    return 0;
}