#include "TcpServer.h"

#include <iostream>
#include <string>


// ============================================================
// Constructor
// ============================================================

TcpServer::TcpServer(int port)
    : port_(port),
      serverSocket_(INVALID_SOCKET),
      initialized_(false) {
}


// ============================================================
// Start TCP Server
// ============================================================

bool TcpServer::Start() {

    WSADATA wsaData;


    // --------------------------------------------------------
    // Initialize Winsock
    // --------------------------------------------------------

    if (WSAStartup(
            MAKEWORD(2, 2),
            &wsaData) != 0) {

        std::cerr
            << "[TCP] WSAStartup failed."
            << std::endl;

        return false;
    }


    initialized_ = true;


    // --------------------------------------------------------
    // Create TCP socket
    // --------------------------------------------------------

    serverSocket_ =
        socket(
            AF_INET,
            SOCK_STREAM,
            IPPROTO_TCP
        );


    if (serverSocket_ == INVALID_SOCKET) {

        std::cerr
            << "[TCP] socket() failed. Error: "
            << WSAGetLastError()
            << std::endl;

        WSACleanup();

        initialized_ = false;

        return false;
    }


    // --------------------------------------------------------
    // Configure server address
    // --------------------------------------------------------

    sockaddr_in serverAddress{};

    serverAddress.sin_family =
        AF_INET;

    serverAddress.sin_addr.s_addr =
        INADDR_ANY;

    serverAddress.sin_port =
        htons(port_);


    // --------------------------------------------------------
    // Bind socket
    // --------------------------------------------------------

    if (bind(
            serverSocket_,
            reinterpret_cast<sockaddr*>(
                &serverAddress
            ),
            sizeof(serverAddress)
        ) == SOCKET_ERROR) {

        std::cerr
            << "[TCP] bind() failed. Error: "
            << WSAGetLastError()
            << std::endl;

        closesocket(serverSocket_);

        serverSocket_ =
            INVALID_SOCKET;

        WSACleanup();

        initialized_ = false;

        return false;
    }


    // --------------------------------------------------------
    // Listen for connections
    // --------------------------------------------------------

    if (listen(
            serverSocket_,
            SOMAXCONN
        ) == SOCKET_ERROR) {

        std::cerr
            << "[TCP] listen() failed. Error: "
            << WSAGetLastError()
            << std::endl;

        closesocket(serverSocket_);

        serverSocket_ =
            INVALID_SOCKET;

        WSACleanup();

        initialized_ = false;

        return false;
    }


    // --------------------------------------------------------
    // Server started successfully
    // --------------------------------------------------------

    std::cout
        << "[TCP] Server listening on port "
        << port_
        << std::endl;


    return true;
}


// ============================================================
// Run TCP Server
// ============================================================

void TcpServer::Run() {

    while (true) {

        // ----------------------------------------------------
        // Wait for client
        // ----------------------------------------------------

        std::cout
            << "[TCP] Waiting for connection..."
            << std::endl;


        SOCKET clientSocket =
            accept(
                serverSocket_,
                nullptr,
                nullptr
            );


        // ----------------------------------------------------
        // Check accept result
        // ----------------------------------------------------

        if (clientSocket == INVALID_SOCKET) {

            // ------------------------------------------------
            // If server socket was closed by Stop(),
            // exit the loop.
            // ------------------------------------------------

            if (serverSocket_ == INVALID_SOCKET) {
                break;
            }


            std::cerr
                << "[TCP] accept() failed. Error: "
                << WSAGetLastError()
                << std::endl;

            continue;
        }


        // ----------------------------------------------------
        // Client connected
        // ----------------------------------------------------

        std::cout
            << "[TCP] Client connected."
            << std::endl;


        // ----------------------------------------------------
        // Receive data
        // ----------------------------------------------------

        char buffer[1024];


        int bytesReceived =
            recv(
                clientSocket,
                buffer,
                sizeof(buffer) - 1,
                0
            );


        if (bytesReceived > 0) {

            buffer[bytesReceived] =
                '\0';


            std::cout
                << "[TCP] Received: "
                << buffer
                << std::endl;


            // ------------------------------------------------
            // Send response
            // ------------------------------------------------

            const std::string response =
                "ACK from Storage Node";


            send(
                clientSocket,
                response.c_str(),
                static_cast<int>(
                    response.size()
                ),
                0
            );
        }
        else if (bytesReceived == 0) {

            std::cout
                << "[TCP] Client disconnected "
                << "without sending data."
                << std::endl;
        }
        else {

            std::cerr
                << "[TCP] recv() failed. Error: "
                << WSAGetLastError()
                << std::endl;
        }


        // ----------------------------------------------------
        // Close client connection
        // ----------------------------------------------------

        closesocket(clientSocket);


        std::cout
            << "[TCP] Client disconnected."
            << std::endl;
    }


    std::cout
        << "[TCP] Server stopped."
        << std::endl;
}


// ============================================================
// Stop TCP Server
// ============================================================

void TcpServer::Stop() {

    if (serverSocket_ != INVALID_SOCKET) {

        closesocket(serverSocket_);

        serverSocket_ =
            INVALID_SOCKET;
    }
}


// ============================================================
// Destructor
// ============================================================

TcpServer::~TcpServer() {

    Stop();


    if (initialized_) {

        WSACleanup();

        initialized_ = false;
    }
}