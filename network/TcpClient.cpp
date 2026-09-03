#include <winsock2.h>
#include <ws2tcpip.h>

#include <iostream>
#include <string>

int main() {

    // Initialize Winsock
    WSADATA wsaData;

    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {

        std::cerr
            << "[TCP Client] WSAStartup failed."
            << std::endl;

        return 1;
    }

    // Create TCP socket
    SOCKET clientSocket =
        socket(
            AF_INET,
            SOCK_STREAM,
            IPPROTO_TCP
        );

    if (clientSocket == INVALID_SOCKET) {

        std::cerr
            << "[TCP Client] socket() failed. Error: "
            << WSAGetLastError()
            << std::endl;

        WSACleanup();

        return 1;
    }

    // Server address
    sockaddr_in serverAddress{};

    serverAddress.sin_family = AF_INET;

    serverAddress.sin_port =
        htons(60051);

    inet_pton(
        AF_INET,
        "127.0.0.1",
        &serverAddress.sin_addr
    );

    // Connect to server
    if (connect(
            clientSocket,
            reinterpret_cast<sockaddr*>(
                &serverAddress
            ),
            sizeof(serverAddress)
        ) == SOCKET_ERROR) {

        std::cerr
            << "[TCP Client] connect() failed. Error: "
            << WSAGetLastError()
            << std::endl;

        closesocket(clientSocket);

        WSACleanup();

        return 1;
    }

    std::cout
        << "[TCP Client] Connected to server."
        << std::endl;

    // Send message
    const std::string message =
        "HELLO FROM STORAGE NODE";

    send(
        clientSocket,
        message.c_str(),
        static_cast<int>(
            message.size()
        ),
        0
    );

    std::cout
        << "[TCP Client] Sent: "
        << message
        << std::endl;

    // Receive response
    char buffer[1024];

    int bytesReceived =
        recv(
            clientSocket,
            buffer,
            sizeof(buffer) - 1,
            0
        );

    if (bytesReceived > 0) {

        buffer[bytesReceived] = '\0';

        std::cout
            << "[TCP Client] Received: "
            << buffer
            << std::endl;
    }

    // Close connection
    closesocket(clientSocket);

    WSACleanup();

    return 0;
}
