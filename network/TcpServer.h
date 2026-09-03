#pragma once

#include <winsock2.h>

class TcpServer {

public:

    explicit TcpServer(int port);

    bool Start();

    void Run();

    void Stop();

    ~TcpServer();

private:

    int port_;

    SOCKET serverSocket_;

    bool initialized_;
};
