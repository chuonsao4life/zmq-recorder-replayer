#include "ZmqBridge.h"
#include <iostream>
#include <string>
#include <csignal>
#include <memory>
#include <functional>

namespace {
std::function<void()> g_bridge_stop_callback;

void signalHandler(int signum) {
    std::cout << "\n[ZmqBridge] Caught signal " << signum << ", shutting down..." << std::endl;
    if (g_bridge_stop_callback) {
        g_bridge_stop_callback();
    }
}

void printHelp() {
    std::cout << "Usage: zmq_bridge [options]\n\n"
              << "Options:\n"
              << "  --tcp-host=<host>        TCP server host (default: 127.0.0.1)\n"
              << "  --tcp-port=<port>        TCP server port (default: 8889)\n"
              << "  --pub-endpoint=<addr>    ZMQ PUB endpoint (default: tcp://192.168.5.21:30003)\n"
              << "  --reconnect-ms=<ms>      Reconnect retry interval in ms (default: 1000)\n"
              << "  -h, --help               Display this help message\n";
}
} // namespace

int main(int argc, char* argv[]) {
    bridge::BridgeConfig config;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--help" || arg == "-h") {
            printHelp();
            return 0;
        } else if (arg.rfind("--tcp-host=", 0) == 0) {
            config.tcp_host = arg.substr(11);
        } else if (arg.rfind("--tcp-port=", 0) == 0) {
            config.tcp_port = static_cast<uint16_t>(std::stoul(arg.substr(11)));
        } else if (arg.rfind("--pub-endpoint=", 0) == 0) {
            config.pub_endpoint = arg.substr(15);
        } else if (arg.rfind("--reconnect-ms=", 0) == 0) {
            config.reconnect_interval_ms = static_cast<uint32_t>(std::stoul(arg.substr(15)));
        } else {
            std::cerr << "Unknown argument: " << arg << "\n";
            printHelp();
            return 1;
        }
    }

    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);

    auto bridge_app = std::make_unique<bridge::ZmqBridge>(std::move(config));
    g_bridge_stop_callback = [&bridge_app]() {
        if (bridge_app) bridge_app->stop();
    };

    if (!bridge_app->run()) {
        std::cerr << "[ZmqBridge] Bridge exited with error." << std::endl;
        return 1;
    }

    std::cout << "[ZmqBridge] Exited cleanly." << std::endl;
    return 0;
}
