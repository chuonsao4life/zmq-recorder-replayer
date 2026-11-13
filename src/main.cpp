#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <csignal>
#include <atomic>
#include <functional>
#include <memory>
#include <thread>
#include "recorder/RecorderEngine.h"
#include "replay/ReplayEngine.h"

namespace {
std::function<void()> g_stop_callback;

void signalHandler(int signum) {
    std::cout << "\n[Signal] Caught signal " << signum << ", initiating graceful shutdown..." << std::endl;
    if (g_stop_callback) {
        g_stop_callback();
    }
}

std::vector<std::string> splitTopics(const std::string& input) {
    std::vector<std::string> result;
    std::stringstream ss(input);
    std::string item;
    while (std::getline(ss, item, ',')) {
        if (!item.empty()) {
            result.push_back(item);
        }
    }
    return result;
}

void printHelp() {
    std::cout << "Usage: zmq_recorder_replay [options]\n\n"
              << "Options:\n"
              << "  --mode=record|replay           Operating mode (default: record)\n"
              << "  --sub-endpoint=<addr>          Recorder subscribe endpoint (default: tcp://192.168.5.21:30003)\n"
              << "  --pub-endpoint=<addr>          Replay publish endpoint (default: tcp://192.168.5.31:30004)\n"
              << "  --out-dir=<path>               Output directory for recorder (default: .)\n"
              << "  --session-dir=<path>           Session zdata directory to replay (e.g. ./session_id/zdata)\n"
              << "  --segment-sec=<seconds>        Segmentation duration (default: 300)\n"
              << "  --max-queue-mb=<mb>            Bounded queue memory limit in MB (default: 256)\n"
              << "  --endian=little|big            Header serialization endianness (default: little)\n"
              << "  --topics=<t1,t2,...>           Comma-separated topics (default: video,hit,ssr,adsb)\n"
              << "  --duration-sec=<seconds>       Auto-stop duration timeout in seconds (0 = infinite)\n"
              << "  --slow-joiner-ms=<ms>          Replay delay before first message (default: 1000)\n"
              << "  -h, --help                     Display this help message\n";
}
} // namespace

int main(int argc, char* argv[]) {
    std::string mode = "record";
    std::string sub_endpoint = "tcp://192.168.5.21:30003";
    std::string pub_endpoint = "tcp://192.168.5.31:30004";
    std::string out_dir = ".";
    std::string session_dir;
    uint32_t segment_sec = 300;
    size_t max_queue_mb = 256;
    bool use_big_endian = false;
    std::vector<std::string> topics = {"video", "hit", "ssr", "adsb"};
    uint32_t duration_sec = 0;
    uint32_t slow_joiner_ms = 1000;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--help" || arg == "-h") {
            printHelp();
            return 0;
        } else if (arg.rfind("--mode=", 0) == 0) {
            mode = arg.substr(7);
        } else if (arg.rfind("--sub-endpoint=", 0) == 0) {
            sub_endpoint = arg.substr(15);
        } else if (arg.rfind("--pub-endpoint=", 0) == 0) {
            pub_endpoint = arg.substr(15);
        } else if (arg.rfind("--out-dir=", 0) == 0) {
            out_dir = arg.substr(10);
        } else if (arg.rfind("--session-dir=", 0) == 0) {
            session_dir = arg.substr(14);
        } else if (arg.rfind("--segment-sec=", 0) == 0) {
            segment_sec = static_cast<uint32_t>(std::stoul(arg.substr(14)));
        } else if (arg.rfind("--max-queue-mb=", 0) == 0) {
            max_queue_mb = std::stoull(arg.substr(15));
        } else if (arg.rfind("--endian=", 0) == 0) {
            std::string endian_val = arg.substr(9);
            use_big_endian = (endian_val == "big");
        } else if (arg.rfind("--topics=", 0) == 0) {
            topics = splitTopics(arg.substr(9));
        } else if (arg.rfind("--duration-sec=", 0) == 0) {
            duration_sec = static_cast<uint32_t>(std::stoul(arg.substr(15)));
        } else if (arg.rfind("--slow-joiner-ms=", 0) == 0) {
            slow_joiner_ms = static_cast<uint32_t>(std::stoul(arg.substr(17)));
        } else {
            std::cerr << "Unknown argument: " << arg << "\n";
            printHelp();
            return 1;
        }
    }

    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);

    if (mode == "record") {
        recorder::RecorderConfig config;
        config.sub_endpoint = sub_endpoint;
        config.out_dir = out_dir;
        config.topics = topics;
        config.segment_sec = segment_sec;
        config.max_queue_mb = max_queue_mb;
        config.use_big_endian = use_big_endian;
        config.duration_sec = duration_sec;

        auto engine = std::make_unique<recorder::RecorderEngine>(std::move(config));
        g_stop_callback = [&engine]() {
            if (engine) engine->stop();
        };

        if (!engine->start()) {
            std::cerr << "Failed to start recorder engine." << std::endl;
            return 1;
        }

        engine->wait();
        engine->printSummary();

    } else if (mode == "replay") {
        if (session_dir.empty()) {
            std::cerr << "ERROR: --session-dir is required in replay mode.\n";
            printHelp();
            return 1;
        }

        recorder::ReplayConfig config;
        config.session_dir = session_dir;
        config.pub_endpoint = pub_endpoint;
        config.topics = topics;
        config.use_big_endian = use_big_endian;
        config.slow_joiner_ms = slow_joiner_ms;
        config.duration_sec = duration_sec;

        auto engine = std::make_unique<recorder::ReplayEngine>(std::move(config));
        g_stop_callback = [&engine]() {
            if (engine) engine->stop();
        };

        if (!engine->run()) {
            std::cerr << "Replay execution encountered errors." << std::endl;
            return 1;
        }

    } else {
        std::cerr << "Invalid mode: " << mode << " (expected 'record' or 'replay')\n";
        return 1;
    }

    std::cout << "[Main] Exited cleanly." << std::endl;
    return 0;
}
