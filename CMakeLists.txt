cmake_minimum_required(VERSION 3.22)
project(zmq_recorder_replay LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Optimization & Compiler warnings
add_compile_options(-O3 -Wall -Wextra -pthread)

# Find Qt6 Core
find_package(Qt6 REQUIRED COMPONENTS Core)

# Find ZeroMQ and cppzmq
find_path(CPPZMQ_INCLUDE_DIR zmq.hpp PATHS /usr/include /usr/local/include)
find_library(ZMQ_LIB NAMES zmq PATHS /usr/lib/x86_64-linux-gnu /usr/lib /usr/local/lib)

if(NOT ZMQ_LIB)
    message(FATAL_ERROR "libzmq not found!")
endif()

set(SOURCES
    src/recorder/TopicWriter.cpp
    src/recorder/RecorderEngine.cpp
    src/replay/TopicReader.cpp
    src/replay/ReplayEngine.cpp
    src/main.cpp
)

set(HEADERS
    src/common/Types.h
    src/common/EndianCodec.h
    src/recorder/BoundedByteQueue.h
    src/recorder/TopicWriter.h
    src/recorder/RecorderEngine.h
    src/replay/TopicReader.h
    src/replay/ReplayEngine.h
)

add_executable(zmq_recorder_replay ${SOURCES} ${HEADERS})

target_include_directories(zmq_recorder_replay PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CPPZMQ_INCLUDE_DIR}
)

target_link_libraries(zmq_recorder_replay PRIVATE
    Qt6::Core
    ${ZMQ_LIB}
    pthread
)

# Target for ZMQ Bridge
set(BRIDGE_SOURCES
    src/bridge/ZmqBridge.cpp
    src/bridge/main_bridge.cpp
)

set(BRIDGE_HEADERS
    src/bridge/ZmqBridge.h
)

add_executable(zmq_bridge ${BRIDGE_SOURCES} ${BRIDGE_HEADERS})

target_include_directories(zmq_bridge PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CPPZMQ_INCLUDE_DIR}
)

target_link_libraries(zmq_bridge PRIVATE
    ${ZMQ_LIB}
    pthread
)
