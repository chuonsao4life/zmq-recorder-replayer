cmake_minimum_required(VERSION 3.22)
project(my_zmq_recorder_replay LANGUAGES CXX)

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

# Source files (Incrementally added across Modules 1 -> 6)
set(SOURCES
    src/main.cpp
    # TODO [Module 4]: src/recorder/TopicWriter.cpp
    # TODO [Module 5]: src/recorder/RecorderEngine.cpp
    # TODO [Module 6]: src/replay/TopicReader.cpp
    # TODO [Module 6]: src/replay/ReplayEngine.cpp
)

set(HEADERS
    # TODO [Module 2]: src/common/Types.h
    # TODO [Module 2]: src/common/EndianCodec.h
    # TODO [Module 3]: src/recorder/BoundedByteQueue.h
    # TODO [Module 4]: src/recorder/TopicWriter.h
    # TODO [Module 5]: src/recorder/RecorderEngine.h
    # TODO [Module 6]: src/replay/TopicReader.h
    # TODO [Module 6]: src/replay/ReplayEngine.h
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
