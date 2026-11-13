#!/bin/bash
set -e
mkdir -p build/lib
ln -sf libQt6Core.so.6 build/lib/libQt6Core.so

g++ -std=c++20 -O3 -Wall -Wextra -pthread \
  -I"src" \
  -isystem /home/a00/qt6_dev/usr/include/x86_64-linux-gnu/qt6/QtCore \
  -isystem /home/a00/qt6_dev/usr/include/x86_64-linux-gnu/qt6 \
  src/recorder/TopicWriter.cpp \
  src/recorder/RecorderEngine.cpp \
  src/replay/TopicReader.cpp \
  src/replay/ReplayEngine.cpp \
  src/main.cpp \
  -L"build/lib" \
  -Wl,-rpath,'$ORIGIN/lib' \
  -lQt6Core -lzmq -lpthread \
  -o build/zmq_recorder_replay_bin

echo "[Build] Successfully compiled build/zmq_recorder_replay_bin"
