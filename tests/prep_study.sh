#!/usr/bin/env bash
TARGET_FILE=$(find . -name "RecorderEngine.cpp" | head -n 1)
HEADER_FILE=$(find . -name "RecorderEngine.h*" | head -n 1)

if [ -z "$TARGET_FILE" ]; then
    echo "[!] Không tìm thấy RecorderEngine.cpp trong thư mục hiện tại!"
    echo "Danh sách các file .cpp đang có trong project:"
    find . -name "*.cpp"
    exit 1
fi

echo "================================================================"
echo " BẢN ĐỒ GIẢI PHẪU NHANH: $TARGET_FILE"
echo "================================================================"
echo "1. TỔNG SỐ DÒNG CODE:"
wc -l "$TARGET_FILE" $HEADER_FILE 2>/dev/null

echo -e "\n2. DANH SÁCH CÁC HÀM (METHODS) THEO THỨ TỰ DÒNG:"
grep -nE "^[a-zA-Z0-9_:<>~*&\s]+RecorderEngine::[a-zA-Z0-9_~]+\(" "$TARGET_FILE"

echo -e "\n3. CÁC ĐIỂM ĐỒNG BỘ ĐA LUỒNG (MUTEX / LOCK / ATOMIC / COND_VAR):"
grep -nE "mutex|lock|cv|condition|atomic|thread|queue" "$TARGET_FILE" $HEADER_FILE 2>/dev/null

echo -e "\n4. CÁC ĐIỂM TƯƠNG TÁC ZMQ & FILE I/O:"
grep -nE "zmq|recv|subscribe|write|open|close|300|256" "$TARGET_FILE"
echo "================================================================"
