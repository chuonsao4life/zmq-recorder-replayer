#pragma once

#include <cstdint>
#include <cstddef>
#include <string>
#include <QtEndian>
#include <QDateTime>
#include <QTimeZone>
#include "Types.h"

namespace recorder {

inline void serializeHeader(uint64_t timestamp_us, uint16_t data_len, uint8_t* dest, bool useBigEndian) {
    if (useBigEndian) {
        qToBigEndian(timestamp_us, dest);
        qToBigEndian(data_len, dest + 8);
    } else {
        qToLittleEndian(timestamp_us, dest);
        qToLittleEndian(data_len, dest + 8);
    }
}

inline void deserializeHeader(const uint8_t* src, uint64_t& timestamp_us, uint16_t& data_len, bool useBigEndian) {
    if (useBigEndian) {
        timestamp_us = qFromBigEndian<uint64_t>(src);
        data_len     = qFromBigEndian<uint16_t>(src + 8);
    } else {
        timestamp_us = qFromLittleEndian<uint64_t>(src);
        data_len     = qFromLittleEndian<uint16_t>(src + 8);
    }
}

inline bool verifyFrameMarkers(const uint8_t* data, size_t len, bool useBigEndian) {
    if (len < 12) return false;  // Minimum valid RDP: SOP(2)+SEQ(2)+SRC_ID(1)+TYPE(1)+LEN(2)+CHKSUM(2)+EOP(2)=12
    uint16_t sop = 0;
    uint16_t eop = 0;
    if (useBigEndian) {
        sop = qFromBigEndian<uint16_t>(data);
        eop = qFromBigEndian<uint16_t>(data + len - 2);
    } else {
        sop = qFromLittleEndian<uint16_t>(data);
        eop = qFromLittleEndian<uint16_t>(data + len - 2);
    }
    return (sop == SOP_MARKER) && (eop == EOP_MARKER);
}

inline std::string formatSessionTimestamp(uint64_t timestamp_us) {
    qint64 ms = static_cast<qint64>(timestamp_us / 1000ULL);
    QDateTime dt = QDateTime::fromMSecsSinceEpoch(ms, QTimeZone::utc());
    return dt.toString(QStringLiteral("yyyyMMdd_HHmm")).toStdString();
}

inline std::string formatSegmentTimestamp(uint64_t timestamp_us) {
    qint64 ms = static_cast<qint64>(timestamp_us / 1000ULL);
    QDateTime dt = QDateTime::fromMSecsSinceEpoch(ms, QTimeZone::utc());
    return dt.toString(QStringLiteral("yyyyMMdd_HHmmss")).toStdString();
}

} // namespace recorder
