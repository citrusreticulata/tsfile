#ifndef WRITER_TSFILE_APPEND_WRITER_H
#define WRITER_TSFILE_APPEND_WRITER_H

#include "tsfile_writer.h"

namespace storage {

/** Tsfile追加写实现
 * 1.目前先只支持plain编码
 * 2.先只支持单一device的写入
 * 3.先尝试支持一条序列的非对齐写入，也即单设备、单序列、一个时间戳列一个数值列。
 * 4.先只支持writeTablet。调用该接口后，立即刷盘。
 * 5.先只支持整数和浮点数
 */
class TsFileAppendWriter: public TsFileWriter {

    public:
    // 检查MS兼容性，如果兼容则返回true，否则说明不能追加写，返回false
    static bool checkMSCompatibility(MeasurementSchema *measurement_schema, bool is_aligned);
    int append_write_tablet(const Tablet &tablet);
    int open(const std::string &file_path, int flags, mode_t mode) override;
    int register_timeseries(const std::string &device_id,
        const MeasurementSchema &measurement_schema) override;

    protected:
    int register_timeseries(const std::string &device_path,
        MeasurementSchema *measurement_schema,
        bool is_aligned) override;
    static void freeMeasurementSchema(MeasurementSchema *ms);

    public:
    // 测试用函数！！！
    void testPrintHeadMagicStr();
    int readBufFromOffset(int32_t offset, char *buf, int32_t buf_size,
        int32_t &read_len);
    int readINT32FromOffset(int32_t offset, int32_t &value);
};

extern bool check_file_exist(const std::string &file_path);

}




#endif  // WRITER_TSFILE_APPEND_WRITER_H