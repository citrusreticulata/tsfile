#include "tsfile_append_writer.h"
#include "utils/errno_define.h"

#include <unistd.h>

#include "chunk_writer.h"
#include "common/config/config.h"
#include "file/tsfile_io_writer.h"
#include "file/write_file.h"
#include "utils/errno_define.h"

using namespace common;

namespace storage{

bool TsFileAppendWriter::checkMSCompatibility(MeasurementSchema *measurement_schema, bool is_aligned){
    // TODO: 支持更多类型的写入，例如更多编码
    // 检查表是否有冲突：1.只支持plain编码 2.只支持整数浮点数类型
    if(measurement_schema == nullptr) return false;
    if(measurement_schema->data_type_ > TSDataType::DOUBLE){
        freeMeasurementSchema(measurement_schema);
        return false;
    } 
    if(measurement_schema->encoding_ != TSEncoding::PLAIN){
        freeMeasurementSchema(measurement_schema);
        return false;
    } 
    if(measurement_schema->compression_type_ != CompressionType::UNCOMPRESSED){
        freeMeasurementSchema(measurement_schema);
        return false;
    }
    if(is_aligned){
        return false;       // TODO:暂不支持追加写
    }
    return true;
}

// 重写覆盖父类
int TsFileAppendWriter::register_timeseries(
    const std::string &device_id, const MeasurementSchema &measurement_schema) {
    MeasurementSchema *ms = new MeasurementSchema(
        measurement_schema.measurement_name_, measurement_schema.data_type_,
        measurement_schema.encoding_, measurement_schema.compression_type_);
    return register_timeseries(device_id, ms, false);
}

// 重写覆盖父类
int TsFileAppendWriter::register_timeseries(
    const std::string &device_path, MeasurementSchema *measurement_schema,
    bool is_aligned) {
    if(!checkMSCompatibility(measurement_schema, is_aligned)){
        // 不满足追加写兼容性，释放并且返回不支持
        freeMeasurementSchema(measurement_schema);
        return E_NOT_SUPPORT;
    }
    
    std::shared_ptr<IDeviceID> device_id =
        std::make_shared<StringArrayDeviceID>(device_path);
    DeviceSchemasMapIter device_iter = schemas_.find(device_id);
    if (device_iter != schemas_.end()) {
        // found existing device
        MeasurementSchemaMap &msm =
            device_iter->second->measurement_schema_map_;
        MeasurementSchemaMapInsertResult ins_res = msm.insert(std::make_pair(
            measurement_schema->measurement_name_, measurement_schema));
        if (UNLIKELY(!ins_res.second)) {
            return E_NOT_SUPPORT;
        }
    } else {
        if (schemas_.size() > 0) {
            // exist device with DIFFERENT id. refuse!!!
            // TODO: 支持多设备
            return E_NOT_SUPPORT;
        }
        MeasurementSchemaGroup *ms_group = new MeasurementSchemaGroup;
        ms_group->is_aligned_ = is_aligned;
        ms_group->measurement_schema_map_.insert(std::make_pair(
            measurement_schema->measurement_name_, measurement_schema));
        schemas_.insert(std::make_pair(device_id, ms_group));
    }
    return E_OK;
}

// 重写覆盖父类
int TsFileAppendWriter::open(const std::string &file_path, int flags, mode_t mode) {
    // std::cout<<"TsFileAppendWriter::open "<< file_path <<std::endl;
    if (check_file_exist(file_path)) {
        return E_ALREADY_EXIST;
    }
    write_file_ = new WriteFile;
    write_file_created_ = true;
    io_writer_ = new TsFileIOWriter;
    int ret = E_OK;
    if (RET_FAIL(write_file_->create(file_path, flags, mode))) {
    } else {
        io_writer_->init(write_file_);
    }
    return ret;
}


void TsFileAppendWriter::freeMeasurementSchema(MeasurementSchema *ms) {
    if (ms != nullptr) {
        if (ms->chunk_writer_ != nullptr) {
            delete ms->chunk_writer_;
            ms->chunk_writer_ = nullptr;
        }
        delete ms;
    }
}

// 测试用函数！！！
void TsFileAppendWriter::testPrintHeadMagicStr(){
    int fd_ = this->write_file_->getFD();
    std::cout<<"get fd_ from this->write_file_: "<<fd_<<std::endl;
    if(fd_ < 0){
        std::cout<<"got fd_ is broken !!!"<<std::endl;
        return;
    }
    // 尝试从fd_中获取信息
    char buf[256];
    ssize_t bytesRead = pread(fd_, &buf, 6, 0);
    std::cout<<"bytesRead="<<bytesRead<<", ";
    buf[255] = '\0';
    if (bytesRead == -1) {
        std::cout<<"读取出错!"<<std::endl;
    } else {
        std::cout<<"文件头内容="<<buf<<std::endl;
        // break;
    }
}

int TsFileAppendWriter::readBufFromOffset(int32_t offset, char *buf, int32_t buf_size, int32_t &read_len){
    int fd_ = this->write_file_->getFD();
    if(fd_ < 0){
        std::cout<<"got fd_ is broken !!!"<<std::endl;
        return E_FILE_READ_ERR;
    }
    int ret = E_OK;
    read_len = 0;
    while (read_len < buf_size) {
        ssize_t pread_size = ::pread(fd_, buf + read_len, buf_size - read_len,
                                    offset + read_len);
        if (pread_size < 0) {
            ret = E_FILE_READ_ERR;
            ////log_err("tsfile reader error, file_path=%s, errno=%d",
            /// file_path_.c_str(), errno);
            break;
        } else if (pread_size == 0) {
            break;
        } else {
            read_len += pread_size;
        }
    }
    return ret;
}

int TsFileAppendWriter::readINT32FromOffset(int32_t offset, int32_t &value){
    int fd_ = this->write_file_->getFD();
    if(fd_ < 0){
        std::cout<<"got fd_ is broken !!!"<<std::endl;
        return E_FILE_READ_ERR;
    }
    ssize_t bytesRead = pread(fd_, &value, sizeof(value), offset);
    if (bytesRead == sizeof(value)) {
        return E_OK;
    } else {
        return E_FILE_READ_ERR;
    }
}

}     // end namespace storage


