/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef COMMON_DEVICE_ID_H
#define COMMON_DEVICE_ID_H

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "common/allocator/byte_stream.h"
#include "constant/tsfile_constant.h"
#include "parser/path_nodes_generator.h"
#include "utils/errno_define.h"

namespace storage {
class IDeviceID {
public:
    virtual ~IDeviceID() = default;
    virtual int serialize(common::ByteStream& write_stream) { return 0; }
    virtual int deserialize(common::ByteStream& read_stream) { return 0; }
    virtual std::string get_table_name() { return ""; }
    virtual int segment_num() { return 0; }
    virtual const std::vector<std::string>& get_segments() const {
        return empty_segments_;
    }
    virtual std::string get_device_name() const { return ""; };
    virtual bool operator<(const IDeviceID& other) { return 0; }
    virtual bool operator==(const IDeviceID& other) { return false; }
    virtual bool operator!=(const IDeviceID& other) { return false; }

protected:
    IDeviceID() : empty_segments_() {}

private:
    const std::vector<std::string> empty_segments_;
};

struct IDeviceIDComparator {
    bool operator()(const std::shared_ptr<IDeviceID>& lhs,
                    const std::shared_ptr<IDeviceID>& rhs) const {
        return *lhs < *rhs;
    }
};

class StringArrayDeviceID : public IDeviceID {
public:
    explicit StringArrayDeviceID(const std::vector<std::string>& segments)
        : segments_(formalize(segments)) {}

    explicit StringArrayDeviceID(const std::string& device_id_string)
        : segments_(split_device_id_string(device_id_string)) {}

    explicit StringArrayDeviceID() : segments_() {}

    ~StringArrayDeviceID() override = default;

    std::string get_device_name() const override {
        return segments_.empty() ? "" : std::accumulate(std::next(segments_.begin()), segments_.end(),
                               segments_.front(),
                               [](std::string a, const std::string& b) {
                                   return std::move(a) + "." + b;
                               });
    };

    int serialize(common::ByteStream& write_stream) override {
        int ret = common::E_OK;
        if (RET_FAIL(common::SerializationUtil::write_var_uint(segment_num(),
                                                               write_stream))) {
            return ret;
                                                               }
        for (const auto& segment : segments_) {
            if (RET_FAIL(common::SerializationUtil::write_var_str(segment,
                                                              write_stream))) {
                return ret;
                                                              }
        }
        return ret;
    }

    int deserialize(common::ByteStream& read_stream) override {
        int ret = common::E_OK;
        uint32_t num_segments;
        if (RET_FAIL(common::SerializationUtil::read_var_uint(num_segments, read_stream))) {
            return ret;
        }
        segments_.clear();
        for (uint32_t i = 0; i < num_segments; ++i) {
            std::string segment;
            if (RET_FAIL(common::SerializationUtil::read_var_str(segment, read_stream))) {
                return ret;
            }
            segments_.push_back(segment);
        }
        return ret;
    }

    std::string get_table_name() override {
        return segments_.empty() ? "" : segments_[0];
    }

    int segment_num() override { return static_cast<int>(segments_.size()); }

    const std::vector<std::string>& get_segments() const override {
        return segments_;
    }

    virtual bool operator<(const IDeviceID& other) override {
        auto other_segments = other.get_segments();
        return std::lexicographical_compare(segments_.begin(), segments_.end(),
                                            other_segments.begin(),
                                            other_segments.end());
    }

    bool operator==(const IDeviceID& other) override {
        auto other_segments = other.get_segments();
        return (segments_.size() == other_segments.size()) &&
               std::equal(segments_.begin(), segments_.end(),
                          other_segments.begin());
    }

    bool operator!=(const IDeviceID& other) override {
        return !(*this == other);
    }

private:
    std::vector<std::string> segments_;

    std::vector<std::string> formalize(
        const std::vector<std::string>& segments) {
        auto it =
            std::find_if(segments.rbegin(), segments.rend(),
                         [](const std::string& seg) { return !seg.empty(); });
        return std::vector<std::string>(segments.begin(), it.base());
    }

    std::vector<std::string> split_device_id_string(
        const std::string& device_id_string) {
        auto splits =
            storage::PathNodesGenerator::invokeParser(device_id_string);
        return split_device_id_string(splits);
    }

    std::vector<std::string> split_device_id_string(
        const std::vector<std::string>& splits) {
        size_t segment_cnt = splits.size();
        std::vector<std::string> final_segments;

        if (segment_cnt == 0) {
            return final_segments;
        }

        if (segment_cnt == 1) {
            // "root" -> {"root"}
            final_segments.push_back(splits[0]);
        } else if (segment_cnt < static_cast<size_t>(
            storage::DEFAULT_SEGMENT_NUM_FOR_TABLE_NAME + 1)) {
            // "root.a" -> {"root", "a"}
            // "root.a.b" -> {"root.a", "b"}
            std::string table_name = std::accumulate(
                splits.begin(), splits.end() - 1, std::string(),
                [](const std::string& a, const std::string& b) {
                    return a.empty() ? b : a + storage::PATH_SEPARATOR + b;
                });
            final_segments.push_back(table_name);
            final_segments.push_back(splits.back());
            } else {
                // "root.a.b.c" -> {"root.a.b", "c"}
                // "root.a.b.c.d" -> {"root.a.b", "c", "d"}
                std::string table_name = std::accumulate(
                    splits.begin(),
                    splits.begin() + storage::DEFAULT_SEGMENT_NUM_FOR_TABLE_NAME,
                    std::string(), [](const std::string& a, const std::string& b) {
                        return a.empty() ? b : a + storage::PATH_SEPARATOR + b;
                    });

                final_segments.emplace_back(std::move(table_name));
                final_segments.insert(
                    final_segments.end(),
                    splits.begin() + storage::DEFAULT_SEGMENT_NUM_FOR_TABLE_NAME,
                    splits.end());
            }

        return final_segments;
    }
};
}

#endif