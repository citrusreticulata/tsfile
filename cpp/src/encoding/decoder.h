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

#ifndef ENCODING_DECODER_H
#define ENCODING_DECODER_H

#include "common/allocator/byte_stream.h"

namespace storage {

class Decoder {
   public:
    Decoder() {}
    virtual ~Decoder() {}
    virtual void reset() = 0;
    virtual bool has_remaining() = 0;
    virtual int read_boolean(bool &ret_value, common::ByteStream &in) = 0;
    virtual int read_int32(int32_t &ret_value, common::ByteStream &in) = 0;
    virtual int read_int64(int64_t &ret_value, common::ByteStream &in) = 0;
    virtual int read_float(float &ret_value, common::ByteStream &in) = 0;
    virtual int read_double(double &ret_value, common::ByteStream &in) = 0;
    virtual int read_String(common::String &ret_value, common::PageArena &pa,
                            common::ByteStream &in) = 0;
};

}  // end namespace storage
#endif  // ENCODING_DECODER_H
