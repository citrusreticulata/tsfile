/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License a
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
#include "common/container/byte_buffer.h"

#include <gtest/gtest.h>

namespace {

class ByteBufferTest : public ::testing::Test {
   protected:
    common::ByteBuffer buffer;

    void SetUp() override { buffer.init(128); }

    void TearDown() override {}
};

TEST_F(ByteBufferTest, Initialization) {
    EXPECT_NE(buffer.get_data(), nullptr);
}

TEST_F(ByteBufferTest, AppendFixedValue) {
    char value[] = "hello";
    uint32_t len = strlen(value) + 1;
    buffer.append_fixed_value(value, len);
    char* read_value = buffer.read(0, len);
    EXPECT_STREQ(read_value, value);
}

TEST_F(ByteBufferTest, AppendVariableValue) {
    char value[] = "World";
    uint32_t len = strlen(value) + 1;

    buffer.append_variable_value(value, len);

    uint32_t read_len;
    char* read_value = buffer.read(0, &read_len);

    EXPECT_EQ(read_len, len);
    EXPECT_STREQ(read_value, value);
}

TEST_F(ByteBufferTest, ExtendMemory) {
    common::ByteBuffer byte_buffer;
    byte_buffer.init(5);
    char value[] = "Extended";
    uint32_t len = strlen(value) + 1;
    buffer.append_fixed_value(value, len);
    char* read_value = buffer.read(0, len);
    EXPECT_STREQ(read_value, value);
}

}  // namespace