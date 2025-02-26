/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
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

package org.apache.tsfile.encoding.decoder;

import org.apache.tsfile.encoding.encoder.FloatEncoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.encoding.TsFileDecodingException;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Decoder for float or double value using rle or two diff. For more info about encoding pattern,
 * see{@link FloatEncoder}
 */
public class FloatDecoder extends Decoder {

  private static final Logger logger = LoggerFactory.getLogger(FloatDecoder.class);
  private final Decoder decoder;

  /** maxPointValue = 10^(maxPointNumber). maxPointNumber can be read from the stream. */
  private double maxPointValue;

  /** flag that indicates whether we have read maxPointNumber and calculated maxPointValue. */
  private boolean isMaxPointNumberRead;

  private BitMap isUnderflowInfo;
  private BitMap valueItselfOverflowInfo;
  private int position = 0;

  public FloatDecoder(TSEncoding encodingType, TSDataType dataType) {
    super(encodingType);
    if (encodingType == TSEncoding.RLE) {
      if (dataType == TSDataType.FLOAT) {
        decoder = new IntRleDecoder();
        logger.debug("tsfile-encoding FloatDecoder: init decoder using int-rle and float");
      } else if (dataType == TSDataType.DOUBLE) {
        decoder = new LongRleDecoder();
        logger.debug("tsfile-encoding FloatDecoder: init decoder using long-rle and double");
      } else {
        throw new TsFileDecodingException(
            String.format("data type %s is not supported by FloatDecoder", dataType));
      }
    } else if (encodingType == TSEncoding.TS_2DIFF) {
      if (dataType == TSDataType.FLOAT) {
        decoder = new DeltaBinaryDecoder.IntDeltaDecoder();
        logger.debug("tsfile-encoding FloatDecoder: init decoder using int-delta and float");
      } else if (dataType == TSDataType.DOUBLE) {
        decoder = new DeltaBinaryDecoder.LongDeltaDecoder();
        logger.debug("tsfile-encoding FloatDecoder: init decoder using long-delta and double");
      } else {
        throw new TsFileDecodingException(
            String.format("data type %s is not supported by FloatDecoder", dataType));
      }
    } else if (encodingType == TSEncoding.RLBE) {
      if (dataType == TSDataType.FLOAT) {
        decoder = new IntRLBEDecoder();
        logger.debug("tsfile-encoding FloatDecoder: init decoder using int-rlbe and float");
      } else if (dataType == TSDataType.DOUBLE) {
        decoder = new LongRLBEDecoder();
        logger.debug("tsfile-encoding FloatDecoder: init decoder using long-rlbe and double");
      } else {
        throw new TsFileDecodingException(
            String.format("data type %s is not supported by FloatDecoder", dataType));
      }
    } else {
      throw new TsFileDecodingException(
          String.format("%s encoding is not supported by FloatDecoder", encodingType));
    }
    isMaxPointNumberRead = false;
  }

  @Override
  public float readFloat(ByteBuffer buffer) {
    readMaxPointValue(buffer);
    int value = decoder.readInt(buffer);
    if (valueItselfOverflowInfo != null && valueItselfOverflowInfo.isMarked(position)) {
      position++;
      return Float.intBitsToFloat(value);
    }
    double result = value / getMaxPointValue();
    position++;
    return (float) result;
  }

  @Override
  public double readDouble(ByteBuffer buffer) {
    readMaxPointValue(buffer);
    long value = decoder.readLong(buffer);
    if (valueItselfOverflowInfo != null && valueItselfOverflowInfo.isMarked(position)) {
      position++;
      return Double.longBitsToDouble(value);
    }
    double result = value / getMaxPointValue();
    position++;
    return result;
  }

  private double getMaxPointValue() {
    if (isUnderflowInfo == null) {
      return maxPointValue;
    } else {
      return isUnderflowInfo.isMarked(position) ? maxPointValue : 1;
    }
  }

  private void readMaxPointValue(ByteBuffer buffer) {
    if (!isMaxPointNumberRead) {
      int maxPointNumber = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
      if (maxPointNumber == Integer.MAX_VALUE) {
        int size = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
        byte[] tmp = new byte[size / 8 + 1];
        buffer.get(tmp, 0, size / 8 + 1);
        isUnderflowInfo = new BitMap(size, tmp);
        maxPointNumber = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
        maxPointValue = Math.pow(10, maxPointNumber);
      } else if (maxPointNumber == Integer.MAX_VALUE - 1) {
        int size = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
        byte[] tmp = new byte[size / 8 + 1];
        buffer.get(tmp, 0, size / 8 + 1);
        isUnderflowInfo = new BitMap(size, tmp);
        tmp = new byte[size / 8 + 1];
        buffer.get(tmp, 0, size / 8 + 1);
        valueItselfOverflowInfo = new BitMap(size, tmp);
        maxPointNumber = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
        maxPointValue = Math.pow(10, maxPointNumber);
      } else if (maxPointNumber <= 0) {
        maxPointValue = 1;
      } else {
        maxPointValue = Math.pow(10, maxPointNumber);
      }
      isMaxPointNumberRead = true;
    }
  }

  @Override
  public boolean hasNext(ByteBuffer buffer) throws IOException {
    if (decoder == null) {
      return false;
    }
    return decoder.hasNext(buffer);
  }

  @Override
  public Binary readBinary(ByteBuffer buffer) {
    throw new TsFileDecodingException("Method readBinary is not supported by FloatDecoder");
  }

  @Override
  public boolean readBoolean(ByteBuffer buffer) {
    throw new TsFileDecodingException("Method readBoolean is not supported by FloatDecoder");
  }

  @Override
  public short readShort(ByteBuffer buffer) {
    throw new TsFileDecodingException("Method readShort is not supported by FloatDecoder");
  }

  @Override
  public int readInt(ByteBuffer buffer) {
    throw new TsFileDecodingException("Method readInt is not supported by FloatDecoder");
  }

  @Override
  public long readLong(ByteBuffer buffer) {
    throw new TsFileDecodingException("Method readLong is not supported by FloatDecoder");
  }

  @Override
  public void reset() {
    this.decoder.reset();
    this.isMaxPointNumberRead = false;
    this.position = 0;
  }
}
