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

package org.apache.tsfile.file.metadata.statistics;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.filter.StatisticsClassException;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

public class LongStatistics extends Statistics<Long> {

  public static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(LongStatistics.class);

  private long minValue;
  private long maxValue;
  private long firstValue;
  private long lastValue;
  private double sumValue;

  @Override
  public TSDataType getType() {
    return TSDataType.INT64;
  }

  /** The output of this method should be identical to the method "serializeStats(outputStream)". */
  @Override
  public int getStatsSize() {
    return 40;
  }

  @Override
  public long getRetainedSizeInBytes() {
    return INSTANCE_SIZE;
  }

  public void initializeStats(long min, long max, long firstValue, long last, double sum) {
    this.minValue = min;
    this.maxValue = max;
    this.firstValue = firstValue;
    this.lastValue = last;
    this.sumValue += sum;
  }

  @Override
  public Long getMinValue() {
    return minValue;
  }

  @Override
  public Long getMaxValue() {
    return maxValue;
  }

  @Override
  public Long getFirstValue() {
    return firstValue;
  }

  @Override
  public Long getLastValue() {
    return lastValue;
  }

  @Override
  public double getSumDoubleValue() {
    return sumValue;
  }

  @Override
  public long getSumLongValue() {
    throw new StatisticsClassException(
        String.format(STATS_UNSUPPORTED_MSG, TSDataType.INT64, "long sum"));
  }

  @Override
  void updateStats(long value) {
    if (isEmpty) {
      initializeStats(value, value, value, value, value);
      isEmpty = false;
    } else {
      updateStats(value, value, value, value);
    }
  }

  @Override
  void updateStats(long[] values, int batchSize) {
    for (int i = 0; i < batchSize; i++) {
      updateStats(values[i]);
    }
  }

  @Override
  public void updateStats(long minValue, long maxValue) {
    if (minValue < this.minValue) {
      this.minValue = minValue;
    }
    if (maxValue > this.maxValue) {
      this.maxValue = maxValue;
    }
  }

  private void updateStats(long minValue, long maxValue, long lastValue, double sumValue) {
    if (minValue < this.minValue) {
      this.minValue = minValue;
    }
    if (maxValue > this.maxValue) {
      this.maxValue = maxValue;
    }
    this.sumValue += sumValue;
    this.lastValue = lastValue;
  }

  private void updateStats(
      long minValue,
      long maxValue,
      long firstValue,
      long lastValue,
      double sumValue,
      long startTime,
      long endTime) {
    if (minValue < this.minValue) {
      this.minValue = minValue;
    }
    if (maxValue > this.maxValue) {
      this.maxValue = maxValue;
    }
    this.sumValue += sumValue;
    // only if endTime greater or equals to the current endTime need we update the last value
    // only if startTime less or equals to the current startTime need we update the first value
    // otherwise, just ignore
    if (startTime <= this.getStartTime()) {
      this.firstValue = firstValue;
    }
    if (endTime >= this.getEndTime()) {
      this.lastValue = lastValue;
    }
  }

  @SuppressWarnings("rawtypes")
  @Override
  protected void mergeStatisticsValue(Statistics stats) {
    if (stats instanceof LongStatistics || stats instanceof IntegerStatistics) {
      if (isEmpty) {
        initializeStats(
            ((Number) stats.getMinValue()).longValue(),
            ((Number) stats.getMaxValue()).longValue(),
            ((Number) stats.getFirstValue()).longValue(),
            ((Number) stats.getLastValue()).longValue(),
            stats.getSumDoubleValue());
        isEmpty = false;
      } else {
        updateStats(
            ((Number) stats.getMinValue()).longValue(),
            ((Number) stats.getMaxValue()).longValue(),
            ((Number) stats.getFirstValue()).longValue(),
            ((Number) stats.getLastValue()).longValue(),
            stats.getSumDoubleValue(),
            stats.getStartTime(),
            stats.getEndTime());
      }
    } else {
      throw new StatisticsClassException(this.getClass(), stats.getClass());
    }
  }

  @Override
  public int serializeStats(OutputStream outputStream) throws IOException {
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.write(minValue, outputStream);
    byteLen += ReadWriteIOUtils.write(maxValue, outputStream);
    byteLen += ReadWriteIOUtils.write(firstValue, outputStream);
    byteLen += ReadWriteIOUtils.write(lastValue, outputStream);
    byteLen += ReadWriteIOUtils.write(sumValue, outputStream);
    return byteLen;
  }

  @Override
  public void deserialize(InputStream inputStream) throws IOException {
    this.minValue = ReadWriteIOUtils.readLong(inputStream);
    this.maxValue = ReadWriteIOUtils.readLong(inputStream);
    this.firstValue = ReadWriteIOUtils.readLong(inputStream);
    this.lastValue = ReadWriteIOUtils.readLong(inputStream);
    this.sumValue = ReadWriteIOUtils.readDouble(inputStream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    this.minValue = ReadWriteIOUtils.readLong(byteBuffer);
    this.maxValue = ReadWriteIOUtils.readLong(byteBuffer);
    this.firstValue = ReadWriteIOUtils.readLong(byteBuffer);
    this.lastValue = ReadWriteIOUtils.readLong(byteBuffer);
    this.sumValue = ReadWriteIOUtils.readDouble(byteBuffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    LongStatistics that = (LongStatistics) o;
    return minValue == that.minValue
        && maxValue == that.maxValue
        && firstValue == that.firstValue
        && lastValue == that.lastValue
        && Double.compare(that.sumValue, sumValue) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), minValue, maxValue, firstValue, lastValue, sumValue);
  }

  @Override
  public String toString() {
    return super.toString()
        + " [minValue:"
        + minValue
        + ",maxValue:"
        + maxValue
        + ",firstValue:"
        + firstValue
        + ",lastValue:"
        + lastValue
        + ",sumValue:"
        + sumValue
        + "]";
  }
}
