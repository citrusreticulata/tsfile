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

package org.apache.tsfile.read.common.block.column;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnEncoding;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;

import java.util.Arrays;

import static java.util.Objects.requireNonNull;
import static org.apache.tsfile.read.common.block.column.ColumnUtil.checkArrayRange;
import static org.apache.tsfile.read.common.block.column.ColumnUtil.checkValidPosition;
import static org.apache.tsfile.read.common.block.column.ColumnUtil.checkValidRegion;

public class RunLengthEncodedColumn implements Column {

  private static final int INSTANCE_SIZE =
      (int) RamUsageEstimator.shallowSizeOfInstance(RunLengthEncodedColumn.class);

  private final Column value;
  private int positionCount;

  public RunLengthEncodedColumn(Column value, int positionCount) {
    requireNonNull(value, "value is null");
    if (value.getPositionCount() != 1) {
      throw new IllegalArgumentException(
          String.format(
              "Expected value to contain a single position but has %s positions",
              value.getPositionCount()));
    }

    if (value instanceof RunLengthEncodedColumn) {
      this.value = ((RunLengthEncodedColumn) value).getValue();
    } else {
      this.value = value;
    }

    if (positionCount < 0) {
      throw new IllegalArgumentException("positionCount is negative");
    }

    this.positionCount = positionCount;
  }

  public Column getValue() {
    return value;
  }

  @Override
  public TSDataType getDataType() {
    return value.getDataType();
  }

  @Override
  public ColumnEncoding getEncoding() {
    return ColumnEncoding.RLE;
  }

  @Override
  public boolean getBoolean(int position) {
    return value.getBoolean(0);
  }

  @Override
  public int getInt(int position) {
    return value.getInt(0);
  }

  @Override
  public long getLong(int position) {
    return value.getLong(0);
  }

  @Override
  public float getFloat(int position) {
    return value.getFloat(0);
  }

  @Override
  public double getDouble(int position) {
    return value.getDouble(0);
  }

  @Override
  public Binary getBinary(int position) {
    return value.getBinary(0);
  }

  @Override
  public Object getObject(int position) {
    return value.getObject(0);
  }

  @Override
  public boolean[] getBooleans() {
    boolean[] res = new boolean[positionCount];
    Arrays.fill(res, value.getBoolean(0));
    return res;
  }

  @Override
  public int[] getInts() {
    int[] res = new int[positionCount];
    Arrays.fill(res, value.getInt(0));
    return res;
  }

  @Override
  public long[] getLongs() {
    long[] res = new long[positionCount];
    Arrays.fill(res, value.getLong(0));
    return res;
  }

  @Override
  public float[] getFloats() {
    float[] res = new float[positionCount];
    Arrays.fill(res, value.getFloat(0));
    return res;
  }

  @Override
  public double[] getDoubles() {
    double[] res = new double[positionCount];
    Arrays.fill(res, value.getDouble(0));
    return res;
  }

  @Override
  public Binary[] getBinaries() {
    Binary[] res = new Binary[positionCount];
    Arrays.fill(res, value.getBinary(0));
    return res;
  }

  @Override
  public Object[] getObjects() {
    Object[] res = new Object[positionCount];
    Arrays.fill(res, value.getObject(0));
    return res;
  }

  @Override
  public TsPrimitiveType getTsPrimitiveType(int position) {
    return value.getTsPrimitiveType(0);
  }

  @Override
  public boolean mayHaveNull() {
    return value.mayHaveNull();
  }

  @Override
  public boolean isNull(int position) {
    return value.isNull(0);
  }

  @Override
  public boolean[] isNull() {
    boolean[] res = new boolean[positionCount];
    Arrays.fill(res, value.isNull(0));
    return res;
  }

  @Override
  public int getPositionCount() {
    return positionCount;
  }

  @Override
  public long getRetainedSizeInBytes() {
    return INSTANCE_SIZE + value.getRetainedSizeInBytes();
  }

  @Override
  public long getSizeInBytes() {
    return value.getSizeInBytes();
  }

  @Override
  public Column getRegion(int positionOffset, int length) {
    checkValidRegion(positionCount, positionOffset, length);
    return new RunLengthEncodedColumn(value, length);
  }

  @Override
  public Column getRegionCopy(int positionOffset, int length) {
    checkValidRegion(positionCount, positionOffset, length);
    Column valueCopy = value.subColumnCopy(0);
    return new RunLengthEncodedColumn(valueCopy, length);
  }

  @Override
  public Column subColumn(int fromIndex) {
    if (fromIndex > positionCount) {
      throw new IllegalArgumentException("fromIndex is not valid");
    }
    return new RunLengthEncodedColumn(value, positionCount - fromIndex);
  }

  @Override
  public Column subColumnCopy(int fromIndex) {
    if (fromIndex > positionCount) {
      throw new IllegalArgumentException("fromIndex is not valid");
    }
    Column valueCopy = value.subColumnCopy(0);
    return new RunLengthEncodedColumn(valueCopy, positionCount - fromIndex);
  }

  @Override
  public Column getPositions(int[] positions, int offset, int length) {
    checkArrayRange(positions, offset, length);

    for (int i = offset; i < offset + length; i++) {
      checkValidPosition(positions[i], positionCount);
    }
    return new RunLengthEncodedColumn(value, length);
  }

  @Override
  public Column copyPositions(int[] positions, int offset, int length) {
    checkArrayRange(positions, offset, length);

    for (int i = offset; i < offset + length; i++) {
      checkValidPosition(positions[i], positionCount);
    }
    return new RunLengthEncodedColumn(value.subColumnCopy(0), length);
  }

  @Override
  public void reverse() {
    // do nothing because the underlying column has only one value
  }

  @Override
  public int getInstanceSize() {
    return INSTANCE_SIZE;
  }

  @Override
  public void setPositionCount(int count) {
    this.positionCount = count;
  }

  @Override
  public void setNull(int start, int end) {
    throw new UnsupportedOperationException(
        String.format(
            "set null of %s is not supported !", RunLengthEncodedColumn.class.getSimpleName()));
  }
}
