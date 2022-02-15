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

package org.apache.iotdb.tsfile.file.header;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.NoStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public class PageHeader {

  private int uncompressedSize;
  private int compressedSize;
  private int numOfValues;
  private Statistics statistics;
  private long maxTimestamp;
  private long minTimestamp;
  private int compressRatio;
  protected long timeIntervalSum;
  protected long timeIntervalSquareSum;

  // this field does not need to be serialized.
  private int serializedSize;

  public PageHeader(int uncompressedSize, int compressedSize, int numOfValues,
      Statistics statistics, long maxTimestamp, long minTimestamp, int compressRatio,
      long timeIntervalSum, long timeIntervalSquareSum) {
    this.uncompressedSize = uncompressedSize;
    this.compressedSize = compressedSize;
    this.numOfValues = numOfValues;
    if (statistics == null) {
      this.statistics = new NoStatistics();
    } else {
      this.statistics = statistics;
    }
    this.maxTimestamp = maxTimestamp;
    this.minTimestamp = minTimestamp;
    this.compressRatio = compressRatio;
    this.timeIntervalSum = timeIntervalSum;
    this.timeIntervalSquareSum = timeIntervalSquareSum;
    serializedSize = calculatePageHeaderSize();
  }

  public static int calculatePageHeaderSize(TSDataType type) {
    return calculatePageHeaderSizeWithoutStatistics() + Statistics.getStatsByType(type).getSerializedSize();
  }

  public static int calculatePageHeaderSizeWithoutStatistics() {
    return 4 * Integer.BYTES + 4 * Long.BYTES;
  }

  public static PageHeader deserializeFrom(InputStream inputStream, TSDataType dataType)
      throws IOException {
    int uncompressedSize = ReadWriteIOUtils.readInt(inputStream);
    int compressedSize = ReadWriteIOUtils.readInt(inputStream);
    int numOfValues = ReadWriteIOUtils.readInt(inputStream);
    long maxTimestamp = ReadWriteIOUtils.readLong(inputStream);
    long minTimestamp = ReadWriteIOUtils.readLong(inputStream);
    int compressRatio = ReadWriteIOUtils.readInt(inputStream);
    long timeIntervalSum = ReadWriteIOUtils.readLong(inputStream);
    long timeIntervalSquareSum = ReadWriteIOUtils.readLong(inputStream);
    Statistics statistics = Statistics.deserialize(inputStream, dataType);
    return new PageHeader(uncompressedSize, compressedSize, numOfValues, statistics, maxTimestamp,
        minTimestamp, compressRatio, timeIntervalSum, timeIntervalSquareSum);
  }

  public static PageHeader deserializeFrom(ByteBuffer buffer, TSDataType dataType)
      throws IOException {
    int uncompressedSize = ReadWriteIOUtils.readInt(buffer);
    int compressedSize = ReadWriteIOUtils.readInt(buffer);
    int numOfValues = ReadWriteIOUtils.readInt(buffer);
    long maxTimestamp = ReadWriteIOUtils.readLong(buffer);
    long minTimestamp = ReadWriteIOUtils.readLong(buffer);
    int compressRatio = ReadWriteIOUtils.readInt(buffer);
    long timeIntervalSum = ReadWriteIOUtils.readLong(buffer);
    long timeIntervalSquareSum = ReadWriteIOUtils.readLong(buffer);
    Statistics statistics = Statistics.deserialize(buffer, dataType);
    return new PageHeader(uncompressedSize, compressedSize, numOfValues, statistics, maxTimestamp,
        minTimestamp, compressRatio, timeIntervalSum, timeIntervalSquareSum);
  }

  /**
   * deserialize from TsFileInput.
   *
   * @param dataType data type
   * @param input TsFileInput
   * @param offset offset
   * @param markerRead read marker (boolean type)
   * @return CHUNK_HEADER object
   * @throws IOException IOException
   */
  public static PageHeader deserializeFrom(TSDataType dataType, TsFileInput input, long offset,
      boolean markerRead)
      throws IOException {
    long offsetVar = offset;
    if (!markerRead) {
      offsetVar++;
    }

    if (dataType == TSDataType.TEXT) {
      int sizeWithoutStatistics = calculatePageHeaderSizeWithoutStatistics();
      ByteBuffer bufferWithoutStatistics = ByteBuffer.allocate(sizeWithoutStatistics);
      ReadWriteIOUtils.readAsPossible(input, offsetVar, bufferWithoutStatistics);
      bufferWithoutStatistics.flip();
      offsetVar += sizeWithoutStatistics;

      Statistics statistics = Statistics.deserialize(input, offsetVar, dataType);
      return deserializePartFrom(statistics, bufferWithoutStatistics);
    } else {
      int size = calculatePageHeaderSize(dataType);
      ByteBuffer buffer = ByteBuffer.allocate(size);
      ReadWriteIOUtils.readAsPossible(input, offsetVar, buffer);
      buffer.flip();
      return deserializeFrom(buffer, dataType);
    }
  }

  private static PageHeader deserializePartFrom(Statistics statistics, ByteBuffer buffer) {
    int uncompressedSize = ReadWriteIOUtils.readInt(buffer);
    int compressedSize = ReadWriteIOUtils.readInt(buffer);
    int numOfValues = ReadWriteIOUtils.readInt(buffer);
    long maxTimestamp = ReadWriteIOUtils.readLong(buffer);
    long minTimestamp = ReadWriteIOUtils.readLong(buffer);
    int compressRatio = ReadWriteIOUtils.readInt(buffer);
    long timeIntervalSum = ReadWriteIOUtils.readLong(buffer);
    long timeIntervalSquareSum = ReadWriteIOUtils.readLong(buffer);
    return new PageHeader(uncompressedSize, compressedSize, numOfValues, statistics, maxTimestamp,
        minTimestamp, compressRatio, timeIntervalSum, timeIntervalSquareSum);
  }

  public int calculatePageHeaderSize() {
    return 4 * Integer.BYTES + 2 * Long.BYTES + statistics.getSerializedSize() + 2 * Long.BYTES;
  }

  public int getSerializedSize() {
    return serializedSize;
  }

  public int getUncompressedSize() {
    return uncompressedSize;
  }

  public void setUncompressedSize(int uncompressedSize) {
    this.uncompressedSize = uncompressedSize;
  }

  public int getCompressedSize() {
    return compressedSize;
  }

  public void setCompressedSize(int compressedSize) {
    this.compressedSize = compressedSize;
  }

  public int getNumOfValues() {
    return numOfValues;
  }

  public void setNumOfValues(int numOfValues) {
    this.numOfValues = numOfValues;
  }

  public Statistics getStatistics() {
    return statistics;
  }

  public long getMaxTimestamp() {
    return maxTimestamp;
  }

  public void setMaxTimestamp(long maxTimestamp) {
    this.maxTimestamp = maxTimestamp;
  }

  public long getMinTimestamp() {
    return minTimestamp;
  }

  public void setMinTimestamp(long minTimestamp) {
    this.minTimestamp = minTimestamp;
  }

  public int getCompressRatio() {
    return compressRatio;
  }

  public void setCompressRatio(int compressRatio) {
    this.compressRatio = compressRatio;
  }

  public long getTimeIntervalSum() {
    return timeIntervalSum;
  }

  public void setTimeIntervalSum(long timeIntervalSum) {
    this.timeIntervalSum = timeIntervalSum;
  }

  public long getTimeIntervalSquareSum() {
    return timeIntervalSquareSum;
  }

  public void setTimeIntervalSquareSum(long timeIntervalSquareSum) {
    this.timeIntervalSquareSum = timeIntervalSquareSum;
  }

  public double calStandardDeviation() {
    double variance = timeIntervalSquareSum / numOfValues - calMeanValue() * calMeanValue();
    return Math.sqrt(variance);
  }

  public double calMeanValue() {
    return 1.0 * timeIntervalSum / numOfValues;
  }


  public int serializeTo(OutputStream outputStream) throws IOException {
    int length = 0;
    length += ReadWriteIOUtils.write(uncompressedSize, outputStream);
    length += ReadWriteIOUtils.write(compressedSize, outputStream);
    length += ReadWriteIOUtils.write(numOfValues, outputStream);
    length += ReadWriteIOUtils.write(maxTimestamp, outputStream);
    length += ReadWriteIOUtils.write(minTimestamp, outputStream);
    length += ReadWriteIOUtils.write(compressRatio, outputStream);
    length += ReadWriteIOUtils.write(timeIntervalSum, outputStream);
    length += ReadWriteIOUtils.write(timeIntervalSquareSum, outputStream);
    length += statistics.serialize(outputStream);
    return length;
  }

  @Override
  public String toString() {
    return "PageHeader{" + "uncompressedSize=" + uncompressedSize + ", compressedSize="
        + compressedSize
        + ", numOfValues=" + numOfValues + ", statistics=" + statistics + ", maxTimestamp="
        + maxTimestamp
        + ", minTimestamp=" + minTimestamp + ", compressRatio=" + compressRatio
        +", timeIntervalSum="+timeIntervalSum+", timeIntervalSquareSum="+timeIntervalSquareSum
        + ", serializedSize=" + serializedSize + '}';
  }
}
