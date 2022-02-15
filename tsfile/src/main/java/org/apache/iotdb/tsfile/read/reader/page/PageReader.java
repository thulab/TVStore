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
package org.apache.iotdb.tsfile.read.reader.page;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

public class PageReader {

  private TSDataType dataType;

  /** decoder for value column */
  private Decoder valueDecoder;

  /** decoder for time column */
  private Decoder timeDecoder;

  /** time column in memory */
  private ByteBuffer timeBuffer;

  /** value column in memory */
  private ByteBuffer valueBuffer;

  private BatchData data = null;

  private Filter filter = null;

  private long deletedAt = Long.MIN_VALUE;

  public PageReader(ByteBuffer pageData, TSDataType dataType, Decoder valueDecoder,
      Decoder timeDecoder,
      Filter filter) {
    this(pageData, dataType, valueDecoder, timeDecoder);
    this.filter = filter;
  }

  public PageReader(ByteBuffer pageData, TSDataType dataType, Decoder valueDecoder,
      Decoder timeDecoder) {
    this.dataType = dataType;
    this.valueDecoder = valueDecoder;
    this.timeDecoder = timeDecoder;
    splitDataToTimeStampAndValue(pageData);
  }

  /**
   * split pageContent into two stream: time and value
   *
   * @param pageData
   *            uncompressed bytes size of time column, time column, value column
   */
  private void splitDataToTimeStampAndValue(ByteBuffer pageData) {
    int timeBufferLength = ReadWriteForEncodingUtils.readUnsignedVarInt(pageData);

    timeBuffer = pageData.slice();
    timeBuffer.limit(timeBufferLength);

    valueBuffer = pageData.slice();
    valueBuffer.position(timeBufferLength);
  }

  public boolean hasNextBatch() throws IOException {
    return timeDecoder.hasNext(timeBuffer);
  }

  /**
   * may return an empty BatchData
   */
  public BatchData nextBatch() throws IOException {
    if (filter == null) {
      data = getAllPageData(1);
    } else {
      data = getAllPageDataWithFilter();
    }

    return data;
  }

  /**
   * may return an empty BatchData
   */
  public BatchData nextBatch(int ratio) throws IOException {
    if (filter == null) {
      data = getAllPageData(ratio);
    } else {
      data = getAllPageDataWithFilter(ratio);
    }

    return data;
  }

  public BatchData currentBatch() {
    return data;
  }

  private BatchData getAllPageData(int compressRatio) throws IOException {

    BatchData pageData = new BatchData(dataType, true);
    long preTimestamp=-1;
    Object preValue=null;
    while (timeDecoder.hasNext(timeBuffer)) {
      long timestamp = timeDecoder.readLong(timeBuffer);

      switch (dataType) {
        case BOOLEAN:
          boolean aBoolean = valueDecoder.readBoolean(valueBuffer);
          if (timestamp > deletedAt) {
            pageData.putTime(timestamp);
            pageData.putBoolean(aBoolean);
          }
          break;
        case INT32:
          int anInt = valueDecoder.readInt(valueBuffer);

          if(preTimestamp!=-1){
            long timeStep=(timestamp-preTimestamp)/compressRatio;
            for(int i=1;i<compressRatio;i++){
              long tmp=timeStep*i+preTimestamp;
              if (tmp > deletedAt && tmp < timestamp) {
                pageData.putTime(tmp);
                int i1 = (int)preValue + (anInt - ((int)preValue)) / compressRatio * i;
                pageData.putInt(i1);
              }
            }
          }
          if (timestamp > deletedAt) {
            pageData.putTime(timestamp);
            pageData.putInt(anInt);
          }
          preValue = anInt;
          break;
        case INT64:
          long aLong = valueDecoder.readLong(valueBuffer);
          if(preTimestamp!=-1){
            long timeStep=(timestamp-preTimestamp)/compressRatio;
            for(int i=1;i<compressRatio;i++){
              long tmp=timeStep*i+preTimestamp;
              if(tmp>=timestamp){
                break;
              }
              if (tmp > deletedAt) {
                pageData.putTime(tmp);
                long i1 = (long)preValue + (aLong - ((long)preValue)) / compressRatio * i;
                pageData.putLong(i1);
              }
            }
          }
          if (timestamp > deletedAt) {
            pageData.putTime(timestamp);
            pageData.putLong(aLong);
          }
          preValue = aLong;
          break;
        case FLOAT:
          float aFloat = valueDecoder.readFloat(valueBuffer);
          if (timestamp > deletedAt) {
            pageData.putTime(timestamp);
            pageData.putFloat(aFloat);
          }
          break;
        case DOUBLE:
          double aDouble = valueDecoder.readDouble(valueBuffer);
          if (timestamp > deletedAt) {
            pageData.putTime(timestamp);
            pageData.putDouble(aDouble);
          }
          break;
        case TEXT:
          Binary aBinary = valueDecoder.readBinary(valueBuffer);
          if (timestamp > deletedAt) {
            pageData.putTime(timestamp);
            pageData.putBinary(aBinary);
          }
          break;
        default:
          throw new UnSupportedDataTypeException(String.valueOf(dataType));
      }
      preTimestamp=timestamp;
    }
    return pageData;
  }

  private BatchData getAllPageDataWithFilter() throws IOException {
    BatchData pageData = new BatchData(dataType, true);

    while (timeDecoder.hasNext(timeBuffer)) {
      long timestamp = timeDecoder.readLong(timeBuffer);

      switch (dataType) {
        case BOOLEAN:
          readBoolean(pageData, timestamp);
          break;
        case INT32:
          readInt(pageData, timestamp);
          break;
        case INT64:
          readLong(pageData, timestamp);
          break;
        case FLOAT:
          readFloat(pageData, timestamp);
          break;
        case DOUBLE:
          readDouble(pageData, timestamp);
          break;
        case TEXT:
          readText(pageData, timestamp);
          break;
        default:
          throw new UnSupportedDataTypeException(String.valueOf(dataType));
      }
    }

    return pageData;
  }

  private BatchData getAllPageDataWithFilter(int ratio) throws IOException {
    BatchData compressData = getAllPageDataWithFilter();
    if(ratio == 1 || compressData == null || compressData.length() < 2){
      return compressData;
    }
    BatchData pageData = new BatchData(dataType, true);
    pageData.putTime(compressData.getTimeByIndex(0));
    pageData.putLong(compressData.getLongByIndex(0));
    long preTime = compressData.getTimeByIndex(0);
    long preValue = compressData.getLongByIndex(0);
    for(int i = 1;i<compressData.length();i++){
      long curTime = compressData.getTimeByIndex(i);
      long curValue = compressData.getLongByIndex(i);

      long timeGap = (curTime-preTime)/ratio;
      double valGap = (curValue-preValue)*1.0/ratio;
      for(int j = 1; j<ratio;j++){
        long tmpTime = preTime+ timeGap*j;
        if(timeGap == 0 || tmpTime>=curTime){
          break;
        }
        long tmpVal = (long)(preValue + valGap*j);
        pageData.putTime(tmpTime);
        pageData.putLong(tmpVal);
      }
      pageData.putTime(curTime);
      pageData.putLong(curValue);
      preTime = curTime;
      preValue = curValue;
    }

    return pageData;
  }

  private void readBoolean(BatchData pageData, long timestamp) {
    boolean aBoolean = valueDecoder.readBoolean(valueBuffer);
    if (timestamp > deletedAt && filter.satisfy(timestamp, aBoolean)) {
      pageData.putTime(timestamp);
      pageData.putBoolean(aBoolean);
    }
  }

  private void readInt(BatchData pageData, long timestamp) {
    int anInt = valueDecoder.readInt(valueBuffer);
    if (timestamp > deletedAt && filter.satisfy(timestamp, anInt)) {
      pageData.putTime(timestamp);
      pageData.putInt(anInt);
    }
  }

  private void readLong(BatchData pageData, long timestamp) {
    long aLong = valueDecoder.readLong(valueBuffer);
    if (timestamp > deletedAt && filter.satisfy(timestamp, aLong)) {
      pageData.putTime(timestamp);
      pageData.putLong(aLong);
    }
  }

  private void readFloat(BatchData pageData, long timestamp) {
    float aFloat = valueDecoder.readFloat(valueBuffer);
    if (timestamp > deletedAt && filter.satisfy(timestamp, aFloat)) {
      pageData.putTime(timestamp);
      pageData.putFloat(aFloat);
    }
  }

  private void readDouble(BatchData pageData, long timestamp) {
    double aDouble = valueDecoder.readDouble(valueBuffer);
    if (timestamp > deletedAt && filter.satisfy(timestamp, aDouble)) {
      pageData.putTime(timestamp);
      pageData.putDouble(aDouble);
    }
  }

  private void readText(BatchData pageData, long timestamp) {
    Binary aBinary = valueDecoder.readBinary(valueBuffer);
    if (timestamp > deletedAt && filter.satisfy(timestamp, aBinary)) {
      pageData.putTime(timestamp);
      pageData.putBinary(aBinary);
    }
  }

  public void close() {
    timeBuffer = null;
    valueBuffer = null;
  }

  public void setDeletedAt(long deletedAt) {
    this.deletedAt = deletedAt;
  }
}
