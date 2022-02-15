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

package org.apache.iotdb.db.query.aggregation.impl;

import java.io.IOException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.aggregation.AggreResultData;
import org.apache.iotdb.db.query.aggregation.AggregateFunction;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;

public class AvgAggrFunc extends AggregateFunction {

  protected double sum = 0.0;
  private int cnt = 0;
  private TSDataType seriesDataType;
  private static final String AVG_AGGR_NAME = "AVG";

  public AvgAggrFunc(String funName, TSDataType seriesDataType) {
    super(funName, TSDataType.DOUBLE);
    this.seriesDataType = seriesDataType;
  }

  @Override
  public void init() {
    resultData.reset();
    sum = 0.0;
    cnt = 0;
  }

  @Override
  public AggreResultData getResult() {
    if (cnt > 0) {
      resultData.setTimestamp(0);
      resultData.setDoubleRet(sum / cnt);
    }
    return resultData;
  }

  @Override
  public void calculateValueFromPageHeader(PageHeader pageHeader) {
    sum += pageHeader.getStatistics().getSum();
    cnt += pageHeader.getNumOfValues();
  }

  @Override
  public void calculateValueFromPageHeader(PageHeader pageHeader, double ratio)
      throws QueryProcessException {
    sum += pageHeader.getStatistics().getSum()*ratio;
    cnt += pageHeader.getNumOfValues()*ratio;
  }

  @Override
  public void calculateValueFromPageData(PageHeader pageHeader, BatchData dataInThisPage, IPointReader unsequenceReader)
      throws IOException {
    calculateValueFromPageData(pageHeader, dataInThisPage, unsequenceReader, false, 0);
  }

  @Override
  public void calculateValueFromPageData(BatchData dataInThisPage, IPointReader unsequenceReader,
      long bound) throws IOException {
    calculateValueFromPageData(null, dataInThisPage, unsequenceReader, true, bound);
  }

  private void calculateValueFromPageData(PageHeader pageHeader, BatchData dataInThisPage, IPointReader unsequenceReader,
      boolean hasBound, long bound) throws IOException {
    int ratio = 1;
    if(pageHeader!=null){
      ratio = pageHeader.getCompressRatio();
    }
    while (dataInThisPage.hasNext() && unsequenceReader.hasNext()) {
      Object sumVal = null;
      long time = Math.min(dataInThisPage.currentTime(), unsequenceReader.current().getTimestamp());
      if (hasBound && time >= bound) {
        break;
      }
      if (dataInThisPage.currentTime() < unsequenceReader.current().getTimestamp()) {
        sumVal = dataInThisPage.currentValue();
        dataInThisPage.next();
        updateMean(seriesDataType, sumVal, ratio);
      } else if (dataInThisPage.currentTime() == unsequenceReader.current().getTimestamp()) {
        sumVal = unsequenceReader.current().getValue().getValue();
        dataInThisPage.next();
        unsequenceReader.next();
        updateMean(seriesDataType, sumVal, ratio);
      } else {
        sumVal = unsequenceReader.current().getValue().getValue();
        unsequenceReader.next();
        updateMean(seriesDataType, sumVal, 1);
      }

    }

    while (dataInThisPage.hasNext()) {
      if (hasBound && dataInThisPage.currentTime() >= bound) {
        break;
      }
      updateMean(seriesDataType, dataInThisPage.currentValue(), ratio);
      dataInThisPage.next();
    }
  }

  private void updateMean(TSDataType type, Object sumVal, int ratio) throws IOException {
    switch (type) {
      case INT32:
        sum += (int) sumVal * ratio;
        break;
      case INT64:
        sum += (long) sumVal*ratio;
        break;
      case FLOAT:
        sum += (float) sumVal*ratio;
        break;
      case DOUBLE:
        sum += (double) sumVal*ratio;
        break;
      case TEXT:
      case BOOLEAN:
      default:
        throw new IOException(
            String.format("Unsupported data type in aggregation %s : %s", getAggreTypeName(), type));
    }
    cnt+=ratio;
  }

  @Override
  public void calculateValueFromUnsequenceReader(IPointReader unsequenceReader)
      throws IOException {
    while (unsequenceReader.hasNext()) {
      TimeValuePair pair = unsequenceReader.next();
      updateMean(seriesDataType, pair.getValue().getValue(), 1);
    }
  }

  @Override
  public void calculateValueFromUnsequenceReader(IPointReader unsequenceReader, long bound)
      throws IOException {
    while (unsequenceReader.hasNext() && unsequenceReader.current().getTimestamp() < bound) {
      TimeValuePair pair = unsequenceReader.next();
      updateMean(seriesDataType, pair.getValue().getValue(), 1);
    }
  }

  @Override
  public void calcAggregationUsingTimestamps(long[] timestamps, int length,
      IReaderByTimestamp dataReader) throws IOException {
    for (int i = 0; i < length; i++) {
      Object value = dataReader.getValueInTimestamp(timestamps[i]);
      if (value != null) {
        updateMean(seriesDataType, value, 1);
      }
    }
  }

  @Override
  public boolean isCalculatedAggregationResult() {
    return false;
  }

  /**
   * Return type name of aggregation
   */
  public String getAggreTypeName() {
    return AVG_AGGR_NAME;
  }
}
