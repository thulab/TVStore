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
package org.apache.iotdb.tsfile.write.chunk;

import java.io.IOException;
import java.math.BigDecimal;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.page.PageWriter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A implementation of {@code IChunkWriter}. {@code ChunkWriterImpl} consists of a {@code
 * ChunkBuffer}, a {@code PageWriter}, and two {@code Statistics}.
 *
 * @see IChunkWriter IChunkWriter
 */
public class ChunkWriterImpl implements IChunkWriter {

  private static final Logger LOG = LoggerFactory.getLogger(ChunkWriterImpl.class);

  private MeasurementSchema measurementSchema;

  /**
   * help to encode data of this series.
   */
  private final ChunkBuffer chunkBuffer;

  /**
   * value writer to encode data.
   */
  private PageWriter pageWriter;

  /**
   * page size threshold.
   */
  private final long pageSizeThreshold;

  private final int maxNumberOfPointsInPage;

  // initial value for this.valueCountInOnePageForNextCheck
  private static final int MINIMUM_RECORD_COUNT_FOR_CHECK = 1500;

  /**
   * value count in a page. It will be reset after calling {@code writePageHeaderAndDataIntoBuff()}
   */
  private int valueCountInOnePage;
  private int valueCountInOnePageForNextCheck;
  private int uncompressedValueCount;

  /**
   * statistic on a stage. It will be reset after calling {@code writeAllPagesOfSeriesToTsFile()}
   */
  private Statistics<?> chunkStatistics;

  /**
   * statistic on a page. It will be reset after calling {@code writePageHeaderAndDataIntoBuff()}
   */
  private Statistics<?> pageStatistics;

  // time of the latest written time value pair, we assume data is written in time order
  private long maxTimestamp;
  private long minTimestamp = Long.MIN_VALUE;

  private int pageCompressRatio=0;
  private int totalVirtualPageNum=0;

  private long timeIntervalSum = 0;
  private long timeIntervalSquareSum = 0;
  private long lastTimestampInOnePage = Long.MIN_VALUE;

  /**
   * @param schema schema of this measurement
   */
  public ChunkWriterImpl(MeasurementSchema schema) {
    this.measurementSchema = schema;
    this.chunkBuffer = new ChunkBuffer(measurementSchema);

    this.pageSizeThreshold = TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
    this.maxNumberOfPointsInPage = TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();
    // initial check of memory usage. So that we have enough data to make an initial prediction
    this.valueCountInOnePageForNextCheck = MINIMUM_RECORD_COUNT_FOR_CHECK;

    // init statistics for this series and page
    this.chunkStatistics = Statistics.getStatsByType(measurementSchema.getType());
    this.pageStatistics = Statistics.getStatsByType(measurementSchema.getType());

    this.pageWriter = new PageWriter();

    this.pageWriter.setTimeEncoder(measurementSchema.getTimeEncoder());
    this.pageWriter.setValueEncoder(measurementSchema.getValueEncoder());
  }

  @Override
  public void write(long time, long value) {
    this.maxTimestamp = time;
    ++valueCountInOnePage;
    ++uncompressedValueCount;
    pageWriter.write(time, value);
    pageStatistics.updateStats(value);
    if (minTimestamp == Long.MIN_VALUE) {
      minTimestamp = time;
    }
    else {
      long interval = time-lastTimestampInOnePage;
      timeIntervalSum+=interval;
      timeIntervalSquareSum+= interval*interval;
    }
    lastTimestampInOnePage = time;
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long time, int value) {
    this.maxTimestamp = time;
    ++valueCountInOnePage;
    ++uncompressedValueCount;
    pageWriter.write(time, value);
    pageStatistics.updateStats(value);
    if (minTimestamp == Long.MIN_VALUE) {
      minTimestamp = time;
    }
    else {
      long interval = time-lastTimestampInOnePage;
      timeIntervalSum+=interval;
      timeIntervalSquareSum+= interval*interval;
    }
    lastTimestampInOnePage = time;
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long time, boolean value) {
    this.maxTimestamp = time;
    ++valueCountInOnePage;
    ++uncompressedValueCount;
    pageWriter.write(time, value);
    pageStatistics.updateStats(value);
    if (minTimestamp == Long.MIN_VALUE) {
      minTimestamp = time;
    }
    else {
      long interval = time-lastTimestampInOnePage;
      timeIntervalSum+=interval;
      timeIntervalSquareSum+= interval*interval;
    }
    lastTimestampInOnePage = time;
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long time, float value) {
    this.maxTimestamp = time;
    ++valueCountInOnePage;
    ++uncompressedValueCount;
    pageWriter.write(time, value);
    pageStatistics.updateStats(value);
    if (minTimestamp == Long.MIN_VALUE) {
      minTimestamp = time;
    }
    else {
      long interval = time-lastTimestampInOnePage;
      timeIntervalSum+=interval;
      timeIntervalSquareSum+= interval*interval;
    }
    lastTimestampInOnePage = time;
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long time, double value) {
    this.maxTimestamp = time;
    ++valueCountInOnePage;
    ++uncompressedValueCount;
    pageWriter.write(time, value);
    pageStatistics.updateStats(value);
    if (minTimestamp == Long.MIN_VALUE) {
      minTimestamp = time;
    }
    else {
      long interval = time-lastTimestampInOnePage;
      timeIntervalSum+=interval;
      timeIntervalSquareSum+= interval*interval;
    }
    lastTimestampInOnePage = time;
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long time, Binary value) {
    this.maxTimestamp = time;
    ++valueCountInOnePage;
    ++uncompressedValueCount;
    pageWriter.write(time, value);
    pageStatistics.updateStats(value);
    if (minTimestamp == Long.MIN_VALUE) {
      minTimestamp = time;
    }
    else {
      long interval = time-lastTimestampInOnePage;
      timeIntervalSum+=interval;
      timeIntervalSquareSum+= interval*interval;
    }
    lastTimestampInOnePage = time;
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long[] timestamps, int[] values, int batchSize) {
    this.maxTimestamp = timestamps[batchSize - 1];
    valueCountInOnePage += batchSize;
    uncompressedValueCount+=batchSize;
    if (minTimestamp == Long.MIN_VALUE) {
      minTimestamp = timestamps[0];
    }
    else {
      long interval = timestamps[0]-lastTimestampInOnePage;
      timeIntervalSum+=interval;
      timeIntervalSquareSum+= interval*interval;
    }
    for(int i = 1;i<batchSize;i++){
      long interval = timestamps[i]-timestamps[i-1];
      timeIntervalSum+=interval;
      timeIntervalSquareSum+= interval*interval;
    }
    lastTimestampInOnePage = timestamps[batchSize-1];
    pageWriter.write(timestamps, values, batchSize);
    pageStatistics.updateStats(values, batchSize);
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long[] timestamps, long[] values, int batchSize) {
    this.maxTimestamp = timestamps[batchSize - 1];
    valueCountInOnePage += batchSize;
    uncompressedValueCount+=batchSize;
    if (minTimestamp == Long.MIN_VALUE) {
      minTimestamp = timestamps[0];
    }
    else {
      long interval = timestamps[0]-lastTimestampInOnePage;
      timeIntervalSum+=interval;
      timeIntervalSquareSum+= interval*interval;
    }
    for(int i = 1;i<batchSize;i++){
      long interval = timestamps[i]-timestamps[i-1];
      timeIntervalSum+=interval;
      timeIntervalSquareSum+= interval*interval;
    }
    lastTimestampInOnePage = timestamps[batchSize-1];
    pageWriter.write(timestamps, values, batchSize);
    pageStatistics.updateStats(values, batchSize);
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long[] timestamps, boolean[] values, int batchSize) {
    this.maxTimestamp = timestamps[batchSize - 1];
    valueCountInOnePage += batchSize;
    uncompressedValueCount+=batchSize;
    if (minTimestamp == Long.MIN_VALUE) {
      minTimestamp = timestamps[0];
    }
    else {
      long interval = timestamps[0]-lastTimestampInOnePage;
      timeIntervalSum+=interval;
      timeIntervalSquareSum+= interval*interval;
    }
    for(int i = 1;i<batchSize;i++){
      long interval = timestamps[i]-timestamps[i-1];
      timeIntervalSum+=interval;
      timeIntervalSquareSum+= interval*interval;
    }
    lastTimestampInOnePage = timestamps[batchSize-1];
    pageWriter.write(timestamps, values, batchSize);
    pageStatistics.updateStats(values, batchSize);
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long[] timestamps, float[] values, int batchSize) {
    this.maxTimestamp = timestamps[batchSize - 1];
    valueCountInOnePage += batchSize;
    uncompressedValueCount+=batchSize;
    if (minTimestamp == Long.MIN_VALUE) {
      minTimestamp = timestamps[0];
    }
    else {
      long interval = timestamps[0]-lastTimestampInOnePage;
      timeIntervalSum+=interval;
      timeIntervalSquareSum+= interval*interval;
    }
    for(int i = 1;i<batchSize;i++){
      long interval = timestamps[i]-timestamps[i-1];
      timeIntervalSum+=interval;
      timeIntervalSquareSum+= interval*interval;
    }
    lastTimestampInOnePage = timestamps[batchSize-1];
    pageWriter.write(timestamps, values, batchSize);
    pageStatistics.updateStats(values, batchSize);
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long[] timestamps, double[] values, int batchSize) {
    this.maxTimestamp = timestamps[batchSize - 1];
    valueCountInOnePage += batchSize;
    uncompressedValueCount+=batchSize;
    if (minTimestamp == Long.MIN_VALUE) {
      minTimestamp = timestamps[0];
    }
    else {
      long interval = timestamps[0]-lastTimestampInOnePage;
      timeIntervalSum+=interval;
      timeIntervalSquareSum+= interval*interval;
    }
    for(int i = 1;i<batchSize;i++){
      long interval = timestamps[i]-timestamps[i-1];
      timeIntervalSum+=interval;
      timeIntervalSquareSum+= interval*interval;
    }
    lastTimestampInOnePage = timestamps[batchSize-1];
    pageWriter.write(timestamps, values, batchSize);
    pageStatistics.updateStats(values, batchSize);
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long[] timestamps, Binary[] values, int batchSize) {
    this.maxTimestamp = timestamps[batchSize - 1];
    valueCountInOnePage += batchSize;
    uncompressedValueCount+=batchSize;
    if (minTimestamp == Long.MIN_VALUE) {
      minTimestamp = timestamps[0];
    }
    else {
      long interval = timestamps[0]-lastTimestampInOnePage;
      timeIntervalSum+=interval;
      timeIntervalSquareSum+= interval*interval;
    }
    for(int i = 1;i<batchSize;i++){
      long interval = timestamps[i]-timestamps[i-1];
      timeIntervalSum+=interval;
      timeIntervalSquareSum+= interval*interval;
    }
    lastTimestampInOnePage = timestamps[batchSize-1];
    pageWriter.write(timestamps, values, batchSize);
    pageStatistics.updateStats(values, batchSize);
    checkPageSizeAndMayOpenANewPage();
  }

  /**
   * check occupied memory size, if it exceeds the PageSize threshold, flush them to given
   * OutputStream.
   */
  private void checkPageSizeAndMayOpenANewPage() {
    if (valueCountInOnePage == maxNumberOfPointsInPage) {
      LOG.debug("current line count reaches the upper bound, write page {}", measurementSchema);
      pageCompressRatio+=1;
      writePage();
    } else if (valueCountInOnePage >= valueCountInOnePageForNextCheck) { // need to check memory size
      // not checking the memory used for every value
      long currentPageSize = pageWriter.estimateMaxMemSize();
      if (currentPageSize > pageSizeThreshold) { // memory size exceeds threshold
        // we will write the current page
        LOG.debug(
            "enough size, write page {}, pageSizeThreshold:{}, currentPateSize:{}, valueCountInOnePage:{}, unCompressedPointInPage:{}",
            measurementSchema.getMeasurementId(), pageSizeThreshold, currentPageSize, valueCountInOnePage, uncompressedValueCount);
        pageCompressRatio+=1;
        writePage();
        valueCountInOnePageForNextCheck = MINIMUM_RECORD_COUNT_FOR_CHECK;
      } else {
        // reset the valueCountInOnePageForNextCheck for the next page
        valueCountInOnePageForNextCheck = (int) (((float) pageSizeThreshold / currentPageSize)
            * valueCountInOnePage);
      }
    }
  }

  /**
   * flush data into {@code IChunkWriter}.
   */
  private void writePage() {
    try {
      chunkBuffer.writePageHeaderAndDataIntoBuff(pageWriter.getUncompressedBytes(),
          uncompressedValueCount, pageStatistics, maxTimestamp, minTimestamp, pageCompressRatio,
          timeIntervalSum, timeIntervalSquareSum);
//      chunkBuffer.writePageHeaderAndDataIntoBuff(pageWriter.getUncompressedBytes(),
//          valueCountInOnePage, pageStatistics, maxTimestamp, minTimestamp, pageCompressRatio);

      // update statistics of this series
      this.chunkStatistics.mergeStatistics(this.pageStatistics);
      totalVirtualPageNum+=pageCompressRatio;
    } catch (IOException e) {
      LOG.error("meet error in pageWriter.getUncompressedBytes(),ignore this page:", e);
    } catch (PageException e) {
      LOG.error(
          "meet error in chunkBuffer.writePageHeaderAndDataIntoBuff, ignore this page:", e);
    } finally {
      // clear start time stamp for next initializing
      minTimestamp = Long.MIN_VALUE;
      lastTimestampInOnePage = Long.MIN_VALUE;
      valueCountInOnePage = 0;
      uncompressedValueCount = 0;
      pageWriter.reset();
      pageCompressRatio = 0;
      timeIntervalSquareSum = 0;
      timeIntervalSum = 0;
      this.pageStatistics = Statistics.getStatsByType(measurementSchema.getType());
    }
  }

  public void updatePageHeader(PageHeader pageHeader){
    pageCompressRatio+=pageHeader.getCompressRatio();
    long interval = 0;
    if(timeIntervalSum >0){
      interval = pageHeader.getMinTimestamp()-maxTimestamp;
    }
    timeIntervalSum+=pageHeader.getTimeIntervalSum() + interval;
    timeIntervalSquareSum+=pageHeader.getTimeIntervalSquareSum() + interval*interval;

    if(pageHeader.getMaxTimestamp()>maxTimestamp){
      maxTimestamp = pageHeader.getMaxTimestamp();
    }
    if((pageHeader.getMinTimestamp()>=0 && pageHeader.getMinTimestamp()<minTimestamp) || (minTimestamp == Long.MIN_VALUE)){
      minTimestamp = pageHeader.getMinTimestamp();
    }

    this.pageStatistics.mergeStatistics(pageHeader.getStatistics());
  }

  public void writePage(BatchData batchData, int targetNum, int unCompressedPointNum) {
    valueCountInOnePage = targetNum;
    this.uncompressedValueCount = unCompressedPointNum;
    switch (batchData.getDataType()) {
      case BOOLEAN:
        for(int i=0;i<targetNum;i++){
          pageWriter.write(batchData.getTimeByIndex(i), batchData.getBooleanByIndex(i));
        }
        break;
      case INT32:
        for(int i=0;i<targetNum;i++){
          pageWriter.write(batchData.getTimeByIndex(i), batchData.getIntByIndex(i));
        }
        break;
      case INT64:
        for(int i=0;i<targetNum;i++){
          pageWriter.write(batchData.getTimeByIndex(i), batchData.getLongByIndex(i));
        }
        break;
      case FLOAT:
        for(int i=0;i<targetNum;i++){
          pageWriter.write(batchData.getTimeByIndex(i), batchData.getFloatByIndex(i));
        }
        break;
      case DOUBLE:
        for(int i=0;i<targetNum;i++){
          pageWriter.write(batchData.getTimeByIndex(i), batchData.getDoubleByIndex(i));
        }
        break;
      case TEXT:
        for(int i=0;i<targetNum;i++){
          pageWriter.write(batchData.getTimeByIndex(i), batchData.getBinaryByIndex(i));
        }
        break;
      default:
        throw new UnSupportedDataTypeException(String.valueOf(batchData.getDataType()));
    }
    writePage();

  }

  @Override
  public void writeToFileWriter(TsFileIOWriter tsfileWriter) throws IOException {
    sealCurrentPage();
    chunkBuffer.writeAllPagesOfSeriesToTsFile(tsfileWriter, chunkStatistics, totalVirtualPageNum);
    chunkBuffer.reset();
    // reset series_statistics
    this.chunkStatistics = Statistics.getStatsByType(measurementSchema.getType());
    pageCompressRatio = 0;
    totalVirtualPageNum = 0;
  }

  @Override
  public long estimateMaxSeriesMemSize() {
    return pageWriter.estimateMaxMemSize() + chunkBuffer.estimateMaxPageMemSize();
  }

  @Override
  public long getCurrentChunkSize() {
    // return the serialized size of the chunk header + all pages
    return ChunkHeader.getSerializedSize(measurementSchema.getMeasurementId()) + chunkBuffer
        .getCurrentDataSize();
  }

  @Override
  public void sealCurrentPage() {
    if (valueCountInOnePage > 0) {
      pageCompressRatio++;
      writePage();
    }
  }

  @Override
  public int getNumOfPages() {
    return chunkBuffer.getNumOfPages();
  }

  public ChunkBuffer getChunkBuffer() {
    return chunkBuffer;
  }

  @Override
  public TSDataType getDataType() {
    return measurementSchema.getType();
  }
}
