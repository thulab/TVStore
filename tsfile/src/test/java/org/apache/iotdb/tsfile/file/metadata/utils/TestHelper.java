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
package org.apache.iotdb.tsfile.file.metadata.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.header.PageHeaderTest;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaDataTest;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaDataTest;
import org.apache.iotdb.tsfile.file.metadata.TimeSeriesMetadataTest;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataTest;
import org.apache.iotdb.tsfile.file.metadata.TsDigest;
import org.apache.iotdb.tsfile.file.metadata.TsDigest.StatisticType;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaDataTest;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

public class TestHelper {

  private static final String MAX_VALUE = "321";
  private static final String MIN_VALUE = "123";
  private static final String SUM_VALUE = "321123";
  private static final String FIRST_VALUE = "1";
  private static final String LAST_VALUE = "222";

  public static TsFileMetaData createSimpleFileMetaData() {
    TsFileMetaData metaData = new TsFileMetaData(generateDeviceIndexMetadataMap(), new HashMap<>());
    metaData.addMeasurementSchema(TestHelper.createSimpleMeasurementSchema());
    metaData.addMeasurementSchema(TestHelper.createSimpleMeasurementSchema());
    metaData.setCreatedBy(TsFileMetaDataTest.CREATED_BY);
    return metaData;
  }

  public static Map<String, TsDeviceMetadataIndex> generateDeviceIndexMetadataMap() {
    Map<String, TsDeviceMetadataIndex> indexMap = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      indexMap.put("device_" + i, createSimpleDeviceIndexMetadata());
    }
    return indexMap;
  }

  public static Map<String, TsDeviceMetadata> generateDeviceMetadataMap() {
    Map<String, TsDeviceMetadata> deviceMetadataMap = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      deviceMetadataMap.put("device_" + i, createSimpleDeviceMetaData());
    }
    return deviceMetadataMap;
  }

  public static TsDeviceMetadataIndex createSimpleDeviceIndexMetadata() {
    TsDeviceMetadataIndex index = new TsDeviceMetadataIndex();
    index.setOffset(0);
    index.setLen(10);
    index.setStartTime(100);
    index.setEndTime(200);
    return index;
  }

  public static TsDeviceMetadata createSimpleDeviceMetaData() {
    TsDeviceMetadata metaData = new TsDeviceMetadata();
    metaData.setStartTime(TsDeviceMetadataTest.START_TIME);
    metaData.setEndTime(TsDeviceMetadataTest.END_TIME);
    metaData.addChunkGroupMetaData(TestHelper.createSimpleChunkGroupMetaData());
    metaData.addChunkGroupMetaData(TestHelper.createSimpleChunkGroupMetaData());
    return metaData;
  }

  public static ChunkGroupMetaData createEmptySeriesChunkGroupMetaData() {
    return new ChunkGroupMetaData("d1", new ArrayList<>(), 0);
  }

  public static ChunkGroupMetaData createSimpleChunkGroupMetaData() {
    ChunkGroupMetaData metaData = new ChunkGroupMetaData(ChunkGroupMetaDataTest.DELTA_OBJECT_UID,
        new ArrayList<>(), 0);
    metaData.addTimeSeriesChunkMetaData(TestHelper.createSimpleTimeSeriesChunkMetaData());
    metaData.addTimeSeriesChunkMetaData(TestHelper.createSimpleTimeSeriesChunkMetaData());
    return metaData;
  }

  public static ChunkMetaData createSimpleTimeSeriesChunkMetaData() {
    ChunkMetaData metaData = new ChunkMetaData(ChunkMetaDataTest.MEASUREMENT_UID,
        ChunkMetaDataTest.DATA_TYPE,
        ChunkMetaDataTest.FILE_OFFSET, ChunkMetaDataTest.START_TIME, ChunkMetaDataTest.END_TIME// ,
        // ChunkMetaDataTest.ENCODING_TYPE
    );
    metaData.setNumOfPoints(ChunkMetaDataTest.NUM_OF_POINTS);
    metaData.setDigest(new TsDigest());
    return metaData;
  }

  public static ChunkMetaData createNotCompleteSimpleTimeSeriesChunkMetaData() throws IOException {
    ChunkMetaData metaData = new ChunkMetaData(ChunkMetaDataTest.MEASUREMENT_UID,
        ChunkMetaDataTest.DATA_TYPE,
        ChunkMetaDataTest.FILE_OFFSET, ChunkMetaDataTest.START_TIME, ChunkMetaDataTest.END_TIME
    );
    metaData.setNumOfPoints(ChunkMetaDataTest.NUM_OF_POINTS);
    metaData.setDigest(createNotCompleteSimpleTsDigest());
    return metaData;
  }

  public static MeasurementSchema createSimpleMeasurementSchema() {
    return new MeasurementSchema(TimeSeriesMetadataTest.measurementUID,
        TSDataType.INT64,
        TSEncoding.RLE);
  }

  public static TsDigest createSimpleTsDigest() throws IOException {
    TsDigest digest = new TsDigest();
    ByteBuffer[] statisticsArray = new ByteBuffer[StatisticType.getTotalTypeNum()];
    statisticsArray[StatisticType.min_value.ordinal()] = ByteBuffer
        .wrap(BytesUtils.stringToBytes(MIN_VALUE));
    statisticsArray[StatisticType.max_value.ordinal()] = ByteBuffer
        .wrap(BytesUtils.stringToBytes(MAX_VALUE));
    statisticsArray[StatisticType.first_value.ordinal()] = ByteBuffer
        .wrap(BytesUtils.stringToBytes(FIRST_VALUE));
    statisticsArray[StatisticType.last_value.ordinal()] = ByteBuffer
        .wrap(BytesUtils.stringToBytes(LAST_VALUE));
    statisticsArray[StatisticType.sum_value.ordinal()] = ByteBuffer
        .wrap(BytesUtils.stringToBytes(SUM_VALUE));
    digest.setStatistics(statisticsArray);
    return digest;
  }

  public static TsDigest createNotCompleteSimpleTsDigest() throws IOException {
    TsDigest digest = new TsDigest();
    ByteBuffer[] statisticsArray = new ByteBuffer[StatisticType.getTotalTypeNum()];
    statisticsArray[StatisticType.first_value.ordinal()] = ByteBuffer
        .wrap(BytesUtils.stringToBytes(FIRST_VALUE));
    statisticsArray[StatisticType.last_value.ordinal()] = ByteBuffer
        .wrap(BytesUtils.stringToBytes(LAST_VALUE));
    statisticsArray[StatisticType.sum_value.ordinal()] = ByteBuffer
        .wrap(BytesUtils.stringToBytes(SUM_VALUE));
    digest.setStatistics(statisticsArray);
    return digest;
  }

  public static List<String> getJSONArray() {
    List<String> jsonMetaData = new ArrayList<String>();
    jsonMetaData.add("fsdfsfsd");
    jsonMetaData.add("424fd");
    return jsonMetaData;
  }

  public static PageHeader createSimplePageHeader() {
    Statistics<?> statistics = Statistics.getStatsByType(PageHeaderTest.DATA_TYPE);
    statistics.setEmpty(false);
    return new PageHeader(PageHeaderTest.UNCOMPRESSED_SIZE,
        PageHeaderTest.COMPRESSED_SIZE, PageHeaderTest.NUM_OF_VALUES,
        statistics, PageHeaderTest.MAX_TIMESTAMO, PageHeaderTest.MIN_TIMESTAMO,
        PageHeaderTest.COMPRESS_RATIO, PageHeaderTest.TIME_INTERVAL_SUM,
        PageHeaderTest.TIME_INTERVAL_SQUARE_SUM);
  }
}
