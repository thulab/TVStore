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

package org.apache.iotdb.db.tools;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.path.PathException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.exception.filter.UnSupportFilterDataTypeException;
import org.apache.iotdb.tsfile.file.footer.ChunkGroupFooter;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsDigest;
import org.apache.iotdb.tsfile.file.metadata.TsDigest.StatisticType;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.BloomFilter;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

public class GenerateMetaData {
  Map<String, Map<String, MeasurementSchema>> deviceMeasurementSchemaMap = new HashMap<>();
  Set<String> groupSet = new HashSet<>();
  MManager mmanager = MManager.getInstance();
  public GenerateMetaData(){
    mmanager.init();
    mmanager.clear();
  }

  public static void main(String[] args) throws IOException, MetadataException, PathException {
    String dirName = "/Users/suyue/input/tsfileCom/redd_low_40/";
    if (args.length == 1) {
      dirName = args[0];
    }
    new GenerateMetaData().handleDir(dirName);
  }

  public void handleDir(String dirName) throws IOException, MetadataException, PathException {
    File file = new File(dirName);
    if(!file.exists()){
      return;
    }
    if(file.isFile() && file.getName().endsWith(".tsfile")){
      handleOneFile(file.getAbsolutePath());
    }
    else if(file.isDirectory()){
      for(File file1:file.listFiles()){
        handleDir(file1.getAbsolutePath());
      }
    }
  }


  public void handleOneFile(String filename) throws IOException, MetadataException, PathException {

    System.out.println("TsFile path:" + filename);
    long length = FSFactoryProducer.getFSFactory().getFile(filename).length();
    System.out.println("TsFile length:" + length);

    // get metadata information
    TsFileSequenceReader reader = new TsFileSequenceReader(filename);
    TsFileMetaData tsFileMetaData = reader.readFileMetadata();
    Map<String, MeasurementSchema> measurementSchemaMap = tsFileMetaData.getMeasurementSchema();
    List<TsDeviceMetadataIndex> tsDeviceMetadataIndexSortedList = tsFileMetaData.getDeviceMap()
        .values()
        .stream()
        .sorted((x, y) -> (int) (x.getOffset() - y.getOffset())).collect(Collectors.toList());
    List<ChunkGroupMetaData> chunkGroupMetaDataTmpList = new ArrayList<>();
    List<TsDeviceMetadata> tsDeviceMetadataSortedList = new ArrayList<>();
    for (TsDeviceMetadataIndex index : tsDeviceMetadataIndexSortedList) {
      TsDeviceMetadata deviceMetadata = reader.readTsDeviceMetaData(index);
      tsDeviceMetadataSortedList.add(deviceMetadata);
      chunkGroupMetaDataTmpList.addAll(deviceMetadata.getChunkGroupMetaDataList());
    }
    List<ChunkGroupMetaData> chunkGroupMetaDataSortedList = chunkGroupMetaDataTmpList.stream()
        .sorted(Comparator.comparingLong(ChunkGroupMetaData::getStartOffsetOfChunkGroup))
        .collect(Collectors.toList());



    for(ChunkGroupMetaData chunkGroupMetaData: chunkGroupMetaDataSortedList){
      deviceMeasurementSchemaMap.putIfAbsent(chunkGroupMetaData.getDeviceID(), new HashMap<>());
      String deviceId = chunkGroupMetaData.getDeviceID();
      System.out.println("Device:"+deviceId);
      //String groupId = deviceId.substring(0, deviceId.lastIndexOf("."));
      String groupId = "root."+deviceId.split("\\.")[1];
      System.out.println("Device:"+deviceId+", Group:"+groupId);
      if(!groupSet.contains(groupId)){
        groupSet.add(groupId);
        mmanager.setStorageGroupToMTree(groupId);
      }
      for(ChunkMetaData chunkMetaData : chunkGroupMetaData.getChunkMetaDataList()){
        String measurementId = chunkMetaData.getMeasurementUid();
        Map<String, MeasurementSchema> newSchemaMap = deviceMeasurementSchemaMap.get(deviceId);
        if(!newSchemaMap.containsKey(measurementId)){
          newSchemaMap.putIfAbsent(measurementId, measurementSchemaMap.get(measurementId));
          MeasurementSchema schama = measurementSchemaMap.get(measurementId);
          mmanager.addPathToMTree(new Path(deviceId, measurementId), schama.getType(),
              schama.getEncodingType(), schama.getCompressor(), schama.getProps());
        }

      }
    }
    //MManager.getInstance().getMetadata().




  }

  private static void printlnBoth(PrintWriter pw, String str) {
    System.out.println(str);
    pw.println(str);
  }

  private static String statisticByteBufferToString(TSDataType tsDataType, TsDigest tsDigest) {
    ByteBuffer[] statistics = tsDigest.getStatistics();
    if (statistics == null) {
      return "TsDigest:[]";
    }
    StringBuilder str = new StringBuilder();
    str.append("TsDigest:[");
    for (int i = 0; i < statistics.length - 1; i++) {
      ByteBuffer value = statistics[i];
      str.append(StatisticType.values()[i]);
      str.append(":");
      if (value == null) {
        str.append("null");
      } else {
        switch (tsDataType) {
          case INT32:
            str.append(BytesUtils.bytesToInt(value.array()));
            break;
          case INT64:
            str.append(BytesUtils.bytesToLong(value.array()));
            break;
          case FLOAT:
            str.append(BytesUtils.bytesToFloat(value.array()));
            break;
          case DOUBLE:
            str.append(BytesUtils.bytesToDouble(value.array()));
            break;
          case TEXT:
            str.append(BytesUtils.bytesToString(value.array()));
            break;
          case BOOLEAN:
            str.append(BytesUtils.bytesToBool(value.array()));
            break;
          default:
            throw new UnSupportFilterDataTypeException(
                "DigestForFilter unsupported datatype : " + tsDataType.toString());
        }
      }
      str.append(",");
    }
    // Note that the last statistic of StatisticType is sum_value, which is double.
    str.append(StatisticType.values()[statistics.length - 1]);
    str.append(":");
    ByteBuffer value = statistics[statistics.length - 1];
    if (value == null) {
      str.append("null");
    } else {
      str.append(BytesUtils.bytesToDouble(value.array()));
    }
    str.append("]");

    return str.toString();
  }

}