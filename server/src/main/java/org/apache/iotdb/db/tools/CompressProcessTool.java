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

import static org.apache.iotdb.db.compress.task.CompressTask.COMPRESS_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.db.compress.controll.DelayFunctionFactory;
import org.apache.iotdb.db.compress.controll.IDelayFunction;
import org.apache.iotdb.db.compress.controll.StrategyWraper;
import org.apache.iotdb.db.compress.manage.CompressManager;
import org.apache.iotdb.db.compress.manage.CompressResource;
import org.apache.iotdb.db.compress.manage.MergeContext;
import org.apache.iotdb.db.compress.recover.MergeLogger;
import org.apache.iotdb.db.compress.task.CompressMultiChunkTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.utils.MergeUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompressProcessTool {

  private static final Logger logger = LoggerFactory.getLogger(
      CompressProcessTool.class);
  private static final String RESOURCE_SUFFIX = TsFileResource.RESOURCE_SUFFIX;
  private static final String TSFILE_SUFFIX = ".tsfile";

  public static void main(String[] args) throws IOException {

//    String[] args2 = {"/Users/suyue/input/debug_bke/root.group_49.1/beforeCompress", "powerLaw", "1.5,0.5,10,4",
//        "5", "4"};
//    args = args2;
    if (args.length != 5 || args.length != 2) {
      System.out.println(
          "Need 2 arguments : TsFileListDir, ratio.");
      System.out.println(
          "Or need 5 arguments : TsFileListDir, delayFun, delayFunParam, concurrency, ratio.");
      return;
    }
    String tsFileDir = args[0];
    String delayFun = "powerLaw";
    String delayFunParam = "1.5,0.5,10,4";
    String concurrencyStr = "5";
    String ratioStr = "4";
    if(args.length==5){
      delayFun = args[1];
      delayFunParam = args[2];
      concurrencyStr = args[3];
      ratioStr = args[4];
    }
    else if(args.length==2){
      ratioStr = args[1];
    }

    IDelayFunction delayFunction = DelayFunctionFactory.generateDelayFunction(delayFun, delayFunParam);

    int concurrency = Integer.parseInt(concurrencyStr);
    float ratio = Float.parseFloat(ratioStr);


    List<TsFileResource> sequenceFiles = findAllResourceInDir(tsFileDir);
    List<MeasurementSchema> measurementSchemas = findAllMeasurementSchema(sequenceFiles);
    List<Path> unmergedSeries = findAllPath(sequenceFiles);
    System.out.println("Total series=" + unmergedSeries.size());
    if (measurementSchemas.size() == 0) {
      System.out.println("No time series can compressing...");
      return;
    }

    String firstDevice = unmergedSeries.get(0).getDevice();
    String storageGroupName = firstDevice.substring(0, firstDevice.lastIndexOf("."));

    CompressManager.getINSTANCE().start();
    doCompress(tsFileDir, storageGroupName, sequenceFiles, measurementSchemas, delayFunction, unmergedSeries,
        ratio, concurrency);
    CompressManager.getINSTANCE().stop();
  }

  public static List<Path> findAllPath(List<TsFileResource> sequenceFiles) throws IOException {
    Set<Path> pathSet = new HashSet<>();
    for (TsFileResource resource : sequenceFiles) {
      TsFileSequenceReader reader = new TsFileSequenceReader(resource.getFile().getAbsolutePath());
      TsFileMetaData fileMetaData = reader.readFileMetadata();
      for (TsDeviceMetadataIndex deviceMetadataIndex : fileMetaData.getDeviceMap().values()) {
        TsDeviceMetadata deviceMetadata = reader.readTsDeviceMetaData(deviceMetadataIndex);
        for (ChunkGroupMetaData chunkGroupMetaData : deviceMetadata.getChunkGroupMetaDataList()) {
          String deviceId = chunkGroupMetaData.getDeviceID();
          for (ChunkMetaData chunkMetaData : chunkGroupMetaData.getChunkMetaDataList()) {
            pathSet.add(new Path(deviceId, chunkMetaData.getMeasurementUid()));
          }
        }
      }

    }
    List<Path> pathList = new ArrayList<>(pathSet);
    return pathList;
  }

  public static List<MeasurementSchema> findAllMeasurementSchema(
      List<TsFileResource> sequenceFiles) throws IOException {
    Set<MeasurementSchema> measurementSchemaSet = new HashSet<>();
    for (TsFileResource resource : sequenceFiles) {
      TsFileSequenceReader reader = new TsFileSequenceReader(resource.getFile().getAbsolutePath());
      TsFileMetaData fileMetaData = reader.readFileMetadata();
      measurementSchemaSet.addAll(fileMetaData.getMeasurementSchemaList());
    }

    List<MeasurementSchema> measurementSchemaList = new ArrayList<>(measurementSchemaSet);
    return measurementSchemaList;
  }

  public static List<TsFileResource> findAllResourceInDir(String path) {
    List<TsFileResource> sequenceFiles = new ArrayList<>();

    File tsfileDirFile = new File(path);
    // path is a file
    if (tsfileDirFile.isFile()) {
      if (path.endsWith(RESOURCE_SUFFIX)) {
        path = path.substring(0, path.indexOf(RESOURCE_SUFFIX));
      }
      TsFileResource tsFileResource = genResource(tsfileDirFile, path);
      if (tsFileResource != null) {
        sequenceFiles.add(tsFileResource);
      }

    } else {
      // path is a dir, handle every file in dir
      for (File file : tsfileDirFile.listFiles()) {
        if (!file.getName().endsWith(TSFILE_SUFFIX)) {
          continue;
        }
        TsFileResource tsFileResource = genResource(tsfileDirFile, file.getAbsolutePath());
        if (tsFileResource != null) {
          sequenceFiles.add(tsFileResource);
        }
      }
    }
    return sequenceFiles;
  }

  public static TsFileResource genResource(File tsfileDirFile, String path) {
    if (hasTsFileAndResource(path)) {
      TsFileResource tsFileResource = new TsFileResource(new File(path));
      try {
        tsFileResource.deSerialize();
        return tsFileResource;
      } catch (IOException e) {
        e.printStackTrace();
        System.err.println(tsFileResource.getFile().getAbsolutePath() + RESOURCE_SUFFIX
            + " deSerialize error, skip ...");
      }
    } else {
      System.out.println(
          "There are no tsfile and resource file in the directory: " + tsfileDirFile
              .getAbsolutePath());
    }
    return null;
  }


  // 1605200267483-101-0.tsfile.resource
  public static boolean hasTsFileAndResource(String path) {
    if (path.endsWith(RESOURCE_SUFFIX)) {
      String tsfilePath = path.substring(0, path.indexOf(RESOURCE_SUFFIX));
      File file = new File(tsfilePath);
      return file.exists();
    } else if (path.endsWith(TSFILE_SUFFIX)) {
      String resourcePath = path + RESOURCE_SUFFIX;
      File file = new File(resourcePath);
      return file.exists();
    } else {
      return false;
    }
  }

  public static void doCompress(String tsFileDir, String storageGroupName, List<TsFileResource> sequenceFileList,
      List<MeasurementSchema> measurementSchemas, IDelayFunction delayFunction,
      List<Path> unmergedSeries, float ratio,
      int concurrentMergeSeriesNum) throws IOException {

    String taskName = storageGroupName + "-compress-" + System.currentTimeMillis();
    logger.info("{} starts to compress {} seqFiles", taskName, sequenceFileList.size());
    long startTime = System.currentTimeMillis();
    long totalFileSize = MergeUtils.collectFileSizes(sequenceFileList);
    Pair<Integer, Integer> actualVirtualPageNum = MergeUtils
        .collectActualVirtualPageNumPair(sequenceFileList);
    int actualPageNum = actualVirtualPageNum.left;
    int virtualPageNum = actualVirtualPageNum.right;
    logger.info("存储组 {},total actualPageNUm={}, total virtualPageNum={} ", taskName,
        actualPageNum, virtualPageNum);

    MergeContext mergeContext = new MergeContext();
    CompressResource resource = new CompressResource(sequenceFileList, Long.MAX_VALUE-1);
    resource.addMeasurements(measurementSchemas);

    MergeLogger mergeLogger = new MergeLogger(tsFileDir);
    mergeLogger.logFiles(resource);
    mergeLogger.logMergeStart();

    int unmergedSeriesNum = unmergedSeries.size();
    int targetPageNum = Math.round(actualPageNum / ratio);
    List<Integer> ans = StrategyWraper
        .calPageMapping(storageGroupName, actualPageNum, virtualPageNum, targetPageNum,
            resource.getSeqFiles(), delayFunction, unmergedSeriesNum);
    logger.info(
        "存储组 {},series 数目为{}.,avg actualPageNum={}, avg virtualPageNum={}, avg targetPageNum={}, futurePageNum={} ",
        taskName, unmergedSeries.size(),
        actualPageNum / unmergedSeriesNum, virtualPageNum / unmergedSeriesNum,
        targetPageNum / unmergedSeriesNum, ans.size());
    CompressMultiChunkTask mergeChunkTask = new CompressMultiChunkTask(mergeContext, taskName,
        mergeLogger, resource, unmergedSeries, concurrentMergeSeriesNum, ans, (actualPageNum-targetPageNum)/unmergedSeriesNum);
    mergeChunkTask.compressSeries(false);

    double elapsedTime = (double) (System.currentTimeMillis() - startTime) / 1000.0;
    double compressRate = 1.0 * mergeContext.getTotalPointWritten() / elapsedTime;
    System.out.println("Compress rate "+compressRate+" points/s.");


    logger.info("{} is cleaning up", taskName);
    resource.clear();
    mergeContext.clear();
    if (mergeLogger != null) {
      mergeLogger.close();
    }

    for (TsFileResource seqFile : resource.getSeqFiles()) {
      File mergeFile = new File(seqFile.getFile().getPath() + COMPRESS_SUFFIX);
      mergeFile.delete();
      seqFile.setCompressing(false);
    }

    if (logger.isInfoEnabled()) {
      double byteRate = totalFileSize / elapsedTime / 1024 / 1024;
      double seriesRate = unmergedSeries.size() / elapsedTime;
      double chunkRate = mergeContext.getTotalChunkWritten() / elapsedTime;
      double fileRate =
          (resource.getSeqFiles().size()) / elapsedTime;
      double ptRate = mergeContext.getTotalPointWritten() / elapsedTime;
      logger.info("{} ends after {}s, byteRate: {}MB/s, seriesRate {}/s, chunkRate: {}/s, "
              + "fileRate: {}/s, ptRate: {}/s",
          taskName, elapsedTime, byteRate, seriesRate, chunkRate, fileRate, ptRate);
    }
  }
}
