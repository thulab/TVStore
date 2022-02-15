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

package org.apache.iotdb.db.compress.task;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.iotdb.db.compress.controll.StrategyWraper;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.compress.manage.CompressResource;
import org.apache.iotdb.db.compress.manage.MergeContext;
import org.apache.iotdb.db.compress.recover.MergeLogger;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.utils.MergeUtils;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CompressTask merges given seqFiles into new ones, which basically consists of three
 * steps: 1. rewrite overflowed, modified or small-sized chunks into temp merge files
 *        2. move the merged chunks in the temp files back to the seqFiles or move the unmerged
 *        chunks in the seqFiles into temp files and replace the seqFiles with the temp files.
 *        3. remove unseqFiles
 */
public class CompressTask implements Callable<Void> {

  public static final String COMPRESS_SUFFIX = ".compress";
  private static final Logger logger = LoggerFactory.getLogger(
      CompressTask.class);

  CompressResource resource;
  String storageGroupSysDir;
  String storageGroupName;
  MergeLogger mergeLogger;
  MergeContext mergeContext = new MergeContext();

  private CompressCallback callback;
  int concurrentMergeSeriesNum;
  String taskName;

  public CompressTask(CompressResource compressResource, String storageGroupSysDir, CompressCallback callback,
      String taskName, int concurrentMergeSeriesNum, String storageGroupName) {
    this.resource = compressResource;
    this.storageGroupSysDir = storageGroupSysDir;
    this.callback = callback;
    this.taskName = taskName;
    this.concurrentMergeSeriesNum = concurrentMergeSeriesNum;
    this.storageGroupName = storageGroupName;
  }

  @Override
  public Void call() throws Exception {
    try  {
      doCompress();
    } catch (Exception e) {
      e.printStackTrace();
      logger.error("Runtime exception in compress {}, {}", taskName, e.getMessage());
      cleanUp(false);
      // call the callback to make sure the StorageGroup exit merging status, but passing 2
      // empty file lists to avoid files being deleted.
      callback.call(resource.getSeqFiles(), false, new File(storageGroupSysDir, MergeLogger.MERGE_LOG_NAME));
      throw e;
    }
    return null;
  }

  private void doCompress() throws IOException, MetadataException {

    logger.info("{} starts to compress {} seqFiles", taskName, resource.getSeqFiles().size());
    long startTime = System.currentTimeMillis();
    long totalFileSize = MergeUtils.collectFileSizes(resource.getSeqFiles());
    Pair<Integer, Integer> actualVirtualPageNum = MergeUtils.collectActualVirtualPageNumPair(resource.getSeqFiles());
    int actualPageNum = actualVirtualPageNum.left;
    int virtualPageNum = actualVirtualPageNum.right;
    int targetPageNum = (int)(actualPageNum/IoTDBDescriptor.getInstance().getConfig().getCompressRatio());
    logger.info("存储组 {},total actualPageNUm={}, total virtualPageNum={} ", taskName,
        actualPageNum, virtualPageNum);


    mergeLogger = new MergeLogger(storageGroupSysDir);
    mergeLogger.logFiles(resource);

    List<MeasurementSchema> measurementSchemas = MManager.getInstance()
        .getSchemaForStorageGroup(storageGroupName);
    resource.addMeasurements(measurementSchemas);

    List<String> storageGroupPaths = MManager.getInstance().getPaths(storageGroupName + ".*");
    List<Path> unmergedSeries = new ArrayList<>();
    for (String path : storageGroupPaths) {
      unmergedSeries.add(new Path(path));
    }
//    actualPageNum/=unmergedSeries.size();
//    virtualPageNum/=unmergedSeries.size();

    mergeLogger.logMergeStart();
    List<Integer> ans = StrategyWraper.calPageMapping(storageGroupName, actualPageNum, virtualPageNum, resource.getSeqFiles(), unmergedSeries.size());
    logger.info("存储组 {},series 数目为{}.,avg actualPageNum={}, avg virtualPageNum={}, futurePageNum={} ", taskName,unmergedSeries.size(),
        actualPageNum, virtualPageNum, ans.size());
    CompressMultiChunkTask mergeChunkTask = new CompressMultiChunkTask(mergeContext, taskName, mergeLogger, resource,
         unmergedSeries, concurrentMergeSeriesNum, ans, (actualPageNum-targetPageNum)/unmergedSeries.size());
    mergeChunkTask.compressSeries(true);
//    mergeChunkTask.mergeSeries();
//
//    CompressFileTask compressFileTask = new CompressFileTask(taskName, mergeContext, mergeLogger, resource,
//        resource.getSeqFiles());
//    compressFileTask.mergeFiles();


    double elapsedTime = (double) (System.currentTimeMillis() - startTime) / 1000.0;
    StorageEngine.getInstance().updateCompressSpeed(1.0*mergeContext.getTotalPointWritten()/elapsedTime);
    cleanUp(true);
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

  void cleanUp(boolean executeCallback) throws IOException {
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


    File logFile = new File(storageGroupSysDir, MergeLogger.MERGE_LOG_NAME);
    if (executeCallback) {
      // compressEndAction(): set isCompressing status and handle modify file.
     callback.call(resource.getSeqFiles(), true, logFile);
    } else {
      logFile.delete();
    }
  }
}
