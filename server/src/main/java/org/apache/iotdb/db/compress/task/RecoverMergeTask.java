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

/**
 * RecoverMergeTask is an extension of CompressTask, which resumes the last merge progress by
 * scanning merge.log using LogAnalyzer and continue the unfinished merge.
 */
public class RecoverMergeTask {
//  public class RecoverMergeTask extends CompressTask {

//  private static final Logger logger = LoggerFactory.getLogger(
//      RecoverMergeTask.class);
//
//  private LogAnalyzer analyzer;
//
//  public RecoverMergeTask(List<TsFileResource> seqFiles, String storageGroupSysDir,
//      CompressCallback callback, String taskName,
//      boolean fullMerge, String storageGroupName) {
//    super(new CompressResource(seqFiles), storageGroupSysDir, callback, taskName, fullMerge,1, storageGroupName);
//  }
//
//  public void recoverMerge(boolean continueMerge) throws IOException, MetadataException {
//    File logFile = new File(storageGroupSysDir, MergeLogger.MERGE_LOG_NAME);
//    if (!logFile.exists()) {
//      logger.info("{} no merge.log, merge recovery ends", taskName);
//      return;
//    }
//    long startTime = System.currentTimeMillis();
//
//    analyzer = new LogAnalyzer(resource, taskName, logFile, storageGroupName);
//    Status status = analyzer.analyze();
//    if (logger.isInfoEnabled()) {
//      logger.info("{} merge recovery status determined: {} after {}ms", taskName, status,
//          (System.currentTimeMillis() - startTime));
//    }
//    switch (status) {
//      case NONE:
//        logFile.delete();
//        break;
//      case MERGE_START:
//        resumeAfterFilesLogged(continueMerge);
//        break;
//      case ALL_TS_MERGED:
//        resumeAfterAllTsMerged(continueMerge);
//        break;
//      case MERGE_END:
//        cleanUp(continueMerge);
//        break;
//      default:
//        throw new UnsupportedOperationException(taskName + " found unrecognized status " + status);
//    }
//    if (logger.isInfoEnabled()) {
//      logger.info("{} merge recovery ends after {}ms", taskName,
//          (System.currentTimeMillis() - startTime));
//    }
//  }
//
//  private void resumeAfterFilesLogged(boolean continueMerge) throws IOException {
//    if (continueMerge) {
//      resumeMergeProgress();
//      calculateConcurrentSeriesNum();
//      if (concurrentMergeSeriesNum == 0) {
//        throw new IOException("Merge cannot be resumed under current memory budget, please "
//            + "increase the budget or disable continueMergeAfterReboot");
//      }
//
//      CompressMultiChunkTask mergeChunkTask = new CompressMultiChunkTask(mergeContext, taskName, mergeLogger, resource,
//          fullMerge, analyzer.getUnmergedPaths(), concurrentMergeSeriesNum);
//      analyzer.setUnmergedPaths(null);
//      mergeChunkTask.mergeSeries();
//
//      CompressFileTask mergeFileTask = new CompressFileTask(taskName, mergeContext, mergeLogger, resource,
//          resource.getSeqFiles());
//      mergeFileTask.mergeFiles();
//    }
//    cleanUp(continueMerge);
//  }
//
//  private void resumeAfterAllTsMerged(boolean continueMerge) throws IOException {
//    if (continueMerge) {
//      resumeMergeProgress();
//      CompressFileTask mergeFileTask = new CompressFileTask(taskName, mergeContext, mergeLogger, resource,
//          analyzer.getUnmergedFiles());
//      analyzer.setUnmergedFiles(null);
//      mergeFileTask.mergeFiles();
//    } else {
//      // NOTICE: although some of the seqFiles may have been truncated in last merge, we do not
//      // recover them here because later TsFile recovery will recover them
//      truncateFiles();
//    }
//    cleanUp(continueMerge);
//  }
//
//  private void resumeMergeProgress() throws IOException {
//    mergeLogger = new MergeLogger(storageGroupSysDir);
//    truncateFiles();
//    recoverChunkCounts();
//  }
//
//  private void calculateConcurrentSeriesNum() throws IOException {
//    long singleSeriesUnseqCost = 0;
//    long maxUnseqCost = 0;
////    for (TsFileResource unseqFile : resource.getUnseqFiles()) {
////      long[] chunkNums = MergeUtils.findTotalAndLargestSeriesChunkNum(unseqFile,
////          resource.getFileReader(unseqFile));
////      long totalChunkNum = chunkNums[0];
////      long maxChunkNum = chunkNums[1];
////      singleSeriesUnseqCost += unseqFile.getFileSize() * maxChunkNum / totalChunkNum;
////      maxUnseqCost += unseqFile.getFileSize();
////    }
//
//    long singleSeriesSeqReadCost = 0;
//    long maxSeqReadCost = 0;
//    long seqWriteCost = 0;
//    for (TsFileResource seqFile : resource.getSeqFiles()) {
//      long[] chunkNums = MergeUtils.findTotalAndLargestSeriesChunkNum(seqFile,
//          resource.getFileReader(seqFile));
//      long totalChunkNum = chunkNums[0];
//      long maxChunkNum = chunkNums[1];
//      long fileMetaSize = MergeUtils.getFileMetaSize(seqFile, resource.getFileReader(seqFile));
//      long newSingleSeriesSeqReadCost =  fileMetaSize * maxChunkNum / totalChunkNum;
//      singleSeriesSeqReadCost = newSingleSeriesSeqReadCost > singleSeriesSeqReadCost ?
//          newSingleSeriesSeqReadCost : singleSeriesSeqReadCost;
//      maxSeqReadCost = fileMetaSize > maxSeqReadCost ? fileMetaSize : maxSeqReadCost;
//      seqWriteCost += fileMetaSize;
//    }
//
//    long memBudget = IoTDBDescriptor.getInstance().getConfig().getMergeMemoryBudget();
//    int lb = 0;
//    int ub = MaxSeriesMergeFileSelector.MAX_SERIES_NUM;
//    int mid = (lb + ub) / 2;
//    while (mid != lb) {
//      long unseqCost = singleSeriesUnseqCost * mid < maxUnseqCost ? singleSeriesUnseqCost * mid :
//          maxUnseqCost;
//      long seqReadCos = singleSeriesSeqReadCost * mid < maxSeqReadCost ?
//          singleSeriesSeqReadCost * mid : maxSeqReadCost;
//      long totalCost = unseqCost + seqReadCos + seqWriteCost;
//      if (totalCost <= memBudget) {
//        lb = mid;
//      } else {
//        ub = mid;
//      }
//      mid = (lb + ub) / 2;
//    }
//    concurrentMergeSeriesNum = lb;
//  }
//
//  // scan the metadata to compute how many chunks are merged/unmerged so at last we can decide to
//  // move the merged chunks or the unmerged chunks
//  private void recoverChunkCounts() throws IOException {
//    logger.info("{} recovering chunk counts", taskName);
//    int fileCnt = 1;
//    for (TsFileResource tsFileResource : resource.getSeqFiles()) {
//      logger.info("{} recovering {}  {}/{}", taskName, tsFileResource.getFile().getName(),
//          fileCnt, resource.getSeqFiles().size());
//      RestorableTsFileIOWriter mergeFileWriter = resource.getMergeFileWriter(tsFileResource);
//      mergeFileWriter.makeMetadataVisible();
//      mergeContext.getUnmergedChunkStartTimes().put(tsFileResource, new HashMap<>());
//      List<Path> pathsToRecover = analyzer.getMergedPaths();
//      int cnt = 0;
//      double progress = 0.0;
//      for(Path path : pathsToRecover) {
//        recoverChunkCounts(path, tsFileResource, mergeFileWriter);
//        if (logger.isInfoEnabled()) {
//          cnt += 1.0;
//          double newProgress = 100.0 * cnt / pathsToRecover.size();
//          if (newProgress - progress >= 1.0) {
//            progress = newProgress;
//            logger.info("{} {}% series count of {} are recovered", taskName, progress,
//                tsFileResource.getFile().getName());
//          }
//        }
//      }
//      fileCnt++;
//    }
//    analyzer.setMergedPaths(null);
//  }
//
//  private void recoverChunkCounts(Path path, TsFileResource tsFileResource,
//      RestorableTsFileIOWriter mergeFileWriter) throws IOException {
//    mergeContext.getUnmergedChunkStartTimes().get(tsFileResource).put(path, new ArrayList<>());
//
//    List<ChunkMetaData> seqFileChunks = resource.queryChunkMetadata(path, tsFileResource);
//    List<ChunkMetaData> mergeFileChunks =
//        mergeFileWriter.getVisibleMetadataList(path.getDevice(), path.getMeasurement(), null);
//    mergeContext.getMergedChunkCnt().compute(tsFileResource, (k, v) -> v == null ?
//        mergeFileChunks.size() : v + mergeFileChunks.size());
//    int seqChunkIndex = 0;
//    int mergeChunkIndex = 0;
//    int unmergedCnt = 0;
//    while (seqChunkIndex < seqFileChunks.size() && mergeChunkIndex < mergeFileChunks.size()) {
//      ChunkMetaData seqChunk = seqFileChunks.get(seqChunkIndex);
//      ChunkMetaData mergedChunk = mergeFileChunks.get(mergeChunkIndex);
//      if (seqChunk.getStartTime() < mergedChunk.getStartTime()) {
//        // this seqChunk is unmerged
//        unmergedCnt ++;
//        seqChunkIndex ++;
//        mergeContext.getUnmergedChunkStartTimes().get(tsFileResource).get(path).add(seqChunk.getStartTime());
//      } else if (mergedChunk.getStartTime() <= seqChunk.getStartTime() &&
//          seqChunk.getStartTime() <= mergedChunk.getEndTime()) {
//        // this seqChunk is merged
//        seqChunkIndex ++;
//      } else {
//        // seqChunk.startTime > mergeChunk.endTime, find next mergedChunk that may cover the
//        // seqChunk
//        mergeChunkIndex ++;
//      }
//    }
//    int finalUnmergedCnt = unmergedCnt;
//    mergeContext.getUnmergedChunkCnt().compute(tsFileResource, (k, v) -> v == null ?
//        finalUnmergedCnt : v + finalUnmergedCnt);
//  }
//
//  private void truncateFiles() throws IOException {
//    logger.info("{} truncating {} files", taskName, analyzer.getFileLastPositions().size());
//    for (Entry<File, Long> entry : analyzer.getFileLastPositions().entrySet()) {
//      File file = entry.getKey();
//      Long lastPosition = entry.getValue();
//      if (file.exists() && file.length() != lastPosition) {
//        try (FileInputStream fileInputStream = new FileInputStream(file)) {
//          FileChannel channel = fileInputStream.getChannel();
//          channel.truncate(lastPosition);
//          channel.close();
//        }
//      }
//    }
//    analyzer.setFileLastPositions(null);
//  }
}
