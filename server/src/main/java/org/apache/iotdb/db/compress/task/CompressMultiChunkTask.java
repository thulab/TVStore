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

import static org.apache.iotdb.db.compress.task.CompressTask.COMPRESS_SUFFIX;
import static org.apache.iotdb.db.engine.storagegroup.TsFileResource.RESOURCE_SUFFIX;
import static org.apache.iotdb.db.utils.QueryUtils.modifyChunkMetaData;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.compress.controll.DownSampleImpl;
import org.apache.iotdb.db.compress.controll.SampleEntropy;
import org.apache.iotdb.db.compress.manage.CompressManager;
import org.apache.iotdb.db.compress.manage.CompressResource;
import org.apache.iotdb.db.compress.manage.MergeContext;
import org.apache.iotdb.db.compress.recover.MergeLogger;
import org.apache.iotdb.db.compress.selector.IMergePathSelector;
import org.apache.iotdb.db.compress.selector.NaivePathSelector;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.path.PathException;
import org.apache.iotdb.db.exception.storageGroup.StorageGroupException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.MNode;
import org.apache.iotdb.db.utils.MergeUtils;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderWithoutFilter;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompressMultiChunkTask {

  private static final Logger logger = LoggerFactory.getLogger(
      CompressResource.class);
  private static int minChunkPointNum = IoTDBDescriptor.getInstance().getConfig()
      .getChunkMergePointThreshold();

  private long minChunkThreshold;

  private MergeLogger mergeLogger;
  private List<Path> unmergedSeries;

  private String taskName;
  private CompressResource resource;
  private MergeContext mergeContext;

//  private AtomicInteger mergedChunkNum = new AtomicInteger();
//  private AtomicInteger unmergedChunkNum = new AtomicInteger();
  private int mergedSeriesCnt;
  private double progress;

  private int concurrentMergeSeriesNum;
  private List<Path> currMergingPaths = new ArrayList<>();

  private static final String FINISH_READ_CHUNK_IN_ONE_TSFILE = "READ_ALL_CHUNKS_IN_ONE_TSFILE";
  private static final String FINISH_DECODING_CHUNK_IN_ONE_TSFILE = "DECODE_ALL_CHUNKS_IN_ONE_TSFILE";
  /**
   * page的压缩映射关系
   */
  private List<Integer> pageMapping;
  long totalPageNum;
  private int tarDiffPageNum;
  //private ThreadLocal<Integer> compressMappingIdx=new ThreadLocal<>();
  private ThreadLocal<Integer> currCompressRatioSum = new ThreadLocal<>();
  private ThreadLocal<Integer> currPageNum = new ThreadLocal<>();
  private ThreadLocal<Integer> currPagePointNum = new ThreadLocal<>();

  private Map<Path, Integer> seriesMappingIdx = new HashMap<>();
  private Map<Path, Integer> currPageDiffNum = new HashMap<>();
  /**
   * 写压缩后的数据的TsFile
   */
  private RestorableTsFileIOWriter compressFileWriter = null;
  private TsFileResource compressFileWriterResource = null;

  /**
   *
   */
  private List<TsFileResource> toBeDeletedFile = new ArrayList<>();

  int CachedChunkNum = IoTDBDescriptor.getInstance().getConfig().getChunkQueueCapacity();

  public CompressMultiChunkTask(MergeContext context, String taskName, MergeLogger mergeLogger,
      CompressResource compressResource, List<Path> unmergedSeries,
      int concurrentMergeSeriesNum, List<Integer> pageMapping, int tarDiffPageNum) {
    this.mergeContext = context;
    this.taskName = taskName;
    this.mergeLogger = mergeLogger;
    this.resource = compressResource;
    this.unmergedSeries = unmergedSeries;
    this.concurrentMergeSeriesNum = concurrentMergeSeriesNum;
    this.pageMapping = pageMapping;
    this.minChunkThreshold =
        IoTDBDescriptor.getInstance().getConfig().getMemtableSizeThreshold() / unmergedSeries
            .size();
    this.totalPageNum = pageMapping.stream().mapToLong(x->x).sum();
    this.tarDiffPageNum = tarDiffPageNum;
  }

  public void compressSeries(boolean isChangeProcessor) throws IOException {
    if (logger.isInfoEnabled()) {
      logger.info("{} starts to compress {} series", taskName, unmergedSeries.size());
    }
    long startTime = System.currentTimeMillis();
    for (Path path : unmergedSeries) {
      seriesMappingIdx.put(path, 0);
      currPageDiffNum.put(path, 0);
    }


    for (int i = 0; i < resource.getSeqFiles().size(); i++) {
      // write data into each seqFile's corresponding temp merge file
      TsFileResource currTsFile = resource.getSeqFiles().get(i);
      List<List<Path>> devicePaths = MergeUtils.splitPathsByDevice(unmergedSeries);
      if (compressFileWriter == null) {
        compressFileWriter = new RestorableTsFileIOWriter(getNextMergeVersionFile(currTsFile.getFile()));
        Map<String, Long> startTimeMap = new ConcurrentHashMap<>(currTsFile.getStartTimeMap());
        Map<String, Long> endTimeMap = new ConcurrentHashMap<>(currTsFile.getEndTimeMap());
        compressFileWriterResource = new TsFileResource(compressFileWriter.getFile(), startTimeMap,
            endTimeMap);
        logger.info("create compress file:{}", compressFileWriterResource.getFile().getAbsolutePath());
      } else {
        for (Entry<String, Long> deviceEndTime : currTsFile.getEndTimeMap().entrySet()) {
          compressFileWriterResource
              .updateEndTime(deviceEndTime.getKey(), deviceEndTime.getValue());
        }
      }
      for (List<Path> pathList : devicePaths) {
        String deviceId = pathList.get(0).getDevice();
        Long currDeviceMinTime = currTsFile.getStartTimeMap().get(deviceId);
        if (currDeviceMinTime == null) {
          continue;
        }



        compressFileWriter.startChunkGroup(deviceId);
        // TODO: use statistics of queries to better rearrange series
        IMergePathSelector pathSelector = new NaivePathSelector(pathList, concurrentMergeSeriesNum);
        while (pathSelector.hasNext()) {
          currMergingPaths = pathSelector.next();
          compressMuiltiSeriesInOneTsFile(currTsFile);
          mergedSeriesCnt += currMergingPaths.size();
          logger.info("Device {} has merged {} series.", deviceId, mergedSeriesCnt);
        }
        compressFileWriter.endChunkGroup(0);
        logger.info("Task :{}, compress TsFile: {} To compress file {}", taskName,
            currTsFile.getFile().getAbsolutePath(), compressFileWriter.getFile().getAbsolutePath());
      }

      toBeDeletedFile.add(currTsFile);
      logCompressProgress(i);
      boolean isLastFile = i + 1 == resource.getSeqFiles().size();
      if(isChangeProcessor){
        cleanCompressedTsFile2(isLastFile);
      }
      else {
        cleanCompressedTsFileForCompressTool(isLastFile);
      }

//      int cnt = 0;
//      int endCnt = 0;
//      for (List<Path> pathList : devicePaths) {
//        for(Path currPath : pathList){
//          cnt++;
//          if (seriesMappingIdx.get(currPath) >= pageMapping.size()) {
//            endCnt++;
//          }
//          logger.info("series {}, current window={}, total window={}", currPath.getFullPath(), seriesMappingIdx.get(currPath), pageMapping.size());
//        }
//      }
//      if(endCnt*2>=cnt){
//        if(toBeDeletedFile!=null && toBeDeletedFile.size()>0){
//          cleanCompressedTsFile2(true);
//          logger.info("{} series of {} series in group {} has finished compressing, so end "
//              + "{} compress task.", endCnt, cnt, toBeDeletedFile.get(0).getFile().getParent(), taskName);
//        }
//        break;
//      }
    }

    if (logger.isInfoEnabled()) {
      logger.info("{} all series are merged after {}ms", taskName,
          System.currentTimeMillis() - startTime);
    }
  }

  private void cleanCompressedTsFileForCompressTool(boolean isLastFile) throws IOException {
    if(toBeDeletedFile==null || toBeDeletedFile.isEmpty() || compressFileWriterResource == null){
      return;
    }
    logger.info("{} moving uncompressed files {} to the new file {}", taskName,
        toBeDeletedFile.toString(), compressFileWriter.getFile().getAbsolutePath());
    TsFileResource firstFile = toBeDeletedFile.get(0);
    String prefixName = getNextMergeVersionFile(firstFile.getFile()).getName();
    boolean isFirstFile = compressFileWriterResource.getFile().getName().equals(prefixName);


      // TsFile 未达到规定size，
      if ( !isLastFile && compressFileWriter.getFile().length() < IoTDBDescriptor.getInstance().getConfig().getTsFileSizeThreshold()) {
          compressFileWriterResource.setClosed(false);
          logger.info("TsFile {} 未达到规定 seal size, last compress file {}.",
              compressFileWriterResource.getFile().getAbsolutePath(), firstFile.getFile().getAbsolutePath());

      }
      else {
        // TsFile 达到规定size, 封口新写的压缩后文件， // 删掉现在的文件+notify delete;
        compressFileWriter.endFile(new Schema(compressFileWriter.getKnownSchema()));
        compressFileWriterResource.setActualPageNum(compressFileWriter.getActualPageNum());
        compressFileWriterResource.setVirtualPageNum(compressFileWriter.getVirtuaPageNum());
        compressFileWriterResource.serialize();
        compressFileWriterResource.close();
        logger.info("TsFile {} 达到规定 seal size, last compress file {}.",
              compressFileWriterResource.getFile().getAbsolutePath(), firstFile.getFile().getAbsolutePath());

        compressFileWriter = null;
        compressFileWriterResource = null;
      }



    for (TsFileResource seqFile : toBeDeletedFile) {
      resource.removeFileReader(seqFile);
      logger.info("Delete tsfile {} in sync process.", seqFile.getFile().getAbsolutePath());
      seqFile.remove();
    }
    toBeDeletedFile.clear();
  }

  private void cleanCompressedTsFile2(boolean isLastFile) throws IOException {
    if(toBeDeletedFile==null || toBeDeletedFile.isEmpty() || compressFileWriterResource == null){
      return;
    }
    logger.info("{} moving uncompressed files {} to the new file {}", taskName,
        toBeDeletedFile.toString(), compressFileWriter.getFile().getAbsolutePath());
    TsFileResource firstFile = toBeDeletedFile.get(0);
    String prefixName = getNextMergeVersionFile(firstFile.getFile()).getName();
    boolean isFirstFile = compressFileWriterResource.getFile().getName().equals(prefixName);

    try{
      // TsFile 未达到规定size，
      if ( !isLastFile && compressFileWriter.getFile().length() < IoTDBDescriptor.getInstance().getConfig().getTsFileSizeThreshold()) {
        if (isFirstFile) {
          // compress file notify engine 文件变化,  删掉现在的文件+notify delete;
          compressFileWriterResource.setClosed(false);
          StorageEngine.getInstance().deleteAndloadCompressTsFile(toBeDeletedFile, compressFileWriterResource);
          compressFileWriterResource.setClosed(false);
          logger.info("TsFile {} 未达到规定 seal size, {} is first file.",
              compressFileWriterResource.getFile().getAbsolutePath(), firstFile.getFile().getAbsolutePath());
        }
        else {
          // 删掉现在的文件+notify delete;
          StorageEngine.getInstance().deleteAndloadCompressTsFile(toBeDeletedFile, null);
          logger.info("TsFile {} 未达到规定 seal size, {} isn't first file.",
              compressFileWriterResource.getFile().getAbsolutePath(), firstFile.getFile().getAbsolutePath());
        }
      }
      else {
        // TsFile 达到规定size, 封口新写的压缩后文件， // 删掉现在的文件+notify delete;
        compressFileWriter.endFile(new Schema(compressFileWriter.getKnownSchema()));
        compressFileWriterResource.setActualPageNum(compressFileWriter.getActualPageNum());
        compressFileWriterResource.setVirtualPageNum(compressFileWriter.getVirtuaPageNum());
        compressFileWriterResource.serialize();
        compressFileWriterResource.close();
        if(isFirstFile){
          StorageEngine.getInstance().deleteAndloadCompressTsFile(toBeDeletedFile, compressFileWriterResource);
          logger.info("TsFile {} 达到规定 seal size, {} is first file.",
              compressFileWriterResource.getFile().getAbsolutePath(), firstFile.getFile().getAbsolutePath());
        }
        else {
          StorageEngine.getInstance().deleteAndloadCompressTsFile(toBeDeletedFile, null);
          logger.info("TsFile {} 达到规定 seal size, {} isn't first file.",
              compressFileWriterResource.getFile().getAbsolutePath(), firstFile.getFile().getAbsolutePath());
        }
        compressFileWriter = null;
        compressFileWriterResource = null;
      }

    }
    catch (StorageEngineException e) {
      e.printStackTrace();
    }

    for (TsFileResource seqFile : toBeDeletedFile) {
      resource.removeFileReader(seqFile);
    }
    toBeDeletedFile.clear();
  }



  private void cleanCompressedTsFile(boolean isLastFile) throws IOException {

    // TsFile 未达到规定size，返回继续写
    if ( !isLastFile && compressFileWriter.getFile().length() < IoTDBDescriptor.getInstance().getConfig()
        .getTsFileSizeThreshold()) {
      return;
    }
    // 执行删除操作
    mergeLogger.logFileMergeStart(compressFileWriter.getFile(), compressFileWriter.getFile().length());
    logger.info("{} moving uncompressed files {} to the new file {}", taskName,
        toBeDeletedFile.toString(), compressFileWriter.getFile().getAbsolutePath());

    compressFileWriter.endFile(new Schema(compressFileWriter.getKnownSchema()));
    compressFileWriterResource.setActualPageNum(compressFileWriter.getActualPageNum());
    compressFileWriterResource.setVirtualPageNum(compressFileWriter.getVirtuaPageNum());
    compressFileWriterResource.serialize();
    mergeLogger.logFileMergeEnd();

    File nextMergeVersionFile = getNextMergeVersionFile(compressFileWriterResource.getFile());
    FileUtils.moveFile(compressFileWriter.getFile(), nextMergeVersionFile);
    FileUtils
        .moveFile(
            new File(compressFileWriterResource.getFile().getAbsolutePath() + RESOURCE_SUFFIX),
            new File(
                nextMergeVersionFile.getAbsolutePath() + RESOURCE_SUFFIX));
    compressFileWriterResource.setFile(nextMergeVersionFile);
    for (TsFileResource seqFile : toBeDeletedFile) {
      resource.removeFileReader(seqFile);
    }
    try {
      StorageEngine.getInstance().deleteAndloadCompressTsFile(toBeDeletedFile, compressFileWriterResource);
    } catch (StorageEngineException e) {
      e.printStackTrace();
    }

    toBeDeletedFile.clear();
    compressFileWriter = null;
    compressFileWriterResource = null;

  }

  private File getNextMergeVersionFile(File seqFile) {
    //1593494623039-101-0.tsfile.compress.resource
    String[] splits = seqFile.getName().replace(TSFILE_SUFFIX, "")
        .replace(COMPRESS_SUFFIX, "").split(IoTDBConstant.TSFILE_NAME_SEPARATOR);
    int mergeVersion = Integer.parseInt(splits[2]) + 1;
    return new File(seqFile.getParentFile(),
        splits[0] + IoTDBConstant.TSFILE_NAME_SEPARATOR + splits[1]
            + IoTDBConstant.TSFILE_NAME_SEPARATOR + mergeVersion + TSFILE_SUFFIX);
  }

  private void logCompressProgress(int id) {
    if (logger.isInfoEnabled()) {
      double newProgress = 100 * id / (double) (resource.getSeqFiles().size());
      if (newProgress - progress >= 1.0) {
        progress = newProgress;
        logger.info("{} has compressed {}% tsfiles.", taskName, progress);
      }
    }
  }

  private void compressMuiltiSeriesInOneTsFile(TsFileResource currTsFile) throws IOException {
    for (Path path : currMergingPaths) {
      MeasurementSchema schema = resource.getSchema(path.getMeasurement());
      compressFileWriter.addSchema(schema);
    }
    logger.info("Task:{}, start compress {} series in tsfile {}", taskName, currMergingPaths.size(), currTsFile.getFile().getAbsolutePath());
    List<Modification>[] modifications = new List[currMergingPaths.size()];
    List<ChunkMetaData>[] seqChunkMeta = new List[currMergingPaths.size()];
    for (int i = 0; i < currMergingPaths.size(); i++) {
      modifications[i] = resource.getModifications(currTsFile, currMergingPaths.get(i));
      seqChunkMeta[i] = resource.queryChunkMetadata(currMergingPaths.get(i), currTsFile);
      modifyChunkMetaData(seqChunkMeta[i], modifications[i]);
    }


    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < currMergingPaths.size(); i++) {
      int finalI = i;
      futures.add(CompressManager.getINSTANCE().submitChunkSubTask(() -> {
        compressOneSeriesInOneTsFile(finalI, seqChunkMeta[finalI], currTsFile, compressFileWriter);
        return null;
      }));
    }
    for (int i = 0; i < currMergingPaths.size(); i++) {
      try {
        futures.get(i).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new IOException(e);
      }
    }

    //mergeLogger.logFilePosition(compressFileWriter.getFile());
    logger.info("Task:{}, end compress {} series in tsfile {}", taskName, currMergingPaths.size(), currTsFile.getFile().getAbsolutePath());

  }

  private void handleNonNumberSeries(List<ChunkMetaData> seqChunkMeta, TsFileResource currTsFile,
      RestorableTsFileIOWriter mergeFileWriter, MeasurementSchema measurementSchema)
      throws IOException {
    LinkedBlockingQueue<Object> chunkQueue = new LinkedBlockingQueue<>(CachedChunkNum);
    TsFileSequenceReader reader = resource.getFileReader(currTsFile);
    logger.debug("Task:{}, start read chunk for {} series in tsfile {}", taskName,
        currMergingPaths.size(), reader.getFileName());
    Future<Integer> chunkReadTask = CompressManager.getINSTANCE().submitReadChunkTask(() -> {
      long st = System.currentTimeMillis();
      int cnt = 0;
      try {
        for (ChunkMetaData currMeta : seqChunkMeta) {
          synchronized (reader) {
            try {
//            long st1 = System.currentTimeMillis();
//            logger.debug("start read {} cnunk in file {}, task {} in {}ms", cnt, reader.getFileName(), taskName, st1);
              chunkQueue.put(new Pair<>(reader.readMemChunk(currMeta), currMeta));
//            logger.debug("end read {} cnunk in file {}, task {} cost {}ms", cnt, reader.getFileName(), taskName, (System.currentTimeMillis()-st1));
              cnt++;
            } catch (IOException e) {
              e.printStackTrace();
            }
          }
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      finally {
        chunkQueue.put(FINISH_READ_CHUNK_IN_ONE_TSFILE);
        logger.info("[COMPRESS] task {} file {}, read all {} chunks cost {} ms, chunk queue size {}, chunk meta size {}",
            taskName, reader.getFileName(), cnt, (System.currentTimeMillis()-st), chunkQueue.size(), seqChunkMeta.size());
        return cnt;
      }
    });


      long st = System.currentTimeMillis();
      int writedChunkNum = 0;
      try {
        while (true) {
          if(chunkQueue.isEmpty()&&(chunkReadTask.isDone() || chunkReadTask.isCancelled())){
            break;
          }
          Object blockItem = chunkQueue.take();
          if (blockItem instanceof String && blockItem.equals(FINISH_READ_CHUNK_IN_ONE_TSFILE)) {
            break;
          }

          synchronized (mergeFileWriter){
            ChunkMetaData chunkMetaData = ((Pair<Chunk, ChunkMetaData>) blockItem).right;
            mergeFileWriter.writeChunk(((Pair<Chunk, ChunkMetaData>) blockItem).left, chunkMetaData);
            writedChunkNum++;
            mergeContext.incTotalChunkWritten();
            mergeContext.incTotalPointWritten(chunkMetaData.getNumOfPoints());
          }
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      } finally {
        logger.info("[COMPRESS] task {} file {}, write all {} {} non-number-chunks, cost {} ms", taskName,
            reader.getFileName(), writedChunkNum, measurementSchema.getMeasurementId(),(System.currentTimeMillis() - st));

      }
  }

  private void compressOneSeriesInOneTsFile(int pathIdx, List<ChunkMetaData> seqChunkMeta, TsFileResource currTsFile,
      RestorableTsFileIOWriter mergeFileWriter)
      throws IOException, StorageGroupException, PathException {
    if(seqChunkMeta== null || seqChunkMeta.isEmpty()){
      return;
    }

    currPageNum.set(0);
    currPagePointNum.set(0);
    currCompressRatioSum.set(0);
    Path currPath=currMergingPaths.get(pathIdx);
    MeasurementSchema measurementSchema = resource.getSchema(currPath.getMeasurement());
    ChunkWriterImpl chunkWriter = resource.getChunkWriter(measurementSchema);
    if(measurementSchema.getType().equals(TSDataType.TEXT) || measurementSchema.getType().equals(TSDataType.BOOLEAN)){
      handleNonNumberSeries(seqChunkMeta, currTsFile, mergeFileWriter, measurementSchema);
      return;
    }

    BatchData[] prePoints = new BatchData[1];

    LinkedBlockingQueue<Object> chunkQueue = new LinkedBlockingQueue<>(CachedChunkNum);
    TsFileSequenceReader reader = resource.getFileReader(currTsFile);
    logger.debug("Task:{}, start read chunk for {} series in tsfile {}", taskName,
        currMergingPaths.size(), reader.getFileName());
    Future<Integer> chunkReadTask = CompressManager.getINSTANCE().submitReadChunkTask(() -> {
      long st = System.currentTimeMillis();
      int cnt = 0;
      try {
        for (ChunkMetaData currMeta : seqChunkMeta) {
          synchronized (reader) {
            try {
//            long st1 = System.currentTimeMillis();
//            logger.debug("start read {} cnunk in file {}, task {} in {}ms", cnt, reader.getFileName(), taskName, st1);
              chunkQueue.put(new Pair<>(reader.readMemChunk(currMeta), currMeta));
//            logger.debug("end read {} cnunk in file {}, task {} cost {}ms", cnt, reader.getFileName(), taskName, (System.currentTimeMillis()-st1));
              cnt++;
            } catch (IOException e) {
              e.printStackTrace();
            }
          }
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      finally {
        chunkQueue.put(FINISH_READ_CHUNK_IN_ONE_TSFILE);
        logger.info("[COMPRESS] task {} file {}, read all {} chunks cost {} ms, chunk queue size {}, chunk meta size {}",
            taskName, reader.getFileName(), cnt, (System.currentTimeMillis()-st), chunkQueue.size(), seqChunkMeta.size());
        return cnt;
      }
    });


    LinkedBlockingQueue<Object> pageQueue = new LinkedBlockingQueue<>();
    logger.debug("Task:{}, start decoding chunk for {} series in tsfile {}", taskName, currMergingPaths.size(), reader.getFileName());
    Future<Boolean> decodingTask = CompressManager.getINSTANCE().submitReadChunkTask(() -> {
      long st = System.currentTimeMillis();
      int writedChunkNum = 0;
      boolean writeEndFlag = false;
      try {
        while (true) {
          if (seriesMappingIdx.get(currPath) >= pageMapping.size() || currPageDiffNum.get(currPath) >= tarDiffPageNum) {
            break;
          }
          if(chunkQueue.isEmpty()&&(chunkReadTask.isDone() || chunkReadTask.isCancelled())){
            break;
          }
          Object blockItem = chunkQueue.take();
          if (blockItem instanceof String && blockItem.equals(FINISH_READ_CHUNK_IN_ONE_TSFILE)) {
            writeEndFlag = true;
            break;
          }

          ChunkReader chunkReader = new ChunkReaderWithoutFilter(((Pair<Chunk, ChunkMetaData>) blockItem).left);
          while (chunkReader.hasNextBatch()) {
            PageHeader pageHeader = chunkReader.nextPageHeader();
            BatchData batchData = chunkReader.nextBatch();
            pageQueue.put(new PageObject(pageHeader, batchData));
          }
          writedChunkNum++;
          mergeContext.incTotalChunkWritten();
          mergeContext.incTotalPointWritten(((Pair<Chunk, ChunkMetaData>) blockItem).right.getNumOfPoints());
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      } finally {
        pageQueue.put(FINISH_DECODING_CHUNK_IN_ONE_TSFILE);
        logger.info("[COMPRESS] task {} file {}, decoding all {} chunks cost {} ms, pageQueue size {}", taskName,
            reader.getFileName(), writedChunkNum, (System.currentTimeMillis() - st), pageQueue.size());
        return writeEndFlag;
      }
    });


    long st = System.currentTimeMillis();
    int writedPageNum = 0;
    while (true){
//      if(seriesMappingIdx.get(currPath) >= pageMapping.size()){
//        break;
//      }
      Object pageItem = null;
      try {
        pageItem = pageQueue.take();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      if(pageItem instanceof String && pageItem.equals(FINISH_DECODING_CHUNK_IN_ONE_TSFILE)){
        break;
      }

      try {
        mergePageV2((PageObject) pageItem, pathIdx, prePoints, chunkWriter);
      } catch (StorageGroupException e) {
        e.printStackTrace();
      } catch (PathException e) {
        e.printStackTrace();
      }
      writedPageNum++;
      //logger.info("Task:{}, end merge chunk for series {} in tsfile {}", taskName, currPath.getMeasurement(), reader.getFileName());
      boolean chunkTooSmall = chunkWriter.getChunkBuffer().getTotalValueCount()<minChunkPointNum || chunkWriter.getCurrentChunkSize()<minChunkThreshold;
      if(!chunkTooSmall){
        synchronized (mergeFileWriter) {
          chunkWriter.writeToFileWriter(mergeFileWriter);
        }
      }
    }

    boolean writeEndFlag = false;
    try {
      writeEndFlag = decodingTask.get();
    } catch (InterruptedException | ExecutionException e) {
      writeEndFlag = true;
      e.printStackTrace();
    }
    logger.info("[COMPRESS] task {} file {}, write all {} pages cost {} ms", taskName, reader.getFileName(), writedPageNum,(System.currentTimeMillis()-st));

    if(prePoints[0] != null && prePoints[0].length()>0){
      boolean isFirstPage = seriesMappingIdx.get(currPath).intValue()==0;
      compressWriteOnePage(prePoints[0], chunkWriter, currPagePointNum.get(), isFirstPage, pathIdx);
      currCompressRatioSum.set(0);
      currPageNum.set(0);
      currPagePointNum.set(0);
      prePoints[0]=null;
    }

    synchronized (mergeFileWriter) {
      chunkWriter.writeToFileWriter(mergeFileWriter);
    }

    logger.info("[COMPRESS] task {} file {}, start writing left chunks.", taskName, reader.getFileName());
    int cnt = 0;
    while (!writeEndFlag || !chunkQueue.isEmpty()){
      Object blockItem = null;
      try {
        blockItem = chunkQueue.take();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
//      finally {
//        writeEndFlag = true;
//      }

      if(blockItem instanceof String && blockItem.equals(FINISH_READ_CHUNK_IN_ONE_TSFILE)){
        //writeEndFlag = true;
        //logger.info("[COMPRESS] task {} file {}, write left {} chunks.", taskName, reader.getFileName(), cnt);
        break;
      }

      synchronized (mergeFileWriter){
        mergeFileWriter.writeChunk(((Pair<Chunk, ChunkMetaData>) blockItem).left, ((Pair<Chunk, ChunkMetaData>) blockItem).right);
        cnt++;
        mergeContext.incTotalChunkWritten();
        mergeContext.incTotalPointWritten(((Pair<Chunk, ChunkMetaData>) blockItem).right.getNumOfPoints());
      }

    }


//    while (true){
//      if((chunkReadTask.isDone()||chunkReadTask.isCancelled())&&chunkQueue.isEmpty()){
//        break;
//      }
//      else {
//        logger.info("chunkReadTask.isDone()={}, chunkQueue.isEmpty()={}",chunkReadTask.isDone(), chunkQueue.isEmpty());
//      }
//
//      Object blockItem = null;
//      try {
//        blockItem = chunkQueue.take();
//      } catch (InterruptedException e) {
//        e.printStackTrace();
//        break;
//      }
//      if(blockItem instanceof String && blockItem.equals(FINISH_READ_CHUNK_IN_ONE_TSFILE)){
//        //logger.info("[COMPRESS] task {} file {}, write left {} chunks.", taskName, reader.getFileName(), cnt);
//        break;
//      }
//
//      synchronized (compressFileWriter){
//        compressFileWriter.writeChunk(((Pair<Chunk, ChunkMetaData>) blockItem).left, ((Pair<Chunk, ChunkMetaData>) blockItem).right);
//        cnt++;
//      }
//
//    }
    logger.info("[COMPRESS] task {} file {}, write left {} chunks.", taskName, reader.getFileName(), cnt);

  }

  class PageObject{
    PageHeader pageHeader;
    BatchData batchData;

    public PageObject(PageHeader pageHeader, BatchData batchData) {
      this.pageHeader = pageHeader;
      this.batchData = batchData;
    }
  }

  private void mergePageV2(PageObject pageObject, int pathIdx, BatchData[] prePoints,
      ChunkWriterImpl chunkWriter) throws StorageGroupException, PathException {
    Path currPath = currMergingPaths.get(pathIdx);

    PageHeader pageHeader = pageObject.pageHeader;
    BatchData batchData = pageObject.batchData;
    mergeContext.incTotalPointWritten(batchData.length());
    currCompressRatioSum.set(currCompressRatioSum.get() + pageHeader.getCompressRatio());
    currPageNum.set(1 + currPageNum.get());
    currPagePointNum.set(currPagePointNum.get() + pageHeader.getNumOfValues());
    writeOnePageToBatchData(batchData, prePoints);
    chunkWriter.updatePageHeader(pageHeader);

    //currPageDiffNum.put(currPath, currPageDiffNum.get(currPath)+1);
    if (currCompressRatioSum.get() > pageMapping.get(Math.min(seriesMappingIdx.get(currPath), pageMapping.size()-1))) {
      currPageDiffNum.put(currPath, currPageDiffNum.get(currPath)+currPageNum.get().intValue()-1);
      boolean isFirstPage = seriesMappingIdx.get(currPath).intValue()==0;
      compressWriteOnePage(prePoints[0], chunkWriter, currPagePointNum.get(), isFirstPage, pathIdx);
      currCompressRatioSum.set(0);
      currPageNum.set(0);
      currPagePointNum.set(0);
      prePoints[0] = null;
      seriesMappingIdx.put(currPath, 1 + seriesMappingIdx.get(currPath));
    }

  }

  /**
   * 将压缩前的page写入BatchData，并准备压缩
   */
  private int writeOnePageToBatchData(BatchData batchData, BatchData[] prePoints) {
    if(prePoints[0]==null || prePoints[0].length()==0){
      prePoints[0] = batchData;
      return batchData.length();
    }
    prePoints[0].putAnBatchData(batchData);
    return batchData.length();
  }


  /**
   * 采样并将压缩后的page写入chunk
   */
  private int compressWriteOnePage(BatchData batchData, ChunkWriterImpl chunkWriter, int pageNum, boolean isFirstPage, int pathIdx)
      throws StorageGroupException, PathException {

    int targetPointNum = batchData.length()/currPageNum.get();
    if(IoTDBDescriptor.getInstance().getConfig().isRemoveAdjacentReaptedPoint()){
      batchData = removeAdjointReapedValue(batchData);
    }
    BatchData sampledPoints=new DownSampleImpl().sample(batchData, targetPointNum);

    chunkWriter.writePage(sampledPoints, sampledPoints.length(), batchData.length());

    if(IoTDBDescriptor.getInstance().getConfig().isCalculateSampleEntropy()){
      handleSampleEntropy(batchData, sampledPoints, pageNum, isFirstPage, pathIdx);
    }

    return sampledPoints.length();
  }

  private void handleSampleEntropy(BatchData batchData, BatchData sampledPoints, int pageNum, boolean isFirstPage, int pathIdx)
      throws StorageGroupException, PathException {
    double originSampleEntropy = 0;
    if(isFirstPage){
      originSampleEntropy = SampleEntropy.calSampleEntropy(batchData, batchData.length());
    }
    if(isFirstPage){
      double currSampleEntropy = SampleEntropy.calSampleEntropy(sampledPoints, sampledPoints.length());
      MNode mNode = MManager.getInstance().getNodeByPathWithCheck(currMergingPaths.get(pathIdx).getFullPath());
      mNode.setCurrSampleEntropy(currSampleEntropy);
      if(!new Double(mNode.getPeakSampleEntropy()).isNaN()){
        if(mNode.getPeakSampleEntropy()>mNode.getOriginSampleEntropy() && currSampleEntropy>mNode.getPeakSampleEntropy()){
          mNode.setPeakSampleEntropy(currSampleEntropy);
        }
        else if(mNode.getPeakSampleEntropy()<mNode.getOriginSampleEntropy() && currSampleEntropy<mNode.getPeakSampleEntropy()){
          mNode.setPeakSampleEntropy(currSampleEntropy);
        }
        return;
      }


      // 更新PeakSampleEntropy
      boolean isIncreasing = true;
      BatchData halfSample = new DownSampleImpl().sample(batchData, batchData.length()/2);
      double peakSampleEntropy = SampleEntropy.calSampleEntropy(halfSample, halfSample.length());
      if(peakSampleEntropy<originSampleEntropy){
        isIncreasing = false;
      }

      int sampleRatio = 4;
      while(sampleRatio<pageNum){
        BatchData partSample = new DownSampleImpl().sample(batchData, batchData.length()/sampleRatio);
        double partSampleEntropy = SampleEntropy.calSampleEntropy(partSample, partSample.length());
        sampleRatio*=2;
        if(isIncreasing){
          peakSampleEntropy = Math.max(peakSampleEntropy, partSampleEntropy);
        }
        else {
          peakSampleEntropy = Math.min(peakSampleEntropy, partSampleEntropy);
        }
      }

      if(isIncreasing){
        peakSampleEntropy = Math.max(peakSampleEntropy, currSampleEntropy);
      }
      else {
        peakSampleEntropy = Math.min(peakSampleEntropy, currSampleEntropy);
      }

      mNode.setOriginSampleEntropy(originSampleEntropy);
      mNode.setPeakSampleEntropy(peakSampleEntropy);
      mNode.setCurrSampleEntropy(currSampleEntropy);
    }
  }


  public static BatchData removeAdjointReapedValue(BatchData data){
    if(data==null ||data.length()<3){
      return data;
    }

    TSDataType dataType = data.getDataType();
    BatchData sampled = new BatchData(data.getDataType(), true);
    sampled.putTime(data.getTimeByIndex(0));
    sampled.putAnObject(data.getValueByIndex(0));
    for(int i = 1;i<data.length()-1;i++){
      switch (dataType) {
        case INT32:
          int curVal = data.getIntByIndex(i);
          int lastVal = data.getIntByIndex(i-1);
          if(!(curVal==lastVal && curVal==data.getIntByIndex(i+1))){
            sampled.putTime(data.getTimeByIndex(i));
            sampled.putAnObject(data.getValueByIndex(i));
          }
          break;
        case INT64:
          if(!(data.getLongByIndex(i)==data.getLongByIndex(i-1) && data.getLongByIndex(i)==data.getLongByIndex(i+1))){
            sampled.putTime(data.getTimeByIndex(i));
            sampled.putAnObject(data.getValueByIndex(i));
          }
          break;
        case FLOAT:
          if(!(data.getFloatByIndex(i)==data.getFloatByIndex(i-1) && data.getFloatByIndex(i)==data.getFloatByIndex(i+1))){
            sampled.putTime(data.getTimeByIndex(i));
            sampled.putAnObject(data.getValueByIndex(i));
          }
          break;
        case DOUBLE:
          if(!(data.getDoubleByIndex(i)==data.getDoubleByIndex(i-1) && data.getDoubleByIndex(i)==data.getDoubleByIndex(i+1))){
            sampled.putTime(data.getTimeByIndex(i));
            sampled.putAnObject(data.getValueByIndex(i));
          }
          break;
        case BOOLEAN:
          if(!(data.getBooleanByIndex(i)==data.getBooleanByIndex(i-1) && data.getBooleanByIndex(i)==data.getBooleanByIndex(i+1))){
            sampled.putTime(data.getTimeByIndex(i));
            sampled.putAnObject(data.getValueByIndex(i));
          }
          break;
        case TEXT:
          if(!(data.getBinaryByIndex(i).equals(data.getBinaryByIndex(i-1))  && data.getBinaryByIndex(i).equals(data.getBinaryByIndex(i+1)) )){
            sampled.putTime(data.getTimeByIndex(i));
            sampled.putAnObject(data.getValueByIndex(i));
          }
          break;
        default:
          return null;
      }
    }
    sampled.putTime(data.getTimeByIndex(data.length() - 1));
    sampled.putAnObject(data.getValueByIndex(data.length() - 1));
    return sampled;
  }

}
