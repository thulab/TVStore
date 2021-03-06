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

package org.apache.iotdb.db.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderWithoutFilter;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MergeUtils {

  private static final Logger logger = LoggerFactory.getLogger(MergeUtils.class);

  private MergeUtils() {
    // util class
  }
  
  public static void writeTVPair(TimeValuePair timeValuePair, IChunkWriter chunkWriter) {
    switch (chunkWriter.getDataType()) {
      case TEXT:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getBinary());
        break;
      case DOUBLE:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getDouble());
        break;
      case BOOLEAN:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getBoolean());
        break;
      case INT64:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getLong());
        break;
      case INT32:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getInt());
        break;
      case FLOAT:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getFloat());
        break;
      default:
        throw new UnsupportedOperationException("Unknown data type " + chunkWriter.getDataType());
    }
  }

  private static List<Path> collectFileSeries(TsFileSequenceReader sequenceReader) throws IOException {
    TsFileMetaData metaData = sequenceReader.readFileMetadata();
    Set<String> deviceIds = metaData.getDeviceMap().keySet();
    Set<String> measurements = metaData.getMeasurementSchema().keySet();
    List<Path> paths = new ArrayList<>();
    for (String deviceId : deviceIds) {
      for (String measurement : measurements) {
        paths.add(new Path(deviceId, measurement));
      }
    }
    return paths;
  }

  public static long collectFileSizes(List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles) {
    long totalSize = 0;
    for (TsFileResource tsFileResource : seqFiles) {
      totalSize += tsFileResource.getFileSize();
    }
    for (TsFileResource tsFileResource : unseqFiles) {
      totalSize += tsFileResource.getFileSize();
    }
    return totalSize;
  }

  public static long collectFileSizes(List<TsFileResource> seqFiles) {
    long totalSize = 0;
    for (TsFileResource tsFileResource : seqFiles) {
      totalSize += tsFileResource.getFileSize();
    }
    return totalSize;
  }

  public static int writeChunkWithoutUnseq(Chunk chunk, IChunkWriter chunkWriter) throws IOException {
    ChunkReader chunkReader = new ChunkReaderWithoutFilter(chunk);
    int ptWritten = 0;
    while (chunkReader.hasNextBatch()) {
      BatchData batchData = chunkReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        writeBatchPoint(batchData, i, chunkWriter);
      }
      ptWritten += batchData.length();
    }
    return ptWritten;
  }

  public static void writeBatchPoint(BatchData batchData, int i, IChunkWriter chunkWriter) {
    switch (chunkWriter.getDataType()) {
      case TEXT:
        chunkWriter.write(batchData.getTimeByIndex(i), batchData.getBinaryByIndex(i));
        break;
      case DOUBLE:
        chunkWriter.write(batchData.getTimeByIndex(i), batchData.getDoubleByIndex(i));
        break;
      case BOOLEAN:
        chunkWriter.write(batchData.getTimeByIndex(i), batchData.getBooleanByIndex(i));
        break;
      case INT64:
        chunkWriter.write(batchData.getTimeByIndex(i), batchData.getLongByIndex(i));
        break;
      case INT32:
        chunkWriter.write(batchData.getTimeByIndex(i), batchData.getIntByIndex(i));
        break;
      case FLOAT:
        chunkWriter.write(batchData.getTimeByIndex(i), batchData.getFloatByIndex(i));
        break;
      default:
        throw new UnsupportedOperationException("Unknown data type " + chunkWriter.getDataType());
    }
  }

  // returns totalChunkNum of a file and the max number of chunks of a series
  public static long[] findTotalAndLargestSeriesChunkNum(TsFileResource tsFileResource,
      TsFileSequenceReader sequenceReader)
      throws IOException {
    long totalChunkNum = 0;
    long maxChunkNum = Long.MIN_VALUE;
    List<Path> paths = collectFileSeries(sequenceReader);

    for (Path path : paths) {
      List<ChunkMetaData> chunkMetaDataList = sequenceReader.getChunkMetadataList(path);
      totalChunkNum += chunkMetaDataList.size();
      maxChunkNum = chunkMetaDataList.size() > maxChunkNum ? chunkMetaDataList.size() : maxChunkNum;
    }
    logger.debug("In file {}, total chunk num {}, series max chunk num {}", tsFileResource,
        totalChunkNum, maxChunkNum);
    return new long[] {totalChunkNum, maxChunkNum};
  }

  public static long getFileMetaSize(TsFileResource seqFile, TsFileSequenceReader sequenceReader) throws IOException {
    long minPos = Long.MAX_VALUE;
    TsFileMetaData fileMetaData = sequenceReader.readFileMetadata();
    Map<String, TsDeviceMetadataIndex> deviceMap = fileMetaData.getDeviceMap();
    for (TsDeviceMetadataIndex metadataIndex : deviceMap.values()) {
      minPos = metadataIndex.getOffset() < minPos ? metadataIndex.getOffset() : minPos;
    }
    return seqFile.getFileSize() - minPos;
  }

  /**
   * Reads chunks of paths in unseqResources and put them in separated lists. When reading a
   * file, this method follows the order of positions of chunks instead of the order of
   * timeseries, which reduce disk seeks.
   * @param paths names of the timeseries
   * @param unseqResources
   * @param mergeResource
   * @return
   * @throws IOException
   */
  public static List<Chunk>[] collectUnseqChunks(List<Path> paths,
      List<TsFileResource> unseqResources, MergeResource mergeResource) throws IOException {
    List<Chunk>[] ret = new List[paths.size()];
    for (int i = 0; i < paths.size(); i++) {
      ret[i] = new ArrayList<>();
    }
    PriorityQueue<MetaListEntry> chunkMetaHeap = new PriorityQueue<>();

    for (TsFileResource tsFileResource : unseqResources) {

      TsFileSequenceReader tsFileReader = mergeResource.getFileReader(tsFileResource);
      // prepare metaDataList
      buildMetaHeap(paths, tsFileReader, mergeResource, tsFileResource, chunkMetaHeap);

      // read chunks order by their position
      collectUnseqChunks(chunkMetaHeap, tsFileReader, ret);
    }
    return ret;
  }

  private static void buildMetaHeap(List<Path> paths, TsFileSequenceReader tsFileReader,
      MergeResource resource, TsFileResource tsFileResource, PriorityQueue<MetaListEntry> chunkMetaHeap)
      throws IOException {
    for (int i = 0; i < paths.size(); i++) {
      Path path = paths.get(i);
      List<ChunkMetaData> metaDataList = tsFileReader.getChunkMetadataList(path);
      if (metaDataList.isEmpty()) {
        continue;
      }
      List<Modification> pathModifications =
          resource.getModifications(tsFileResource, path);
      if (!pathModifications.isEmpty()) {
        QueryUtils.modifyChunkMetaData(metaDataList, pathModifications);
      }
      MetaListEntry entry = new MetaListEntry(i, metaDataList);
      if (entry.hasNext()) {
        entry.next();
        chunkMetaHeap.add(entry);
      }
    }
  }

  private static void collectUnseqChunks(PriorityQueue<MetaListEntry> chunkMetaHeap,
      TsFileSequenceReader tsFileReader, List<Chunk>[] ret) throws IOException {
    while (!chunkMetaHeap.isEmpty()) {
      MetaListEntry metaListEntry = chunkMetaHeap.poll();
      ChunkMetaData currMeta = metaListEntry.current();
      Chunk chunk = tsFileReader.readMemChunk(currMeta);
      ret[metaListEntry.pathId].add(chunk);
      if (metaListEntry.hasNext()) {
        metaListEntry.next();
        chunkMetaHeap.add(metaListEntry);
      }
    }
  }

  public static boolean isChunkOverflowed(TimeValuePair timeValuePair, ChunkMetaData metaData) {
    return timeValuePair != null
        && timeValuePair.getTimestamp() < metaData.getEndTime();
  }

  public static boolean isChunkTooSmall(int ptWritten, ChunkMetaData chunkMetaData,
      boolean isLastChunk, int minChunkPointNum) {
    return ptWritten > 0 || (minChunkPointNum >= 0 && chunkMetaData.getNumOfPoints() < minChunkPointNum
        && !isLastChunk);
  }

  public static List<List<Path>> splitPathsByDevice(List<Path> paths) {
    if (paths.isEmpty()) {
      return Collections.emptyList();
    }
    paths.sort(Comparator.comparing(Path::getDevice));

    String currDevice = null;
    List<Path> currList = null;
    List<List<Path>> ret = new ArrayList<>();
    for (Path path : paths) {
      if (currDevice == null) {
        currDevice = path.getDevice();
        currList = new ArrayList<>();
        currList.add(path);
      } else if (path.getDevice().equals(currDevice)) {
        currList.add(path);
      } else {
        ret.add(currList);
        currDevice = path.getDevice();
        currList = new ArrayList<>();
        currList.add(path);
      }
    }
    ret.add(currList);
    return ret;
  }

  public static int collectActualPageNum(List<TsFileResource> seqFiles) {
    int totalSize = 0;
    for (TsFileResource tsFileResource : seqFiles) {
      if(tsFileResource.isClosed()){
        totalSize += tsFileResource.getActualPageNum();
        logger.info("File {}, actualPageNUm={}, virtualPageNUm={} ", tsFileResource.getFile().getAbsolutePath(),
            tsFileResource.getActualPageNum(), tsFileResource.getVirtualPageNum());
      }
    }
    return totalSize;
  }

  public static Pair<Integer, Integer> collectActualVirtualPageNumPair(List<TsFileResource> seqFiles) {
    int totalVirtualSize = 0;
    int totalActualSize = 0;
    for (TsFileResource tsFileResource : seqFiles) {
      if(tsFileResource.isClosed()){
        totalVirtualSize += tsFileResource.getVirtualPageNum();
        totalActualSize += tsFileResource.getActualPageNum();
        logger.info("File {}, actualPageNUm={}, virtualPageNUm={} ", tsFileResource.getFile().getAbsolutePath(),
            tsFileResource.getActualPageNum(), tsFileResource.getVirtualPageNum());
      }

    }
    return new Pair<>(totalActualSize, totalVirtualSize);
  }

  public static class MetaListEntry implements Comparable<MetaListEntry>{
    private int pathId;
    private int listIdx;
    private List<ChunkMetaData> chunkMetaDataList;

    public MetaListEntry(int pathId, List<ChunkMetaData> chunkMetaDataList) {
      this.pathId = pathId;
      this.listIdx = -1;
      this.chunkMetaDataList = chunkMetaDataList;
    }

    @Override
    public int compareTo(MetaListEntry o) {
      return Long.compare(this.current().getOffsetOfChunkHeader(),
          o.current().getOffsetOfChunkHeader());
    }

    public ChunkMetaData current() {
      return chunkMetaDataList.get(listIdx);
    }

    public boolean hasNext() {
      return listIdx + 1 < chunkMetaDataList.size();
    }

    public ChunkMetaData next() {
      return chunkMetaDataList.get(++listIdx);
    }

    public int getPathId() {
      return pathId;
    }
  }
}
