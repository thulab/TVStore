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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderWithFilter;
import org.apache.iotdb.tsfile.utils.Pair;

public class CompressRatioPrinter {

  public static void main(String[] args) throws IOException {

//    String[] args2 = {"/Users/suyue/input/1605778455264-101-1.tsfile", "root.group_1.d.s1", "1", String.valueOf(Long.MAX_VALUE)};
//    args = args2;
    if(args.length != 4){
      System.out.println("Need 4 arguments : TsFileListDir, seriesPath, startTime, endTime.");
    }
    String tsFileDir = args[0];
    String seriesPath = args[1];
    long startTime = Math.max(0L, Long.parseLong(args[2]));
    long endTime = Math.min(Long.MAX_VALUE, Long.parseLong(args[3]));

    List<Pair<Long, PageHeader>> ans = new ArrayList<>();
    File tsfileDirFile = new File(tsFileDir);
    if(tsfileDirFile.isFile()){
      List<Pair<Long, PageHeader>> timeRatioList = handleOneFile(tsfileDirFile.getAbsolutePath(), seriesPath, startTime, endTime);
      ans.addAll(timeRatioList);
    }
    else {
      for(File file:tsfileDirFile.listFiles()){
        if(!file.getName().endsWith(".tsfile")){
          continue;
        }
        List<Pair<Long, PageHeader>> timeRatioList = handleOneFile(file.getAbsolutePath(), seriesPath, startTime, endTime);
        ans.addAll(timeRatioList);
      }
    }

    Collections.sort(ans, new Comparator<Pair<Long, PageHeader>>() {
      @Override
      public int compare(Pair<Long, PageHeader> o1, Pair<Long, PageHeader> o2) {
        return o1.left - o2.left>0?1:-1;
      }
    });

    System.out.println("Series:"+seriesPath+", time:["+startTime+","+endTime+"]");
    System.out.println("startTime,endTime,compressRatio,count,sum");
    for(Pair<Long, PageHeader> pair:ans){
      PageHeader pageHeader = pair.right;
      System.out.println(pair.left+","+pageHeader.getMaxTimestamp()+","+pageHeader.getCompressRatio()+","+pageHeader.getNumOfValues()+","+pageHeader.getStatistics().getSum());
    }
  }

  public static List<Pair<Long, PageHeader>> handleOneFile(String fileName, String serieName, long startTime, long endTime)
      throws IOException {

    List<Pair<Long, PageHeader>> timeRatioList = new ArrayList<>();

    Filter filter = new AndFilter(TimeFilter.gt(startTime), TimeFilter.lt(endTime));
    TsFileSequenceReader fileSequenceReader = new TsFileSequenceReader(fileName);
    ChunkLoaderImpl chunkLoader = new ChunkLoaderImpl(fileSequenceReader);
    List<ChunkMetaData> chunkMetaDataList = fileSequenceReader.getChunkMetadataList(new Path(serieName));
    for(ChunkMetaData chunkMetaData:chunkMetaDataList){
      long cs = chunkMetaData.getStartTime();
      long ce = chunkMetaData.getEndTime();
      if(endTime<cs || startTime> ce){
        continue;
      }
      ChunkReader chunkReader = new ChunkReaderWithFilter(chunkLoader.getChunk(chunkMetaData), filter);
      while (chunkReader.hasNextBatch()){
        PageHeader pageHeader = chunkReader.nextPageHeader();
        timeRatioList.add(new Pair<>(pageHeader.getMinTimestamp(),pageHeader));
        chunkReader.skipPageData();
      }
    }
    return timeRatioList;
  }
}
