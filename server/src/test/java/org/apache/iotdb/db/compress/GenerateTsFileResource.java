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
package org.apache.iotdb.db.compress;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.read.TsFileRestorableReader;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderWithoutFilter;

public class GenerateTsFileResource {

  public static void main(String[] args) throws IOException {

    String path = "/Users/suyue/input/debug";

    File file = new File(path);
//    // 如果这个路径是文件夹
//    if (file.isDirectory()) {
//      // 获取该文件夹内所有的文件
//      File[] files = file.listFiles();
//      for (File list : files) {
//        if(list.isFile() && list.getName().endsWith(".tsfile")){
//          TsFileRestorableReader tsFileRestorableReader = new TsFileRestorableReader(list.getAbsolutePath());
//          String filename = list.getName().substring(0,list.getName().lastIndexOf("."));
//          System.out.println(filename);
//          list.renameTo(new File(path+"/"+filename));
//        }
//      }
//    }


    // 如果这个路径是文件夹
    if (file.isDirectory()) {
      // 获取该文件夹内所有的文件
      File[] files = file.listFiles();
      for (File list : files) {
        if(list.isFile()&& list.getName().endsWith(".tsfile")){
          System.out.println(list.getAbsolutePath());
          try{
            new GenerateTsFileResource().recoverResourceFromReader(new TsFileResource(new File(list.getAbsolutePath()) ));
          }
          catch (Exception e){
            e.printStackTrace();
          }


        }
      }
    }


  }

  /**
   * this function does not modify the position of the file reader.
   */

  private void recoverResourceFromReader(TsFileResource tsFileResource) throws IOException {
    int actualPageNum = 0;
    int virtualPageNum = 0;
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(tsFileResource.getFile().getAbsolutePath())) {
      MetadataQuerierByFileImpl metadataQuerierByFile = new MetadataQuerierByFileImpl(reader);
      TsFileMetaData metaData = metadataQuerierByFile.getWholeFileMetadata();
      List<TsDeviceMetadataIndex> deviceMetadataIndexList = new ArrayList<>(
          metaData.getDeviceMap().values());
      ChunkLoaderImpl chunkLoader = new ChunkLoaderImpl(reader);
      for (TsDeviceMetadataIndex index : deviceMetadataIndexList) {
        TsDeviceMetadata deviceMetadata = reader.readTsDeviceMetaData(index);
        for (ChunkGroupMetaData chunkGroupMetaData : deviceMetadata
            .getChunkGroupMetaDataList()) {
          for (ChunkMetaData chunkMetaData : chunkGroupMetaData.getChunkMetaDataList()) {
            ChunkReader chunkReader = new ChunkReaderWithoutFilter(chunkLoader.getChunk(chunkMetaData));
            while (chunkReader.hasNextBatch()){
              PageHeader pageHeader = chunkReader.nextPageHeader();
              chunkReader.skipPageData();
              if(pageHeader!=null){
                virtualPageNum+=pageHeader.getCompressRatio();
                actualPageNum++;
              }
            }
            tsFileResource.updateStartTime(chunkGroupMetaData.getDeviceID(),
                chunkMetaData.getStartTime());
            tsFileResource
                .updateEndTime(chunkGroupMetaData.getDeviceID(), chunkMetaData.getEndTime());
          }
        }
      }
    }
    System.out.println("file "+tsFileResource.getFile().getAbsolutePath()+", actualPageNum="+actualPageNum+", virtualPageNum="+virtualPageNum);
    tsFileResource.setActualPageNum(actualPageNum);
    tsFileResource.setVirtualPageNum(virtualPageNum);
    // write .resource file
    tsFileResource.serialize();
  }

}
