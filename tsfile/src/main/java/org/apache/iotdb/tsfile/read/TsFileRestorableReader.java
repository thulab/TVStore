/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.tsfile.read;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderWithoutFilter;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TsFileRestorableReader extends TsFileSequenceReader {

  private static final Logger logger = LoggerFactory.getLogger(TsFileRestorableReader.class);

  public TsFileRestorableReader(String file) throws IOException {
    this(file, true);
  }

  public TsFileRestorableReader(String file, boolean autoRepair)
      throws IOException {
    //if autoRepair == true, then it means the file is likely broken, so we can not read metadata
    //otherwise, the user may consider that either the file is complete, or the user can accept an
    // Exception when reading broken data. Therefore, we set loadMetadata as true in this case.
    super(file, !autoRepair);
    if (autoRepair) {
      try {
        checkAndRepair();
      } catch (Throwable e) {
        close();
        throw e;
      }
      loadMetadataSize();
    }
  }

  /**
   * Checks if the file is incomplete, and if so, tries to repair it.
   */
  private void checkAndRepair() throws IOException {
    // Check if file is damaged
    if (!isComplete()) {
      // Try to close it
      logger.info("File {} has no correct tail magic, try to repair...", file);
      RestorableTsFileIOWriter rWriter = new RestorableTsFileIOWriter(FSFactoryProducer.getFSFactory().getFile(file));
      TsFileWriter writer = new TsFileWriter(rWriter);
      // This writes the right magic string
      writer.close();
    }
  }

  public static void main(String[] args) throws IOException {

    String path = "/Users/suyue/input/tsfileCom";

    File file = new File(path);
    // 如果这个路径是文件夹
    if (file.isDirectory()) {
      // 获取该文件夹内所有的文件
      File[] files = file.listFiles();
      for (File list : files) {
        if(list.isFile()){
          TsFileRestorableReader tsFileRestorableReader = new TsFileRestorableReader(list.getAbsolutePath());
          String filename = list.getName().substring(0,list.getName().lastIndexOf("."));
          System.out.println(filename);
          list.renameTo(new File(path+"/"+filename));
        }
      }
    }

  }
}
