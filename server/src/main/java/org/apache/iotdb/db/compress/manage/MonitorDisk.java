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
package org.apache.iotdb.db.compress.manage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.metadata.MManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The main class of multiple directories. Used to allocate folders to data files.
 */
public class MonitorDisk {

  private static final Logger logger = LoggerFactory.getLogger(MonitorDisk.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private final String LINUX_FILE_SIZE_CMD = "du -sm %s";
  private List<String> sequenceFileFolders;


  private int MAX_SIZE;

  private final long fileSizeInMB = config.getTsFileSizeThreshold()/1048576;
  private final double ratio = config.getCompressRatio();

  private MonitorDisk() {
    MAX_SIZE = (int)IoTDBDescriptor.getInstance().getConfig().getDiskSizeUpBoundInMB();
    sequenceFileFolders =
        new ArrayList<>(Arrays.asList(IoTDBDescriptor.getInstance().getConfig().getDataDirs()));
  }

  public boolean isNeedStartCompress(){
    double ingestSpeed = StorageEngine.getInstance().getLastIngestRate();
    double compressSpeed = StorageEngine.getInstance().getLastCompressRate();
    int sgNum = MManager.getInstance().getAllStorageGroupNames().size();
    long triggerSizeInMB = (long) (MAX_SIZE - sgNum*fileSizeInMB/ratio - ingestSpeed*fileSizeInMB/compressSpeed);

    triggerSizeInMB=Math.round(triggerSizeInMB*config.getCompressTriggerSizeRatio());
    long sizeInMB = calDataSize();

    if(sizeInMB>= triggerSizeInMB){
      logger.info("Total Data Size = : {} MB,disk up threshold = : {} MB, start compressing ..." , sizeInMB, triggerSizeInMB);
      return true;
    }
    return false;
  }

  public long calDataSize(){
    BufferedReader in;
    Process pro = null;
    Runtime runtime = Runtime.getRuntime();
    long sizeInMB=0;
    for (String path_ : sequenceFileFolders) {
      String command = String.format(LINUX_FILE_SIZE_CMD, path_);
      try {
        pro = runtime.exec(command);
        in = new BufferedReader(new InputStreamReader(pro.getInputStream()));
        String line;
        while ((line = in.readLine()) != null) {
          String size = line.split("\\s+")[0];

          sizeInMB += Long.parseLong(size);
        }
        in.close();
      } catch (IOException e) {
        logger.info("Execute command failed: " + command);
      }
    }
    logger.error("Total Data Size = : {} MB" ,sizeInMB);
    return sizeInMB;
  }

  public static MonitorDisk getInstance() {
    return DirectoriesHolder.INSTANCE;
  }

  private static class DirectoriesHolder {

    private static final MonitorDisk INSTANCE = new MonitorDisk();
  }


}
