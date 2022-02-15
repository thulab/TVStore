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
package org.apache.iotdb.db.compress.controll;

import java.util.List;
import org.apache.iotdb.db.compress.manage.MonitorDisk;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StrategyWraper {

  private static final Logger logger = LoggerFactory.getLogger(StrategyWraper.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  public static List<Integer> calPageMapping(String group, int actualPageNum, int virtualPageNum,
      int targetPageNum, List<TsFileResource> seqFiles, IDelayFunction delayFunction, int seriesNum) {

    IDelayStrategy strategy = new SimpleStrategy();
    logger.info("storage Group {}: actualPageNum={}, virtualPageNum={}, targetPageNum={}", group,
        actualPageNum, virtualPageNum, targetPageNum);
    return strategy
        .strategy(delayFunction, actualPageNum, virtualPageNum, targetPageNum,
            seqFiles, seriesNum);
  }

  public static List<Integer> calPageMapping(String group, int actualPageNum, int virtualPageNum,
      List<TsFileResource> seqFiles, int seriesNum) {

    IDelayStrategy strategy = new SimpleStrategy();
    int targetPageNum = (int)(actualPageNum/config.getCompressRatio());
    logger.info("storage Group {}: actualPageNum={}, virtualPageNum={}, targetPageNum={}", group, actualPageNum, virtualPageNum, targetPageNum);
    return  strategy.strategy(DelayFunctionFactory.getInstance(), actualPageNum, virtualPageNum, targetPageNum, seqFiles, seriesNum);




//    // 压缩时无写入负载
//    if(!config.isHavingWriteWorkloadWhileCompressing()){
//      IDelayStrategy strategy = new SimpleStrategy();
//      // 当前所有数据文件的大小
//      long dataFileSizeInMB = MonitorDisk.getInstance().calDataSize();
//      int targetPageNum = (int)(1.0*config.getDiskSizeLowBoundInMB()/dataFileSizeInMB*actualPageNum);
//      logger.info("storage Group {}: actualPageNum={}, virtualPageNum={}, targetPageNum={}", group, actualPageNum, virtualPageNum, targetPageNum);
//      return  strategy.strategy(DelayFunctionFactory.getInstance(), actualPageNum, virtualPageNum, targetPageNum, seqFiles, seriesNum);
//    }
//    else{
//      // 压缩时有写入负载
//      IDelayStrategy strategy = new SimpleStrategy();
//      double compressWriteDataSize = Math.min(config.getOneCompressWriteDataThreshold(), config.getDiskSizeLowBoundInMB()/config.getLowBoundToWriteDataRatio());
//      long dataFileSizeInMB = MonitorDisk.getInstance().calDataSize();
//
//      double molecule = (dataFileSizeInMB-config.getDiskSizeLowBoundInMB())/compressWriteDataSize + 2;
//      double denominator = 1-config.getDiskWriteSpeed()/config.getDiskReadSpeed();
//
//      int targetPageNum = (int)(denominator*actualPageNum/molecule);
//      logger.info("storage Group {}: c{}, dataFileSizeInMB={}, molecule={}, denominator={}, denominator/molecule={}.",
//          group, compressWriteDataSize, dataFileSizeInMB, molecule, denominator, (denominator/molecule) );
//      logger.info("storage Group {}: fileNum={}, actualPageNum={}, virtualPageNum={}, targetPageNum={}",
//          group, seqFiles.size(),actualPageNum, virtualPageNum, targetPageNum);
//      return  strategy.strategy(DelayFunctionFactory.getInstance(), actualPageNum, virtualPageNum, targetPageNum, seqFiles, seriesNum);
//    }
  }



}
