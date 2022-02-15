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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleStrategy implements IDelayStrategy {

  private static final Logger logger = LoggerFactory.getLogger(SimpleStrategy.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final Random random = new Random(1230189);
  @Override
  // delayFunction 描述的是窗口的对应关系
  public List<Integer> strategy(IDelayFunction delayFunction, int totalActualPageNum,
      int totalVirtualPageNum, int targetPageNum, List<TsFileResource> seqFiles, int seriesNum) {

//    List<Integer> actualPageNumList = new ArrayList<>();
//    List<Integer> virtualPageNumList = new ArrayList<>();
//    for(TsFileResource tsFileResource:seqFiles){
//      actualPageNumList.add(tsFileResource.getActualPageNum());
//      virtualPageNumList.add(tsFileResource.getVirtualPageNum());
//    }

    totalActualPageNum/=seriesNum;
    totalVirtualPageNum/=seriesNum;
    targetPageNum/=seriesNum;

    // 这次压缩过程中，衰减窗口已经覆盖的虚拟page个数
    long sum = 0;
    // 衰减函数的当前参数，截取段的上界
    int up = 0;
    // 衰减函数的当前参数，截取段的下界
    int low = 0;
    // 这次压缩过程中尚待压缩的实际page个数
    double leftActualPageNum = totalActualPageNum;
    // 这次压缩过程中被压缩的实际page已经覆盖的原始page的个数
    double currVirtualPageNum = 0;
    int idx = 0;
    for(; idx < seqFiles.size(); idx++){
      currVirtualPageNum+=seqFiles.get(idx).getVirtualPageNum()*1.0/seriesNum;
      leftActualPageNum-=seqFiles.get(idx).getActualPageNum()*1.0/seriesNum;
      while (sum < currVirtualPageNum) {
        up++;
        sum += delayFunction.delayFun(up);
      }

//      while (up - low > targetPageNum) {
//        up++;
//        sum += delayFunction.delayFun(up);
//        while (sum > totalVirtualPageNum) {
//          low++;
//          sum -= delayFunction.delayFun(low);
//        }
//      }
      //System.out.println("targetPageNum="+targetPageNum +"leftPageNum="+leftActualPageNum+ ", expected="+(leftActualPageNum+(up-low)));
      logger.info("存储组={}, targetPageNum={}, leftActualPageNum={}, currExpectedPageNum={}",
          seqFiles.get(idx).getFile().getAbsolutePath(), targetPageNum, leftActualPageNum, (leftActualPageNum+(up-low)));
      if(targetPageNum>=leftActualPageNum+(up-low)){
        break;
      }

      if(idx<seqFiles.size()-1){
//        double beforeRatio = 1.0*(totalActualPageNum-leftActualPageNum)/(up-low);
        double beforeRatio = 1.0*(totalActualPageNum-leftActualPageNum)/(up-low);
        double avgNextActualPage = seqFiles.get(idx+1).getActualPageNum()*1.0/seriesNum;
        double expectedPageNumIfCompress = leftActualPageNum-avgNextActualPage+(up-low)+avgNextActualPage/beforeRatio;
        double expectedPageNumIfNotCompressNext = leftActualPageNum+(up-low);

        //System.out.println("targetPageNum="+targetPageNum+", nextPageNum="+expectedPageNum);
        logger.info("存储组={}, targetPageNum={}, nextExpectedPageNumIfCompress={}, expectedPageNumIfNotCompressNext={}",
            seqFiles.get(idx).getFile().getAbsolutePath(), targetPageNum, expectedPageNumIfCompress, expectedPageNumIfNotCompressNext);

        if(targetPageNum> expectedPageNumIfCompress && random.nextDouble()>expectedPageNumIfCompress/targetPageNum){
          break;
        }
      }
    }

    while (up - low > targetPageNum) {
      up++;
      sum += delayFunction.delayFun(up);
      while (sum > totalVirtualPageNum) {
        low++;
        sum -= delayFunction.delayFun(low);
      }
    }
    if(idx>=seqFiles.size()){
      logger.info("存储组={}, totalFileNum=compressFileNum={}, compressActualPageNum={}, "
              + "unCompressActualPageNum={}, afterCompressedActualPageNum={}, targetPageNum={}, low={}, up={}",
          seqFiles.get(0).getFile().getAbsolutePath(), seqFiles.size(), 
          (totalActualPageNum-leftActualPageNum), leftActualPageNum, (up-low)+leftActualPageNum, targetPageNum, low, up);
    }
    else{
      logger.info("存储组={}, totalFileNum={}, compressFileNum={}, compressActualPageNum={}, "
              + "unCompressActualPageNum={}, afterCompressedActualPageNum={}, targetPageNum={}, low={}, up={}",
          seqFiles.get(idx).getFile().getAbsolutePath(), seqFiles.size(), idx,
          (totalActualPageNum-leftActualPageNum), leftActualPageNum, (up-low)+leftActualPageNum, targetPageNum, low, up);
    }

    List<Integer> ans = new ArrayList<>();
    for (int i = up; i > low; i--) {
      ans.add(delayFunction.delayFun(i));
    }
    System.out.println("low="+low+", up="+up+", targetPageNum="+targetPageNum+"afterCompressedActualPageNum="+(up-low+leftActualPageNum));
    return ans;
  }



//  //@Override
//  // delayFunction 描述的是窗口的对应关系
//  public List<Integer> strategy2(IDelayFunction delayFunction, int totalActualPageNum,
//      int totalVirtualPageNum, int targetPageNum, List<TsFileResource> seqFiles, int seriesNum) {
//    totalActualPageNum/=seriesNum;
//    totalVirtualPageNum/=seriesNum;
//    targetPageNum/=seriesNum;
//
//    // 这次压缩过程中，衰减窗口已经覆盖的虚拟page个数
//    long sum = 0;
//    // 衰减函数的当前参数，截取段的上界
//    int up = 0;
//    // 衰减函数的当前参数，截取段的下界
//    int low = 0;
//
//    while (sum < uncompressPageNum) {
//      up++;
//      sum += delayFunction.delayFun(up);
//    }

 // }

//  @Override
//  public List<Integer> strategy(IDelayFunction delayFunction, int currentPageNum,
//      int uncompressPageNum, int targetPageNum) {
//
//    long sum = 0;
//    int up = 0;
//    int low = 0;
//    while (sum < uncompressPageNum) {
//      up++;
//      sum += delayFunction.delayFun(up);
//    }
//
//    while (up - low > targetPageNum) {
//      up++;
//      sum += delayFunction.delayFun(up);
//      while (sum > uncompressPageNum) {
//        low++;
//        sum -= delayFunction.delayFun(low);
//      }
//    }
//    List<Integer> ans = new ArrayList<>();
//    for (int i = up; i > low; i--) {
//      ans.add(delayFunction.delayFun(i));
//    }
//    return ans;
//  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public int next() {
    return 0;
  }
}
