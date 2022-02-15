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

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;

public class SampleEntropy {

  public static double calSampleEntropy(BatchData batchData, int len){
    return calSampleEntropy(batchData, len, 2, 0.2d);
  }

  public static double calSampleEntropy(BatchData batchData, int len, int m, double r) {
    double A = 0;
    double B = 0;
    for(int i = 0;i<len-m;i++){
      double xmiSum = 0;
      for(int j = 0;j<len-m+1;j++){
        double lineMax = 0;

        for(int k = 0;k<m;k++){
          double tmp = Math.abs(getSubValue(batchData, i+k, j+k));
          lineMax = lineMax>tmp?lineMax:tmp;
        }
        //System.out.println("lineMax = "+lineMax);
        if(lineMax<=r){
          xmiSum++;
        }
      }
      xmiSum--;
      B+=xmiSum;
    }

    m+=1;
    for(int i = 0;i<len-m+1;i++){
      double xmiSum = 0;
      for(int j = 0;j<len-m+1;j++){
        double lineMax = 0;

        for(int k = 0;k<m;k++){
          double tmp = Math.abs(getSubValue(batchData, i+k, j+k));
          lineMax = lineMax>tmp?lineMax:tmp;
        }
        System.out.println(lineMax);
        if(lineMax<=r){
          xmiSum++;
        }
      }
      xmiSum--;
      A+=xmiSum;
    }
    System.out.println("A="+A+", B="+B);

    return -Math.log(A/B);
  }


  public static double getSubValue(BatchData batchData, int i, int j) {
    switch (batchData.getDataType()) {
      case INT32:
        return  batchData.getIntByIndex(i) - batchData.getIntByIndex(j);
      case INT64:
        return batchData.getLongByIndex(i) - batchData.getLongByIndex(j);
      case FLOAT:
        return batchData.getFloatByIndex(i) - batchData.getFloatByIndex(j);
      case DOUBLE:
        return batchData.getDoubleByIndex(i) - batchData.getDoubleByIndex(j);
      default:
        throw new RuntimeException("Datatype "+batchData.getDataType().name()+" can't calculate sample entropy.");
    }
  }


  public static void main(String[] args) throws Exception {
    BatchData batchData = new BatchData(TSDataType.INT32, false);
//    for(long i = 1;i<=5;i++){
//      batchData.putTime(i);
//    }

    batchData.putInt(1);

    batchData.putInt(3);

    batchData.putInt(2);

    batchData.putInt(3);

    batchData.putInt(2);

    batchData.putInt(3);


    System.out.println(calSampleEntropy(batchData, 6,2, 0.2d));
//    batchData.putInt(1);
  }
}
