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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.iotdb.db.compress.controll.DownSampleImpl;
import org.apache.iotdb.db.compress.controll.PowerLaw;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;

public class Lab2 {


  public static void main(String[] args) throws IOException, ParseException {
    String dir = "DataForLab2";
//    String testFileName = "Train.csv";
    String testFileName = "Train.csv";
    Lab2 lab2 = new Lab2();
    BatchData rawData = lab2.readDataFromCSV(testFileName);

    BatchData iotdbData = lab2.iotdbSampling(rawData, 0.02, 10, 5, 1.5, 0.5, 10, 4);
    //lab2.writeDataToCSV(iotdbData, "iotdbData.csv");

    BatchData summaryStoreData = lab2.summaryStoreAvg(rawData, 1.5, 0.5, 10, 4);
    //lab2.writeDataToCSV(dir, summaryStoreData, "summaryStoreData.csv", true);

//    BatchData fixedFrequencyData = lab2.fixedFrequencySampling(rawData, 10);
//    lab2.writeDataToCSV(fixedFrequencyData, "fixedFrequencyData.csv");
//
//    BatchData ttlData = lab2.ttlSampling(rawData, 10);
//    lab2.writeDataToCSV(ttlData, "ttlData.csv");
//
//    BatchData allData = lab2.ttlSampling(rawData, 1);
//    lab2.writeDataToCSV(allData, "rawData.csv");

  }

  BatchData iotdbSampling(BatchData rawData, double ttlRatio, int ratio, int windowUnit, double p,
      double q, double r, double s) {
    BatchData batchData = new BatchData(TSDataType.INT32, true);

    int totalLen = rawData.length();
    int targetLen = totalLen / ratio;
    int unCompressLen = (int) Math.round(totalLen * ttlRatio);

    int targetCompressLen = targetLen - unCompressLen;

    int actualCompressUnitLen = (totalLen - unCompressLen) / windowUnit;
    int targetCompressUnitLen = targetCompressLen / windowUnit;
    double targetUnitRatio = actualCompressUnitLen * 1.0 / targetCompressUnitLen;
    System.out.println("压缩后目标点数：" + targetLen + ", 不压缩原始点数：" + unCompressLen
        + ", 可压缩点的压缩后目标数：" + targetCompressLen + ", 不压缩倍数：" + ttlRatio);

    System.out.println("actualCompressUnitLen = " + actualCompressUnitLen
        + ", targetCompressUnitLen = " + targetCompressUnitLen + ", targetRatio = "
        + targetUnitRatio);

    PowerLaw powerLaw = new PowerLaw(p, q, r, s);
    List<Integer> pageMapping = iotdbHelper(powerLaw, actualCompressUnitLen, targetCompressUnitLen);

    int idx = 0;
    DownSampleImpl downSample = new DownSampleImpl();
    for (int wlen : pageMapping) {
      if (wlen <= 1) {
        break;
      }

      int endIdx = Math.min(totalLen - unCompressLen, idx + wlen * windowUnit);
      BatchData pageRawData = new BatchData(TSDataType.INT32, true);
      for (int i = idx; i < endIdx; i++) {
        pageRawData.putTime(rawData.getTimeByIndex(i));
        pageRawData.putInt(rawData.getIntByIndex(i));
      }
      BatchData pageCompressData = downSample.sample(pageRawData, windowUnit);
      idx = endIdx;
      batchData.putAnBatchData(pageCompressData);

//      for(int i = pageCompressData.length()-1;i>=0;i--){
//        batchData.putTime(pageCompressData.getTimeByIndex(i));
//        batchData.putInt(pageCompressData.getIntByIndex(i));
//      }
    }

    while (idx < totalLen) {
      batchData.putTime(rawData.getTimeByIndex(idx));
      batchData.putInt(rawData.getIntByIndex(idx));
      idx++;
    }
    System.out.println("iotdbSampling write data :" + batchData.length() + ", actual data ratio:"
        + 1.0 * totalLen / batchData.length());
    System.out.println();

    return batchData;
  }

  public static List<Integer> iotdbHelper(PowerLaw powerLaw, int actualPageNum, int targetPageNum) {
    List<Integer> pageRatio = new ArrayList<>();
    for (int i = 0; i < actualPageNum; i++) {
      pageRatio.add(1);
    }

    int[] targetNum = {targetPageNum};

    for (int i = 0; i < targetNum.length; i++) {
      int target = targetNum[i];
      List<Integer> pageMapping = powerLaw.strategy(powerLaw, pageRatio.size(), target, pageRatio);
      pageRatio = powerLaw.strategy2(pageRatio, pageMapping, target);
      System.out.println("cnt=" + pageRatio.size());
      System.out.println(
          "window num = " + pageRatio.size() + ", ratio = " + (1.0 * actualPageNum / pageRatio
              .size()));
    }
    return pageRatio;
  }

  BatchData summaryStoreAvg(BatchData rawData, double p, double q, double r, double s) {
    List<Integer> pageMapping = new ArrayList<>();
    int totalLen = rawData.length();
    BatchData batchData = new BatchData(TSDataType.INT32, true);
    PowerLaw powerLaw = new PowerLaw(p, q, r, s);
    int wid = 0;
    int wlen = 0;
    int rawDataId = rawData.length() - 1;
    while (rawDataId >= 0) {

      wlen = powerLaw.delayFun(++wid);
      if (wlen % 2 != 0) {
        Random random = new Random(wlen);
        wlen += random.nextDouble() > 0.5 ? 1 : 0;
      }
      wlen /= 2;

      pageMapping.add(wlen);
      long totalTime = 0;
      int totalValue = 0;
      int cnt = wlen;
      while (rawDataId >= 0 && cnt > 0) {
        totalTime += rawData.getTimeByIndex(rawDataId);
        totalValue += rawData.getIntByIndex(rawDataId);
        rawDataId--;
        cnt--;
      }
      batchData.putTime(totalTime / (wlen - cnt));
      batchData.putInt(totalValue / (wlen - cnt));
    }

    Collections.reverse(pageMapping);
    System.out.print("actual Ratio: [");
    for (int pageRatio : pageMapping) {
      System.out.print(pageRatio + ",");
    }
    System.out.println("]");
    System.out.println("summaryStoreAvg write data :" + batchData.length() + ", actual data ratio:"
        + 1.0 * totalLen / batchData.length());
    System.out.println();
    return batchData;
  }

  BatchData fixedFrequencySampling(BatchData rawData, int ratio) {
    int totalLen = rawData.length();
    BatchData batchData = new BatchData(TSDataType.INT32, true);
    for (int i = 0; i < rawData.length(); i += ratio) {
      batchData.putTime(rawData.getTimeByIndex(i));
      batchData.putInt(rawData.getIntByIndex(i));
    }

    System.out.print("actual Ratio: [");
    for (int i = 0; i < rawData.length(); i += ratio) {
      System.out.print(ratio + ",");
    }
    System.out.println("]");
    System.out.println(
        "fixedFrequencySampling write data :" + batchData.length() + ", actual data ratio:"
            + 1.0 * totalLen / batchData.length());
    System.out.println();
    return batchData;
  }

  BatchData ttlSampling(BatchData rawData, int ratio) {
    int totalLen = rawData.length();
    BatchData batchData = new BatchData(TSDataType.INT32, true);
    int ansCnt = rawData.length() / ratio;
    for (int i = rawData.length() - ansCnt; i < rawData.length(); i++) {
      batchData.putTime(rawData.getTimeByIndex(i));
      batchData.putInt(rawData.getIntByIndex(i));
    }

    System.out.println("ttlSampling write data :" + batchData.length() + ", actual data ratio:"
        + 1.0 * totalLen / batchData.length());
    System.out.println();
    return batchData;
  }

  void writeDataToCSV(BatchData batchData, String fileName) throws IOException {
    writeDataToCSV("DataForLab2", batchData, fileName, false);
  }

  /**
   * Write time-value pair to csv.
   *
   * @param batchData data
   * @param fileName path
   */
  void writeDataToCSV(String dir, BatchData batchData, String fileName, boolean isReverse) throws IOException {
    checkAndMakeDir(dir);
    BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(dir + fileName));

    if (isReverse) {
      for (int i = batchData.length() - 1; i >= 0; i--) {
        long time = batchData.getTimeByIndex(i);
        int value = batchData.getIntByIndex(i);
        String line = time + "," + value;
        bufferedWriter.write(line);
        bufferedWriter.newLine();
      }
    } else {
      for (int i = 0; i < batchData.length(); i++) {
        long time = batchData.getTimeByIndex(i);
        int value = batchData.getIntByIndex(i);
        String line = time + "," + value;
        bufferedWriter.write(line);
        bufferedWriter.newLine();
      }
    }
    bufferedWriter.flush();
    bufferedWriter.close();
  }


  /**
   * Check if the directory exists, if not, create it.
   *
   * @param dir dir path
   */
  void checkAndMakeDir(String dir) {
    File dirFile = new File(dir);
    if (!dirFile.exists()) {
      dirFile.mkdirs();
    }
  }

  /**
   * Read time-value pair from csv.
   */
  BatchData readDataFromCSV(String testFileName) throws IOException, ParseException {
    String dirStr = "/Users/suyue/input/testPredictData/";
//    String testFileName = "Train.csv";
    String timeFormat = "dd-MM-yyyy HH:mm";
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(timeFormat);

    BufferedReader fileReader = new BufferedReader(new FileReader(dirStr + testFileName));
    BatchData batchData = new BatchData(TSDataType.INT32, true);
    String line = fileReader.readLine();
    int cnt = 0;
    while ((line = fileReader.readLine()) != null) {
      String[] tmp = line.split(",");
      long time = simpleDateFormat.parse(tmp[1]).getTime();
      int value = Integer.parseInt(tmp[2]);
      batchData.putTime(time);
      batchData.putInt(value);
      cnt++;
      //System.out.println("Time = "+tmp[1]+", value = "+value+"; timestamp = "+ time);
    }
    fileReader.close();
    System.out.println("Total read line : " + cnt);
    System.out.println("Batch data size : " + batchData.length());
    return batchData;
  }

}
