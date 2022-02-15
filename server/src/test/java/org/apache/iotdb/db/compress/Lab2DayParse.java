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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.iotdb.db.compress.controll.DownSampleImpl;
import org.apache.iotdb.db.compress.controll.PowerLaw;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.utils.Pair;

public class Lab2DayParse {

  public static final String wdir = "DataForLab2ID/";
  public static final String rdir = "DataForLab2Day/";
  public static void main(String[] args) throws IOException, ParseException {

    String testFileName = "TrainDay.csv";
    Lab2DayParse lab2 = new Lab2DayParse();
    Map<Long, Pair<String, String>> time2TimestrAndID = lab2.readDataFromCSV(testFileName);


    String[] fileNames = {"iotdbData.csv", "summaryStoreData.csv", "fixedFrequencyData.csv", "ttlData.csv", "rawData.csv"};
    lab2.checkAndMakeDir(wdir);
    for(String name:fileNames){
      lab2.writeDataToCSV(wdir, rdir, "DataForLab2"+name, time2TimestrAndID);
    }


  }





  /**
   * Write time-value pair to csv.
   *
   * @param fileName path
   */
  void writeDataToCSV(String wdir, String rdir, String fileName, Map<Long, Pair<String, String>> time2TimestrAndID) throws IOException {
    checkAndMakeDir(wdir);
    BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(wdir + fileName));
    BufferedReader fileReader = new BufferedReader(new FileReader(rdir + fileName));
    String rLine;
    while((rLine=fileReader.readLine())!=null){
      String[] rItems = rLine.split(",");
      long timeStamp = Long.parseLong(rItems[0]);

      timeStamp = (timeStamp-1345824000000L)/86400000 * 86400000 + 1345824000000L;
      Pair<String, String> pair = time2TimestrAndID.get(timeStamp);
      if(pair==null){
        System.out.println("time = "+timeStamp+" not esists.");
      }
      String wline = pair.left+","+pair.right+","+rItems[1];
      bufferedWriter.write(wline);
      bufferedWriter.newLine();
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

//  /**
//   * Read time-value pair from csv.
//   */
//  Map<String, String> readDataFromCSV2(String testFileName) throws IOException, ParseException {
//    String dirStr = "/Users/suyue/input/testPredictData/";
////    String testFileName = "Train.csv";
//    String timeFormat = "yyyy-MM-dd";
//    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(timeFormat);
//
//    BufferedReader fileReader = new BufferedReader(new FileReader(dirStr + testFileName));
//    BatchData batchData = new BatchData(TSDataType.DOUBLE, true);
//    String line = fileReader.readLine();
//    int cnt = 0;
//    while ((line = fileReader.readLine()) != null) {
//      String[] tmp = line.split(",");
//      long time = simpleDateFormat.parse(tmp[0]).getTime();
//      Double value = Double.parseDouble(tmp[2]);
//      batchData.putTime(time);
//      batchData.putDouble(value);
//      cnt++;
//      System.out.println("Time = "+tmp[0]+", value = "+value+"; timestamp = "+ time);
//    }
//    fileReader.close();
//    System.out.println("Total read line : " + cnt);
//    System.out.println("Batch data size : " + batchData.length());
//    return batchData;
//  }

  /**
   * Read time-value pair from csv.
   */
  Map<Long, Pair<String,String>> readDataFromCSV(String testFileName) throws IOException, ParseException {
    String dirStr = "/Users/suyue/input/testPredictData/";
//    String testFileName = "Train.csv";
    String timeFormat = "yyyy-MM-dd";
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(timeFormat);

    BufferedReader fileReader = new BufferedReader(new FileReader(dirStr + testFileName));
    Map<Long, Pair<String,String>> ans = new HashMap<>();
    String line = fileReader.readLine();
    int cnt = 0;
    while ((line = fileReader.readLine()) != null) {
      String[] tmp = line.split(",");
      long time = simpleDateFormat.parse(tmp[0]).getTime();
      Double value = Double.parseDouble(tmp[2]);

      Pair pair = new Pair(tmp[0], tmp[1]);
      ans.put(time, pair);
      cnt++;
      System.out.println("Time = "+tmp[0]+", value = "+value+"; timestamp = "+ time);
    }
    fileReader.close();
    System.out.println("Total read line : " + cnt);
    System.out.println("Batch data size : " + ans.size());
    return ans;
  }

}
