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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;

public class DownSampleImpl implements Isampling {

  public static Number[][] largestTriangleThreeBuckets(Number[][] data, Integer threshold) {
    Number[][] sampled = new Number[threshold][];
    if (data == null) {
      throw new NullPointerException("Cannot cope with a null data input array.");
    }
    if (threshold <= 2) {
      throw new RuntimeException("What am I supposed to do with that?");
    }
    if (data.length <= 2 || data.length <= threshold) {
      return data;
    }
    int sampled_index = 0;
    double every = (double) (data.length - 2) / (double) (threshold - 2);
    //System.out.println(": " + every);
    int a = 0, next_a = 0;
    Number[] max_area_point = null;
    double max_area, area;

    sampled[sampled_index++] = data[a];

    for (int i = 0; i < threshold - 2; i++) {
      double avg_x = 0.0D, avg_y = 0.0D;
      int avg_range_start = (int) Math.floor((i + 1) * every) + 1;
      int avg_range_end = (int) Math.floor((i + 2) * every) + 1;
      avg_range_end = avg_range_end < data.length ? avg_range_end : data.length;
      int avg_range_length = (int) (avg_range_end - avg_range_start);
      while (avg_range_start < avg_range_end) {
        avg_x = avg_x + data[avg_range_start][0].doubleValue();
        avg_y += data[avg_range_start][1].doubleValue();
        avg_range_start++;
      }
      avg_x /= avg_range_length;
      avg_y /= avg_range_length;

      int range_offs = (int) Math.floor((i + 0) * every) + 1;
      int range_to = (int) Math.floor((i + 1) * every) + 1;

      double point_a_x = data[a][0].doubleValue();
      double point_a_y = data[a][1].doubleValue();

      max_area = area = -1;

      while (range_offs < range_to) {
        area = Math.abs(
            (point_a_x - avg_x) * (data[range_offs][1].doubleValue() - point_a_y) -
                (point_a_x - data[range_offs][0].doubleValue()) * (avg_y - point_a_y)
        ) * 0.5D;
        if (area > max_area) {
          max_area = area;
          max_area_point = data[range_offs];
          next_a = range_offs;
        }
        range_offs++;
      }
      sampled[sampled_index++] = max_area_point;
      a = next_a;
    }

    sampled[sampled_index++] = data[data.length - 1];
    return sampled;
  }

  public static Number[][] largestTriangleThreeBucketsTime(Number[][] data, Integer threshold) {
    Number[][] sampled = new Number[threshold][];
    if (data == null) {
      throw new NullPointerException("Cannot cope with a null data input array.");
    }
    if (threshold <= 2) {
      throw new RuntimeException("What am I supposed to do with that?");
    }
    if (data.length <= 2 || data.length <= threshold) {
      return data;
    }
    int bucket_interval = (int) ((data[data.length - 1][0].longValue() - data[0][0].longValue())
        / threshold);
    int sampled_index = 0;
    double every = (double) (data.length - 2) / (double) (threshold - 2);
    int a = 0, next_a = 0;
    Number[] max_area_point = null;
    double max_area, area;

    sampled[sampled_index++] = data[a];

    for (int i = 0; i < threshold - 2; i++) {
      double avg_x = 0.0D, avg_y = 0.0D;
      int avg_range_start = (int) Math.floor((i + 1) * every) + 1;
      int avg_range_end = (int) Math.floor((i + 2) * every) + 1;
      avg_range_end = avg_range_end < data.length ? avg_range_end : data.length;
      int avg_range_length = (int) (avg_range_end - avg_range_start);
      while (avg_range_start < avg_range_end) {
        avg_x = avg_x + data[avg_range_start][0].doubleValue();
        avg_y += data[avg_range_start][1].doubleValue();
        avg_range_start++;
      }
      avg_x /= avg_range_length;
      avg_y /= avg_range_length;

      int range_offs = (int) Math.floor((i + 0) * every) + 1;
      int range_to = (int) Math.floor((i + 1) * every) + 1;

      double point_a_x = data[a][0].doubleValue();
      double point_a_y = data[a][1].doubleValue();

      max_area = area = -1;

      long ending_time = data[range_offs][0].longValue() + bucket_interval;

      while (range_offs < range_to) { // && data[range_offs][0].longValue() < ending_time) {
        area = Math.abs(
            (point_a_x - avg_x) * (data[range_offs][1].doubleValue() - point_a_y) -
                (point_a_x - data[range_offs][0].doubleValue()) * (avg_y - point_a_y)
        ) * 0.5D;
        if (area > max_area) {
          max_area = area;
          max_area_point = new Number[]{ending_time, data[range_offs][1]};
          next_a = range_offs;
        }
        range_offs++;
      }
      sampled[sampled_index++] = max_area_point;
      a = next_a;
    }

    sampled[sampled_index++] = data[data.length - 1];
    return sampled;
  }

  @Override
  public BatchData sample(BatchData data, int threshold) {
    if (data == null) {
      throw new NullPointerException("Cannot cope with a null BatchData.");
    }
    BatchData sampled = new BatchData(data.getDataType(), true);

    if (data.length() <= 2 || data.length() <= threshold || threshold <= 2) {
      return data;
    }
    int sampled_index = 0;
    double every = (double) (data.length() - 2) / (double) (threshold - 2);
    //System.out.println(": " + every);
    int a = 0, next_a = 0;
    double max_area, area;

    //sampled[sampled_index++] = data[a];

    sampled.putTime(data.getTimeByIndex(a));
    sampled.putAnObject(data.getValueByIndex(a));

    for (int i = 0; i < threshold - 2; i++) {
      long avg_x = 0L;
      double avg_y = 0.0D;
      int avg_range_start = (int) Math.floor((i + 1) * every) + 1;
      int avg_range_end = (int) Math.floor((i + 2) * every) + 1;
      avg_range_end = avg_range_end < data.length() ? avg_range_end : data.length();
      int avg_range_length = (int) (avg_range_end - avg_range_start);
      while (avg_range_start < avg_range_end) {

        avg_x += data.getTimeByIndex(avg_range_start);
        avg_y += ((Number) data.getValueByIndex(avg_range_start)).doubleValue();
        avg_range_start++;
      }
      avg_x /= avg_range_length;
      avg_y /= avg_range_length;

      int range_offs = (int) Math.floor((i + 0) * every) + 1;
      int range_to = (int) Math.floor((i + 1) * every) + 1;

      long point_a_x = data.getTimeByIndex(a);
      double point_a_y = ((Number) data.getValueByIndex(a)).doubleValue();

      max_area = area = -1;
      int max_area_idx = 0;
      while (range_offs < range_to) {
        area = Math.abs(
            (point_a_x - avg_x) * (((Number) data.getValueByIndex(range_offs)).doubleValue()
                - point_a_y) -
                (point_a_x - data.getTimeByIndex(a)) * (avg_y - point_a_y)
        ) * 0.5D;

        if (area > max_area) {
          max_area = area;
          max_area_idx = range_offs;
          next_a = range_offs;
        }
        range_offs++;
      }
      sampled.putTime(data.getTimeByIndex(max_area_idx));
      sampled.putAnObject(data.getValueByIndex(max_area_idx));
      a = next_a;
    }
    sampled.putTime(data.getTimeByIndex(data.length() - 1));
    sampled.putAnObject(data.getValueByIndex(data.length() - 1));
    return sampled;
  }


  public static void main(String[] args) throws IOException {
    BatchData batchData = new BatchData(TSDataType.FLOAT, true);

    BufferedReader bufferedReader = new BufferedReader(new FileReader(
        new File("/Users/suyue/lab/test/redd_high_1x/redd_high_1x_1309275191471803_1310923546007813.txt")));
    BufferedWriter bufferedWriter=new BufferedWriter(new FileWriter("p2.txt"));
    String line;
    while((line=bufferedReader.readLine())!=null){
      String[] tv = line.split(",");
      if(tv.length == 2){
        batchData.putTime(Long.parseLong(tv[0]));
       // batchData.putFloat(Float.parseFloat(tv[1]));
       batchData.putFloat( (float) Math.atan(Double.parseDouble(tv[1])));
      }
    }

    BatchData sampledBatchdata = new DownSampleImpl().sample(batchData, batchData.length()/236);
    System.out.println("start");
    for (int i = 0; i < sampledBatchdata.length(); i++) {
      //String out = sampledBatchdata.getTimeByIndex(i)+", "+sampledBatchdata.getValueByIndex(i);
      String out = sampledBatchdata.getTimeByIndex(i)+", "+ Math.tan(1.0* sampledBatchdata.getFloatByIndex(i));
      System.out.println(out);
      bufferedWriter.write(out);
      bufferedWriter.newLine();
    }
    bufferedWriter.flush();
    bufferedWriter.close();
  }
}