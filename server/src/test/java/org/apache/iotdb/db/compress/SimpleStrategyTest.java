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
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.compress.controll.PowerFunction;
import org.apache.iotdb.db.compress.controll.PowerLaw;
import org.apache.iotdb.db.compress.controll.SimpleStrategy;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

public class SimpleStrategyTest {

//  public static void main(String[] args) {
//    SimpleStrategy simpleStrategy = new SimpleStrategy();
//    List<TsFileResource> list = new SimpleStrategyTest().generateTsResourceList();
//    // PowerLaw powerLaw = new PowerLaw(2.7,0.7,2,30);
//    // new PowerLaw(1,1,1,2)
//
//    List<Integer> ans=simpleStrategy.strategy(new PowerLaw(1,1,12,1),
//        1304352+161923,  1304352+161923,237457 , list,1);
//    int sum =0;
//    for(int i = 0;i<ans.size();i++){
//      sum+=ans.get(i);
//      System.out.println("i="+i+", elem="+ans.get(i)+", sum="+sum);
//    }
//    System.out.println("sum="+sum);
//  }
//List<TsFileResource> generateTsResourceList(){
//  List<TsFileResource> list = new ArrayList<>();
//  for(int i = 0;i<16;i++){
//    list.add(new FakeTsResource(new File("/users/suyue/input/ee.txt"), 80000, 80000));
//  }
//  return list;
//}


  public static void main(String[] args) {

    double lowBound = 35720;

    double compressWriteDataSize = lowBound/2.5;
    long dataFileSizeInMB = 65246;

    double molecule = (dataFileSizeInMB-lowBound)/compressWriteDataSize + 2;
    double denominator = 1-469.0/953.0;

    double r= (denominator/molecule);
    System.out.println("r="+1/r+", denominator/molecule=" +r);


    SimpleStrategy simpleStrategy = new SimpleStrategy();
    List<TsFileResource> list = new SimpleStrategyTest().generateTsResourceList();
    // PowerLaw powerLaw = new PowerLaw(2.7,0.7,2,30);
    // new PowerLaw(1,1,1,2)
    List<Integer> ans=simpleStrategy.strategy(new PowerLaw(2,0.8,60,3),
        87144*2,  (int)(87144*2*7),9533  , list,1);

//    List<Integer> ans=simpleStrategy.strategy(new PowerLaw(1,1,12,1),
//        1304352+161923,  1304352+161923,237457 , list,1);
    int sum =0;
    for(int i = 0;i<ans.size();i++){
      sum+=ans.get(i);
      System.out.println("i="+i+", elem="+ans.get(i)+", sum="+sum);
    }
    System.out.println("sum="+sum+", r="+1/r+", compressWriteDataSize="+compressWriteDataSize);
  }





  List<TsFileResource> generateTsResourceList(){
    List<TsFileResource> list = new ArrayList<>();
    for(int i = 0;i<2;i++){
      list.add(new FakeTsResource(new File("/users/suyue/input/ee.txt"), 87144, 87144));
    }
    return list;
  }


  class FakeTsResource extends TsFileResource{

    public FakeTsResource(File file, int actual, int virtual) {
      super(file);
      this.setActualPageNum(actual);
      this.setVirtualPageNum(virtual);
    }
  }
}
