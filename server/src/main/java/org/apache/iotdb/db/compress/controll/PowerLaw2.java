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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * R of size S, R * 2^(p-1) of size S * 2^q, ..., R * k^(p-1) of size S * k^q, ...
 * Decay b(n) = O(n^(-q/(p+q)))
 */
public class PowerLaw2 implements IDelayFunction {

  private static Map<Integer, Integer> k2WindowCountMap = new ConcurrentHashMap<>();

  private static Map<Integer, Integer> k2WindowLenMap = new ConcurrentHashMap<>();

  private static int maxK = 0;

  private double p, q, r, s;

  public PowerLaw2(double p, double q, double r, double s) {
    k2WindowCountMap.putIfAbsent(0, 0);
    k2WindowLenMap.putIfAbsent(0, 0);
    this.p = p;
    this.q = q;
    this.r = r;
    this.s = s;
  }

  @Override
  public int delayFun(int x) {
    int maxWindowCount = k2WindowCountMap.get(maxK);
    if (maxWindowCount > x) {
      // 在map中找,第一个长度大于等于x的k
      for (int i = 0; i < maxK; i++) {
        long len = k2WindowCountMap.get(i);
        if (len >= x) {
          return k2WindowLenMap.get(i);
        }
      }
    } else {
      int tmpK = maxK;
      int tmpCount = maxWindowCount;
      while (tmpCount < x) {
        tmpK++;
        tmpCount += r * Math.pow(tmpK, p - 1);
        //System.out.println("k = "+ tmpK+"; count = "+tmpCount);
        k2WindowLenMap.putIfAbsent(tmpK, (int) (s * Math.pow(q, tmpK)));
        k2WindowCountMap.putIfAbsent(tmpK, tmpCount);
        maxK = tmpK;
      }
      return k2WindowLenMap.get(tmpK);
    }
    return k2WindowLenMap.get(maxK);
  }

  public static Map<Integer, Integer> getK2WindowCountMap() {
    return k2WindowCountMap;
  }

  public static Map<Integer, Integer> getK2WindowLenMap() {
    return k2WindowLenMap;
  }

  public static int getMaxK() {
    return maxK;
  }

  public static int calSum(int k, double p, double q, double r, double s){
    PowerLaw2 powerLaw2 = new PowerLaw2(p, q, r, s);
    int sum = 0;
    for(int i = 0; i<k;i++){
      sum += powerLaw2.delayFun(k);
    }
    return sum;
  }

  public static void main(String[] args) {
    PowerLaw2 powerLaw = new PowerLaw2(3, 2, 15, 1);
    //PowerLaw powerLaw = new PowerLaw(3,1,2,30);
    long len = 0;
    int cnt = 0;
    int first = powerLaw.delayFun(1);
    for(int i = 1;i<18288/9.989294063667549;i++){
      int tmplen = powerLaw.delayFun(i);
      if(tmplen%2!=0){
        Random random = new Random(tmplen);
        tmplen+= random.nextDouble()>0.5?1:0;
      }

      len+=tmplen/2;
//      System.out.println("window "+i+", len = "+powerLaw.delayFun(i));
      System.out.print(powerLaw.delayFun(i)+",");
      cnt++;
    }
    System.out.println();
    System.out.println("first="+first);
    System.out.println("window num = "+len+", ratio = "+(len*1.0/cnt));
  }



//  public List<Integer> strategy2(List<Integer> pageRatio, List<Integer> pageMapping, int target) {
//
//    Collections.reverse(pageRatio);
//    Collections.reverse(pageMapping);
//
//    int diff = pageRatio.size() - target;
//    List<Integer> ans = new ArrayList<>();
//   // int lastMin = 0;
//    int j = 0;
//    for(int x : pageMapping){
//      int tmp=0;
//      while(j<pageRatio.size() && tmp<x){
//        diff--;
//        tmp+=pageRatio.get(j);
//        j++;
//      }
//
////      while(tmp>lastMax){
////        diff++;
////        j--;
////        tmp-=pageRatio.get(j);
////      }
//      diff++;
//      ans.add(tmp);
////      lastMax=lastMax<tmp?lastMax:tmp;
////      if(diff<=0){
////        break;
////      }
//    }
//    while(j<pageRatio.size()){
//      ans.add(pageRatio.get(j));
//      j++;
//    }
//
//    System.out.print("[");
//    for (int i = 0; i < ans.size(); i++) {
//      System.out.print(ans.get(i)+",");
//    }
//    System.out.print("]");
//    System.out.println();
//    return ans;
//  }


//  public List<Integer> strategy2(List<Integer> pageRatio, List<Integer> pageMapping, int target) {
//
//    int diff = pageRatio.size() - target;
//    List<Integer> ans = new ArrayList<>();
//    int lastMax = 100000000;
//    int j = 0;
//    int pmIdx = 0;
//    int lastPage = 0;
//    while (pmIdx< pageMapping.size()){
//      int x = pageMapping.get(pmIdx++);
//      int tmp=lastPage;
//      while(j<pageRatio.size() && tmp<x){
//        diff--;
//        tmp+=pageRatio.get(j);
//        j++;
//      }
//
////      while(tmp>lastMax){
////        diff++;
////        j--;
////        tmp-=pageRatio.get(j);
////      }
//      diff++;
//      ans.add(Math.min(x, tmp));
//      lastMax=lastMax<tmp?lastMax:tmp;
//
//      lastPage = tmp - x;
//
//      if(diff<=0){
//        break;
//      }
//    }
//    while(j<pageRatio.size()){
//      ans.add(pageRatio.get(j));
//      j++;
//    }
//
//    System.out.print("actual Ratio: [");
//    for (int i = 0; i < ans.size(); i++) {
//      System.out.print(ans.get(i)+",");
//    }
//    System.out.print("]");
//    System.out.println();
//    return ans;
//  }


  public List<Integer> strategy(IDelayFunction delayFunction, int totalActualPageNum,
       int targetPageNum, List<Integer> pageRatio) {

    Set<Integer> distinct = new HashSet<>();

    int totalVirtualPageNum=0;
    for(int r:pageRatio){
      totalVirtualPageNum+=r;
    }


    // 这次压缩过程中，衰减窗口已经覆盖的虚拟page个数
    long sum = 0;
    // 衰减函数的当前参数，截取段的上界
    int up = 0;
    // 衰减函数的当前参数，截取段的下界
    int low = 0;
    // 这次压缩过程中尚待压缩的实际page个数
    double leftActualPageNum = 0;
//    // 这次压缩过程中被压缩的实际page已经覆盖的原始page的个数
//    double currVirtualPageNum = 0;
    int idx = 0;

    while (sum < totalVirtualPageNum) {
      up++;
      sum += delayFunction.delayFun(up);
      distinct.add(delayFunction.delayFun(up));
    }

    while (up - low > targetPageNum) {
      up++;
      sum += delayFunction.delayFun(up);
      while (sum > totalVirtualPageNum) {
        low++;
        sum -= delayFunction.delayFun(low);
      }
    }

//    int distinctSum=0;
//    for(int x:distinct){
//      distinctSum+=x;
//    }
//
//    int disE = distinct.size();
//    int disN = 0;
//    while ((up - low)+ disE*disN< targetPageNum){
//      int tmp = delayFunction.delayFun(up);
//      disN+=tmp/distinctSum;
//      up--;
//    }

    //System.out.println("targetPageNum="+targetPageNum +", leftPageNum="+leftActualPageNum+ ", expected="+(leftActualPageNum+(up-low)));

    //
    List<Integer> ans = new ArrayList<>();
    for (int i = up; i > low; i--) {
      ans.add(delayFunction.delayFun(i));
    }

    System.out.print("delayFunction:[");
    for (int i = 0; i < ans.size(); i++) {
      System.out.print(ans.get(i)+",");
    }
    System.out.print("]");
    System.out.println();
    System.out.println("low="+low+", up="+up+", targetPageNum="+targetPageNum+", afterCompressedActualPageNum="+(up-low+leftActualPageNum));
    return ans;
  }
  public List<Integer> strategy2(List<Integer> pageRatio, List<Integer> pageMapping, int target, int ttlNUm) {

    int diff = pageRatio.size() - target;
    List<Integer> ans = new ArrayList<>();
    int lastMax = 100000000;
    int j = 0;
    int pmIdx = 0;
    int pmDiff = 0;
    int lastx=pageMapping.get(pmIdx);
    while (pmIdx< pageMapping.size()){
      int x = pageMapping.get(pmIdx++);
      int tmp=0;
      while(j<pageRatio.size() && tmp<x){
        diff--;
        tmp+=pageRatio.get(j);
        j++;
      }

      while(tmp>lastMax){
        diff++;
        j--;
        tmp-=pageRatio.get(j);
      }
      diff++;
      ans.add(tmp);

//      pmDiff+=(x-tmp);
//      if(pmDiff>=x){
//        pmDiff-=x;
//        System.out.println("pmIdx="+pmIdx+", x="+x);
//        pmIdx--;
//      }
      lastMax=lastMax<tmp?lastMax:tmp;
      if(diff<0){
        break;
      }
    }
    while(j<pageRatio.size()){
      ans.add(pageRatio.get(j));
      j++;
    }

    System.out.print("actual Ratio: [");
    for (int i = 0; i < ans.size(); i++) {
      System.out.print(ans.get(i)+",");
    }

    for(int i = 0;i<ttlNUm;i++){
      System.out.print("1,");
    }
    System.out.print("]");
    System.out.println();
    return ans;
  }
}
