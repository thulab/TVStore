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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

/**
 * R of size S, R * 2^(p-1) of size S * 2^q, ..., R * k^(p-1) of size S * k^q, ...
 * Decay b(n) = O(n^(-q/(p+q)))
 */
public class PowerLaw implements IDelayFunction {

  private static Map<Integer, Integer> k2WindowCountMap = new ConcurrentHashMap<>();

  private static Map<Integer, Integer> k2WindowLenMap = new ConcurrentHashMap<>();

  private static int maxK = 0;

  private double p, q, r, s;

  public PowerLaw(double p, double q, double r, double s) {
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
        k2WindowLenMap.putIfAbsent(tmpK, (int) (s * Math.pow(tmpK, q)));
        k2WindowCountMap.putIfAbsent(tmpK, tmpCount);
        maxK = tmpK;
      }
      return k2WindowLenMap.get(tmpK);
    }
    return k2WindowLenMap.get(maxK);
  }

  public static void main(String[] args) {
    PowerLaw powerLaw = new PowerLaw(1.5, 0.5, 10, 4);
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

//  public static void main(String[] args) {
//    PowerLaw powerLaw = new PowerLaw(2,0.4,25,2.5);
//    //PowerLaw powerLaw = new PowerLaw(3,1,2,30);
//    long len = 0;
//    int cnt = 0;
//    int first = powerLaw.delayFun(1);
//
//    System.out.print("[");
//    for (int i = 1252127; i < 1252328; i++) {
//      len+=powerLaw.delayFun(i);
////      System.out.println("window "+i+", len = "+powerLaw.delayFun(i));
//   //   System.out.println(cnt+": "+ powerLaw.delayFun(i));
//      cnt++;
//      System.out.print(powerLaw.delayFun(i)+",");
//    }
//    System.out.print("]");
//
////    for(int i = 0;i<1000;i++){
////      len+=powerLaw.delayFun(i);
//////      System.out.println("window "+i+", len = "+powerLaw.delayFun(i));
////      System.out.println(cnt+": "+ powerLaw.delayFun(i));
////      cnt++;
////    }
//    System.out.println("first="+first);
//    System.out.println("window num = "+len+", ratio = "+(len*1.0/cnt));
//  }

//  public static void main(String[] args) {
//    PowerLaw powerLaw = new PowerLaw(2,0.4,25,2.5);
//    List<Integer> pageRatio=new ArrayList<>();
//    for(int i = 0;i<1000*5;i++){
//      pageRatio.add(1);
//    }
//
//    int[] targetNum={1000, 1000/5, 1000/5/5};
//
//    for(int i = 1; i<3;i++){
//      int target = targetNum[i-1];
//      List<Integer> pageMapping=powerLaw.strategy(powerLaw, pageRatio.size(), target, pageRatio);
//      pageRatio=powerLaw.strategy2(pageRatio, pageMapping, target);
//      System.out.println("cnt="+pageRatio.size());
//      System.out.println("window num = "+pageRatio.size()+", ratio = "+(1000*5*1.0/pageRatio.size()));
//    }
//  }

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


  public List<Integer> strategy2(List<Integer> pageRatio, List<Integer> pageMapping, int target) {

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
      if(diff<=0){
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
    System.out.print("]");
    System.out.println();
    return ans;
  }

  public List<Integer> strategy2(List<Integer> pageRatio, List<Integer> pageMapping, int target, int ttlNUm) {

    int diff = pageRatio.size() - target;
    List<Integer> ans = new ArrayList<>();
    int lastMax = 100000000;
    int j = 0;
    int pmIdx = 0;
    int lastx = 0;
    while (pmIdx< pageMapping.size()){
      int x = pageMapping.get(pmIdx++);
      int tmp=lastx;
      while(j<pageRatio.size() && tmp<x){
        diff--;
        tmp+=pageRatio.get(j);
        j++;
      }

//      while(tmp>lastMax){
//        diff++;
//        j--;
//        tmp-=pageRatio.get(j);
//      }
      diff++;
      lastx = Math.abs(tmp-x);
      ans.add(Math.min(tmp, x));

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


  public List<Integer> strategy(IDelayFunction delayFunction, int totalActualPageNum,
       int targetPageNum, List<Integer> pageRatio) {

//    Set<Integer> distinct = new HashSet<>();
    Map<Integer, Integer> key2num = new HashMap<>();

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
      int len = delayFunction.delayFun(up);
      sum += len;
      key2num.putIfAbsent(len, 0);
      key2num.put(len, key2num.get(len)+1);
    }

    while (up - low > targetPageNum) {
      up++;
      int len = delayFunction.delayFun(up);
      sum += len;
      key2num.putIfAbsent(len, 0);
      key2num.put(len, key2num.get(len)+1);
      while (sum > totalVirtualPageNum) {
        low++;
        len = delayFunction.delayFun(low);
        sum -= len;
        key2num.put(len, key2num.get(len)-1);
      }
    }

    List<Integer> distinctKey = new ArrayList<>(key2num.keySet());
    Collections.sort(distinctKey);
    List<Integer> distinctKeySum = new ArrayList<>();
    int keySum = 0;
    for(Integer key:distinctKey){
      keySum+=key;
      distinctKeySum.add(keySum);
    }


    int add = 0;
    int distinctKeySumId = distinctKeySum.size()-1;
    while(up-low+add+1<targetPageNum){
      int tmp = delayFunction.delayFun(up);
      while(distinctKeySum.get(distinctKeySumId)>tmp){
        distinctKeySumId--;
      }
      int addNum =  Math.round(tmp*1.0f/distinctKeySum.get(distinctKeySumId));
      for(int i = 0;i<=distinctKeySumId;i++){
        int preKey = distinctKey.get(distinctKeySumId);
        key2num.put(distinctKey.get(preKey), key2num.get(preKey)+addNum);
        add+=addNum;
      }
      key2num.put(tmp, key2num.get(tmp)-1);
      up--;
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

    for(int key : distinctKey){
      for(int i = 0;i < key2num.get(key);i++){
        ans.add(key);
      }
    }
    Collections.reverse(ans);

//    for (int i = up; i > low; i--) {
//      ans.add(delayFunction.delayFun(i));
//    }

    System.out.print("delayFunction:[");
    for (int i = 0; i < ans.size(); i++) {
      System.out.print(ans.get(i)+",");
    }
    System.out.print("]");
    System.out.println();
    System.out.println("low="+low+", up="+up+", targetPageNum="+targetPageNum+", afterCompressedActualPageNum="+(up-low+leftActualPageNum));
    return ans;
  }


  void handle (List<Integer> ans, int targetPageNum, Set<Integer> distinct){
    int distinctSum=0;
    for(int x:distinct){
      distinctSum+=x;
    }

//    int disE = distinct.size();
//    int disN = 0;
//    while ((up - low)+ disE*disN< targetPageNum){
//      int tmp = delayFunction.delayFun(up);
//      disN+=tmp/distinctSum;
//      up--;
//    }
  }
}
