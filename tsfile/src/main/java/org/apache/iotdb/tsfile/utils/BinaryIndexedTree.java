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
package org.apache.iotdb.tsfile.utils;

public class BinaryIndexedTree {
  public int length;
  private int[] tree;
  /**
   * 为了统一下标，所以tree[0]不被使用，数组有效范围1~length。
   * */
  public BinaryIndexedTree(int length){
    this.length=length;
    tree=new int[length+1];
  }
  /**
   * 计算1~index范围内和
   * index一直减去lowBit(index)，直到index为0
   * */
  public int sum(int index){
    if (index<1&&index>length) {
      throw new IllegalArgumentException("Out of Range!");
    }
    int sum=0;
    while (index>0) {
      sum+=tree[index];
      index-=lowBit(index);
    }
    return sum;
  }
  /**
   * 计算start~end范围内和
   * */
  public int  sum(int start,int end) {
    return sum(end)-sum(start-1);
  }
  /**
   * index一直加上lowBit(index)，直到index为length。这些位置的值都加上value
   * */
  public void put(int index,int value){
    if (index<1&&index>length) {
      throw new IllegalArgumentException("Out of Range!");
    }
    while (index<=length) {
      tree[index]+=value;
      index+=lowBit(index);
    }
  }
  /**
   * index一直减去lowBit(index)，直到index为length。这些位置的值都加上value
   * */
  public int get(int index){
    if (index<1&&index>length) {
      throw new IllegalArgumentException("Out of Range!");
    }
    int sum=tree[index];
    int z=index-lowBit(index);
    index--;
    while (index!=z) {
      sum-=tree[index];
      index-=lowBit(index);
    }
    return sum;
  }
  /**
   * 保留k的二进制最低位1的值。例如，1110保留最低位1即0010.
   * */
  private static int lowBit(int k){
    return k&-k;
  }
}
