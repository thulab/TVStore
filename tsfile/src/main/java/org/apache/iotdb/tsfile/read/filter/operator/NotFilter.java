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
package org.apache.iotdb.tsfile.read.filter.operator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.iotdb.tsfile.read.filter.DigestForFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.BinaryIndexedTree;
import org.apache.iotdb.tsfile.utils.Pair;

/**
 * NotFilter necessary. Use InvertExpressionVisitor
 */
public class NotFilter implements Filter, Serializable {

  private static final long serialVersionUID = 584860326604020881L;
  private Filter that;

  public NotFilter(Filter that) {
    this.that = that;
  }

  @Override
  public boolean satisfy(DigestForFilter digest) {
    return !that.satisfy(digest);
  }

  @Override
  public boolean satisfy(long time, Object value) {
    return !that.satisfy(time, value);
  }

  /**
   * Notice that, if the not filter only contains value filter, this method may return false, this
   * may cause misunderstanding.
   */
  @Override
  public boolean satisfyStartEndTime(long startTime, long endTime) {
    return !that.satisfyStartEndTime(startTime, endTime);
  }

  @Override
  public boolean containStartEndTime(long startTime, long endTime) {
    return !that.satisfyStartEndTime(startTime, endTime);
  }

  @Override
  public List<Pair<Long, Long>> findIntersection(long startTime, long endTime) {
    List<Pair<Long, Long>> ans = new ArrayList<>();
    long base = startTime-1;
    BinaryIndexedTree binaryIndexedTree = new BinaryIndexedTree((int)(endTime-base)+2);
    PriorityQueue<Integer> heap = new PriorityQueue<>();
    for(Pair<Long, Long> pair : that.findIntersection(startTime, endTime)){
      binaryIndexedTree.put((int)(pair.left-base), 1);
      binaryIndexedTree.put((int)(pair.right-base)+1, -1);
      heap.add((int)(pair.left-base));
      heap.add((int)(pair.right-base));
    }
    heap.add((int)(startTime-base));
    heap.add((int)(endTime-base));


    while(!heap.isEmpty()){
      long left = -1, right = -1;
      while(!heap.isEmpty()){
        int cur = heap.remove();
        while (!heap.isEmpty()&&heap.peek().intValue()==cur){
          heap.remove();
        }
        if(binaryIndexedTree.sum(cur) == 0){
          left = cur;
          break;
        }
      }
      if(left<0){
        continue;
      }

      while(!heap.isEmpty()){
        int cur = heap.remove();
        while (!heap.isEmpty()&&heap.peek().intValue()==cur){
          heap.remove();
        }
        if(binaryIndexedTree.sum(cur) == 0 && binaryIndexedTree.sum(cur+1)>0){
          right = cur;
          break;
        }
      }

      if(right<left){
        ans.add(new Pair<>(left+base, left+base));
      }
      else{
        ans.add(new Pair<>(left+base, right+base));
      }

    }

    return ans;
  }

  @Override
  public Filter clone() {
    return new NotFilter(that.clone());
  }

  public Filter getFilter() {
    return this.that;
  }

  @Override
  public String toString() {
    return "NotFilter: " + that;
  }

}
