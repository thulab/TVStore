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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import org.apache.iotdb.tsfile.read.filter.DigestForFilter;
import org.apache.iotdb.tsfile.read.filter.basic.BinaryFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.BinaryIndexedTree;
import org.apache.iotdb.tsfile.utils.Pair;

/**
 * Either of the left and right operators of AndExpression must satisfy the condition.
 */
public class OrFilter extends BinaryFilter implements Serializable {

  private static final long serialVersionUID = -968055896528472694L;

  public OrFilter(Filter left, Filter right) {
    super(left, right);
  }

  @Override
  public String toString() {
    return "(" + left + " || " + right + ")";
  }

  @Override
  public Filter clone() {
    return new OrFilter(left.clone(), right.clone());
  }

  @Override
  public boolean satisfy(DigestForFilter digest) {
    return left.satisfy(digest) || right.satisfy(digest);
  }

  @Override
  public boolean satisfy(long time, Object value) {
    return left.satisfy(time, value) || right.satisfy(time, value);
  }

  @Override
  public boolean satisfyStartEndTime(long startTime, long endTime) {
    return left.satisfyStartEndTime(startTime, endTime) || right
        .satisfyStartEndTime(startTime, endTime);
  }

  @Override
  public boolean containStartEndTime(long startTime, long endTime) {
    return left.containStartEndTime(startTime, endTime) || right
        .containStartEndTime(startTime, endTime);
  }

//  @Override
//  public List<Pair<Long, Long>> findIntersection(long startTime, long endTime) {
//    List<Pair<Long, Long>> ans = new ArrayList<>();
//    List<Pair<Long, Long>> liftAns = left.findIntersection(startTime, endTime);
//    List<Pair<Long, Long>> rightAns = right.findIntersection(startTime, endTime);
//    long base = startTime-1;
//    BinaryIndexedTree binaryIndexedTree = new BinaryIndexedTree((int)(endTime-base)+2);
//    PriorityQueue<Integer> heap = new PriorityQueue<>();
//    for(Pair<Long, Long> pair : liftAns){
//      binaryIndexedTree.put((int)(pair.left-base), 1);
//      binaryIndexedTree.put((int)(pair.right-base)+1, -1);
//      heap.add((int)(pair.left-base));
//      heap.add((int)(pair.right-base));
//    }
//    for(Pair<Long, Long> pair : rightAns){
//      binaryIndexedTree.put((int)(pair.left-base), 1);
//      binaryIndexedTree.put((int)(pair.right-base)+1, -1);
//      heap.add((int)(pair.left-base));
//      heap.add((int)(pair.right-base));
//    }
//
//
//    while(!heap.isEmpty()){
//      long left = -1, right = -1;
//      while(!heap.isEmpty()){
//        int cur = heap.remove();
//        while (!heap.isEmpty()&&heap.peek().intValue()==cur){
//          heap.remove();
//        }
//        if(binaryIndexedTree.sum(cur) == 1){
//          left = cur;
//          break;
//        }
//      }
//      if(left<0){
//        continue;
//      }
//
//      while(!heap.isEmpty()){
//        int cur = heap.remove();
//        while (!heap.isEmpty()&&heap.peek().intValue()==cur){
//          heap.remove();
//        }
//        if(binaryIndexedTree.sum(cur) == 1 && binaryIndexedTree.sum(cur+1)<1){
//          right = cur;
//          break;
//        }
//      }
//
//      if(right<left){
//        ans.add(new Pair<>(left+base, left+base));
//      }
//      else{
//        ans.add(new Pair<>(left+base, right+base));
//      }
//
//    }
//
//    return ans;
//  }

  @Override
  public List<Pair<Long, Long>> findIntersection(long startTime, long endTime) {
    List<Pair<Long, Long>> ans = new ArrayList<>();
    List<Pair<Long, Long>> liftAns = left.findIntersection(startTime, endTime);
    List<Pair<Long, Long>> rightAns = right.findIntersection(startTime, endTime);


    Set<Long> numElem = new HashSet<>();
    for(Pair<Long, Long> pair : liftAns){
      numElem.add(pair.left);
      numElem.add(pair.right);
      numElem.add(pair.right+1);
    }

    for(Pair<Long, Long> pair : rightAns){
      numElem.add(pair.left);
      numElem.add(pair.right);
      numElem.add(pair.right+1);
    }
    numElem.add(startTime);
    numElem.add(endTime);
    numElem.add(endTime+1);
    List<Long> elemList = new ArrayList<>(numElem);
    Collections.sort(elemList);
    Map<Long, Integer> numIdxMap = new HashMap<>();
    for(int i = 0;i<elemList.size();i++){
      numIdxMap.put(elemList.get(i), i+1);
    }
    BinaryIndexedTree binaryIndexedTree = new BinaryIndexedTree(numIdxMap.size()+2);
    PriorityQueue<Integer> heap = new PriorityQueue<>();
    for(Pair<Long, Long> pair : liftAns){
      binaryIndexedTree.put(numIdxMap.get(pair.left), 1);
      binaryIndexedTree.put(numIdxMap.get(pair.right+1), -1);
      heap.add(numIdxMap.get(pair.left));
      heap.add(numIdxMap.get(pair.right));
    }
    for(Pair<Long, Long> pair : rightAns){
      binaryIndexedTree.put(numIdxMap.get(pair.left), 1);
      binaryIndexedTree.put(numIdxMap.get(pair.right+1), -1);
      heap.add(numIdxMap.get(pair.left));
      heap.add(numIdxMap.get(pair.right));
    }


    while(!heap.isEmpty()){
      long left = -1, right = -1;
      while(!heap.isEmpty()){
        int cur = heap.remove();
        while (!heap.isEmpty()&&heap.peek().intValue()==cur){
          heap.remove();
        }
        if(binaryIndexedTree.sum(cur) >= 1){
          left = elemList.get(cur-1);
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
        if(binaryIndexedTree.sum(cur) >= 1 && binaryIndexedTree.sum(cur+1)<1){
          right = elemList.get(cur-1);
          break;
        }
      }

      if(right<left){
        ans.add(new Pair<>(left, left));
      }
      else{
        ans.add(new Pair<>(left, right));
      }

    }

    return ans;
  }


}
