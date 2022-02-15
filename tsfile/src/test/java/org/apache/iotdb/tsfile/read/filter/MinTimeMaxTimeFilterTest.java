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
package org.apache.iotdb.tsfile.read.filter;

import java.util.List;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.utils.Pair;
import org.junit.Assert;
import org.junit.Test;

public class MinTimeMaxTimeFilterTest {

  long minTime = 100;
  long maxTime = 200;

  @Test
  public void testEq() {
    Filter timeEq = TimeFilter.eq(10L);
    Assert.assertFalse(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));
    List<Pair<Long, Long>> intervalList;
    long left, right;
    intervalList = timeEq.findIntersection(minTime, maxTime);
    Assert.assertEquals(0, intervalList.size());

    timeEq = TimeFilter.eq(100L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(timeEq.containStartEndTime(minTime, minTime));

    timeEq = TimeFilter.eq(150L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));
    intervalList = timeEq.findIntersection(minTime, maxTime);
    Assert.assertEquals(1, intervalList.size());
    left = intervalList.get(0).left;
    right = intervalList.get(0).right;
    Assert.assertEquals(150L, left);
    Assert.assertEquals(150L, right);

    timeEq = TimeFilter.eq(200L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));
    intervalList = timeEq.findIntersection(minTime, maxTime);
    Assert.assertEquals(1, intervalList.size());
    left = intervalList.get(0).left;
    right = intervalList.get(0).right;
    Assert.assertEquals(200L, left);
    Assert.assertEquals(200L, right);

    timeEq = TimeFilter.eq(300L);
    Assert.assertFalse(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));
    intervalList = timeEq.findIntersection(minTime, maxTime);
    Assert.assertEquals(0, intervalList.size());

    Filter valueEq = ValueFilter.eq(100);
    Assert.assertTrue(valueEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(valueEq.satisfyStartEndTime(minTime, maxTime));
    intervalList = valueEq.findIntersection(minTime, maxTime);
    Assert.assertEquals(1, intervalList.size());
    left = intervalList.get(0).left;
    right = intervalList.get(0).right;
    Assert.assertEquals(minTime, left);
    Assert.assertEquals(maxTime, right);
  }

  @Test
  public void testGt() {
    List<Pair<Long, Long>> intervalList;
    long left, right;

    Filter timeEq = TimeFilter.gt(10L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(timeEq.containStartEndTime(minTime, maxTime));
    intervalList = timeEq.findIntersection(minTime, maxTime);
    Assert.assertEquals(1, intervalList.size());
    left = intervalList.get(0).left;
    right = intervalList.get(0).right;
    Assert.assertEquals(minTime, left);
    Assert.assertEquals(maxTime, right);

    timeEq = TimeFilter.gt(100L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));
    intervalList = timeEq.findIntersection(minTime, maxTime);
    Assert.assertEquals(1, intervalList.size());
    left = intervalList.get(0).left;
    right = intervalList.get(0).right;
    Assert.assertEquals(minTime+1, left);
    Assert.assertEquals(maxTime, right);

    timeEq = TimeFilter.gt(200L);
    Assert.assertFalse(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));
    intervalList = timeEq.findIntersection(minTime, maxTime);
    Assert.assertEquals(0, intervalList.size());

    timeEq = TimeFilter.gt(300L);
    Assert.assertFalse(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));
    intervalList = timeEq.findIntersection(minTime, maxTime);
    Assert.assertEquals(0, intervalList.size());

    Filter valueEq = ValueFilter.gt(100);
    Assert.assertTrue(valueEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(valueEq.containStartEndTime(minTime, maxTime));
    intervalList = valueEq.findIntersection(minTime, maxTime);
    Assert.assertEquals(1, intervalList.size());
    left = intervalList.get(0).left;
    right = intervalList.get(0).right;
    Assert.assertEquals(minTime, left);
    Assert.assertEquals(maxTime, right);
  }

  @Test
  public void testGtEq() {
    List<Pair<Long, Long>> intervalList;
    long left, right;

    Filter timeEq = TimeFilter.gtEq(10L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(timeEq.containStartEndTime(minTime, maxTime));
    intervalList = timeEq.findIntersection(minTime, maxTime);
    Assert.assertEquals(1, intervalList.size());
    left = intervalList.get(0).left;
    right = intervalList.get(0).right;
    Assert.assertEquals(minTime, left);
    Assert.assertEquals(maxTime, right);

    timeEq = TimeFilter.gtEq(100L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(timeEq.containStartEndTime(minTime, maxTime));
    intervalList = timeEq.findIntersection(minTime, maxTime);
    Assert.assertEquals(1, intervalList.size());
    left = intervalList.get(0).left;
    right = intervalList.get(0).right;
    Assert.assertEquals(minTime, left);
    Assert.assertEquals(maxTime, right);

    timeEq = TimeFilter.gtEq(200L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));
    intervalList = timeEq.findIntersection(minTime, maxTime);
    Assert.assertEquals(1, intervalList.size());
    left = intervalList.get(0).left;
    right = intervalList.get(0).right;
    Assert.assertEquals(maxTime, left);
    Assert.assertEquals(maxTime, right);

    timeEq = TimeFilter.gtEq(300L);
    Assert.assertFalse(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));
    intervalList = timeEq.findIntersection(minTime, maxTime);
    Assert.assertEquals(0, intervalList.size());


    Filter valueEq = ValueFilter.gtEq(100);
    Assert.assertTrue(valueEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(valueEq.containStartEndTime(minTime, maxTime));
    intervalList = valueEq.findIntersection(minTime, maxTime);
    Assert.assertEquals(1, intervalList.size());
    left = intervalList.get(0).left;
    right = intervalList.get(0).right;
    Assert.assertEquals(minTime, left);
    Assert.assertEquals(maxTime, right);


    valueEq = ValueFilter.gtEq(150);
    Assert.assertTrue(valueEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(valueEq.containStartEndTime(minTime, maxTime));
    intervalList = valueEq.findIntersection(minTime, maxTime);
    Assert.assertEquals(1, intervalList.size());
    left = intervalList.get(0).left;
    right = intervalList.get(0).right;
    Assert.assertEquals(minTime, left);
    Assert.assertEquals(maxTime, right);
  }

  @Test
  public void testLt() {
    Filter timeEq = TimeFilter.lt(10L);
    Assert.assertFalse(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));

    timeEq = TimeFilter.lt(100L);
    Assert.assertFalse(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));

    timeEq = TimeFilter.lt(200L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));

    timeEq = TimeFilter.lt(300L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(timeEq.containStartEndTime(minTime, maxTime));

    Filter valueEq = ValueFilter.lt(100);
    Assert.assertTrue(valueEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(valueEq.containStartEndTime(minTime, maxTime));
  }

  @Test
  public void testLtEq() {
    Filter timeEq = TimeFilter.ltEq(10L);
    Assert.assertFalse(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));

    timeEq = TimeFilter.ltEq(100L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));

    timeEq = TimeFilter.ltEq(200L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(timeEq.containStartEndTime(minTime, maxTime));

    timeEq = TimeFilter.ltEq(300L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(timeEq.containStartEndTime(minTime, maxTime));

    Filter valueEq = ValueFilter.ltEq(100);
    Assert.assertTrue(valueEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(valueEq.containStartEndTime(minTime, maxTime));
  }

  @Test
  public void testAnd() {
    List<Pair<Long, Long>> intervalList;
    long left, right;
    Filter andFilter = FilterFactory.and(TimeFilter.gt(10L), TimeFilter.lt(50));
    Assert.assertFalse(andFilter.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(andFilter.containStartEndTime(minTime, maxTime));
    intervalList = andFilter.findIntersection(minTime, maxTime);
    Assert.assertEquals(0, intervalList.size());


    andFilter = FilterFactory.and(TimeFilter.gt(100L), TimeFilter.lt(200));
    Assert.assertTrue(andFilter.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(andFilter.containStartEndTime(minTime, maxTime));
    intervalList = andFilter.findIntersection(minTime, maxTime);
    Assert.assertEquals(1, intervalList.size());
    left = intervalList.get(0).left;
    right = intervalList.get(0).right;
    Assert.assertEquals(minTime+1, left);
    Assert.assertEquals(maxTime-1, right);


    andFilter = FilterFactory.and(TimeFilter.gt(99L), TimeFilter.lt(201));
    Assert.assertTrue(andFilter.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(andFilter.containStartEndTime(minTime, maxTime));
    intervalList = andFilter.findIntersection(minTime, maxTime);
    Assert.assertEquals(1, intervalList.size());
    left = intervalList.get(0).left;
    right = intervalList.get(0).right;
    Assert.assertEquals(minTime, left);
    Assert.assertEquals(maxTime, right);

    andFilter = FilterFactory.and(TimeFilter.gt(150L), TimeFilter.lt(250L));
    Assert.assertTrue(andFilter.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(andFilter.containStartEndTime(minTime, maxTime));
    intervalList = andFilter.findIntersection(minTime, maxTime);
    Assert.assertEquals(1, intervalList.size());
    left = intervalList.get(0).left;
    right = intervalList.get(0).right;
    Assert.assertEquals(151L, left);
    Assert.assertEquals(maxTime, right);

    andFilter = FilterFactory.and(TimeFilter.gt(50L), TimeFilter.lt(150L));
    Assert.assertTrue(andFilter.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(andFilter.containStartEndTime(minTime, maxTime));
    intervalList = andFilter.findIntersection(minTime, maxTime);
    Assert.assertEquals(1, intervalList.size());
    left = intervalList.get(0).left;
    right = intervalList.get(0).right;
    Assert.assertEquals(minTime, left);
    Assert.assertEquals(149L, right);


    andFilter = FilterFactory.and(TimeFilter.gt(15606529943L), TimeFilter.lt(15606589943L));
    intervalList = andFilter.findIntersection(14010150001L, 18724800000L);
    Assert.assertEquals(1, intervalList.size());
    left = intervalList.get(0).left;
    right = intervalList.get(0).right;
    Assert.assertEquals(15606529944L, left);
    Assert.assertEquals(15606589942L, right);
  }

  @Test
  public void testOr() {
    Filter orFilter = FilterFactory.or(TimeFilter.gt(10L), TimeFilter.lt(50));
    Assert.assertTrue(orFilter.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(orFilter.containStartEndTime(minTime, maxTime));
}

  @Test
  public void testNotEq() {
    Filter timeEq = TimeFilter.notEq(10L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));

    long startTime = 10, endTime = 10;
    Assert.assertFalse(timeEq.satisfyStartEndTime(startTime, endTime));
    Assert.assertFalse(timeEq.containStartEndTime(5, 50));
  }

  @Test
  public void testNot() {
    Filter not = FilterFactory.not(TimeFilter.ltEq(10L));
    Assert.assertTrue(not.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(not.containStartEndTime(minTime, maxTime));

    not = FilterFactory.not(TimeFilter.ltEq(100L));
    Assert.assertFalse(not.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(not.containStartEndTime(minTime, maxTime));

    not = FilterFactory.not(TimeFilter.ltEq(200L));
    Assert.assertFalse(not.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(not.containStartEndTime(minTime, maxTime));

    not = FilterFactory.not(TimeFilter.ltEq(300L));
    Assert.assertFalse(not.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(not.containStartEndTime(minTime, maxTime));

    not = FilterFactory.not(ValueFilter.ltEq(100));
    Assert.assertFalse(not.satisfyStartEndTime(minTime, maxTime));

  }
}
