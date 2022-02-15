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

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.tsfile.read.filter.DigestForFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.basic.UnaryFilter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterType;
import org.apache.iotdb.tsfile.utils.Pair;

/**
 * less than.
 *
 * @param <T> comparable data type
 */
public class Lt<T extends Comparable<T>> extends UnaryFilter<T> {

  private static final long serialVersionUID = -2088181659871608986L;

  public Lt(T value, FilterType filterType) {
    super(value, filterType);
  }

  @Override
  public boolean satisfy(DigestForFilter digest) {
    if (filterType == FilterType.TIME_FILTER) {
      return ((Long) value) > digest.getMinTime();
    } else {
      if (digest.isMinValueNull()) {
        return true;
      }
      return value.compareTo(digest.getMinValue()) > 0;
    }
  }

  @Override
  public boolean satisfy(long time, Object value) {
    Object v = filterType == FilterType.TIME_FILTER ? time : value;
    return this.value.compareTo((T) v) > 0;
  }

  @Override
  public boolean satisfyStartEndTime(long startTime, long endTime) {
    if (filterType == FilterType.TIME_FILTER) {
      long time = (Long) value;
      if (time <= startTime) {
        return false;
      }
      return true;
    } else {
      return true;
    }
  }

  @Override
  public boolean containStartEndTime(long startTime, long endTime) {
    if (filterType == FilterType.TIME_FILTER) {
      long time = (Long) value;
      if (endTime < time) {
        return true;
      } else {
        return false;
      }
    } else {
      return true;
    }
  }

  @Override
  public List<Pair<Long, Long>> findIntersection(long startTime, long endTime) {
    List<Pair<Long, Long>> ans = new ArrayList<>();
    if (filterType == FilterType.TIME_FILTER) {
      long time = (Long) value;
      if (startTime < time) {
        ans.add(new Pair<>(startTime, Math.min(endTime, time-1)));
      }
    } else {
      ans.add(new Pair<>(startTime, endTime));
    }
    return ans;
  }

  @Override
  public Filter clone() {
    return new Lt(value, filterType);
  }

  @Override
  public String toString() {
    return getFilterType() + " < " + value;
  }
}
