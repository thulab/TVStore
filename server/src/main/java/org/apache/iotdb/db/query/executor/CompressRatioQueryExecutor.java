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
package org.apache.iotdb.db.query.executor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.path.PathException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.CompressRatioDataSet;
import org.apache.iotdb.db.query.dataset.EngineDataSetWithValueFilter;
import org.apache.iotdb.db.query.dataset.EngineDataSetWithoutValueFilter;
import org.apache.iotdb.db.query.dataset.ListDataSet;
import org.apache.iotdb.db.query.reader.IAggregateReader;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.resourceRelated.SeqResourceIterateReader;
import org.apache.iotdb.db.query.reader.seriesRelated.SeriesReaderByTimestamp;
import org.apache.iotdb.db.query.reader.seriesRelated.SeriesReaderWithoutValueFilter;
import org.apache.iotdb.db.query.timegenerator.EngineTimeGenerator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

/**
 * IoTDB query executor.
 */
public class CompressRatioQueryExecutor {

  private QueryExpression queryExpression;

  public CompressRatioQueryExecutor(QueryExpression queryExpression) {
    this.queryExpression = queryExpression;
  }

  /**
   * without filter or with global time filter.
   */
  public QueryDataSet executeWithoutValueFilter(QueryContext context)
      throws StorageEngineException, IOException {

    Filter timeFilter = null;
    if (queryExpression.hasQueryFilter()) {
      timeFilter = ((GlobalTimeExpression) queryExpression.getExpression()).getFilter();
    }

    List<IAggregateReader> readersOfSequenceData = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();
    for (Path path : queryExpression.getSelectedSeries()) {

      // add data type
      dataTypes.add(TSDataType.INT32);

      QueryDataSource queryDataSource = QueryResourceManager.getInstance()
          .getQueryDataSource(path, context, timeFilter);
      // add additional time filter if TTL is set
      timeFilter = queryDataSource.updateTimeFilter(timeFilter);

      IAggregateReader reader = new SeqResourceIterateReader(queryDataSource.getSeriesPath(),
          queryDataSource.getSeqResources(), timeFilter, context, false);
      readersOfSequenceData.add(reader);
    }

    return new CompressRatioDataSet(queryExpression.getSelectedSeries(), dataTypes, readersOfSequenceData);
  }

  /**
   * executeWithValueFilter query.
   *
   * @return QueryDataSet object
   * @throws StorageEngineException StorageEngineException
   */
  public QueryDataSet executeWithValueFilter(QueryContext context) throws StorageEngineException, IOException {
    return executeWithoutValueFilter(context);
  }

}
