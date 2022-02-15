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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.path.PathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.query.aggregation.AggreResultData;
import org.apache.iotdb.db.query.aggregation.AggregateFunction;
import org.apache.iotdb.db.query.aggregation.impl.AvgAggrFunc;
import org.apache.iotdb.db.query.aggregation.impl.CountAggrFunc;
import org.apache.iotdb.db.query.aggregation.impl.LastAggrFunc;
import org.apache.iotdb.db.query.aggregation.impl.MaxTimeAggrFunc;
import org.apache.iotdb.db.query.aggregation.impl.MaxValueAggrFunc;
import org.apache.iotdb.db.query.aggregation.impl.SumAggrFunc;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.AggreResultDataPointReader;
import org.apache.iotdb.db.query.dataset.EngineDataSetWithoutValueFilter;
import org.apache.iotdb.db.query.factory.AggreFuncFactory;
import org.apache.iotdb.db.query.reader.IAggregateReader;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.resourceRelated.SeqResourceIterateReader;
import org.apache.iotdb.db.query.reader.resourceRelated.UnseqResourceMergeReader;
import org.apache.iotdb.db.query.reader.seriesRelated.SeriesReaderByTimestamp;
import org.apache.iotdb.db.query.timegenerator.EngineTimeGenerator;
import org.apache.iotdb.tsfile.common.constant.StatisticConstant;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AggregateEngineExecutor {

  private List<Path> selectedSeries;
  private List<String> aggres;
  private IExpression expression;
  private String estimationMethod = IoTDBDescriptor.getInstance().getConfig().getWindowEstimationMethod();
  private double sigmo_in_compress_point_exist_judge = IoTDBDescriptor.getInstance().getConfig().getSigmoInCompressPointExistJudge();
  private static final Logger logger = LoggerFactory.getLogger(AggregateEngineExecutor.class);
  private final String POINT_RATIO = "point_ratio";
  private final String TIME_RATIO = "time_ratio";
  private boolean isJudgeExists = false;

  /**
   * aggregation batch calculation size.
   **/
  private int aggregateFetchSize;

  /**
   * constructor.
   */
  public AggregateEngineExecutor(List<Path> selectedSeries, List<String> aggres,
      IExpression expression) {
    this.selectedSeries = selectedSeries;
    this.aggres = aggres;
    this.expression = expression;
    this.aggregateFetchSize = 10 * IoTDBDescriptor.getInstance().getConfig().getFetchSize();
    if(!(estimationMethod.equals(POINT_RATIO)||estimationMethod.equals(TIME_RATIO))){
      logger.warn("Estimation Method {} isn't exists, using default {}.", estimationMethod, POINT_RATIO);
      estimationMethod = POINT_RATIO;
    }
    logger.info("Estimation Method is {}.", estimationMethod);
  }

  /**
   * execute aggregate function with only time filter or no filter.
   *
   * @param context query context
   */
  public QueryDataSet executeWithoutValueFilter(QueryContext context)
      throws StorageEngineException, IOException, QueryProcessException {
    Filter timeFilter = null;
    if (expression != null) {
      timeFilter = ((GlobalTimeExpression) expression).getFilter();
    }

    List<IAggregateReader> readersOfSequenceData = new ArrayList<>();
    List<IPointReader> readersOfUnSequenceData = new ArrayList<>();
    List<AggregateFunction> aggregateFunctions = new ArrayList<>();
    for (int i = 0; i < selectedSeries.size(); i++) {
      // construct AggregateFunction
      TSDataType tsDataType = MManager.getInstance()
          .getSeriesType(selectedSeries.get(i).getFullPath());
      AggregateFunction function = AggreFuncFactory.getAggrFuncByName(aggres.get(i), tsDataType);
      function.init();
      aggregateFunctions.add(function);

      QueryDataSource queryDataSource = QueryResourceManager.getInstance()
          .getQueryDataSource(selectedSeries.get(i), context, timeFilter);
      // add additional time filter if TTL is set
      timeFilter = queryDataSource.updateTimeFilter(timeFilter);

      // sequence reader for sealed tsfile, unsealed tsfile, memory
      IAggregateReader seqResourceIterateReader;
      if (function instanceof MaxTimeAggrFunc || function instanceof LastAggrFunc) {
        seqResourceIterateReader = new SeqResourceIterateReader(queryDataSource.getSeriesPath(),
            queryDataSource.getSeqResources(), timeFilter, context, true);
      } else {
        seqResourceIterateReader = new SeqResourceIterateReader(queryDataSource.getSeriesPath(),
            queryDataSource.getSeqResources(), timeFilter, context, false);
      }

      // unseq reader for all chunk groups in unSeqFile, memory
      IPointReader unseqResourceMergeReader = new UnseqResourceMergeReader(
          queryDataSource.getSeriesPath(),
          queryDataSource.getUnseqResources(), context, timeFilter);

      readersOfSequenceData.add(seqResourceIterateReader);
      readersOfUnSequenceData.add(unseqResourceMergeReader);
    }
    List<AggreResultData> aggreResultDataList = new ArrayList<>();
    //TODO use multi-thread
    for (int i = 0; i < selectedSeries.size(); i++) {
      AggreResultData aggreResultData = aggregateWithoutValueFilter(aggregateFunctions.get(i),
          readersOfSequenceData.get(i), readersOfUnSequenceData.get(i), timeFilter);
      aggreResultDataList.add(aggreResultData);
    }
    return constructDataSet(aggreResultDataList);
  }

  private AggreResultData aggregateWithoutValueFilter2(AggregateFunction function,
      IAggregateReader sequenceReader, IPointReader unSequenceReader, Filter filter)
      throws IOException, QueryProcessException {
    if (function instanceof MaxTimeAggrFunc || function instanceof LastAggrFunc) {
      return handleLastMaxTimeWithOutTimeGenerator(function, sequenceReader, unSequenceReader,
          filter);
    }

    while (sequenceReader.hasNext()) {
      PageHeader pageHeader = sequenceReader.nextPageHeader();
      //System.out.println(pageHeader.getMinTimestamp()+","+pageHeader.getMaxTimestamp());
      // judge if overlap with unsequence data
      if (canUseHeader(function, pageHeader, unSequenceReader, filter)) {
        // cal by pageHeader
        function.calculateValueFromPageHeader(pageHeader);
        sequenceReader.skipPageData();
        logger.info("calculateValueFromPageHeader: function {}, page window rage [{},{}].",function.getFunName(),
            pageHeader.getMinTimestamp(), pageHeader.getMaxTimestamp());
      } else {
        String funcName = function.getFunName();

        if(pageHeader!=null && pageHeader.getCompressRatio()>1 && estimationMethod.equals(TIME_RATIO)&&
            (funcName.equals(StatisticConstant.SUM) || funcName.equals(StatisticConstant.COUNT) || funcName.equals(StatisticConstant.AVG)) ){
          logger.info("function {}, page window rage [{},{}].",function.getFunName(),
              pageHeader.getMinTimestamp(), pageHeader.getMaxTimestamp());

          //BatchData batchData = sequenceReader.nextBatch();
          sequenceReader.skipPageData();
          List<Pair<Long, Long>> intervalList = new ArrayList<>();
          if(filter!=null){
            intervalList.addAll(filter.findIntersection(pageHeader.getMinTimestamp(), pageHeader.getMaxTimestamp()));
          }
          long occupiedTime = 0l;
          for(Pair<Long, Long> pair : intervalList){
            occupiedTime = pair.right-pair.left+1;
          }
          if(occupiedTime==0){
            continue;
          }
          double ratio = 1.0*occupiedTime/(pageHeader.getMaxTimestamp()-pageHeader.getMinTimestamp());
          double avgInterval = (pageHeader.getMaxTimestamp()-pageHeader.getMinTimestamp())*1.0/ pageHeader.getNumOfValues() * pageHeader.getCompressRatio();

          logger.info("function {}, page window range [{},{}], occupiedTime length is {}, occupiedTime range [{}, {}], {}%,"
                  + "Page ratio:{}, count:{}, sum:{}, max:{}, min:{}, avg interval:{}",function.getFunName(),
              pageHeader.getMinTimestamp(), pageHeader.getMaxTimestamp(), occupiedTime, intervalList.get(0).left, intervalList.get(0).right,
              ratio*100,
              pageHeader.getCompressRatio(), pageHeader.getNumOfValues(), pageHeader.getStatistics().getSum(),
              pageHeader.getStatistics().getMax(), pageHeader.getStatistics().getMin(),
              avgInterval);

          if(avgInterval < occupiedTime && ratio>0.0025){
            function.calculateValueFromPageHeader(pageHeader, ratio);
          }
          while (unSequenceReader.hasNext() && unSequenceReader.current().getTimestamp()<=intervalList.get(intervalList.size()-1).right){
            unSequenceReader.next();
          }
        }
        else{
          // cal by pageData
          BatchData batchData = sequenceReader.nextBatch();
          if(batchData!=null){
            function.calculateValueFromPageData(pageHeader, batchData, unSequenceReader);
          }

          if(pageHeader!=null && logger.isInfoEnabled()){
            double avgInterval = (pageHeader.getMaxTimestamp()-pageHeader.getMinTimestamp())*1.0/ pageHeader.getNumOfValues() * pageHeader.getCompressRatio();
            logger.info("function {}, page window range [{},{}],"
                    + "Page ratio:{}, count:{}, sum:{}, max:{}, min:{}, avg interval:{}",
                function.getFunName(), pageHeader.getMinTimestamp(), pageHeader.getMaxTimestamp(),
                pageHeader.getCompressRatio(), pageHeader.getNumOfValues(), pageHeader.getStatistics().getSum(),
                pageHeader.getStatistics().getMax(), pageHeader.getStatistics().getMin(),
                avgInterval);
          }
        }

      }

      if (function.isCalculatedAggregationResult()) {
        return function.getResult();
      }
    }

    // cal with unsequence data
    if (unSequenceReader.hasNext()) {
      function.calculateValueFromUnsequenceReader(unSequenceReader);
    }
    return function.getResult();
  }

  /**
   * calculation aggregate result with only time filter or no filter for one series.
   *
   * @param function aggregate function
   * @param sequenceReader sequence data reader
   * @param unSequenceReader unsequence data reader
   * @param filter time filter or null
   * @return one series aggregate result data
   */
  private AggreResultData aggregateWithoutValueFilter(AggregateFunction function,
      IAggregateReader sequenceReader, IPointReader unSequenceReader, Filter filter)
      throws IOException, QueryProcessException {
    if (function instanceof MaxTimeAggrFunc || function instanceof LastAggrFunc) {
      return handleLastMaxTimeWithOutTimeGenerator(function, sequenceReader, unSequenceReader,
          filter);
    }

    while (sequenceReader.hasNext()) {
      PageHeader pageHeader = sequenceReader.nextPageHeader();
      //System.out.println(pageHeader.getMinTimestamp()+","+pageHeader.getMaxTimestamp());
      // judge if overlap with unsequence data
      if (canUseHeader(function, pageHeader, unSequenceReader, filter)) {
        // cal by pageHeader
        function.calculateValueFromPageHeader(pageHeader);
        sequenceReader.skipPageData();
      } else {
        // 1x
        if(pageHeader == null || pageHeader.getCompressRatio() <= 1){
          // cal by pageData
          function.calculateValueFromPageData(pageHeader, sequenceReader.nextBatch(), unSequenceReader);
        }
        else {
          // compress data query
          String funcName = function.getFunName();
          if(filter==null){
            // no filter
            function.calculateValueFromPageHeader(pageHeader, 1.0);
            sequenceReader.skipPageData();
            function.calculateValueFromUnsequenceReader(unSequenceReader, pageHeader.getMaxTimestamp()+1);
            continue;
          }

          if(estimationMethod.equals(TIME_RATIO)&&
              (funcName.equals(StatisticConstant.SUM) || funcName.equals(StatisticConstant.COUNT) || funcName.equals(StatisticConstant.AVG))){
            logger.info("function {}, page window rage [{},{}]",function.getFunName(),
                pageHeader.getMinTimestamp(), pageHeader.getMaxTimestamp());
            List<Pair<Long, Long>> intervalList = filter.findIntersection(pageHeader.getMinTimestamp(), pageHeader.getMaxTimestamp());
            if(intervalList.isEmpty()){
              sequenceReader.skipPageData();
              function.calculateValueFromUnsequenceReader(unSequenceReader, pageHeader.getMaxTimestamp()+1);
              continue;
            }
            long occupiedTime = 0l;
            for(Pair<Long, Long> pair : intervalList){
              occupiedTime = pair.right-pair.left+1;
            }
            double sigmo = pageHeader.calStandardDeviation();
            double mu = pageHeader.calMeanValue();
            double interval = sigmo_in_compress_point_exist_judge*sigmo;
            logger.info("function {}, page window range [{},{}], occupiedTime length is {}, occupiedTime range [{}, {}], {}%,"
                    + "Page ratio:{}, count:{}, sum:{}, max:{}, min:{}, sigmo:{}, mu:{}, avg interval:{}",function.getFunName(),
                pageHeader.getMinTimestamp(), pageHeader.getMaxTimestamp(), occupiedTime, intervalList.get(0).left, intervalList.get(0).right,
                (occupiedTime*1.0/(pageHeader.getMaxTimestamp()-pageHeader.getMinTimestamp()))*100,
                pageHeader.getCompressRatio(), pageHeader.getNumOfValues(), pageHeader.getStatistics().getSum(),
                pageHeader.getStatistics().getMax(), pageHeader.getStatistics().getMin(), sigmo, mu,
                (pageHeader.getMaxTimestamp()-pageHeader.getMinTimestamp())*1.0/ pageHeader.getNumOfValues() * pageHeader.getCompressRatio());

            boolean isExists = true;
            if(isJudgeExists){
              BatchData batchData = sequenceReader.nextBatch();
              if(batchData==null || !batchData.hasNext()){
                if(occupiedTime> (mu+interval)* pageHeader.getCompressRatio() || occupiedTime<(mu-interval)* pageHeader.getCompressRatio()){
                  // non
                  isExists = false;
                  logger.info("occupiedTime range [{}, {}] no point in page, interval range {},{}",
                      intervalList.get(0).left, intervalList.get(0).right, (mu-interval)* pageHeader.getCompressRatio(), (mu+interval)* pageHeader.getCompressRatio());
                }
              }
            }
            else {
              sequenceReader.skipPageData();
            }
            if(isExists){
              // have point
              function.calculateValueFromPageHeader(pageHeader, 1.0*occupiedTime/(pageHeader.getMaxTimestamp()-pageHeader.getMinTimestamp()));
              logger.info("occupiedTime range [{}, {}] have point in page, occupiedTime ratio:{},interval range {},{}",
                  intervalList.get(0).left, intervalList.get(0).right, 1.0*occupiedTime/(pageHeader.getMaxTimestamp()-pageHeader.getMinTimestamp()),
                  (mu-interval)* pageHeader.getCompressRatio(), (mu+interval)* pageHeader.getCompressRatio());
            }
            function.calculateValueFromUnsequenceReader(unSequenceReader, pageHeader.getMaxTimestamp()+1);

          }
          else {
            // cal by pageData
            BatchData batchData = sequenceReader.nextBatch();
            function.calculateValueFromPageData(pageHeader, batchData, unSequenceReader);
            function.calculateValueFromUnsequenceReader(unSequenceReader, pageHeader.getMaxTimestamp()+1);
          }

        }
      }

      if (function.isCalculatedAggregationResult()) {
        return function.getResult();
      }
    }

    // cal with unsequence data
    if (unSequenceReader.hasNext()) {
      function.calculateValueFromUnsequenceReader(unSequenceReader);
    }
    return function.getResult();
  }

  /**
   * determine whether pageHeader can be used to compute aggregation results.
   */
  private boolean canUseHeader(AggregateFunction function, PageHeader pageHeader,
      IPointReader unSequenceReader, Filter filter)
      throws IOException, QueryProcessException {
    // if page data is memory data.
    if (pageHeader == null) {
      return false;
    }

    long minTime = pageHeader.getMinTimestamp();
    long maxTime = pageHeader.getMaxTimestamp();

    // If there are points in the page that do not satisfy the time filter,
    // page header cannot be used to calculate.
    if (filter != null && !filter.containStartEndTime(minTime, maxTime)) {
      return false;
    }

    // cal unsequence data with timestamps between pages.
    function.calculateValueFromUnsequenceReader(unSequenceReader, minTime);

    return !(unSequenceReader.hasNext() && unSequenceReader.current().getTimestamp() <= maxTime);

  }

  /**
   * handle last and max_time aggregate function with only time filter or no filter.
   *
   * @param function aggregate function
   * @param sequenceReader sequence data reader
   * @param unSequenceReader unsequence data reader
   * @return BatchData-aggregate result
   */
  private AggreResultData handleLastMaxTimeWithOutTimeGenerator(AggregateFunction function,
      IAggregateReader sequenceReader, IPointReader unSequenceReader, Filter timeFilter)
      throws IOException, QueryProcessException {
    long lastBatchTimeStamp = Long.MIN_VALUE;
    boolean isChunkEnd = false;
    while (sequenceReader.hasNext()) {
      PageHeader pageHeader = sequenceReader.nextPageHeader();
      // judge if overlap with unsequence data
      if (canUseHeader(function, pageHeader, unSequenceReader, timeFilter)) {
        // cal by pageHeader
        function.calculateValueFromPageHeader(pageHeader);
        sequenceReader.skipPageData();

        if (lastBatchTimeStamp > pageHeader.getMinTimestamp()) {
          // the chunk is end.
          isChunkEnd = true;
        } else {
          // current page and last page are in the same chunk.
          lastBatchTimeStamp = pageHeader.getMinTimestamp();
        }
      } else {
        // cal by pageData
        BatchData batchData = sequenceReader.nextBatch();
        if (batchData.length() > 0) {
          if (lastBatchTimeStamp > batchData.currentTime()) {
            // the chunk is end.
            isChunkEnd = true;
          } else {
            // current page and last page are in the same chunk.
            lastBatchTimeStamp = batchData.currentTime();
          }
          function.calculateValueFromPageData(pageHeader, batchData, unSequenceReader);
        }
      }
      if (isChunkEnd) {
        break;
      }
    }

    // cal with unsequence data
    if (unSequenceReader.hasNext()) {
      function.calculateValueFromUnsequenceReader(unSequenceReader);
    }
    return function.getResult();
  }


  /**
   * execute aggregate function with value filter.
   *
   * @param context query context.
   */
  public QueryDataSet executeWithValueFilter(QueryContext context)
      throws StorageEngineException, PathException, IOException {

    EngineTimeGenerator timestampGenerator = new EngineTimeGenerator(expression, context);
    List<IReaderByTimestamp> readersOfSelectedSeries = new ArrayList<>();
    for (Path path : selectedSeries) {
      SeriesReaderByTimestamp seriesReaderByTimestamp = new SeriesReaderByTimestamp(path, context);
      readersOfSelectedSeries.add(seriesReaderByTimestamp);
    }

    List<AggregateFunction> aggregateFunctions = new ArrayList<>();
    for (int i = 0; i < selectedSeries.size(); i++) {
      TSDataType type = MManager.getInstance().getSeriesType(selectedSeries.get(i).getFullPath());
      AggregateFunction function = AggreFuncFactory.getAggrFuncByName(aggres.get(i), type);
      function.init();
      aggregateFunctions.add(function);
    }
    List<AggreResultData> batchDataList = aggregateWithValueFilter(aggregateFunctions,
        timestampGenerator,
        readersOfSelectedSeries);
    return constructDataSet(batchDataList);
  }

  /**
   * calculation aggregate result with value filter.
   */
  private List<AggreResultData> aggregateWithValueFilter(
      List<AggregateFunction> aggregateFunctions,
      EngineTimeGenerator timestampGenerator,
      List<IReaderByTimestamp> readersOfSelectedSeries)
      throws IOException {

    while (timestampGenerator.hasNext()) {

      // generate timestamps for aggregate
      long[] timeArray = new long[aggregateFetchSize];
      int timeArrayLength = 0;
      for (int cnt = 0; cnt < aggregateFetchSize; cnt++) {
        if (!timestampGenerator.hasNext()) {
          break;
        }
        timeArray[timeArrayLength++] = timestampGenerator.next();
      }

      // cal part of aggregate result
      for (int i = 0; i < readersOfSelectedSeries.size(); i++) {
        aggregateFunctions.get(i).calcAggregationUsingTimestamps(timeArray, timeArrayLength,
            readersOfSelectedSeries.get(i));
      }
    }

    List<AggreResultData> aggreResultDataArrayList = new ArrayList<>();
    for (AggregateFunction function : aggregateFunctions) {
      aggreResultDataArrayList.add(function.getResult());
    }
    return aggreResultDataArrayList;
  }

  /**
   * using aggregate result data list construct QueryDataSet.
   *
   * @param aggreResultDataList aggregate result data list
   */
  private QueryDataSet constructDataSet(List<AggreResultData> aggreResultDataList)
      throws IOException {
    List<TSDataType> dataTypes = new ArrayList<>();
    List<IPointReader> resultDataPointReaders = new ArrayList<>();
    for (AggreResultData resultData : aggreResultDataList) {
      dataTypes.add(resultData.getDataType());
      resultDataPointReaders.add(new AggreResultDataPointReader(resultData));
    }
    return new EngineDataSetWithoutValueFilter(selectedSeries, dataTypes, resultDataPointReaders);
  }
}
