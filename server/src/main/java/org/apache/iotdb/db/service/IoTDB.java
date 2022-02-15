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
package org.apache.iotdb.db.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import org.apache.iotdb.db.concurrent.IoTDBDefaultThreadExceptionHandler;
import org.apache.iotdb.db.conf.IoTDBConfigCheck;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.adapter.ManageDynamicParameters;
import org.apache.iotdb.db.conf.adapter.IoTDBConfigDynamicAdapter;
import org.apache.iotdb.db.cost.statistic.Measurement;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.cache.CacheHitRatioMonitor;
import org.apache.iotdb.db.engine.merge.manage.MergeManager;
import org.apache.iotdb.db.engine.flush.FlushManager;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.compress.manage.CompressManager;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.path.PathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.storageGroup.StorageGroupException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.monitor.StatMonitor;
import org.apache.iotdb.db.qp.physical.crud.BatchInsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.executor.AggregateEngineExecutor;
import org.apache.iotdb.db.query.executor.EngineQueryRouter;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.sync.receiver.SyncServerManager;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDB implements IoTDBMBean {

  private static final Logger logger = LoggerFactory.getLogger(IoTDB.class);
  private final String mbeanName = String.format("%s:%s=%s", IoTDBConstant.IOTDB_PACKAGE,
      IoTDBConstant.JMX_TYPE, "IoTDB");
  private RegisterManager registerManager = new RegisterManager();

  public static IoTDB getInstance() {
    return IoTDBHolder.INSTANCE;
  }

  public static void main(String[] args) {
    IoTDBConfigCheck.getInstance().checkConfig();
    IoTDB daemon = IoTDB.getInstance();
    daemon.active();
  }

  public void active() {
    StartupChecks checks = new StartupChecks().withDefaultTest();
    try {
      checks.verify();
    } catch (StartupException e) {
      // TODO: what are some checks
      logger.error("{}: failed to start because some checks failed. ",
          IoTDBConstant.GLOBAL_DB_NAME, e);
      return;
    }
    try {
      setUp();
    } catch (StartupException e) {
      logger.error("meet error while starting up.", e);
      deactivate();
      logger.error("{} exit", IoTDBConstant.GLOBAL_DB_NAME);
      return;
    }
    logger.info("{} has started.", IoTDBConstant.GLOBAL_DB_NAME);
  }

  private void setUp() throws StartupException {
    logger.info("Setting up IoTDB...");

    Runtime.getRuntime().addShutdownHook(new IoTDBShutdownHook());
    setUncaughtExceptionHandler();

    // When registering statMonitor, we should start recovering some statistics
    // with latest values stored
    // Warn: registMonitor() method should be called after systemDataRecovery()
    if (IoTDBDescriptor.getInstance().getConfig().isEnableStatMonitor()) {
      StatMonitor.getInstance().recovery();
    }

    initMManager();
    registerManager.register(StorageEngine.getInstance());
    registerManager.register(MultiFileLogNodeManager.getInstance());
    registerManager.register(JMXService.getInstance());
    registerManager.register(JDBCService.getInstance());
    registerManager.register(Monitor.getInstance());
    registerManager.register(StatMonitor.getInstance());
    registerManager.register(Measurement.INSTANCE);
    registerManager.register(ManageDynamicParameters.getInstance());
    registerManager.register(SyncServerManager.getInstance());
    registerManager.register(TVListAllocator.getInstance());
    registerManager.register(FlushManager.getInstance());
    registerManager.register(UpgradeSevice.getINSTANCE());
    registerManager.register(MergeManager.getINSTANCE());
    registerManager.register(CacheHitRatioMonitor.getInstance());
    registerManager.register(MetricsService.getInstance());
    registerManager.register(CompressManager.getINSTANCE());
    JMXService.registerMBean(getInstance(), mbeanName);

    logger.info("IoTDB is set up.");
  }

  private void deactivate() {
    logger.info("Deactivating IoTDB...");
    registerManager.deregisterAll();
    JMXService.deregisterMBean(mbeanName);
    logger.info("IoTDB is deactivated.");
  }

  private void initMManager(){
    MManager.getInstance().init();
    IoTDBConfigDynamicAdapter.getInstance().setInitialized(true);
    logger.debug(
        "After initializing, max memTable num is {}, tsFile threshold is {}, memtableSize is {}",
        IoTDBDescriptor.getInstance().getConfig().getMaxMemtableNumber(),
        IoTDBDescriptor.getInstance().getConfig().getTsFileSizeThreshold(),
        IoTDBDescriptor.getInstance().getConfig().getMemtableSizeThreshold());

  }

  @Override
  public void stop() {
    deactivate();
  }

  private void setUncaughtExceptionHandler() {
    Thread.setDefaultUncaughtExceptionHandler(new IoTDBDefaultThreadExceptionHandler());
  }

  private static class IoTDBHolder {

    private static final IoTDB INSTANCE = new IoTDB();

    private IoTDBHolder() {

    }
  }

  public void register(String processorName, String diviceName, String sensorName, String dataType,
      String encoding)
      throws StorageEngineException, MetadataException, PathException, StorageGroupException, IOException {
    MManager.getInstance().setStorageGroupToMTree(processorName);
    String path = processorName + "." + diviceName + "." + sensorName;
    MManager.getInstance().addPathToMTree(path, dataType, encoding);
    StorageEngine.getInstance()
        .addTimeSeries(new Path(path), TSDataType.valueOf(dataType),
            TSEncoding.valueOf(encoding), CompressionType
                .valueOf(TSFileDescriptor.getInstance().getConfig().getCompressor()),
            Collections.emptyMap());
  }


  public void insert(InsertPlan insertPlan) throws QueryProcessException, StorageEngineException {
    StorageEngine.getInstance().insert(insertPlan);
  }

  public void insertBatch(BatchInsertPlan insertPlan) throws StorageEngineException {
    StorageEngine.getInstance().insertBatch(insertPlan);
  }

  public long queryLastTime(String steramId) throws StorageEngineException {
     return Math.round(query(steramId, "max_time", -1,-1));
  }
  public double query(String steramId, String aggreFun, long startTime, long endTime)
      throws StorageEngineException {

    Path path = new Path(steramId);
    IExpression timeExpression = null;
    if(endTime > 0){
      Filter andFilter = FilterFactory.and(TimeFilter.gt(startTime), TimeFilter.lt(endTime));
      timeExpression=new GlobalTimeExpression(andFilter);
    }

    QueryContext context = new QueryContext(QueryResourceManager.getInstance().assignJobId());
    double res  = 0;
    try {
      QueryDataSet dataSet = new EngineQueryRouter().aggregate(new ArrayList<Path>(){{add(path);}},
          new ArrayList<String>(){{add(aggreFun);}}, timeExpression, context);

      if(dataSet.hasNext()){
        RowRecord rowRecord = dataSet.next();
        String strVal = rowRecord.getFields().get(0).getStringValue();
        res = Double.parseDouble(strVal);
      }
    } catch (QueryFilterOptimizationException e) {
      e.printStackTrace();
    } catch (QueryProcessException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    finally {
      QueryResourceManager.getInstance().endQueryForGivenJob(context.getJobId());
    }
    return res;
  }

  public void syncClose() throws StorageEngineException {
    StorageEngine.getInstance().syncCloseAllProcessor();
    StorageEngine.getInstance().syncCloseCompress();
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    deactivate();
  }

}
