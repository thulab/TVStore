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
package org.apache.iotdb.db.conf;

import java.io.File;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iotdb.db.engine.merge.selector.MergeFileStrategy;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.service.TSServiceImpl;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBConfig {

  private static final Logger logger = LoggerFactory.getLogger(IoTDBConfig.class);
  static final String CONFIG_NAME = "iotdb-engine.properties";
  private static final String MULTI_DIR_STRATEGY_PREFIX =
      "org.apache.iotdb.db.conf.directories.strategy.";
  private static final String DEFAULT_MULTI_DIR_STRATEGY = "MaxDiskUsableSpaceFirstStrategy";
  
  /**
   * Port which the metrics service listens to.
   */
  private int metricsPort = 8181;

  /* Names of Watermark methods */
  public static final String WATERMARK_GROUPED_LSB = "GroupBasedLSBMethod";

  private String rpcAddress = "0.0.0.0";

  /**
   * whether to use thrift compression.
   */
  private boolean rpcThriftCompressionEnable = false;

  /**
   * Port which the JDBC server listens to.
   */
  private int rpcPort = 6667;

  /**
   * Max concurrent client number
   */
  private int rpcMaxConcurrentClientNum = 65535;

  /**
   * Memory allocated for the read process
   */
  private long allocateMemoryForWrite = Runtime.getRuntime().maxMemory() * 6 / 10;

  /**
   * Memory allocated for the write process
   */
  private long allocateMemoryForRead = Runtime.getRuntime().maxMemory() * 3 / 10;

  /**
   * Is dynamic parameter adapter enable.
   */
  private boolean enableParameterAdapter = true;

  /**
   * Is the write ahead log enable.
   */
  private boolean enableWal = true;

  private volatile boolean readOnly = false;

  /**
   * When a certain amount of write ahead logs is reached, they will be flushed to the disk. It is
   * possible to lose at most flush_wal_threshold operations.
   */
  private int flushWalThreshold = 10000;

  /**
   * this variable set timestamp precision as millisecond, microsecond or nanosecond
   */
  private String timestampPrecision = "ms";

  /**
   * The cycle when write ahead log is periodically forced to be written to disk(in milliseconds) If
   * set this parameter to 0 it means call outputStream.force(true) after every each insert
   */
  private long forceWalPeriodInMs = 10;

  /**
   * Size of log buffer in each log node(in byte). If WAL is enabled and the size of a insert plan
   * is smaller than this parameter, then the insert plan will be rejected by WAL.
   */
  private int walBufferSize = 16 * 1024 * 1024;

  /**
   * system base dir, stores all system metadata and wal
   */
  private String baseDir = "data";

  /**
   * System directory, including version file for each storage group and metadata
   */
  private String systemDir = "data/system";

  /**
   * Schema directory, including storage set of values.
   */
  private String schemaDir = "data/system/schema";

  /**
   * Query directory, stores temporary files of query
   */
  private String queryDir = "data/query";

  /**
   * Data directory of data. It can be settled as dataDirs = {"data1", "data2", "data3"};
   */
  private String[] dataDirs = {"data/data"};

  /**
   * Strategy of multiple directories.
   */
  private String multiDirStrategyClassName = null;

  /**
   * Wal directory.
   */
  private String walFolder = "data/wal";

  /**
   * Data directory for index files (KV-match indexes).
   */
  private String indexFileDir = "data/index";

  /**
   * Maximum MemTable number in MemTable pool.
   */
  private int maxMemtableNumber = 20;

  /**
   * The amount of data that is read every time when IoTDB merges data.
   */
  private int fetchSize = 10000;

  /**
   * How many threads can concurrently flush. When <= 0, use CPU core number.
   */
  private int concurrentFlushThread = Runtime.getRuntime().availableProcessors();

  private ZoneId zoneID = ZoneId.systemDefault();

  /**
   * When a TsFile's file size (in byte) exceed this, the TsFile is forced closed.
   */
  private long tsFileSizeThreshold = 512 * 1024 * 1024L;

  /**
   * When a memTable's size (in byte) exceeds this, the memtable is flushed to disk.
   */
  private long memtableSizeThreshold = 128 * 1024 * 1024L;

  /**
   * whether to cache meta data(ChunkMetaData and TsFileMetaData) or not.
   */
  private boolean metaDataCacheEnable = true;
  /**
   * Memory allocated for fileMetaData cache in read process
   */
  private long allocateMemoryForFileMetaDataCache = allocateMemoryForRead * 3 / 19;

  /**
   * Memory allocated for chunkMetaData cache in read process
   */
  private long allocateMemoryForChumkMetaDataCache = allocateMemoryForRead * 6 / 19;

  /**
   * The statMonitor writes statistics info into IoTDB every backLoopPeriodSec secs. The default
   * value is 5s.
   */
  private int backLoopPeriodSec = 5;
  /**
   * Set true to enable statistics monitor service, false to disable statistics service.
   */
  private boolean enableStatMonitor = false;
  /**
   * Set the time interval when StatMonitor performs delete detection. The default value is 600s.
   */
  private int statMonitorDetectFreqSec = 60 * 10;
  /**
   * Set the maximum time to keep monitor statistics information in IoTDB. The default value is
   * 600s.
   */
  private int statMonitorRetainIntervalSec = 60 * 10;

  /**
   * Cache size of {@code checkAndGetDataTypeCache} in {@link MManager}.
   */
  private int mManagerCacheSize = 400000;

  /**
   * Is external sort enable.
   */
  private boolean enableExternalSort = true;

  /**
   * The threshold of items in external sort. If the number of chunks participating in sorting
   * exceeds this threshold, external sorting is enabled, otherwise memory sorting is used.
   */
  private int externalSortThreshold = 60;

  /**
   * Is this IoTDB instance a receiver of sync or not.
   */
  private boolean isSyncEnable = true;
  /**
   * If this IoTDB instance is a receiver of sync, set the server port.
   */
  private int syncServerPort = 5555;
  /**
   * Set the language version when loading file including error information, default value is "EN"
   */
  private String languageVersion = "EN";

  private String ipWhiteList = "0.0.0.0/0";
  /**
   * Examining period of cache file reader : 100 seconds.
   */
  private long cacheFileReaderClearPeriod = 100000;

  /**
   * Replace implementation class of JDBC service
   */
  private String rpcImplClassName = TSServiceImpl.class.getName();

  /**
   * Is stat performance of sub-module enable.
   */
  private boolean enablePerformanceStat = false;

  /**
   * The display of stat performance interval in ms.
   */
  private long performanceStatDisplayInterval = 60000;

  /**
   * The memory used for stat performance.
   */
  private int performanceStatMemoryInKB = 20;
  /**
   * whether use chunkBufferPool.
   */
  private boolean chunkBufferPoolEnable = false;

  /**
   * Switch of watermark function
   */
  private boolean enableWatermark = false;

  /**
   * Secret key for watermark
   */
  private String watermarkSecretKey = "QWERTYUIOP*&=";

  /**
   * Bit string of watermark
   */
  private String watermarkBitString = "11001010010101";

  /**
   * Watermark method and parameters
   */
  private String watermarkMethod = "GroupBasedLSBMethod(embed_row_cycle=5,embed_lsb_num=5)";

  /**
   * Switch of creating schema automatically
   */
  private boolean enableAutoCreateSchema = false;

  /**
   * Storage group level when creating schema automatically is enabled
   */
  private int defaultStorageGroupLevel = 2;

  /**
   * BOOLEAN encoding when creating schema automatically is enabled
   */
  private TSEncoding defaultBooleanEncoding = TSEncoding.RLE;

  /**
   * INT32 encoding when creating schema automatically is enabled
   */
  private TSEncoding defaultInt32Encoding = TSEncoding.RLE;

  /**
   * INT64 encoding when creating schema automatically is enabled
   */
  private TSEncoding defaultInt64Encoding = TSEncoding.RLE;

  /**
   * FLOAT encoding when creating schema automatically is enabled
   */
  private TSEncoding defaultFloatEncoding = TSEncoding.GORILLA;

  /**
   * DOUBLE encoding when creating schema automatically is enabled
   */
  private TSEncoding defaultDoubleEncoding = TSEncoding.GORILLA;

  /**
   * TEXT encoding when creating schema automatically is enabled
   */
  private TSEncoding defaultTextEncoding = TSEncoding.PLAIN;

  /**
   * How much memory (in byte) can be used by a single merge task.
   */
  private long mergeMemoryBudget = (long) (Runtime.getRuntime().maxMemory() * 0.2);

  /**
   * How many threads will be set up to perform upgrade tasks.
   */
  private int upgradeThreadNum = 1;

  /**
   * How many threads will be set up to perform main merge tasks.
   */
  private int mergeThreadNum = 1;

  /**
   * How many threads will be set up to perform merge chunk sub-tasks.
   */
  private int mergeChunkSubThreadNum = 4;

  /**
   * If one merge file selection runs for more than this time, it will be ended and its current
   * selection will be used as final selection. Unit: millis. When < 0, it means time is unbounded.
   */
  private long mergeFileSelectionTimeBudget = 30 * 1000;

  /**
   * When set to true, if some crashed merges are detected during system rebooting, such merges will
   * be continued, otherwise, the unfinished parts of such merges will not be continued while the
   * finished parts still remain as they are.
   */
  private boolean continueMergeAfterReboot = true;

  /**
   * A global merge will be performed each such interval, that is, each storage group will be merged
   * (if proper merge candidates can be found). Unit: second.
   */
  private long mergeIntervalSec = 2 * 3600L;

  /**
   * When set to true, all merges becomes full merge (the whole SeqFiles are re-written despite how
   * much they are overflowed). This may increase merge overhead depending on how much the SeqFiles
   * are overflowed.
   */
  private boolean forceFullMerge = false;

  /**
   * During a merge, if a chunk with less number of chunks than this parameter, the chunk will be
   * merged with its succeeding chunks even if it is not overflowed, until the merged chunks reach
   * this threshold and the new chunk will be flushed.
   */
  private int chunkMergePointThreshold = 20480;

  private MergeFileStrategy mergeFileStrategy = MergeFileStrategy.MAX_SERIES_NUM;

  /**
   * Default system file storage is in local file system (unsupported)
   */
  private FSType systemFileStorageFs = FSType.LOCAL;

  /**
   * Default TSfile storage is in local file system
   */
  private FSType tsFileStorageFs = FSType.LOCAL;

  /**
   * Default core-site.xml file path is /etc/hadoop/conf/core-site.xml
   */
  private String coreSitePath = "/etc/hadoop/conf/core-site.xml";

  /**
   * Default hdfs-site.xml file path is /etc/hadoop/conf/hdfs-site.xml
   */
  private String hdfsSitePath = "/etc/hadoop/conf/hdfs-site.xml";

  /**
   * Default HDFS ip is localhost
   */
  private String hdfsIp = "localhost";

  /**
   * Default HDFS port is 9000
   */
  private String hdfsPort = "9000";

  /**
   * Default DFS NameServices is hdfsnamespace
   */
  private String dfsNameServices = "hdfsnamespace";

  /**
   * Default DFS HA name nodes are nn1 and nn2
   */
  private String dfsHaNamenodes = "nn1,nn2";

  /**
   * Default DFS HA automatic failover is enabled
   */
  private boolean dfsHaAutomaticFailoverEnabled = true;

  /**
   * Default DFS client failover proxy provider is "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
   */
  private String dfsClientFailoverProxyProvider = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider";

  /**
   * whether use kerberos to authenticate hdfs
   */
  private boolean useKerberos = false;

  /**
   * full path of kerberos keytab file
   */
  private String kerberosKeytabFilePath = "/path";

  /**
   * kerberos pricipal
   */
  private String kerberosPrincipal = "principal";

  /**
   * default TTL for storage groups that are not set TTL by statements, in ms
   * Notice: if this property is changed, previous created storage group which are not set TTL will
   * also be affected.
   */
  private long defaultTTL = Long.MAX_VALUE;

  /**
   * Whether enable lossy compress.
   */
  private boolean enableCompress = true;
  /**
   * When data size is larger than diskSizeUpBoundInMB, lossy compress will start.
   */
  private long diskSizeUpBoundInMB = 500;

  /**
   * When data size is smaller than diskSizeUpBoundInMB, lossy compress will stop.
   */
  private long diskSizeLowBoundInMB = 250;

  /**
   * How many thread will be set up to perform compress main tasks, 1 by default.
   */
  private int compressThreadNum = 1;
  /**
   * How many thread will be set up to perform compress chunk sub-tasks, 4 by default.
   */
  private int compressChunkSubthreadNum = 4;

  /**
   * When set to true, if some crashed compresses are detected during system rebooting, such compresses will
   * be continued, otherwise, the unfinished parts of such compresses will not be continued while the
   * finished parts still remain as they are.
   */
  private boolean continueCompressAfterReboot = true;

  /**
   * If timestamp of data is within compress_ttl(in ms), then the data won't be compressed.
   * When compress_ttl is less than or equal to 0, then compress_ttl is valid, all data can be compressed.
   */
  private long compressTTL = -1;

  private String delayFunction = "power";

  private String dalayFunctionParameters = "0.5";

  // disk read speed MB/s
  private double diskReadSpeed = 50;
  // disk write speed MB/s
  private double diskWriteSpeed = 25;
  // max data write in one compress process, MB
  private double oneCompressWriteDataThreshold = diskSizeLowBoundInMB/2;
  // the ratio of low bound and data write in one compress task, (low bound/data write in one ratio) (double)
  private double lowBoundToWriteDataRatio =3;
  // ratio of one compress process
  private double compressRatio=2;
  // Compression trigger timing ratio
  private double compressTriggerSizeRatio=0.9;

  private String uncompressStorageGroupList = "";

  private boolean havingWriteWorkloadWhileCompressing = true;

  private int chunkQueueCapacity = 50;

  private String windowEstimationMethod = "time_ratio";

  private boolean enableRecoverRangeQuery = false;

  private long dataSizeCheckIntervalInSecond = 5;
  private double compressionRatioDiff = 3;
  private double sigmoInCompressPointExistJudge = 2;

  // Whether to remove adjacent duplicate values or not in compress task
  private boolean removeAdjacentReaptedPoint=false;

  // Is sample entropy calculated
  private boolean isCalculateSampleEntropy = false;

  public IoTDBConfig() {
    // empty constructor
  }

  public ZoneId getZoneID() {
    return zoneID;
  }

  void updatePath() {
    formulateFolders();
    confirmMultiDirStrategy();
  }


  /**
   * if the folders are relative paths, add IOTDB_HOME as the path prefix
   */
  private void formulateFolders() {
    List<String> dirs = new ArrayList<>();
    dirs.add(baseDir);
    dirs.add(systemDir);
    dirs.add(schemaDir);
    dirs.add(walFolder);
    dirs.add(indexFileDir);
    dirs.add(queryDir);
    dirs.addAll(Arrays.asList(dataDirs));

    for (int i = 0; i < 4; i++) {
      addHomeDir(dirs, i);
    }

    if (TSFileDescriptor.getInstance().getConfig().getTSFileStorageFs().equals(FSType.HDFS)) {
      String[] hdfsIps = TSFileDescriptor.getInstance().getConfig().getHdfsIp();
      String hdfsDir = "hdfs://";
      if (hdfsIps.length > 1) {
        hdfsDir += TSFileDescriptor.getInstance().getConfig().getDfsNameServices();
      } else {
        hdfsDir += hdfsIps[0] + ":" + TSFileDescriptor.getInstance().getConfig().getHdfsPort();
      }
      for (int i = 5; i < dirs.size(); i++) {
        String dir = dirs.get(i);
        dir = hdfsDir + File.separatorChar + dir;
        dirs.set(i, dir);
      }
    } else {
      for (int i = 5; i < dirs.size(); i++) {
        addHomeDir(dirs, i);
      }
    }
    baseDir = dirs.get(0);
    systemDir = dirs.get(1);
    schemaDir = dirs.get(2);
    walFolder = dirs.get(3);
    indexFileDir = dirs.get(4);
    queryDir = dirs.get(5);
    for (int i = 0; i < dataDirs.length; i++) {
      dataDirs[i] = dirs.get(i + 6);
    }
  }

  private void addHomeDir(List<String> dirs, int i) {
    String dir = dirs.get(i);
    String homeDir = System.getProperty(IoTDBConstant.IOTDB_HOME, null);
    if (!new File(dir).isAbsolute() && homeDir != null && homeDir.length() > 0) {
      if (!homeDir.endsWith(File.separator)) {
        dir = homeDir + File.separatorChar + dir;
      } else {
        dir = homeDir + dir;
      }
      dirs.set(i, dir);
    }
  }

  private void confirmMultiDirStrategy() {
    if (getMultiDirStrategyClassName() == null) {
      multiDirStrategyClassName = DEFAULT_MULTI_DIR_STRATEGY;
    }
    if (!getMultiDirStrategyClassName().contains(".")) {
      multiDirStrategyClassName = MULTI_DIR_STRATEGY_PREFIX + multiDirStrategyClassName;
    }

    try {
      Class.forName(multiDirStrategyClassName);
    } catch (ClassNotFoundException e) {
      logger.warn("Cannot find given directory strategy {}, using the default value",
          getMultiDirStrategyClassName(), e);
      setMultiDirStrategyClassName(MULTI_DIR_STRATEGY_PREFIX + DEFAULT_MULTI_DIR_STRATEGY);
    }
  }

  public String[] getDataDirs() {
    return dataDirs;
  }

  public int getMetricsPort() {
    return metricsPort;
  }

  public void setMetricsPort(int metricsPort) {
    this.metricsPort = metricsPort;
  }

  public String getRpcAddress() {
    return rpcAddress;
  }

  void setRpcAddress(String rpcAddress) {
    this.rpcAddress = rpcAddress;
  }

  public int getRpcPort() {
    return rpcPort;
  }

  void setRpcPort(int rpcPort) {
    this.rpcPort = rpcPort;
  }

  public void setTimestampPrecision(String timestampPrecision) {
    this.timestampPrecision = timestampPrecision;
  }

  public String getTimestampPrecision() {
    return timestampPrecision;
  }

  public boolean isEnableWal() {
    return enableWal;
  }

  public void setEnableWal(boolean enableWal) {
    this.enableWal = enableWal;
  }

  public int getFlushWalThreshold() {
    return flushWalThreshold;
  }

  public void setFlushWalThreshold(int flushWalThreshold) {
    this.flushWalThreshold = flushWalThreshold;
  }

  public long getForceWalPeriodInMs() {
    return forceWalPeriodInMs;
  }

  public void setForceWalPeriodInMs(long forceWalPeriodInMs) {
    this.forceWalPeriodInMs = forceWalPeriodInMs;
  }

  public String getSystemDir() {
    return systemDir;
  }

  void setSystemDir(String systemDir) {
    this.systemDir = systemDir;
  }

  public String getSchemaDir() {
    return schemaDir;
  }

  void setSchemaDir(String schemaDir) {
    this.schemaDir = schemaDir;
  }

  public String getQueryDir() {
    return queryDir;
  }

  public void setQueryDir(String queryDir) {
    this.queryDir = queryDir;
  }

  public String getWalFolder() {
    return walFolder;
  }

  void setWalFolder(String walFolder) {
    this.walFolder = walFolder;
  }

  void setDataDirs(String[] dataDirs) {
    this.dataDirs = dataDirs;
  }

  public String getMultiDirStrategyClassName() {
    return multiDirStrategyClassName;
  }

  void setMultiDirStrategyClassName(String multiDirStrategyClassName) {
    this.multiDirStrategyClassName = multiDirStrategyClassName;
  }

  public String getIndexFileDir() {
    return indexFileDir;
  }

  private void setIndexFileDir(String indexFileDir) {
    this.indexFileDir = indexFileDir;
  }

  public int getFetchSize() {
    return fetchSize;
  }

  void setFetchSize(int fetchSize) {
    this.fetchSize = fetchSize;
  }

  public int getMaxMemtableNumber() {
    return maxMemtableNumber;
  }

  public void setMaxMemtableNumber(int maxMemtableNumber) {
    this.maxMemtableNumber = maxMemtableNumber;
  }

  public int getConcurrentFlushThread() {
    return concurrentFlushThread;
  }

  void setConcurrentFlushThread(int concurrentFlushThread) {
    this.concurrentFlushThread = concurrentFlushThread;
  }

  void setZoneID(ZoneId zoneID) {
    this.zoneID = zoneID;
  }

  public long getTsFileSizeThreshold() {
    return tsFileSizeThreshold;
  }

  public void setTsFileSizeThreshold(long tsFileSizeThreshold) {
    this.tsFileSizeThreshold = tsFileSizeThreshold;
  }

  public int getBackLoopPeriodSec() {
    return backLoopPeriodSec;
  }

  void setBackLoopPeriodSec(int backLoopPeriodSec) {
    this.backLoopPeriodSec = backLoopPeriodSec;
  }

  public boolean isEnableStatMonitor() {
    return enableStatMonitor;
  }

  public void setEnableStatMonitor(boolean enableStatMonitor) {
    this.enableStatMonitor = enableStatMonitor;
  }

  public int getRpcMaxConcurrentClientNum() {
    return rpcMaxConcurrentClientNum;
  }

  public void setRpcMaxConcurrentClientNum(int rpcMaxConcurrentClientNum) {
    this.rpcMaxConcurrentClientNum = rpcMaxConcurrentClientNum;
  }

  public int getStatMonitorDetectFreqSec() {
    return statMonitorDetectFreqSec;
  }

  void setStatMonitorDetectFreqSec(int statMonitorDetectFreqSec) {
    this.statMonitorDetectFreqSec = statMonitorDetectFreqSec;
  }

  public int getStatMonitorRetainIntervalSec() {
    return statMonitorRetainIntervalSec;
  }

  void setStatMonitorRetainIntervalSec(int statMonitorRetainIntervalSec) {
    this.statMonitorRetainIntervalSec = statMonitorRetainIntervalSec;
  }

  public int getmManagerCacheSize() {
    return mManagerCacheSize;
  }

  void setmManagerCacheSize(int mManagerCacheSize) {
    this.mManagerCacheSize = mManagerCacheSize;
  }

  public boolean isSyncEnable() {
    return isSyncEnable;
  }

  void setSyncEnable(boolean syncEnable) {
    isSyncEnable = syncEnable;
  }

  public int getSyncServerPort() {
    return syncServerPort;
  }

  void setSyncServerPort(int syncServerPort) {
    this.syncServerPort = syncServerPort;
  }

  public String getLanguageVersion() {
    return languageVersion;
  }

  void setLanguageVersion(String languageVersion) {
    this.languageVersion = languageVersion;
  }

  public String getBaseDir() {
    return baseDir;
  }

  public void setBaseDir(String baseDir) {
    this.baseDir = baseDir;
  }

  public String getIpWhiteList() {
    return ipWhiteList;
  }

  public void setIpWhiteList(String ipWhiteList) {
    this.ipWhiteList = ipWhiteList;
  }

  public long getCacheFileReaderClearPeriod() {
    return cacheFileReaderClearPeriod;
  }

  public void setCacheFileReaderClearPeriod(long cacheFileReaderClearPeriod) {
    this.cacheFileReaderClearPeriod = cacheFileReaderClearPeriod;
  }

  public boolean isReadOnly() {
    return readOnly;
  }

  public void setReadOnly(boolean readOnly) {
    this.readOnly = readOnly;
  }

  public String getRpcImplClassName() {
    return rpcImplClassName;
  }

  public void setRpcImplClassName(String rpcImplClassName) {
    this.rpcImplClassName = rpcImplClassName;
  }

  public int getWalBufferSize() {
    return walBufferSize;
  }

  void setWalBufferSize(int walBufferSize) {
    this.walBufferSize = walBufferSize;
  }

  public boolean isChunkBufferPoolEnable() {
    return chunkBufferPoolEnable;
  }

  void setChunkBufferPoolEnable(boolean chunkBufferPoolEnable) {
    this.chunkBufferPoolEnable = chunkBufferPoolEnable;
  }

  public long getMergeMemoryBudget() {
    return mergeMemoryBudget;
  }

  public void setMergeMemoryBudget(long mergeMemoryBudget) {
    this.mergeMemoryBudget = mergeMemoryBudget;
  }

  public int getMergeThreadNum() {
    return mergeThreadNum;
  }

  public void setMergeThreadNum(int mergeThreadNum) {
    this.mergeThreadNum = mergeThreadNum;
  }

  public boolean isContinueMergeAfterReboot() {
    return continueMergeAfterReboot;
  }

  public void setContinueMergeAfterReboot(boolean continueMergeAfterReboot) {
    this.continueMergeAfterReboot = continueMergeAfterReboot;
  }

  public long getMergeIntervalSec() {
    return mergeIntervalSec;
  }

  public void setMergeIntervalSec(long mergeIntervalSec) {
    this.mergeIntervalSec = mergeIntervalSec;
  }

  public boolean isEnableParameterAdapter() {
    return enableParameterAdapter;
  }

  public void setEnableParameterAdapter(boolean enableParameterAdapter) {
    this.enableParameterAdapter = enableParameterAdapter;
  }

  public long getAllocateMemoryForWrite() {
    return allocateMemoryForWrite;
  }

  public void setAllocateMemoryForWrite(long allocateMemoryForWrite) {
    this.allocateMemoryForWrite = allocateMemoryForWrite;
  }

  public long getAllocateMemoryForRead() {
    return allocateMemoryForRead;
  }

  public void setAllocateMemoryForRead(long allocateMemoryForRead) {
    this.allocateMemoryForRead = allocateMemoryForRead;
  }

  public boolean isEnableExternalSort() {
    return enableExternalSort;
  }

  public void setEnableExternalSort(boolean enableExternalSort) {
    this.enableExternalSort = enableExternalSort;
  }

  public int getExternalSortThreshold() {
    return externalSortThreshold;
  }

  public void setExternalSortThreshold(int externalSortThreshold) {
    this.externalSortThreshold = externalSortThreshold;
  }

  public boolean isEnablePerformanceStat() {
    return enablePerformanceStat;
  }

  public void setEnablePerformanceStat(boolean enablePerformanceStat) {
    this.enablePerformanceStat = enablePerformanceStat;
  }

  public long getPerformanceStatDisplayInterval() {
    return performanceStatDisplayInterval;
  }

  public void setPerformanceStatDisplayInterval(long performanceStatDisplayInterval) {
    this.performanceStatDisplayInterval = performanceStatDisplayInterval;
  }

  public int getPerformanceStatMemoryInKB() {
    return performanceStatMemoryInKB;
  }

  public void setPerformanceStatMemoryInKB(int performanceStatMemoryInKB) {
    this.performanceStatMemoryInKB = performanceStatMemoryInKB;
  }

  public boolean isForceFullMerge() {
    return forceFullMerge;
  }

  public void setForceFullMerge(boolean forceFullMerge) {
    this.forceFullMerge = forceFullMerge;
  }

  public int getChunkMergePointThreshold() {
    return chunkMergePointThreshold;
  }

  public void setChunkMergePointThreshold(int chunkMergePointThreshold) {
    this.chunkMergePointThreshold = chunkMergePointThreshold;
  }

  public long getMemtableSizeThreshold() {
    return memtableSizeThreshold;
  }

  public void setMemtableSizeThreshold(long memtableSizeThreshold) {
    this.memtableSizeThreshold = memtableSizeThreshold;
  }

  public MergeFileStrategy getMergeFileStrategy() {
    return mergeFileStrategy;
  }

  public void setMergeFileStrategy(
      MergeFileStrategy mergeFileStrategy) {
    this.mergeFileStrategy = mergeFileStrategy;
  }

  public int getMergeChunkSubThreadNum() {
    return mergeChunkSubThreadNum;
  }

  public void setMergeChunkSubThreadNum(int mergeChunkSubThreadNum) {
    this.mergeChunkSubThreadNum = mergeChunkSubThreadNum;
  }

  public long getMergeFileSelectionTimeBudget() {
    return mergeFileSelectionTimeBudget;
  }

  public void setMergeFileSelectionTimeBudget(long mergeFileSelectionTimeBudget) {
    this.mergeFileSelectionTimeBudget = mergeFileSelectionTimeBudget;
  }

  public boolean isRpcThriftCompressionEnable() {
    return rpcThriftCompressionEnable;
  }

  public void setRpcThriftCompressionEnable(boolean rpcThriftCompressionEnable) {
    this.rpcThriftCompressionEnable = rpcThriftCompressionEnable;
  }

  public boolean isMetaDataCacheEnable() {
    return metaDataCacheEnable;
  }

  public void setMetaDataCacheEnable(boolean metaDataCacheEnable) {
    this.metaDataCacheEnable = metaDataCacheEnable;
  }

  public long getAllocateMemoryForFileMetaDataCache() {
    return allocateMemoryForFileMetaDataCache;
  }

  public void setAllocateMemoryForFileMetaDataCache(long allocateMemoryForFileMetaDataCache) {
    this.allocateMemoryForFileMetaDataCache = allocateMemoryForFileMetaDataCache;
  }

  public long getAllocateMemoryForChumkMetaDataCache() {
    return allocateMemoryForChumkMetaDataCache;
  }

  public void setAllocateMemoryForChumkMetaDataCache(long allocateMemoryForChumkMetaDataCache) {
    this.allocateMemoryForChumkMetaDataCache = allocateMemoryForChumkMetaDataCache;
  }

  public boolean isEnableWatermark() {
    return enableWatermark;
  }

  public void setEnableWatermark(boolean enableWatermark) {
    this.enableWatermark = enableWatermark;
  }

  public String getWatermarkSecretKey() {
    return watermarkSecretKey;
  }

  public void setWatermarkSecretKey(String watermarkSecretKey) {
    this.watermarkSecretKey = watermarkSecretKey;
  }

  public String getWatermarkBitString() {
    return watermarkBitString;
  }

  public void setWatermarkBitString(String watermarkBitString) {
    this.watermarkBitString = watermarkBitString;
  }

  public void setWatermarkMethod(String watermarkMethod) {
    this.watermarkMethod = watermarkMethod;
  }

  public String getWatermarkMethod() {
    return this.watermarkMethod;
  }

  public String getWatermarkMethodName() {
    return watermarkMethod.split("\\(")[0];
  }

  public int getWatermarkParamMarkRate() {
    return Integer.parseInt(getWatermarkParamValue("embed_row_cycle", "5"));
  }

  public int getWatermarkParamMaxRightBit() {
    return Integer.parseInt(getWatermarkParamValue("embed_lsb_num", "5"));
  }

  public String getWatermarkParamValue(String key, String defaultValue) {
    String res = getWatermarkParamValue(key);
    if (res != null) {
      return res;
    }
    return defaultValue;
  }

  public String getWatermarkParamValue(String key) {
    String pattern = key + "=(\\w*)";
    Pattern r = Pattern.compile(pattern);
    Matcher m = r.matcher(watermarkMethod);
    if (m.find() && m.groupCount() > 0) {
      return m.group(1);
    }
    return null;
  }

  public boolean isAutoCreateSchemaEnabled() {
    return enableAutoCreateSchema;
  }

  public void setAutoCreateSchemaEnabled(boolean enableAutoCreateSchema) {
    this.enableAutoCreateSchema = enableAutoCreateSchema;
  }

  public int getDefaultStorageGroupLevel() {
    return defaultStorageGroupLevel;
  }

  public void setDefaultStorageGroupLevel(int defaultStorageGroupLevel) {
    this.defaultStorageGroupLevel = defaultStorageGroupLevel;
  }

  public TSEncoding getDefaultBooleanEncoding() {
    return defaultBooleanEncoding;
  }

  public void setDefaultBooleanEncoding(TSEncoding defaultBooleanEncoding) {
    this.defaultBooleanEncoding = defaultBooleanEncoding;
  }

  public void setDefaultBooleanEncoding(String defaultBooleanEncoding) {
    this.defaultBooleanEncoding = TSEncoding.valueOf(defaultBooleanEncoding);
  }

  public TSEncoding getDefaultInt32Encoding() {
    return defaultInt32Encoding;
  }

  public void setDefaultInt32Encoding(TSEncoding defaultInt32Encoding) {
    this.defaultInt32Encoding = defaultInt32Encoding;
  }

  public void setDefaultInt32Encoding(String defaultInt32Encoding) {
    this.defaultInt32Encoding = TSEncoding.valueOf(defaultInt32Encoding);
  }

  public TSEncoding getDefaultInt64Encoding() {
    return defaultInt64Encoding;
  }

  public void setDefaultInt64Encoding(TSEncoding defaultInt64Encoding) {
    this.defaultInt64Encoding = defaultInt64Encoding;
  }

  public void setDefaultInt64Encoding(String defaultInt64Encoding) {
    this.defaultInt64Encoding = TSEncoding.valueOf(defaultInt64Encoding);
  }

  public TSEncoding getDefaultFloatEncoding() {
    return defaultFloatEncoding;
  }

  public void setDefaultFloatEncoding(TSEncoding defaultFloatEncoding) {
    this.defaultFloatEncoding = defaultFloatEncoding;
  }

  public void setDefaultFloatEncoding(String defaultFloatEncoding) {
    this.defaultFloatEncoding = TSEncoding.valueOf(defaultFloatEncoding);
  }

  public TSEncoding getDefaultDoubleEncoding() {
    return defaultDoubleEncoding;
  }

  public void setDefaultDoubleEncoding(TSEncoding defaultDoubleEncoding) {
    this.defaultDoubleEncoding = defaultDoubleEncoding;
  }

  public void setDefaultDoubleEncoding(String defaultDoubleEncoding) {
    this.defaultDoubleEncoding = TSEncoding.valueOf(defaultDoubleEncoding);
  }

  public TSEncoding getDefaultTextEncoding() {
    return defaultTextEncoding;
  }

  public void setDefaultTextEncoding(TSEncoding defaultTextEncoding) {
    this.defaultTextEncoding = defaultTextEncoding;
  }

  public void setDefaultTextEncoding(String defaultTextEncoding) {
    this.defaultTextEncoding = TSEncoding.valueOf(defaultTextEncoding);
  }

  public FSType getSystemFileStorageFs() {
    return systemFileStorageFs;
  }

  public void setSystemFileStorageFs(String systemFileStorageFs) {
    this.systemFileStorageFs = FSType.valueOf(systemFileStorageFs);
  }

  public FSType getTsFileStorageFs() {
    return tsFileStorageFs;
  }

  public void setTsFileStorageFs(String tsFileStorageFs) {
    this.tsFileStorageFs = FSType.valueOf(tsFileStorageFs);
  }

  public String getCoreSitePath() {
    return coreSitePath;
  }

  public void setCoreSitePath(String coreSitePath) {
    this.coreSitePath = coreSitePath;
  }

  public String getHdfsSitePath() {
    return hdfsSitePath;
  }

  public void setHdfsSitePath(String hdfsSitePath) {
    this.hdfsSitePath = hdfsSitePath;
  }

  public String[] getHdfsIp() {
    return hdfsIp.split(",");
  }

  public void setHdfsIp(String[] hdfsIp) {
    this.hdfsIp = String.join(",", hdfsIp);
  }

  public String getHdfsPort() {
    return hdfsPort;
  }

  public void setHdfsPort(String hdfsPort) {
    this.hdfsPort = hdfsPort;
  }

  public int getUpgradeThreadNum() {
    return upgradeThreadNum;
  }

  public void setUpgradeThreadNum(int upgradeThreadNum) {
    this.upgradeThreadNum = upgradeThreadNum;
  }

  public String getDfsNameServices() {
    return dfsNameServices;
  }

  public void setDfsNameServices(String dfsNameServices) {
    this.dfsNameServices = dfsNameServices;
  }

  public String[] getDfsHaNamenodes() {
    return dfsHaNamenodes.split(",");
  }

  public void setDfsHaNamenodes(String[] dfsHaNamenodes) {
    this.dfsHaNamenodes = String.join(",", dfsHaNamenodes);
  }

  public boolean isDfsHaAutomaticFailoverEnabled() {
    return dfsHaAutomaticFailoverEnabled;
  }

  public void setDfsHaAutomaticFailoverEnabled(boolean dfsHaAutomaticFailoverEnabled) {
    this.dfsHaAutomaticFailoverEnabled = dfsHaAutomaticFailoverEnabled;
  }

  public String getDfsClientFailoverProxyProvider() {
    return dfsClientFailoverProxyProvider;
  }

  public void setDfsClientFailoverProxyProvider(String dfsClientFailoverProxyProvider) {
    this.dfsClientFailoverProxyProvider = dfsClientFailoverProxyProvider;
  }

  public boolean isUseKerberos() {
    return useKerberos;
  }

  public void setUseKerberos(boolean useKerberos) {
    this.useKerberos = useKerberos;
  }

  public String getKerberosKeytabFilePath() {
    return kerberosKeytabFilePath;
  }

  public void setKerberosKeytabFilePath(String kerberosKeytabFilePath) {
    this.kerberosKeytabFilePath = kerberosKeytabFilePath;
  }

  public String getKerberosPrincipal() {
    return kerberosPrincipal;
  }

  public void setKerberosPrincipal(String kerberosPrincipal) {
    this.kerberosPrincipal = kerberosPrincipal;
  }

  public long getDefaultTTL() {
    return defaultTTL;
  }

  public void setDefaultTTL(long defaultTTL) {
    this.defaultTTL = defaultTTL;
  }

  public boolean isEnableCompress() {
    return enableCompress;
  }

  public void setEnableCompress(boolean enableCompress) {
    this.enableCompress = enableCompress;
  }

  public long getDiskSizeUpBoundInMB() {
    return diskSizeUpBoundInMB;
  }

  public void setDiskSizeUpBoundInMB(long diskSizeUpBoundInMB) {
    this.diskSizeUpBoundInMB = diskSizeUpBoundInMB;
  }

  public long getDiskSizeLowBoundInMB() {
    return diskSizeLowBoundInMB;
  }

  public void setDiskSizeLowBoundInMB(long diskSizeLowBoundInMB) {
    this.diskSizeLowBoundInMB = diskSizeLowBoundInMB;
  }

  public int getCompressThreadNum() {
    return compressThreadNum;
  }

  public void setCompressThreadNum(int compressThreadNum) {
    this.compressThreadNum = compressThreadNum;
  }

  public int getCompressChunkSubthreadNum() {
    return compressChunkSubthreadNum;
  }

  public void setCompressChunkSubthreadNum(int compressChunkSubthreadNum) {
    this.compressChunkSubthreadNum = compressChunkSubthreadNum;
  }

  public boolean isContinueCompressAfterReboot() {
    return continueCompressAfterReboot;
  }

  public void setContinueCompressAfterReboot(boolean continueCompressAfterReboot) {
    this.continueCompressAfterReboot = continueCompressAfterReboot;
  }

  public long getCompressTTL() {
    return compressTTL;
  }

  public void setCompressTTL(long compressTTL) {
    this.compressTTL = compressTTL;
  }

  public String getDelayFunction() {
    return delayFunction;
  }

  public void setDelayFunction(String delayFunction) {
    this.delayFunction = delayFunction;
  }

  public String getDalayFunctionParameters() {
    return dalayFunctionParameters;
  }

  public void setDalayFunctionParameters(String dalayFunctionParameters) {
    this.dalayFunctionParameters = dalayFunctionParameters;
  }

  public double getDiskReadSpeed() {
    return diskReadSpeed;
  }

  public void setDiskReadSpeed(double diskReadSpeed) {
    this.diskReadSpeed = diskReadSpeed;
  }

  public double getDiskWriteSpeed() {
    return diskWriteSpeed;
  }

  public void setDiskWriteSpeed(double diskWriteSpeed) {
    this.diskWriteSpeed = diskWriteSpeed;
  }

  public double getOneCompressWriteDataThreshold() {
    return oneCompressWriteDataThreshold;
  }

  public void setOneCompressWriteDataThreshold(double oneCompressWriteDataThreshold) {
    this.oneCompressWriteDataThreshold = oneCompressWriteDataThreshold;
  }

  public double getLowBoundToWriteDataRatio() {
    return lowBoundToWriteDataRatio;
  }

  public void setLowBoundToWriteDataRatio(double lowBoundToWriteDataRatio) {
    this.lowBoundToWriteDataRatio = lowBoundToWriteDataRatio;
  }

  public double getCompressRatio() {
    return compressRatio;
  }

  public void setCompressRatio(double compressRatio) {
    this.compressRatio = compressRatio;
  }

  public double getCompressTriggerSizeRatio() {
    return compressTriggerSizeRatio;
  }

  public void setCompressTriggerSizeRatio(double compressTriggerSizeRatio) {
    this.compressTriggerSizeRatio = compressTriggerSizeRatio;
  }

  public String getUncompressStorageGroupList() {
    return uncompressStorageGroupList;
  }

  public void setUncompressStorageGroupList(String uncompressStorageGroupList) {
    this.uncompressStorageGroupList = uncompressStorageGroupList;
  }

  public boolean isHavingWriteWorkloadWhileCompressing() {
    return havingWriteWorkloadWhileCompressing;
  }

  public void setHavingWriteWorkloadWhileCompressing(boolean havingWriteWorkloadWhileCompressing) {
    this.havingWriteWorkloadWhileCompressing = havingWriteWorkloadWhileCompressing;
  }

  public int getChunkQueueCapacity() {
    return chunkQueueCapacity;
  }

  public void setChunkQueueCapacity(int chunkQueueCapacity) {
    this.chunkQueueCapacity = chunkQueueCapacity;
  }

  public String getWindowEstimationMethod() {
    return windowEstimationMethod;
  }

  public void setWindowEstimationMethod(String windowEstimationMethod) {
    this.windowEstimationMethod = windowEstimationMethod;
  }

  public boolean isEnableRecoverRangeQuery() {
    return enableRecoverRangeQuery;
  }

  public void setEnableRecoverRangeQuery(boolean enableRecoverRangeQuery) {
    this.enableRecoverRangeQuery = enableRecoverRangeQuery;
  }

  public long getDataSizeCheckIntervalInSecond() {
    return dataSizeCheckIntervalInSecond;
  }

  public void setDataSizeCheckIntervalInSecond(long dataSizeCheckIntervalInSecond) {
    this.dataSizeCheckIntervalInSecond = dataSizeCheckIntervalInSecond;
  }

  public double getCompressionRatioDiff() {
    return compressionRatioDiff;
  }

  public void setCompressionRatioDiff(double compressionRatioDiff) {
    this.compressionRatioDiff = compressionRatioDiff;
  }

  public double getSigmoInCompressPointExistJudge() {
    return sigmoInCompressPointExistJudge;
  }

  public void setSigmoInCompressPointExistJudge(double sigmoInCompressPointExistJudge) {
    this.sigmoInCompressPointExistJudge = sigmoInCompressPointExistJudge;
  }

  public boolean isRemoveAdjacentReaptedPoint() {
    return removeAdjacentReaptedPoint;
  }

  public void setRemoveAdjacentReaptedPoint(boolean removeAdjacentReaptedPoint) {
    this.removeAdjacentReaptedPoint = removeAdjacentReaptedPoint;
  }

  public boolean isCalculateSampleEntropy() {
    return isCalculateSampleEntropy;
  }

  public void setCalculateSampleEntropy(boolean calculateSampleEntropy) {
    isCalculateSampleEntropy = calculateSampleEntropy;
  }
}
