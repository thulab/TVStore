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

package org.apache.iotdb.db.compress.manage;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.compress.task.CompressTask;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CompressManager provides a ThreadPool to queue and run all compress tasks to restrain the total
 * resources occupied by compress and manages a Timer to periodically issue a compress task.
 */
public class CompressManager implements IService {

  private static final Logger logger = LoggerFactory.getLogger(
      CompressManager.class);
  private static final CompressManager INSTANCE = new CompressManager();

  private AtomicInteger threadCnt = new AtomicInteger();
  private ThreadPoolExecutor compressTaskPool;
  private ThreadPoolExecutor compressChunkSubTaskPool;
  private ThreadPoolExecutor readChunkSubTaskPool;
  private ScheduledExecutorService timedCompressThreadPool;

  private static final long COMPRESS_INTERVAL_IN_SECOND = IoTDBDescriptor.getInstance().getConfig().getDataSizeCheckIntervalInSecond();

  private CompressManager() {
  }

  public static CompressManager getINSTANCE() {
    return INSTANCE;
  }

  public void submitMainTask(CompressTask compressTask) {
    compressTaskPool.submit(compressTask);
  }

  public Future submitChunkSubTask(Callable callable) {
    return compressChunkSubTaskPool.submit(callable);
  }

  public Future submitReadChunkTask(Callable callable) {
    return readChunkSubTaskPool.submit(callable);
  }

  @Override
  public void start() {
    boolean enableCompress = IoTDBDescriptor.getInstance().getConfig().isEnableCompress();
    if(!enableCompress){
      logger.debug("CompressManager won't start as user config.");
      return;
    }
    if (compressTaskPool == null) {
      int threadNum = IoTDBDescriptor.getInstance().getConfig().getCompressThreadNum();
      if (threadNum <= 0) {
        threadNum = 1;
      }

      int chunkSubThreadNum = IoTDBDescriptor.getInstance().getConfig().getCompressChunkSubthreadNum();
      if (chunkSubThreadNum <= 0) {
        chunkSubThreadNum = 1;
      }

      compressTaskPool =
          (ThreadPoolExecutor) Executors.newFixedThreadPool(threadNum,
              r -> new Thread(r, "CompressThread-" + threadCnt.getAndIncrement()));
      compressChunkSubTaskPool =
          (ThreadPoolExecutor) Executors.newFixedThreadPool(threadNum * chunkSubThreadNum,
              r -> new Thread(r, "CompressChunkSubThread-" + threadCnt.getAndIncrement()));
      readChunkSubTaskPool =
          (ThreadPoolExecutor) Executors.newFixedThreadPool(threadNum * chunkSubThreadNum,
              r -> new Thread(r, "ReadChunkSubThread-" + threadCnt.getAndIncrement()));
      long compressIntervalInSecond = COMPRESS_INTERVAL_IN_SECOND;
      if (compressIntervalInSecond > 0) {
        timedCompressThreadPool = Executors.newSingleThreadScheduledExecutor( r -> new Thread(r,
            "TimedCompressThread"));
        timedCompressThreadPool.scheduleAtFixedRate(this::compressAll, compressIntervalInSecond,
            compressIntervalInSecond, TimeUnit.SECONDS);
      }
      logger.info("CompressManager started");
    }
  }

  @Override
  public void stop() {
    if (compressTaskPool != null) {
      if (timedCompressThreadPool != null) {
        timedCompressThreadPool.shutdownNow();
      }
      compressTaskPool.shutdownNow();
      compressChunkSubTaskPool.shutdownNow();
      readChunkSubTaskPool.shutdownNow();
      logger.info("Waiting for task pool to shut down");
      while (!compressTaskPool.isTerminated() || !compressChunkSubTaskPool.isTerminated() ||
          !timedCompressThreadPool.isTerminated() || !readChunkSubTaskPool.isTerminated()) {
        // wait
      }
      compressTaskPool = null;
      compressChunkSubTaskPool = null;
      readChunkSubTaskPool = null;
      timedCompressThreadPool = null;
      logger.info("CompressManager stopped");
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.COMPRESS_SERVICE;
  }



//  private void diskMonitor(){
//    try {
//      Inotify i = new Inotify();
//      InotifyEventListener e = new InotifyEventListener() {
//
//        @Override
//        public void filesystemEventOccurred(InotifyEvent e) {
//          System.out.println("inotify event occurred!");
//        }
//
//        @Override
//        public void queueFull(EventQueueFull e) {
//          System.out.println("inotify event queue: " + e.getSource() +
//              " is full!");
//        }
//
//      };
//      i.addInotifyEventListener(e);
//      i.addWatch(System.getProperty("user.home"), Constants.IN_ACCESS);
//    } catch (UnsatisfiedLinkError e) {
//      System.err.println("unsatisfied link error");
//    } catch (UserLimitException e) {
//      System.err.println("user limit exception");
//    } catch (SystemLimitException e) {
//      System.err.println("system limit exception");
//    } catch (InsufficientKernelMemoryException e) {
//      System.err.println("insufficient kernel memory exception");
//    }
//  }

  private void compressAll() {
    boolean needCompress = MonitorDisk.getInstance().isNeedStartCompress();
    if(needCompress){
      try {
        StorageEngine.getInstance().compressAll();
      } catch (StorageEngineException e) {
        logger.error("Cannot perform a global compress because", e);
      }
    }
  }
}
