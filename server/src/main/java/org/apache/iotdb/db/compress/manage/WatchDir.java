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

/*
 * This source code is provided to illustrate the usage of a given feature
 * or technique and has been deliberately simplified. Additional steps
 * required for a production-quality application, such as security checks,
 * input validation and proper error handling, might not be present in
 * this sample code.
 */


import java.nio.file.*;
import static java.nio.file.StandardWatchEventKinds.*;
import static java.nio.file.LinkOption.*;
import java.nio.file.attribute.*;
import java.io.IOException;

/**
 * Example to watch a directory (or tree) for changes to files.
 */

public class WatchDir {

  private final WatchService watcher;
  private final boolean recursive;
  private boolean trace = false;
  private int count;

  @SuppressWarnings("unchecked")
  static <T> WatchEvent<T> cast(WatchEvent<?> event) {
    return (WatchEvent<T>)event;
  }

  /**
   * Register the given directory with the WatchService
   */
  private void register(Path dir) throws IOException {
    WatchKey key = dir.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
    count++;
    if (trace)
      System.out.format("register: %s\n", dir);
  }

  /**
   * Register the given directory, and all its sub-directories, with the
   * WatchService.
   */
  private void registerAll(final Path start) throws IOException {
    // register directory and sub-directories
    Files.walkFileTree(start, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
          throws IOException
      {
        register(dir);
        return FileVisitResult.CONTINUE;
      }
    });
  }

  /**
   * Creates a WatchService and registers the given directory
   */
  WatchDir(Path dir, boolean recursive) throws IOException {
    this.watcher = FileSystems.getDefault().newWatchService();
    this.recursive = recursive;

    if (recursive) {
      System.out.format("Scanning %s ...\n", dir);
      registerAll(dir);
      System.out.println("Done.");
    } else {
      register(dir);
    }

    // enable trace after initial registration
    this.trace = true;
  }

  /**
   * Process all events for keys queued to the watcher
   */
  void processEvents() {
    for (;;) {

      // wait for key to be signalled
      WatchKey key;
      try {
        key = watcher.take();
      } catch (InterruptedException x) {
        return;
      }

      for (WatchEvent<?> event: key.pollEvents()) {
        WatchEvent.Kind kind = event.kind();

        // TBD - provide example of how OVERFLOW event is handled
        if (kind == OVERFLOW) {
          continue;
        }

        // Context for directory entry event is the file name of entry
        WatchEvent<Path> ev = cast(event);
        Path name = ev.context();
        Path child = ((Path)key.watchable()).resolve(name);

        // print out event
        System.out.format("%s: %s\n", event.kind().name(), child);

        // if directory is created, and watching recursively, then
        // register it and its sub-directories
        if (recursive && (kind == ENTRY_CREATE)) {
          checkCompress();
          try {
            if (Files.isDirectory(child, NOFOLLOW_LINKS)) {
              registerAll(child);
            }
          } catch (IOException x) {
            // ignore to keep sample readbale
          }
        }
      }

      // reset key
      boolean valid = key.reset();
      if (!valid) {
        // directory no longer accessible
        count--;
        if (count == 0)
          break;
      }
    }
  }

  private void checkCompress() {

  }

  static void usage() {
    System.err.println("usage: java WatchDir [-r] dir");
    System.exit(-1);
  }

  public static void main(String[] args) throws IOException {
    // parse arguments
    args=new String[]{"-r","/Users/suyue/lab/code/apache/incubator-iotdb/data"};
    if (args.length == 0 || args.length > 2)
      usage();
    boolean recursive = false;
    int dirArg = 0;
    if (args[0].equals("-r")) {
      if (args.length < 2)
        usage();
      recursive = true;
      dirArg++;
    }

    // register directory and process its events
    Path dir = Paths.get(args[dirArg]);
    new WatchDir(dir, recursive).processEvents();
  }
}