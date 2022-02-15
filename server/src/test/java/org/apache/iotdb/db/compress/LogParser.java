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
package org.apache.iotdb.db.compress;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class LogParser {

  // 11ge
  public static void main(String[] args) throws IOException {
   // File file = new File("/Users/suyue/lab/code/apache/incubator-iotdb/server/target/iotdb-server-0.9.1/logs/log_error.log");

    File file = new File("/Users/suyue/lab/code/apache/incubator-iotdb/server/log");
    FileReader reader = new FileReader(file);
    BufferedReader bufferedReader = new BufferedReader(reader);
    String line = null;
    int cnt=0;
    while ((line = bufferedReader.readLine())!=null){
     cnt+=5;
     System.out.println(line.split(" ")[11]);
    }
    bufferedReader.close();
    reader.close();
  }
}
