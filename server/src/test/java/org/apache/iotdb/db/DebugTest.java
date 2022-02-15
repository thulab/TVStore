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
package org.apache.iotdb.db;

import static org.apache.iotdb.db.integration.Constant.count;
import static org.apache.iotdb.db.integration.Constant.min_time;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DebugTest {
  private static IoTDB daemon;
  private static final String TIMESTAMP_STR = "Time";
  private static final String TEMPERATURE_STR = "root.group_5.d.s5";
  @Before
  public void setUp() throws Exception {
    //IoTDBDescriptor.getInstance().getConfig().setBaseDir("/Volumes/sy/data/data");
   // IoTDBDescriptor.getInstance().getConfig().setSy("/Volumes/sy/data/data");

    EnvironmentUtils.closeStatMonitor();
    daemon = IoTDB.getInstance();


    daemon.active();
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
  }

//  @After
//  public void tearDown() throws Exception {
//    daemon.stop();
//    EnvironmentUtils.cleanEnv();
//  }

  @Test
  public void test() throws SQLException {
    try (Connection connection = DriverManager.
        getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet = statement.execute(
          "select count(s5) from root.group_5.d");

      Assert.assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet();) {
        cnt = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(TIMESTAMP_STR) + "," +
              resultSet.getString(count(TEMPERATURE_STR));
          System.out.println(ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

    }
  }
}
