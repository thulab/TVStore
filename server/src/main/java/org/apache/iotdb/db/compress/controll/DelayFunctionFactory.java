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
package org.apache.iotdb.db.compress.controll;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DelayFunctionFactory {

  private static IDelayFunction instance=new DelayFunctionFactory().getDelayFunction();

  private IDelayFunction function;
  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final Logger logger = LoggerFactory.getLogger(DelayFunctionFactory.class);

  private DelayFunctionFactory() {
    String delayFunction = config.getDelayFunction();
    String parameters = config.getDalayFunctionParameters();
    function = generateDelayFunction(delayFunction, parameters);
  }

  public static IDelayFunction generateDelayFunction(String delayFunction, String parameters) {
    //检查用户指定的衰减函数
    if(delayFunction.equalsIgnoreCase("powerlaw")){
      String[] parameterArray = parameters.split(",");
      double p = Double.parseDouble(parameterArray[0]);
      double q = Double.parseDouble(parameterArray[1]);
      double r = Double.parseDouble(parameterArray[2]);
      double s = Double.parseDouble(parameterArray[3]);
      return new PowerLaw(p, q, r, s);
    }
    else if(delayFunction.equalsIgnoreCase("power")){
      double num = Double.parseDouble(parameters);
      return new PowerFunction(num);
    }
    else if(delayFunction.equalsIgnoreCase("exponent")){
      double num = Double.parseDouble(parameters);
      return new ExpDelayFunction(num);
    }
    else{
      logger.error("unsupported delay function {}, param {}", delayFunction, parameters);
      return null;
    }
  }

  public static IDelayFunction getInstance() {
    return instance;
  }

  private IDelayFunction getDelayFunction(){
    return function;
  }
}
