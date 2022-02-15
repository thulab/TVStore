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

import org.apache.iotdb.db.compress.controll.ExpDelayFunction;
import org.apache.iotdb.db.compress.controll.IDelayFunction;
import org.apache.iotdb.db.compress.controll.PowerFunction;
import org.apache.iotdb.db.compress.controll.PowerLaw;

public class DelayFunction {


  public static void main(String[] args) {

    ExpDelayFunction expDelayFunction = new ExpDelayFunction(1.5);
    PowerFunction powerFunction = new PowerFunction(1.5);
    PowerLaw powerLaw = new PowerLaw(2, 1.5, 10, 1);
    IDelayFunction[] delayFunctionArr = {expDelayFunction, powerFunction, powerLaw};

    int num = 200;
    for(IDelayFunction function : delayFunctionArr){
      System.out.print("[");
      for(int i = 0;i<num;i++){
        System.out.print(function.delayFun(i)+",");
      }
      System.out.print("]");
      System.out.println();
    }


  }



}
