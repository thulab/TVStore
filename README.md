<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# TVStore
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)


TVStore is a time-series store that can automatically bound storage space to a user-provided size. It is capable of constraining storage costs as large volumes of data keep flowing into a single database instance at a high speed.

TVStore incorporates a novel *time-varying compression* framework to compress older data by higher ratios and newer data by lower ratios, while maintaining high accuracy in many workloads. Its decisions on *storage bounding* are properly chosen based on tight theoretical bounds to minimize information loss and computation cost. The implementation of TVStore builds upon Apache IoTDB. For details see our paper at FAST'22:
* Yanzhe An, Yue Su, Yuqing Zhu, and Jianmin Wang. **"TVStore: Automatically Bounding Time Series Storage via Time-Varying Compression."** In 20th USENIX Conference on File and Storage Technologies (FAST 22). 2022. [[pdf](https://www.usenix.org/conference/fast22/technical-sessions)]  

TVStore is extensively evaluated using [TVStore-benchmark](https://github.com/thulab/TVStore-benchmark). Along the same lines, the authors have also the following related works:
* [IginX](https://github.com/thulab/IginX): an open-source clustering system for multi-dimensional scaling of standalone time series databases, e.g., TVStore
* [Record-breaking benchmarking](https://link.springer.com/chapter/10.1007/978-3-030-94437-7_2): tuning a cluster of IginX and IoTDB(by a special branch) instances to get [a record-breaking result](https://arxiv.org/abs/2107.09351) in the TPCx-IoT benchmarking tests

# Quick Start

This short guide will walk you through the basic process of using TVStore.
## Prerequisites

To use TVstore, you need to have:

1. Java >= 1.8 (1.8, 11, and 13 are verified. Please make sure the environment path has been set)
2. Maven >= 3.1 (If you want to compile and install TVstore from source code)
3. Set the max open files num as 65535 to avoid "too many open files" problem.

## Build from source

You can download the source code from:

```
git clone https://github.com/thulab/TVStore
```

Under the root path of TVStore:

```
> mvn clean package -DskipTests
```

Then the binary version (including both server and client) can be found at **distribution/target/apache-iotdb-{project.version}-incubating-bin.zip**

> NOTE: Directories "service-rpc/target/generated-sources/thrift" and "server/target/generated-sources/antlr3" need to be added to sources roots to avoid compilation errors in IDE.

### Configurations

configuration files are under "conf" folder

  * environment config module (`iotdb-env.bat`, `iotdb-env.sh`),
  * system config module (`tsfile-format.properties`, `iotdb-engine.properties`)
  * log config module (`logback.xml`).
  
```
enable_compress=true
#Uncompressed storage groups, if multiple, separated by commas
uncompress_storage_group_list=root.SYSTEM
# Maximum disk capacity
disk_size_up_bound_in_mb=400
# How many thread will be set up to perform compress main tasks, 1 by default.
# Set to 1 when less than or equal to 0. Storage group level concurrency
compress_thread_num=1

# How many thread will be set up to perform compress chunk sub-tasks, 4 by default.
# Set to 1 when less than or equal to 0.That is, the number of concurrent series within a single storage group
compress_chunk_subthread_num=4

# compress_ttl in ms, if timestamp of data is within compress_ttl, then the data won't be compressed.
# when compress_ttl is less than or equal to 0, then compress_ttl is valid, all data can be compressed.
compress_ttl=-1

# compress speed points/s
disk_read_speed=
# compress ratio Proportion of single compression
compress_ratio=2
#compress trigger size ratio, less than 1
compress_trigger_size_ratio=0.9


# delay function name: powerLaw, exponent, power
delay_function=powerLaw

# paramters of delay function. Separate multiple parameters with commas。
# p,q,r,s;
delay_function_parameter=1.5,0.5,10,4

# chunk cache maximum
chunk_queue_capacity=100
# disk size check period
data_size_check_interval_in_second=300
# Ratio of maximum compression ratio to minimum compression ratio between different storage groups
compression_ratio_diff=3
```


## Start

You can go through the following step to test the installation, if there is no error after execution, the installation is completed.

### Start TVStore

Users can start IoTDB by the start-server script under the sbin folder.

```
# Unix/OS X
> sbin/start-server.sh

# Windows
> sbin\start-server.bat
```


### Use TVStore

#### Use Cli

TVStore offers different ways to interact with server, here we introduce basic steps of using Cli tool to insrert and query data.

After installing TVstore, there is a default user 'root', its default password is also 'root'. Users can use this
default user to login Cli to use TVStore. The startup script of Cli is the start-client script in the folder sbin. When executing the script, user should assign
IP, PORT, USER_NAME and PASSWORD. The default parameters are "-h 127.0.0.1 -p 6667 -u root -pw -root".

Here is the command for starting the Cli:

```
# Unix/OS X
> sbin/start-client.sh -h 127.0.0.1 -p 6667 -u root -pw root

# Windows
> sbin\start-client.bat -h 127.0.0.1 -p 6667 -u root -pw root
```

The command line client is interactive so if everything is ready you should see the welcome logo and statements:

```
 _____       _________  ______   ______
|_   _|     |  _   _  ||_   _ `.|_   _ \
  | |   .--.|_/ | | \_|  | | `. \ | |_) |
  | | / .'`\ \  | |      | |  | | |  __'.
 _| |_| \__. | _| |_    _| |_.' /_| |__) |
|_____|'.__.' |_____|  |______.'|_______/  version x.x.x


IoTDB> login successfully
IoTDB>
```

### Stop TVStore

The server can be stopped with ctrl-C or the following script:

```
# Unix/OS X
> sbin/stop-server.sh

# Windows
> sbin\stop-server.bat
```

## Only build server

Under the root path of incubator-iotdb:

```
> mvn clean package -pl server -am -DskipTests
```

After build, the IoTDB server will be at the folder "server/target/iotdb-server-{project.version}".


## Only build client

Under the root path of incubator-iotdb:

```
> mvn clean package -pl client -am -DskipTests
```

After build, the IoTDB client will be at the folder "client/target/iotdb-client-{project.version}".
