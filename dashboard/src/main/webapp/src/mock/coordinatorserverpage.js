/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import Mock from 'mockjs'

Mock.mock(/\/coordinator\/conf/, 'get', {
  code: 0,
  data: [
    {
      argumentKey: 'awt.toolkit',
      argumentValue: 'sun.awt.X11.XToolkit'
    },
    {
      argumentKey: 'java.specification.version',
      argumentValue: '11'
    },
    {
      argumentKey: 'sun.cpu.isalist',
      argumentValue: ''
    },
    {
      argumentKey: 'sun.jnu.encoding',
      argumentValue: 'UTF-8'
    },
    {
      argumentKey: 'rss.jetty.http.port',
      argumentValue: '29998'
    },
    {
      argumentKey: 'rss.coordinator.default.app.priority',
      argumentValue: '5'
    },
    {
      argumentKey: 'java.vm.vendor',
      argumentValue: 'Oracle Corporation'
    },
    {
      argumentKey: 'sun.arch.data.model',
      argumentValue: '64'
    },
    {
      argumentKey: 'log4j2.configurationFile',
      argumentValue: 'file:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/conf/log4j2.xml'
    },
    {
      argumentKey: 'java.vendor.url',
      argumentValue: 'http://java.oracle.com/'
    },
    {
      argumentKey: 'rss.coordinator.app.expired',
      argumentValue: '60000'
    },
    {
      argumentKey: 'user.timezone',
      argumentValue: 'Asia/Shanghai'
    },
    {
      argumentKey: 'java.vm.specification.version',
      argumentValue: '11'
    },
    {
      argumentKey: 'os.name',
      argumentValue: 'Linux'
    },
    {
      argumentKey: 'sun.java.launcher',
      argumentValue: 'SUN_STANDARD'
    },
    {
      argumentKey: 'user.country',
      argumentValue: 'US'
    },
    {
      argumentKey: 'sun.boot.library.path',
      argumentValue: '/home/hadoop/jdk-11.0.2/lib'
    },
    {
      argumentKey: 'rss.coordinator.select.partition.strategy',
      argumentValue: 'CONTINUOUS'
    },
    {
      argumentKey: 'sun.java.command',
      argumentValue:
        'org.apache.uniffle.coordinator.CoordinatorServer --conf /root/rss-0.10.0-SNAPSHOT-hadoop2.8/conf/coordinator.conf'
    },
    {
      argumentKey: 'jdk.debug',
      argumentValue: 'release'
    },
    {
      argumentKey: 'sun.cpu.endian',
      argumentValue: 'little'
    },
    {
      argumentKey: 'user.home',
      argumentValue: '/home/hadoop'
    },
    {
      argumentKey: 'rss.coordinator.server.heartbeat.timeout',
      argumentValue: '30000'
    },
    {
      argumentKey: 'user.language',
      argumentValue: 'en'
    },
    {
      argumentKey: 'java.specification.vendor',
      argumentValue: 'Oracle Corporation'
    },
    {
      argumentKey: 'java.version.date',
      argumentValue: '2019-01-15'
    },
    {
      argumentKey: 'java.home',
      argumentValue: '/home/hadoop/jdk-11.0.2'
    },
    {
      argumentKey: 'file.separator',
      argumentValue: '/'
    },
    {
      argumentKey: 'line.separator',
      argumentValue: '\n'
    },
    {
      argumentKey: 'rss.coordinator.quota.default.app.num',
      argumentValue: '25'
    },
    {
      argumentKey: 'java.specification.name',
      argumentValue: 'Java Platform API Specification'
    },
    {
      argumentKey: 'java.vm.specification.vendor',
      argumentValue: 'Oracle Corporation'
    },
    {
      argumentKey: 'java.awt.graphicsenv',
      argumentValue: 'sun.awt.X11GraphicsEnvironment'
    },
    {
      argumentKey: 'sun.management.compiler',
      argumentValue: 'HotSpot 64-Bit Tiered Compilers'
    },
    {
      argumentKey: 'rss.coordinator.dynamicClientConf.enabled',
      argumentValue: 'true'
    },
    {
      argumentKey: 'rss.coordinator.quota.default.path',
      argumentValue: 'file:///root/rss-0.10.0-SNAPSHOT-hadoop2.8/conf/userQuota.properties'
    },
    {
      argumentKey: 'java.runtime.version',
      argumentValue: '11.0.2+9'
    },
    {
      argumentKey: 'user.name',
      argumentValue: 'hadoop'
    },
    {
      argumentKey: 'path.separator',
      argumentValue: ':'
    },
    {
      argumentKey: 'os.version',
      argumentValue: '4.18.0-193.6.3.el8_2.v1.4.x86_64'
    },
    {
      argumentKey: 'java.runtime.name',
      argumentValue: 'OpenJDK Runtime Environment'
    },
    {
      argumentKey: 'rss.coordinator.access.loadChecker.memory.percentage',
      argumentValue: '10.0'
    },
    {
      argumentKey: 'rss.coordinator.dynamicClientConf.path',
      argumentValue: 'file:///root/rss-0.10.0-SNAPSHOT-hadoop2.8/conf/dynamic_client.conf'
    },
    {
      argumentKey: 'file.encoding',
      argumentValue: 'UTF-8'
    },
    {
      argumentKey: 'java.vm.name',
      argumentValue: 'OpenJDK 64-Bit Server VM'
    },
    {
      argumentKey: 'java.vendor.version',
      argumentValue: '18.9'
    },
    {
      argumentKey: 'log.path',
      argumentValue: '/root/cluster-data/logs/coordinator.log'
    },
    {
      argumentKey: 'rss.coordinator.exclude.nodes.file.path',
      argumentValue: 'file:///root/rss-0.10.0-SNAPSHOT-hadoop2.8/conf/exclude_nodes'
    },
    {
      argumentKey: 'java.vendor.url.bug',
      argumentValue: 'http://bugreport.java.com/bugreport/'
    },
    {
      argumentKey: 'java.io.tmpdir',
      argumentValue: '/tmp'
    },
    {
      argumentKey: 'java.version',
      argumentValue: '11.0.2'
    },
    {
      argumentKey: 'user.dir',
      argumentValue: '/root/rss-0.10.0-SNAPSHOT-hadoop2.8'
    },
    {
      argumentKey: 'os.arch',
      argumentValue: 'amd64'
    },
    {
      argumentKey: 'java.vm.specification.name',
      argumentValue: 'Java Virtual Machine Specification'
    },
    {
      argumentKey: 'java.awt.printerjob',
      argumentValue: 'sun.print.PSPrinterJob'
    },
    {
      argumentKey: 'coordinator.id',
      argumentValue: '10.55.123.88-29997'
    },
    {
      argumentKey: 'sun.os.patch.level',
      argumentValue: 'unknown'
    },
    {
      argumentKey: 'rss.rpc.server.port',
      argumentValue: '29997'
    },
    {
      argumentKey: 'java.library.path',
      argumentValue: '/usr/java/packages/lib:/usr/lib64:/lib64:/lib:/usr/lib'
    },
    {
      argumentKey: 'java.vendor',
      argumentValue: 'Oracle Corporation'
    },
    {
      argumentKey: 'java.vm.info',
      argumentValue: 'mixed mode'
    },
    {
      argumentKey: 'java.vm.version',
      argumentValue: '11.0.2+9'
    },
    {
      argumentKey: 'sun.io.unicode.encoding',
      argumentValue: 'UnicodeLittle'
    },
    {
      argumentKey: 'rss.coordinator.shuffle.nodes.max',
      argumentValue: '2'
    },
    {
      argumentKey: 'java.class.version',
      argumentValue: '55.0'
    }
  ],
  errMsg: 'success'
})

Mock.mock(/\/coordinator\/info/, 'get', {
  code: 0,
  data: {
    serverIp: '192.168.1.1',
    coordinatorId: '192.168.1.1-29997',
    serverPort: '29997',
    serverWebPort: '29998'
  },
  errMsg: 'success'
})

export default Mock
