---
layout: page
displayTitle: Uniffle Shuffle Server Guide
title: Uniffle Shuffle Server Guide
description: Uniffle Shuffle Server Guide
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---
# Troubleshooting

## Where is the Uniffle log file?

Uniffle logs are stored in the `$RSS_LOG_DIR`, which defaults to `${RSS_HOME}/logs`. The common log file names are `coordinator.log`, `shuffle-server.log`, `dashboard.log`.

## Audit logs

The Uniffle cluster provides audit logs for each process. You can also find audit logs in the log directory, the log file names are `coordinator_rpc_audit.log`, `shuffle_server_rpc_audit.log`, `shuffle_server_storage_audit.log`.

| Audit log name                   | Configuration                         | Default | Description                                                                 |
|----------------------------------|---------------------------------------|---------|-----------------------------------------------------------------------------|
| coordinator rpc audit log        | rss.coordinator.rpc.audit.log.enabled | true    | Record coordinator rpc operation audit.                                     |
| shuffle server rpc audit log     | rss.server.rpc.audit.log.enabled      | true    | Record shuffle server rpc operation audit.                                  |
| shuffle server storage audit log | rss.server.storage.audit.log.enabled  | false   | The server will log audit records for every disk write and delete operation |

Based on the above audit logs, you can check the operation details and the operation time cost.

## Uniffle remote debug

### Debugging Uniffle processes

Java remote debugging makes it easier to debug Uniffle at the source level without modifying any code. You will need to set the JVM remote debugging parameters before starting the process. There are several ways to add the remote debugging parameters; you can export the following configuration properties in shell or conf/rss-env.sh:

```shell
# Java 8
export DASHBOARD_JAVA_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5004"
export COORDINATOR_JAVA_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5006"
export SHUFFLE_SERVER_JAVA_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005"

# Java 11
export DASHBOARD_JAVA_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=*:5004"
export COORDINATOR_JAVA_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=*:5006"
export SHUFFLE_SERVER_JAVA_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=*:5005"
```

In general, you can use `<PROCESS>_JAVA_OPTS` to specify how an Uniffle process should be attached to.

suspend={y | n} will decide whether the JVM process waits until the debugger connects or not.

address determines which port the Uniffle process will use to be attached to by a debugger. If left blank, it will choose an open port by itself.

After completing this setup, learn how [To attach](#to-attach).

### To attach

There exists a [comprehensive tutorial on how to attach to and debug a Java process in IntelliJ](https://www.jetbrains.com/help/idea/attaching-to-local-process.html).

Start the process or a shell command of interest, then create a new java remote configuration,
set the debug server's host and port, and start the debug session. If you set a breakpoint which can be reached, the IDE
will enter debug mode. You can inspect the current context's variables, call stack, thread list, and expression
evaluation.

## Resource Leak Detection

If you are operating your Uniffle cluster it is possible you may notice a message in the logs like:

```
[ERROR] ResourceLeakDetector - LEAK: ByteBuf.release() was not called before it's garbage-collected. See https://netty.io/wiki/reference-counted-objects.html for more information.
```

Uniffle has a built-in detection mechanism to help identify potential resource leaks. This message indicates there is a bug in the Uniffle code which is causing a resource leak. If this message appears during cluster operation, please open a GitHub Issue as a bug report and share your log message and any relevant stack traces that are shared with it.