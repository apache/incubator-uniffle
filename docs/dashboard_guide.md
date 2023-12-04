---
layout: page
displayTitle: Dashboard Guide
title: Dashboard Guide
description: Dashboard Guide
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
# Dashboard Guide

## Summary
This document explains how to install and start Uniffle's dashboard.

### Configure related parameters
In $RSS_HOME/conf directory, configure the dashboard data request port and front-end access port in the coordinator
``` shell
## Front-end access port
rss.dashboard.http.port 19997
## The dashboard request data port, which is the Coordinator's HTTP port
## coordinator.hostname is the hostname or IP address of a Coordinator
coordinator.web.address http://coordinator.hostname:19998/
```

### Start the dashboard process
In the $RSS_HOME/bin directory, start with a script.
``` shell
## Start dashboard
sh start-dashboard.sh

## Close dashboard
sh stop-dashboard.sh
