---
layout: page
displayTitle: Uniffle Migration Guide
title: Uniffle Migration Guide
description: Uniffle Migration Guide
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

# Upgrading from Coordinator 0.6 to 0.7

+ Since we have reconstructed the class file under coordinator, for the `rss.coordinator.access.checkers` parameter, the original value `org.apache.unifle.coordinator.AccessClusterLoadChecker` has been replaced with `org.apache.unifle.coordinator.access.checker.AccessClusterLoadChecker`, `org.apache.unifle.coordinator.AccessCandidatesChecker` has been replaced with `org.apache.unifle.coordinator.access.checker.AccessCandidatesChecker`, In addition, `org.apache.unifle.coordinator.access.checker.AccessQuotaChecker` has been added as the default checker.