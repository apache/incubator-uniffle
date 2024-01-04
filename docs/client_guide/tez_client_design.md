---
layout: page
displayTitle: Deploy Tez Client Plugin & Configurations
title: Deploy Tez Client Plugin & Configurations
description: Deploy Tez Client Plugin & Configurations
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

# Tez Client On RSS Design
This docs describe the design about Tez Client on RSS, include why and how to design it.

## Why design Tez Client
Now, uniffle support Spark and MR client，whereas does not support tez.
Considering tez is also widely used, so design client-tez module and implements the function.

## How to implement Tez Client
Before introduce design, let's show you the local tez shuffle:

![tez_local_shuffle](../asset/tez_local_shuffle.png)

From the flow diagram, we find that tez client interact 















