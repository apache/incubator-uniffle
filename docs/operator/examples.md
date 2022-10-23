<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one or more
  ~ contributor license agreements.  See the NOTICE file distributed with
  ~ this work for additional information regarding copyright ownership.
  ~ The ASF licenses this file to You under the Apache License, Version 2.0
  ~ (the "License"); you may not use this file except in compliance with
  ~ the License.  You may obtain a copy of the License at
  ~
  ~    http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  -->

# Examples

We need to create configMap first which saves coordinators, shuffleServers and log4j's configuration(we can refer
to [configuration](../../deploy/kubernetes/operator/examples/configuration.yaml)).

Coordinator is a stateless service, when upgrading, we can directly update the configuration and then update the image.

Shuffle server is a stateful service, and the upgrade operation is more complicated, so we show examples of different
upgrade modes.

- [Full Upgrade](../../deploy/kubernetes/operator/examples/full-upgrade)
- [Full Restart](../../deploy/kubernetes/operator/examples/full-restart)
- [Partition Upgrade](../../deploy/kubernetes/operator/examples/partition-upgrade)
- [Specific Upgrade](../../deploy/kubernetes/operator/examples/specific-upgrade)
