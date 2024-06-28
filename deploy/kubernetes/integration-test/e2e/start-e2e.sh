#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -o errexit
set -o nounset
set -o pipefail

TEST_REGISTRY=${TEST_REGISTRY-"docker.io/library"}
BUILD_NEW_CLUSTER=${BUILD_NEW_CLUSTER-"true"}
BUILD_RSS_IMAGE=${BUILD_RSS_IMAGE-"true"}
BUILD_RSS_OPERATOR=${BUILD_RSS_OPERATOR-"true"}

echo "TEST_REGISTRY: $TEST_REGISTRY"
echo "BUILD_NEW_CLUSTER: $BUILD_NEW_CLUSTER"
echo "BUILD_RSS_IMAGE: $BUILD_RSS_IMAGE"
echo "BUILD_RSS_OPERATOR: $BUILD_RSS_OPERATOR"

./set-up-local-cluster.sh --registry "$TEST_REGISTRY" --build-kind-cluster "$BUILD_NEW_CLUSTER" \
          --build-rss-image "$BUILD_RSS_IMAGE" --build-operator "$BUILD_RSS_OPERATOR"
