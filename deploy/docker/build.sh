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

set -o pipefail
set -o nounset   # exit the script if you try to use an uninitialised variable
set -o errexit   # exit the script if any statement returns a non-true return value

EXAMPLE_DIR=$(cd "$(dirname "$0")"; pwd)
RSS_DIR="$EXAMPLE_DIR/../.."

# build RSS docker image
cd "$RSS_DIR/deploy/kubernetes/docker"
IMAGE_VERSION=head ./build.sh --push-image false

# patch conf/server.conf
cp "$RSS_DIR/conf/server.conf" "$EXAMPLE_DIR/uniffle/"
sed -i -e "s%rss.storage.basePath .*%rss.storage.basePath /tmp/rss%" "$EXAMPLE_DIR/uniffle/server.conf"
sed -i -e "s/rss.storage.type .*/rss.storage.type MEMORY_LOCALFILE/" "$EXAMPLE_DIR/uniffle/server.conf"
sed -i -e "s/rss.coordinator.quorum .*/rss.coordinator.quorum rss-coordinator-1:19999,rss-coordinator-2:19999/" "$EXAMPLE_DIR/uniffle/server.conf"
sed -i -e "s/rss.server.buffer.capacity .*/rss.server.buffer.capacity 200mb/" "$EXAMPLE_DIR/uniffle/server.conf"
sed -i -e "s/rss.server.read.buffer.capacity .*/rss.server.read.buffer.capacity 100mb/" "$EXAMPLE_DIR/uniffle/server.conf"
sed -i -e "s/rss.server.disk.capacity .*/rss.server.disk.capacity 100m/" "$EXAMPLE_DIR/uniffle/server.conf"

# build RSS example docker image
docker build -t rss-server-example "$EXAMPLE_DIR/uniffle"

# build Spark example docker image
cp "$RSS_DIR/client-spark/spark3-shaded/target/"rss-client-spark3-shaded-*.jar "$EXAMPLE_DIR/spark/"
docker build -t rss-spark-example "$EXAMPLE_DIR/spark"

