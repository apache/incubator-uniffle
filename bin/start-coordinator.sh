#!/usr/bin/env bash

#
# Tencent is pleased to support the open source community by making
# Firestorm-Spark remote shuffle server available.
#
# Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use
# this file except in compliance with the License. You may obtain a copy of the
# License at
#
# https://opensource.org/licenses/Apache-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OF ANY KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations under the License.
#

set -o pipefail
set -e

COORDINATOR_HOME="$(
  cd "$(dirname "$0")/.."
  pwd
)"
CONF_FILE="./conf/coordinator.conf "
MAIN_CLASS="com.tencent.rss.coordinator.CoordinatorServer"

source "${COORDINATOR_HOME}/bin/rss-env.sh"
source "${COORDINATOR_HOME}/bin/utils.sh"

echo "Check process existence"
is_jvm_process_running $JPS $MAIN_CLASS

cd $COORDINATOR_HOME

JAR_DIR="./jars"
CLASSPATH=""

for file in $(ls ${JAR_DIR}/coordinator/*.jar 2>/dev/null); do
  CLASSPATH=$CLASSPATH:$file
done

echo "class path is $CLASSPATH"

JVM_ARGS=" -server \
          -Xmx8g \
          -XX:+UseG1GC \
          -XX:MaxGCPauseMillis=200 \
          -XX:ParallelGCThreads=20 \
          -XX:ConcGCThreads=5 \
          -XX:InitiatingHeapOccupancyPercent=45 "

ARGS=""
if [ -f ./conf/log4j.properties ]; then
  ARGS="$ARGS -Dlog4j.configuration=file:./conf/log4j.properties"
else
  echo "Exit with error: $conf/log4j.properties file doesn't exist."
  exit 1
fi

$RUNNER $ARGS $JVM_ARGS -cp $CLASSPATH $MAIN_CLASS --conf $CONF_FILE $@ &

echo $! >$COORDINATOR_HOME/currentpid
