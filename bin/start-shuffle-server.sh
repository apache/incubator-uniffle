#!/usr/bin/env bash

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
set -e

SHUFFLE_SERVER_HOME="$(
  cd "$(dirname "$0")/.."
  pwd
)"

cd $SHUFFLE_SERVER_HOME

source "${SHUFFLE_SERVER_HOME}/bin/rss-env.sh"
source "${SHUFFLE_SERVER_HOME}/bin/utils.sh"

if [ -z "$HADOOP_HOME" ]; then
  echo "No env HADOOP_HOME."
  exit 1
fi

HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
HADOOP_DEPENDENCY=`$HADOOP_HOME/bin/hadoop classpath --glob`

CONF_FILE="./conf/server.conf "
MAIN_CLASS="org.apache.uniffle.server.ShuffleServer"

echo "Check process existence"
is_jvm_process_running $JPS $MAIN_CLASS

JAR_DIR="./jars"
CLASSPATH=""

for file in $(ls ${JAR_DIR}/server/*.jar 2>/dev/null); do
  CLASSPATH=$CLASSPATH:$file
done

if [ -z "$HADOOP_CONF_DIR" ]; then
  echo "No env HADOOP_CONF_DIR."
  exit 1
fi

if [ -z "$XMX_SIZE" ]; then
  echo "No jvm xmx size"
  exit 1
fi

echo "Using Hadoop from $HADOOP_HOME"

CLASSPATH=$CLASSPATH:$HADOOP_CONF_DIR:$HADOOP_DEPENDENCY
JAVA_LIB_PATH="-Djava.library.path=$HADOOP_HOME/lib/native"

JVM_ARGS=" -server \
          -Xmx${XMX_SIZE} \
          -Xms${XMX_SIZE} \
          -XX:+UseG1GC \
          -XX:MaxGCPauseMillis=200 \
          -XX:ParallelGCThreads=20 \
          -XX:ConcGCThreads=5 \
          -XX:InitiatingHeapOccupancyPercent=20 \
          -XX:G1HeapRegionSize=32m \
          -XX:+UnlockExperimentalVMOptions \
          -XX:G1NewSizePercent=10 \
          -XX:+PrintGC \
          -XX:+PrintAdaptiveSizePolicy \
          -XX:+PrintGCDateStamps \
          -XX:+PrintGCTimeStamps \
          -XX:+PrintGCDetails \
          -Xloggc:./logs/gc-%t.log"

ARGS=""

LOG_CONF_FILE="./conf/log4j.shuffle_server.properties"
if [ -f ${LOG_CONF_FILE} ]; then
  ARGS="$ARGS -Dlog4j.configuration=file:${LOG_CONF_FILE}"
else
  echo "Exit with error: ${LOG_CONF_FILE} file doesn't exist."
  exit 1
fi

$RUNNER $ARGS $JVM_ARGS $JAVA_LIB_PATH -cp $CLASSPATH $MAIN_CLASS --conf $CONF_FILE $@ &

echo $! >$SHUFFLE_SERVER_HOME/currentpid
