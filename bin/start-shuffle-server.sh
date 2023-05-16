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
set -o nounset   # exit the script if you try to use an uninitialised variable
set -o errexit   # exit the script if any statement returns a non-true return value

source "$(dirname "$0")/utils.sh"
load_rss_env

cd "$RSS_HOME"

SHUFFLE_SERVER_CONF_FILE="${RSS_CONF_DIR}/server.conf"
JAR_DIR="${RSS_HOME}/jars"
LOG_CONF_FILE="${RSS_CONF_DIR}/log4j.properties"
LOG_PATH="${RSS_LOG_DIR}/shuffle_server.log"
OUT_PATH="${RSS_LOG_DIR}/shuffle_server.out"

if [ -z "${XMX_SIZE:-}" ]; then
  echo "No env XMX_SIZE."
  exit 1
fi
echo "Shuffle Server JVM XMX size: ${XMX_SIZE}"
if [ -n "${RSS_IP:-}" ]; then
  echo "Shuffle Server RSS_IP: ${RSS_IP}"
fi

# glibc use an arena memory allocator that causes virtual
# memory usage to explode. This interacts badly
# with the many threads that we use in rss. Tune the variable
# down to prevent vmem explosion.
# glibc's default value of MALLOC_ARENA_MAX is 8 * CORES.
# After referring to hadoop/presto, the default MALLOC_ARENA_MAX of shuffle server is set to 4.
export MALLOC_ARENA_MAX=${MALLOC_ARENA_MAX:-4}

MAIN_CLASS="org.apache.uniffle.server.ShuffleServer"

HADOOP_DEPENDENCY="$("$HADOOP_HOME/bin/hadoop" classpath --glob)"

echo "Check process existence"
RPC_PORT=`grep '^rss.rpc.server.port' $SHUFFLE_SERVER_CONF_FILE |awk '{print $2}'`
is_port_in_use $RPC_PORT


CLASSPATH=""

for file in $(ls ${JAR_DIR}/server/*.jar 2>/dev/null); do
  CLASSPATH=$CLASSPATH:$file
done

mkdir -p "${RSS_LOG_DIR}"
mkdir -p "${RSS_PID_DIR}"

CLASSPATH=$CLASSPATH:$HADOOP_CONF_DIR:$HADOOP_DEPENDENCY
JAVA_LIB_PATH="-Djava.library.path=$HADOOP_HOME/lib/native"

echo "class path is $CLASSPATH"

MAX_DIRECT_MEMORY_OPTS=""
if [ -n "${MAX_DIRECT_MEMORY_SIZE:-}" ]; then
  MAX_DIRECT_MEMORY_OPTS="-XX:MaxDirectMemorySize=$MAX_DIRECT_MEMORY_SIZE"
fi

JVM_ARGS=" -server \
          -Xmx${XMX_SIZE} \
          -Xms${XMX_SIZE} \
          ${MAX_DIRECT_MEMORY_OPTS} \
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
          -Xloggc:${RSS_LOG_DIR}/gc-%t.log"

JAVA11_EXTRA_ARGS=" -XX:+IgnoreUnrecognizedVMOptions \
          -Xlog:gc:tags,time,uptime,level"

ARGS=""

if [ -f ${LOG_CONF_FILE} ]; then
  ARGS="$ARGS -Dlog4j.configuration=file:${LOG_CONF_FILE} -Dlog.path=${LOG_PATH}"
else
  echo "Exit with error: ${LOG_CONF_FILE} file doesn't exist."
  exit 1
fi

$RUNNER $ARGS $JVM_ARGS $JAVA11_EXTRA_ARGS $JAVA_LIB_PATH -cp $CLASSPATH $MAIN_CLASS --conf "$SHUFFLE_SERVER_CONF_FILE" $@ &> $OUT_PATH &

get_pid_file_name shuffle-server
echo $! >${RSS_PID_DIR}/${pid_file}
