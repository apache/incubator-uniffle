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
load_rss_env 0

cd "$RSS_HOME"

DASHBOARD_CONF_FILE="${RSS_CONF_DIR}/dashboard.conf"
JAR_DIR="${RSS_HOME}/jars"
LOG_CONF_FILE="${RSS_CONF_DIR}/log4j2.xml"
LOG_PATH="${RSS_LOG_DIR}/dashboard.log"

MAIN_CLASS="org.apache.uniffle.dashboard.web.JettyServerFront"

echo "Check process existence"
is_jvm_process_running "$JPS" $MAIN_CLASS

CLASSPATH=""

for file in $(ls ${JAR_DIR}/dashboard/*.jar 2>/dev/null); do
  CLASSPATH=$CLASSPATH:$file
done

mkdir -p "${RSS_LOG_DIR}"
mkdir -p "${RSS_PID_DIR}"

echo "class path is $CLASSPATH"

JVM_ARGS=" -server \
          -Xmx${XMX_SIZE:-8g} \
          -Xms${XMX_SIZE:-8g} \
          -XX:+UseG1GC \
          -XX:MaxGCPauseMillis=200 \
          -XX:ParallelGCThreads=20 \
          -XX:ConcGCThreads=5 \
          -XX:InitiatingHeapOccupancyPercent=45 \
          -XX:+PrintCommandLineFlags"

GC_LOG_ARGS_LEGACY=" -XX:+PrintGC \
          -XX:+PrintAdaptiveSizePolicy \
          -XX:+PrintGCDateStamps \
          -XX:+PrintGCTimeStamps \
          -XX:+PrintTenuringDistribution \
          -XX:+PrintPromotionFailure \
          -XX:+PrintGCApplicationStoppedTime \
          -XX:+PrintGCCause \
          -XX:+PrintGCDetails \
          -Xloggc:${RSS_LOG_DIR}/gc-%t.log"

GC_LOG_ARGS_NEW=" -XX:+IgnoreUnrecognizedVMOptions \
          -Xlog:gc*:file=${RSS_LOG_DIR}/dashboard-gc-%t.log:tags,uptime,time,level:filecount=5,filesize=102400"


ARGS=""

if [ -f ${LOG_CONF_FILE} ]; then
  ARGS="$ARGS -Dlog4j2.configurationFile=file:${LOG_CONF_FILE} -Dlog.path=${LOG_PATH}"
else
  echo "Exit with error: ${LOG_CONF_FILE} file doesn't exist."
  exit 1
fi

version=$($RUNNER -version 2>&1 | awk -F[\".] '/version/ {print $2}')
if [[ "$version" -lt "9" ]]; then
  GC_ARGS=$GC_LOG_ARGS_LEGACY
else
  GC_ARGS=$GC_LOG_ARGS_NEW
fi

UNIFFLE_DASHBOARD_JAVA_OPTS=${UNIFFLE_DASHBOARD_JAVA_OPTS:-""}
$RUNNER ${UNIFFLE_DASHBOARD_JAVA_OPTS} $ARGS $JVM_ARGS $GC_ARGS -cp $CLASSPATH $MAIN_CLASS --conf "$DASHBOARD_CONF_FILE" $@ &

get_pid_file_name dashboard
echo $! >${RSS_PID_DIR}/${pid_file}
