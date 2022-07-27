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

CONF_FILE="./conf/server.conf "
MAIN_CLASS="com.tencent.rss.client.tools.Decommission"

JAR_DIR="./jars"
CLASSPATH=""

for file in $(ls ${JAR_DIR}/server/*.jar 2>/dev/null); do
  CLASSPATH=$CLASSPATH:$file
done

ARGS=""
if [ -f ./conf/log4j.properties ]; then
  ARGS="$ARGS -Dlog4j.configuration=file:./conf/log4j.properties"
else
  echo "Exit with error: $conf/log4j.properties file doesn't exist."
  exit 1
fi

if [ ! -n "$1" ]; then
  echo "usage:"$0" on/off"
  exit 1
fi

$RUNNER $ARGS -cp $CLASSPATH $MAIN_CLASS --conf $CONF_FILE -s $1
