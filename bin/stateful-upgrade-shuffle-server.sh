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

# Touch file to make shuffle-server's state dump
echo "Enable file-existence-based trigger to make state dump"
FILE_NAME=`grep '^rss.server.stateful.upgrade.trigger.status.file.path' $SHUFFLE_SERVER_CONF_FILE |awk '{print $2}'`
touch $FILE_NAME

# Wait the process exit
echo "Wait shuffle-server exit"
get_pid_file_name shuffle-server
PID_FILE_PATH=${RSS_PID_DIR}/${pid_file}
PID=`cat $PID_FILE_PATH`

start=`date +%s`
while:
do
  if is_process_running ${PID}; then
    sleep 2
  else
    break
  fi
done
end=`date +%s`
time=`echo $start $end | awk '{print $2-$1}'`
echo "Shuffle-server exit, costs $time(s)"

# Start from the state
echo "Start shuffle-server from state"
bash ./bin/start-shuffle-server.sh -r
