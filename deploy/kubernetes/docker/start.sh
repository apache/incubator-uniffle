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

basedir='/data/rssadmin/rss'
cd $basedir || exit

coordinator_conf=$basedir'/conf/coordinator.conf'
echo "coordinator_conf: $coordinator_conf"
server_conf=$basedir'/conf/server.conf'
echo "server_conf: $server_conf"


if [ "$SERVICE_NAME" == "coordinator" ];then

    bash ${basedir}/bin/start-coordinator.sh &
    sleep 10
    while : ; do
        pid=$(/usr/bin/lsof -i:"${COORDINATOR_RPC_PORT}" -sTCP:LISTEN)
        if [ "$pid" = "" ]; then
          break
        else
          echo "coordinator pid:$pid is alive"
          sleep 10
        fi
    done
fi

if [ "$SERVICE_NAME" == "server" ];then

    bash ${basedir}/bin/start-shuffle-server.sh &
    sleep 10
    while : ; do
        pid=$(/usr/bin/lsof -i:"$SERVER_RPC_PORT" -sTCP:LISTEN)
        if [ "$pid" = "" ]; then
          break
        else
          echo "shuffle server pid:$pid is alive"
          sleep 10
        fi
    done
fi
