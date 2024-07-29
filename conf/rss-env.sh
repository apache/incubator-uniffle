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

# JAVA_HOME=<java_home_dir>
# HADOOP_HOME=<hadoop_home_dir>
XMX_SIZE="8g" # Coordinator/ShuffleServer JVM XMX size

# Variables for the all components
# RSS_HOME, RSS home directory (Default: parent directory of the script).
# If you want to set it to another place, please make sure the RSS_HOME
# specified externally is respected, by defining it as follows:
#
# RSS_HOME=${RSS_HOME:-{another_rss_home_path}}
#
# RSS_CONF_DIR, RSS configuration directory (Default: ${RSS_HOME}/conf)
# Similar with RSS_HOME, RSS_CONF_DIR should respect external env variable.
#
# RSS_CONF_DIR=${RSS_CONF_DIR:-{another_rss_conf_dir}}

# RSS_CONF_DIR, RSS configuration directory (Default: ${RSS_HOME}/conf)
# HADOOP_CONF_DIR, Hadoop configuration directory (Default: ${HADOOP_HOME}/etc/hadoop)
# RSS_PID_DIR, Where the pid file is stored (Default: ${RSS_HOME})
# RSS_LOG_DIR, Where log files are stored (Default: ${RSS_HOME}/logs)

# Variables for the dashboard
# DASHBOARD_JAVA_OPTS, JVM options for the dashboard, will override the default JVM options
# DASHBOARD_BASE_JVM_ARGS, Base JVM options for the dashboard
# DASHBOARD_XMX_SIZE, Dashboard JVM XMX size (Default: ${XMX_SIZE})

# Variables for the coordinator
# COORDINATOR_JAVA_OPTS, JVM options for the coordinator, will override the default JVM options
# COORDINATOR_BASE_JVM_ARGS, Base JVM options for the coordinator
# COORDINATOR_XMX_SIZE, Coordinator JVM XMX size (Default: ${XMX_SIZE})

# Variables for the shuffle server
# RSS_IP, IP address Shuffle Server binds to on this node (Default: first non-loopback ipv4)
# MAX_DIRECT_MEMORY_SIZE Shuffle Server JVM off heap memory size (Default: not set)
# MALLOC_ARENA_MAX, Set the number of memory arenas for Shuffle Server (Default: 4)
# SHUFFLE_SERVER_JAVA_OPTS, JVM options for the shuffle server, will override the default JVM options
# SHUFFLE_SERVER_BASE_JVM_ARGS, Base JVM options for the shuffle server
# SHUFFLE_SERVER_XMX_SIZE, Shuffle Server JVM XMX size (Default: ${XMX_SIZE})
# SHUFFLE_SERVER_STORAGE_AUDIT_LOG_PATH, Shuffle Server storage audit log path (Default: ${RSS_LOG_DIR}/shuffle_server_storage_audit.log)
# SHUFFLE_SERVER_RPC_AUDIT_LOG_PATH, Shuffle Server RPC audit log path (Default: ${RSS_LOG_DIR}/shuffle_server_rpc_audit.log)
