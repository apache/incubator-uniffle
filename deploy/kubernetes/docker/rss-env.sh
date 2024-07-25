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

HADOOP_HOME="/data/rssadmin/hadoop"
RUNNER="${JAVA_HOME}/bin/java"
JPS="${JAVA_HOME}/bin/jps"

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
# LOG_CONF_FILE, Log4j2 configuration file (Default: ${RSS_CONF_DIR}/log4j2.xml)

# Variables for the dashboard
# UNIFFLE_DASHBOARD_JAVA_OPTS, JVM options for the dashboard, will override the default JVM options
# UNIFFLE_DASHBOARD_DEFAULT_JVM_ARGS, Default JVM options for the dashboard
# UNIFFLE_DASHBOARD_XMX_SIZE, Dashboard JVM XMX size (Default: ${XMX_SIZE})
# DASHBOARD_CONF_FILE, Dashboard configuration file (Default: ${RSS_CONF_DIR}/dashboard.conf)

# Variables for the coordinator
# UNIFFLE_COORDINATOR_JAVA_OPTS, JVM options for the coordinator, will override the default JVM options
# UNIFFLE_COORDINATOR_DEFAULT_JVM_ARGS, Default JVM options for the coordinator
# UNIFFLE_COORDINATOR_XMX_SIZE, Coordinator JVM XMX size (Default: ${XMX_SIZE})
# COORDINATOR_CONF_FILE, Coordinator configuration file (Default: ${RSS_CONF_DIR}/coordinator.conf)

# Variables for the shuffle server
# RSS_IP, IP address Shuffle Server binds to on this node (Default: first non-loopback ipv4)
# MAX_DIRECT_MEMORY_SIZE Shuffle Server JVM off heap memory size (Default: not set)
# MALLOC_ARENA_MAX, Set the number of memory arenas for Shuffle Server (Default: 4)
# UNIFFLE_SHUFFLE_SERVER_JAVA_OPTS, JVM options for the shuffle server, will override the default JVM options
# UNIFFLE_SHUFFLE_SERVER_DEFAULT_JVM_ARGS, Default JVM options for the shuffle server
# UNIFFLE_SHUFFLE_SERVER_XMX_SIZE, Shuffle Server JVM XMX size (Default: ${XMX_SIZE})
# SHUFFLE_SERVER_CONF_FILE, Shuffle Server configuration file (Default: ${RSS_CONF_DIR}/server.conf)
# SHUFFLE_SERVER_STORAGE_AUDIT_LOG_PATH, Shuffle Server storage audit log path (Default: ${RSS_LOG_DIR}/shuffle_server_storage_audit.log)
# SHUFFLE_SERVER_RPC_AUDIT_LOG_PATH, Shuffle Server RPC audit log path (Default: ${RSS_LOG_DIR}/shuffle_server_rpc_audit.log)
