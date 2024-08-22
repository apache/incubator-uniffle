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

# Common utils
set -o nounset   # exit the script if you try to use an uninitialised variable
set -o errexit   # exit the script if any statement returns a non-true return value

function check_use_java8() {
    set +o nounset
    if [ -n "${JAVA_HOME}" ]; then
      JAVA="${JAVA_HOME}/bin/java"
    elif [ "$(command -v java)" ]; then
      JAVA="java"
    else
      echo "JAVA_HOME is not set" >&2
      exit 1
    fi
    set -o nounset

    JAVA_VERSION=$($JAVA -version 2>&1 | awk -F '"' '/version/ {print $2}')
    if [[ $JAVA_VERSION != 1.8.* ]]; then
      echo "Unexpected Java version: $JAVA_VERSION. Java 8 is required for release."
      exit 1
    fi
}

