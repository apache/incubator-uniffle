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
set -x

source "$(dirname "$0")/utils.sh"
check_use_java8

ASF_USERNAME=${ASF_USERNAME:?"ASF_USERNAME is required"}
ASF_PASSWORD=${ASF_PASSWORD:?"ASF_PASSWORD is required"}

PROJECT_DIR="$(cd "$(dirname "$0")"/..; pwd)"
cd $PROJECT_DIR

upload_nexus_staging() {
  echo "Deploying rss-client-spark2-shaded"
  mvn clean install -DskipTests -Papache-release,spark2 \
    -s "${PROJECT_DIR}/release/asf-settings.xml" \
    -pl :rss-client-spark2-shaded -Dmaven.javadoc.skip=true -am
  mvn deploy -DskipTests -Papache-release,spark2 \
    -s "${PROJECT_DIR}/release/asf-settings.xml" \
    -pl :rss-client-spark2-shaded -Dmaven.javadoc.skip=true

  echo "Deploying rss-client-spark3-shaded"
  mvn clean install -DskipTests -Papache-release,spark3 \
    -s "${PROJECT_DIR}/release/asf-settings.xml" \
    -pl :rss-client-spark3-shaded -Dmaven.javadoc.skip=true -am
  mvn deploy -DskipTests -Papache-release,spark3 \
    -s "${PROJECT_DIR}/release/asf-settings.xml" \
    -pl :rss-client-spark3-shaded -Dmaven.javadoc.skip=true
}

upload_nexus_staging
