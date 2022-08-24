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
set -o errexit   # exit the script if any statement returns a non-true return value
set -e

SCRIPT_DIR="$(cd "`dirname "$0"`"; pwd)"
BASE_DIR="$(cd "`dirname "$0"`/.."; pwd)"

source "$SCRIPT_DIR/rss-env.sh"
if [ -z "$RSS_PID_DIR" ]; then
  RSS_PID_DIR="${BASE_DIR}"
fi

source "$SCRIPT_DIR/utils.sh"

common_shutdown "shuffle-server" "${RSS_PID_DIR}"
