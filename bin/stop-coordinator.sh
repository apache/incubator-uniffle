#!/usr/bin/env bash

#
# Tencent is pleased to support the open source community by making
# Firestorm-Spark remote shuffle server available.
#
# Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use
# this file except in compliance with the License. You may obtain a copy of the
# License at
#
# https://opensource.org/licenses/Apache-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OF ANY KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations under the License.
#

set -o pipefail
set -o nounset   # exit the script if you try to use an uninitialised variable
set -o errexit   # exit the script if any statement returns a non-true return value
set -e

SCRIPT_DIR="$(cd "`dirname "$0"`"; pwd)"
BASE_DIR="$(cd "`dirname "$0"`/.."; pwd)"

source "$SCRIPT_DIR/utils.sh"
common_shutdown "coordinator" ${BASE_DIR}