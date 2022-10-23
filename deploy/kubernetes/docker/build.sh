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

function exit_with_usage() {
  set +x
  echo "./build.sh - Tool for building docker images of Remote Shuffle Service"
  echo ""
  echo "Usage:"
  echo "+------------------------------------------------------------------------------------------------------+"
  echo "| ./build.sh [--hadoop-version <hadoop version>] [--registry <registry url>] [--author <author name>]  |"
  echo "+------------------------------------------------------------------------------------------------------+"
  exit 1
}

UNKNOWN_REGISTRY="UNKNOWN_REGISTRY"

REGISTRY=$UNKNOWN_REGISTRY
HADOOP_VERSION=2.8.5
AUTHOR=$(whoami)

while (( "$#" )); do
  case $1 in
    --registry)
      REGISTRY="$2"
      shift
      ;;
    --hadoop-version)
      HADOOP_VERSION="$2"
      shift
      ;;
    --author)
      AUTHOR="$2"
      shift
      ;;
    --help)
      exit_with_usage
      ;;
    --*)
      echo "Error: $1 is not supported"
      exit_with_usage
      ;;
    -*)
      break
      ;;
    *)
      echo "Error: $1 is not supported"
      exit_with_usage
      ;;
  esac
  shift
done

if [ "$REGISTRY" == $UNKNOWN_REGISTRY ]; \
  then echo "please set registry url"; exit; \
fi

HADOOP_FILE=hadoop-${HADOOP_VERSION}.tar.gz
HADOOP_URL=https://archive.apache.org/dist/hadoop/core/hadoop-${HADOOP_VERSION}/${HADOOP_FILE}
echo "HADOOP_URL is $HADOOP_URL"
if [ ! -e "$HADOOP_FILE" ]; \
  then wget "$HADOOP_URL"; \
  else echo "${HADOOP_FILE} has been downloaded"; \
fi

RSS_DIR=../../..
cd $RSS_DIR || exit
RSS_VERSION=$(mvn help:evaluate -Dexpression=project.version 2>/dev/null | grep -v "INFO" | grep -v "WARNING" | tail -n 1)
RSS_FILE=rss-${RSS_VERSION}.tgz
if [ ! -e "$RSS_FILE" ]; \
  then sh ./build_distribution.sh; \
  else echo "$RSS_FILE has been built"; \
fi
cd "$OLDPWD" || exit
cp "$RSS_DIR/$RSS_FILE" .

GIT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
GIT_COMMIT=$(git describe --dirty --always --tags | sed 's/-/./g')
echo "image version: ${IMAGE_VERSION:=$RSS_VERSION-$GIT_COMMIT}"
IMAGE=$REGISTRY/rss-server:$IMAGE_VERSION
echo "building image: $IMAGE"
docker build -t "$IMAGE" \
             --build-arg RSS_VERSION="$RSS_VERSION" \
             --build-arg HADOOP_VERSION="$HADOOP_VERSION" \
             --build-arg AUTHOR="$AUTHOR" \
             --build-arg GIT_COMMIT="$GIT_COMMIT" \
             --build-arg GIT_BRANCH="$GIT_BRANCH" \
             -f Dockerfile --no-cache .

echo "pushing image: $IMAGE"
docker push "$IMAGE"
