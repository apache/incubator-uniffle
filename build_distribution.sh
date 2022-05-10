#! /usr/bin/env bash

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
set -e
set -x
set -u

NAME="rss"
MVN="mvn"
RSS_HOME="$(
  cd "$(dirname "$0")"
  pwd
)"

function exit_with_usage() {
  set +x
  echo "$0 - tool for making binary distributions of Rmote Shuffle Service"
  echo ""
  echo "usage:"
  exit 1
}

cd $RSS_HOME

if [ -z "$JAVA_HOME" ]; then
  echo "Error: JAVA_HOME is not set, cannot proceed."
  exit -1
fi

if [ $(command -v git) ]; then
  GITREV=$(git rev-parse --short HEAD 2>/dev/null || :)
  if [ ! -z "$GITREV" ]; then
    GITREVSTRING=" (git revision $GITREV)"
  fi
  unset GITREV
fi

VERSION=$("$MVN" help:evaluate -Dexpression=project.version $@ 2>/dev/null |
  grep -v "INFO" |
  grep -v "WARNING" |
  tail -n 1)

echo "RSS version is $VERSION"

export MAVEN_OPTS="${MAVEN_OPTS:--Xmx2g -XX:ReservedCodeCacheSize=1g}"

# Store the command as an array because $MVN variable might have spaces in it.
# Normal quoting tricks don't work.
# See: http://mywiki.wooledge.org/BashFAQ/050
BUILD_COMMAND=("$MVN" clean package -DskipTests $@)

# Actually build the jar
echo -e "\nBuilding with..."
echo -e "\$ ${BUILD_COMMAND[@]}\n"

"${BUILD_COMMAND[@]}"


# Make directories
DISTDIR="rss-$VERSION"
rm -rf "$DISTDIR"
mkdir -p "${DISTDIR}/jars"
echo "RSS ${VERSION}${GITREVSTRING} built" >"${DISTDIR}/RELEASE"
echo "Build flags: $@" >>"$DISTDIR/RELEASE"
mkdir -p "${DISTDIR}/logs"

SERVER_JAR_DIR="${DISTDIR}/jars/server"
mkdir -p $SERVER_JAR_DIR
SERVER_JAR="${RSS_HOME}/server/target/shuffle-server-${VERSION}.jar"
echo "copy $SERVER_JAR to ${SERVER_JAR_DIR}"
cp $SERVER_JAR ${SERVER_JAR_DIR}
cp "${RSS_HOME}"/server/target/jars/* ${SERVER_JAR_DIR}

COORDINATOR_JAR_DIR="${DISTDIR}/jars/coordinator"
mkdir -p $COORDINATOR_JAR_DIR
COORDINATOR_JAR="${RSS_HOME}/coordinator/target/coordinator-${VERSION}.jar"
echo "copy $COORDINATOR_JAR to ${COORDINATOR_JAR_DIR}"
cp $COORDINATOR_JAR ${COORDINATOR_JAR_DIR}
cp "${RSS_HOME}"/coordinator/target/jars/* ${COORDINATOR_JAR_DIR}

CLIENT_JAR_DIR="${DISTDIR}/jars/client"
mkdir -p $CLIENT_JAR_DIR

BUILD_COMMAND_SPARK2=("$MVN" clean package -Pspark2 -pl client-spark/spark2 -DskipTests -am $@)

# Actually build the jar
echo -e "\nBuilding with..."
echo -e "\$ ${BUILD_COMMAND_SPARK2[@]}\n"

"${BUILD_COMMAND_SPARK2[@]}"

SPARK_CLIENT2_JAR_DIR="${CLIENT_JAR_DIR}/spark2"
mkdir -p $SPARK_CLIENT2_JAR_DIR

SPARK_CLIENT2_JAR="${RSS_HOME}/client-spark/spark2/target/shaded/rss-client-spark2-${VERSION}-shaded.jar"
echo "copy $SPARK_CLIENT2_JAR to ${SPARK_CLIENT2_JAR_DIR}"
cp $SPARK_CLIENT2_JAR ${SPARK_CLIENT2_JAR_DIR}

BUILD_COMMAND_SPARK3=("$MVN" clean package -Pspark3 -pl client-spark/spark3 -DskipTests -am $@)

echo -e "\nBuilding with..."
echo -e "\$ ${BUILD_COMMAND_SPARK3[@]}\n"
"${BUILD_COMMAND_SPARK3[@]}"

SPARK_CLIENT3_JAR_DIR="${CLIENT_JAR_DIR}/spark3"
mkdir -p $SPARK_CLIENT3_JAR_DIR
SPARK_CLIENT3_JAR="${RSS_HOME}/client-spark/spark3/target/shaded/rss-client-spark3-${VERSION}-shaded.jar"
echo "copy $SPARK_CLIENT3_JAR to ${SPARK_CLIENT3_JAR_DIR}"
cp $SPARK_CLIENT3_JAR $SPARK_CLIENT3_JAR_DIR

BUILD_COMMAND_MR=("$MVN" clean package -Pmr -pl client-mr -DskipTests -am $@)
echo -e "\nBuilding with..."
echo -e "\$ ${BUILD_COMMAND_MR[@]}\n"
"${BUILD_COMMAND_MR[@]}"
MR_CLIENT_JAR_DIR="${CLIENT_JAR_DIR}/mr"
mkdir -p $MR_CLIENT_JAR_DIR
MR_CLIENT_JAR="${RSS_HOME}/client-mr/target/shaded/rss-client-mr-${VERSION}-shaded.jar"
echo "copy $MR_CLIENT_JAR to ${MR_CLIENT_JAR_DIR}"
cp $MR_CLIENT_JAR $MR_CLIENT_JAR_DIR

cp -r bin $DISTDIR
cp -r conf $DISTDIR

rm -rf "rss-$VERSION.tgz"
tar czf "rss-$VERSION.tgz" $DISTDIR
rm -rf $DISTDIR
