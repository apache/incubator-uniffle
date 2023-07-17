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
set -u

NAME="rss"
MVN="mvn"
RSS_HOME="$(
  cd "$(dirname "$0")"
  pwd
)"

function exit_with_usage() {
  set +x
  echo "./build_distribution.sh - Tool for making binary distributions of Remote Shuffle Service"
  echo ""
  echo "Usage:"
  echo "+------------------------------------------------------------------------------------------------------+"
  echo "| ./build_distribution.sh [--spark2-profile <spark2 profile id>] [--spark2-mvn <custom maven options>] |"
  echo "|                         [--spark3-profile <spark3 profile id>] [--spark3-mvn <custom maven options>] |"
  echo "|                         [--hadoop-profile <hadoop profile id>]                                       |"
  echo "|                         <maven build options>                                                        |"
  echo "+------------------------------------------------------------------------------------------------------+"
  exit 1
}

SPARK2_PROFILE_ID="spark2"
SPARK2_MVN_OPTS=""
SPARK3_PROFILE_ID="spark3"
SPARK3_MVN_OPTS=""
HADOOP_PROFILE_ID="hadoop2.8"
NAME=none
while (( "$#" )); do
  case $1 in
    --spark2-profile)
      SPARK2_PROFILE_ID="$2"
      shift
      ;;
    --spark2-mvn)
      SPARK2_MVN_OPTS=$2
      shift
      ;;
    --spark3-profile)
      SPARK3_PROFILE_ID="$2"
      shift
      ;;
    --spark3-mvn)
      SPARK3_MVN_OPTS=$2
      shift
      ;;
    --hadoop-profile)
      HADOOP_PROFILE_ID=$2
      shift
      ;;
    --name)
      NAME="$2"
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

# Dependencies version
HADOOP_VERSION=$("$MVN" help:evaluate -Dexpression=hadoop.version -P$HADOOP_PROFILE_ID $@ 2>/dev/null\
    | grep -v "INFO"\
    | grep -v "WARNING"\
    | tail -n 1)
SPARK2_VERSION=$("$MVN" help:evaluate -Dexpression=spark.version -P$SPARK2_PROFILE_ID $@ $SPARK2_MVN_OPTS 2>/dev/null\
    | grep -v "INFO"\
    | grep -v "WARNING"\
    | tail -n 1)
SPARK3_VERSION=$("$MVN" help:evaluate -Dexpression=spark.version -P$SPARK3_PROFILE_ID $@ $SPARK3_MVN_OPTS 2>/dev/null\
    | grep -v "INFO"\
    | grep -v "WARNING"\
    | tail -n 1)

if [ "$NAME" == "none" ]; then
  NAME=$HADOOP_PROFILE_ID
fi

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
DISTDIR="rss-$VERSION-$NAME"
rm -rf "$DISTDIR"
mkdir -p "${DISTDIR}/jars"
echo "RSS ${VERSION}${GITREVSTRING} built for Hadoop ${HADOOP_VERSION} Spark2 ${SPARK2_VERSION} Spark3 ${SPARK3_VERSION}" >"${DISTDIR}/RELEASE"
echo "Build flags: --spark2-profile '$SPARK2_PROFILE_ID' --spark2-mvn '$SPARK2_MVN_OPTS' --spark3-profile '$SPARK3_PROFILE_ID' --spark3-mvn '$SPARK3_MVN_OPTS' --hadoop-profile '$HADOOP_PROFILE_ID' $@" >>"$DISTDIR/RELEASE"
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

CLI_JAR_DIR="${DISTDIR}/jars/cli"
mkdir -p $CLI_JAR_DIR
CLI_JAR="${RSS_HOME}/cli/target/cli-${VERSION}.jar"
echo "copy $CLI_JAR to ${CLI_JAR_DIR}"
cp $CLI_JAR ${CLI_JAR_DIR}
cp "${RSS_HOME}"/cli/target/jars/* ${CLI_JAR_DIR}

CLIENT_JAR_DIR="${DISTDIR}/jars/client"
mkdir -p $CLIENT_JAR_DIR

BUILD_COMMAND_SPARK2=("$MVN" clean package -P$SPARK2_PROFILE_ID -pl client-spark/spark2 -DskipTests -am $@ $SPARK2_MVN_OPTS)

# Actually build the jar
echo -e "\nBuilding with..."
echo -e "\$ ${BUILD_COMMAND_SPARK2[@]}\n"

"${BUILD_COMMAND_SPARK2[@]}"

SPARK_CLIENT2_JAR_DIR="${CLIENT_JAR_DIR}/spark2"
mkdir -p $SPARK_CLIENT2_JAR_DIR

SPARK_CLIENT2_JAR="${RSS_HOME}/client-spark/spark2/target/shaded/rss-client-spark2-${VERSION}-shaded.jar"
echo "copy $SPARK_CLIENT2_JAR to ${SPARK_CLIENT2_JAR_DIR}"
cp $SPARK_CLIENT2_JAR ${SPARK_CLIENT2_JAR_DIR}

BUILD_COMMAND_SPARK3=("$MVN" clean package -P$SPARK3_PROFILE_ID -pl client-spark/spark3 -DskipTests -am $@ $SPARK3_MVN_OPTS)

echo -e "\nBuilding with..."
echo -e "\$ ${BUILD_COMMAND_SPARK3[@]}\n"
"${BUILD_COMMAND_SPARK3[@]}"

SPARK_CLIENT3_JAR_DIR="${CLIENT_JAR_DIR}/spark3"
mkdir -p $SPARK_CLIENT3_JAR_DIR
SPARK_CLIENT3_JAR="${RSS_HOME}/client-spark/spark3/target/shaded/rss-client-spark3-${VERSION}-shaded.jar"
echo "copy $SPARK_CLIENT3_JAR to ${SPARK_CLIENT3_JAR_DIR}"
cp $SPARK_CLIENT3_JAR $SPARK_CLIENT3_JAR_DIR

BUILD_COMMAND_MR=("$MVN" clean package -Pmr,$HADOOP_PROFILE_ID -pl client-mr/core -DskipTests -am $@)
echo -e "\nBuilding with..."
echo -e "\$ ${BUILD_COMMAND_MR[@]}\n"
"${BUILD_COMMAND_MR[@]}"
MR_CLIENT_JAR_DIR="${CLIENT_JAR_DIR}/mr"
mkdir -p $MR_CLIENT_JAR_DIR
MR_CLIENT_JAR="${RSS_HOME}/client-mr/core/target/shaded/rss-client-mr-${VERSION}-shaded.jar"
echo "copy $MR_CLIENT_JAR to ${MR_CLIENT_JAR_DIR}"
cp $MR_CLIENT_JAR $MR_CLIENT_JAR_DIR

BUILD_COMMAND_TEZ=("$MVN" clean package -Ptez,$HADOOP_PROFILE_ID -pl client-tez -DskipTests -am $@)
echo -e "\nBuilding with..."
echo -e "\$ ${BUILD_COMMAND_TEZ[@]}\n"
"${BUILD_COMMAND_TEZ[@]}"
TEZ_CLIENT_JAR_DIR="${CLIENT_JAR_DIR}/tez"
mkdir -p $TEZ_CLIENT_JAR_DIR
TEZ_CLIENT_JAR="${RSS_HOME}/client-tez/target/shaded/rss-client-tez-${VERSION}-shaded.jar"
echo "copy $TEZ_CLIENT_JAR to ${TEZ_CLIENT_JAR_DIR}"
cp $TEZ_CLIENT_JAR $TEZ_CLIENT_JAR_DIR

cp -r bin $DISTDIR
cp -r conf $DISTDIR

rm -rf "rss-$VERSION-$NAME.tgz"
tar czf "rss-$VERSION-$NAME.tgz" $DISTDIR
rm -rf $DISTDIR
