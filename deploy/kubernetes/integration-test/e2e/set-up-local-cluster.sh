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

set -o errexit
set -o nounset
set -o pipefail

STEP_BUILD_KIND_CLUSTER="true"
STEP_BUILD_NEW_RSS_IMAGE="true"
STEP_BUILD_NEW_OPERATOR="true"

KIND_K8S_IMAGE=kindest/node:v1.22.15@sha256:7d9708c4b0873f0fe2e171e2b1b7f45ae89482617778c1c875f1053d4cef2e41
KIND_CLUSTER_NAME=rss-test
TEST_REGISTRY="docker.io/library"
TEST_VERSION=$KIND_CLUSTER_NAME

RSS_BUILD_DIR=../../docker
OPERATOR_BUILD_DIR=../../operator

function pre_check() {
  docker -v
  res=$?
  if [ $res -eq 0 ]; then
    echo "--->>>docker has been installed"
  else
    echo "--->>>please install docker"
    exit
  fi

  kind --version
  res=$?
  if [ $res -eq 0 ]; then
    echo "--->>>kind has been installed"
  else
    echo "--->>>please install kind"
    exit
  fi
}

function prepare_local_k8s_cluster() {
  # delete old cluster with the same name
  kind delete cluster --name ${KIND_CLUSTER_NAME}
  sleep 5
  # create new cluster
  kind create cluster --name ${KIND_CLUSTER_NAME} --image ${KIND_K8S_IMAGE} --config kind-config
  # change context of kubeConfig
  kubectl cluster-info --context kind-${KIND_CLUSTER_NAME}
}

function build_rss_image() {
  cd $RSS_BUILD_DIR
  export IMAGE_VERSION=$TEST_VERSION
  ./build.sh --registry $TEST_REGISTRY
  cd "$OLDPWD"
}

function build_operator_image() {
  cd $OPERATOR_BUILD_DIR
  export REGISTRY=$TEST_REGISTRY
  export VERSION=$TEST_VERSION
  make docker-push
  cd "$OLDPWD"
}

pre_check

while (("$#")); do
  case $1 in
  --registry)
    if [ -n "$2" ]; then
      TEST_REGISTRY=$2
    fi
    shift
    ;;
  --build-kind-cluster)
    STEP_BUILD_KIND_CLUSTER="$2"
    shift
    ;;
  --build-rss-image)
    STEP_BUILD_NEW_RSS_IMAGE="$2"
    shift
    ;;
  --build-operator)
    STEP_BUILD_NEW_OPERATOR="$2"
    shift
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

# build k8s environment
if [ "$STEP_BUILD_KIND_CLUSTER" == "true" ]; then
  echo "--->>>try to delete the old cluster and create a new cluster"
  prepare_local_k8s_cluster
  sleep 60
fi

# create rss-operator in environment
if [ "$STEP_BUILD_NEW_OPERATOR" == "true" ]; then
  # build crd object
  echo "--->>>create rss crd in cluster"
  kubectl create -f ../../operator/config/crd/bases/uniffle.apache.org_remoteshuffleservices.yaml
  sleep 5
  # build operator image
  echo "--->>>try to build test image of rss-operator"
  build_operator_image
  # generate operator yaml
  echo "--->>>try to generate operator yaml from template"
  export RSS_WEBHOOK_IMAGE=$TEST_REGISTRY/rss-webhook:$TEST_VERSION
  export RSS_CONTROLLER_IMAGE=$TEST_REGISTRY/rss-controller:$TEST_VERSION
  envsubst <template/rss-controller-template.yaml >rss-controller.yaml
  envsubst <template/rss-webhook-template.yaml >rss-webhook.yaml
  # build rss-operator
  echo "--->>>try to apply rss-operator in cluster"
  kubectl apply -f rss-controller.yaml
  kubectl apply -f rss-webhook.yaml
  kubectl apply -f template/metrics-server.yaml
  echo "--->>>wait some time for rss-operator to be ready"
  sleep 60
fi

# generate rss object yaml
if [ "$STEP_BUILD_NEW_RSS_IMAGE" == "true" ]; then
  echo "--->>>try to build test image of rss"
  build_rss_image
fi

echo "--->>>try to load image of rss"
export RSS_SERVER_IMAGE=$TEST_REGISTRY/rss-server:$TEST_VERSION
kind load docker-image --name ${KIND_CLUSTER_NAME} "${RSS_SERVER_IMAGE}"

echo "--->>>try to generate rss object yaml from template"
envsubst <template/rss-template.yaml >rss.yaml

# build rss object
echo "--->>>try to apply a rss object in cluster"
kubectl apply -f rss.yaml
echo "--->>>wait some time for the rss cluster to be ready"
sleep 30

target_cnt=3
target_times=5
times=0
for ((i = 1; i <= 15; i = i + 1)); do
  running_cnt=$(kubectl get pod -nkube-system | grep -E "rss-coordinator|rss-shuffle-server" | grep -v "NAME" | grep -c "Running")
  echo "--->>>running count: $running_cnt currently"
  if [ "$running_cnt" -eq $target_cnt ]; then
    times=$((times + 1))
    if [ $times -eq $target_times ]; then
      echo "rss running normally!"
      exit
    fi
  else
    echo "invalid running count"
    times=0
  fi
  sleep 60
done
