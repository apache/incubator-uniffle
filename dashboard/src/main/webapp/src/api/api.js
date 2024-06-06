/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import http from "@/utils/http";
// Create a Coordinator information interface
export function getCoordinatorServerInfo(params,headers) {
    return http.get('/coordinator/info', params, headers, 0)
}

// Create a coordinator configuration file interface
export function getCoordinatorConf(params,headers) {
    return http.get('/coordinator/conf', params, headers, 0)
}

// Create an interface for the total number of nodes
export function getShufflegetStatusTotal(params,headers) {
    return http.get('/server/nodes/summary', params, headers, 0)
}

// Create an interface for activeNodes
export function getShuffleActiveNodes(params,headers) {
    return http.get('/server/nodes?status=active', params, headers, 0)
}

// Create an interface for lostNodes
export function getShuffleLostList(params,headers) {
    return http.get('/server/nodes?status=lost', params, headers, 0)
}

// Create an interface for unhealthyNodes
export function getShuffleUnhealthyList(params,headers) {
    return http.get('/server/nodes?status=unhealthy', params, headers, 0)
}

// Create an interface for decommissioningNodes
export function getShuffleDecommissioningList(params,headers) {
    return http.get('/server/nodes?status=decommissioning', params, headers, 0)
}

// Create an interface for decommissionedNodes
export function getShuffleDecommissionedList(params,headers) {
    return http.get('/server/nodes?status=decommissioned', params, headers, 0)
}

// Create an interface for excludeNodes
export function getShuffleExcludeNodes(params,headers) {
    return http.get('/server/nodes?status=excluded', params, headers, 0)
}

// Total number of interfaces for new App
export function getAppTotal(params,headers) {
    return http.get('/app/total', params, headers, 0)
}

// Create an interface for the app basic information list
export function getApplicationInfoList(params,headers) {
    return http.get('/app/appInfos', params, headers, 0)
}

// Create an interface for the number of apps for a user
export function getTotalForUser(params,headers) {
    return http.get('/app/userTotal', params, headers, 0)
}

// Obtain the configured coordinator server list
export function getAllCoordinatorAddrees(params) {
    return http.get('/coordinator/coordinatorList', params, {}, 1)
}
