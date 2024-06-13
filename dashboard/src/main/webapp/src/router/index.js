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

import { createRouter, createWebHashHistory } from 'vue-router'
import ApplicationPage from '@/pages/ApplicationPage.vue'
import CoordinatorServerPage from '@/pages/CoordinatorServerPage.vue'
import ShuffleServerPage from '@/pages/ShuffleServerPage.vue'
import ActiveNodeListPage from '@/pages/serverstatus/ActiveNodeListPage'
import DecommissioningNodeListPage from '@/pages/serverstatus/DecommissioningNodeListPage'
import DecommissionednodeListPage from '@/pages/serverstatus/DecommissionednodeListPage'
import LostNodeList from '@/pages/serverstatus/LostNodeList'
import UnhealthyNodeListPage from '@/pages/serverstatus/UnhealthyNodeListPage'
import ExcludeNodeList from '@/pages/serverstatus/ExcludeNodeList'

const routes = [
  {
    path: '/coordinatorserverpage',
    name: 'coordinatorserverpage',
    component: CoordinatorServerPage
  },
  {
    path: '/shuffleserverpage',
    name: 'shuffleserverpage',
    component: ShuffleServerPage,
    redirect: '/shuffleserverpage/activeNodeList',
    children: [
      {
        path: '/shuffleserverpage/activeNodeList',
        name: 'activeNodeList',
        component: ActiveNodeListPage
      },
      {
        path: '/shuffleserverpage/decommissioningNodeList',
        name: 'decommissioningNodeList',
        component: DecommissioningNodeListPage
      },
      {
        path: '/shuffleserverpage/decommissionedNodeList',
        name: 'decommissionedNodeList',
        component: DecommissionednodeListPage
      },
      {
        path: '/shuffleserverpage/lostNodeList',
        name: 'lostNodeList',
        component: LostNodeList
      },
      {
        path: '/shuffleserverpage/unhealthyNodeList',
        name: 'unhealthyNodeList',
        component: UnhealthyNodeListPage
      },
      {
        path: '/shuffleserverpage/excludeNodeList',
        name: 'excludeNodeList',
        component: ExcludeNodeList
      }
    ]
  },
  {
    path: '/applicationpage',
    name: 'applicationpage',
    component: ApplicationPage
  },
  {
    path: '/nullpage',
    name: 'nullpage',
    beforeEnter: (to, from, next) => {
      next(false)
    },
    component: ApplicationPage
  }
]

const router = createRouter({
  history: createWebHashHistory(),
  routes
})

export default router
