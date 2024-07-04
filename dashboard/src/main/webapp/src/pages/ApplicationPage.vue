<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one or more
  ~ contributor license agreements.  See the NOTICE file distributed with
  ~ this work for additional information regarding copyright ownership.
  ~ The ASF licenses this file to You under the Apache License, Version 2.0
  ~ (the "License"); you may not use this file except in compliance with
  ~ the License.  You may obtain a copy of the License at
  ~
  ~    http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  -->

<template>
  <div>
    <el-row :gutter="20">
      <el-col :span="4">
        <el-card class="box-card" shadow="hover">
          <template #header>
            <div class="card-header">
              <span class="cardtile">APPS TOTAL</span>
            </div>
          </template>
          <div class="appcnt">{{ pageData.apptotal.appTotality }}</div>
        </el-card>
      </el-col>
    </el-row>
    <el-divider />
    <el-tag>User App ranking</el-tag>
    <div>
      <el-table :data="pageData.userAppCount" height="250" style="width: 100%">
        <el-table-column prop="userName" label="UserName" min-width="180" />
        <el-table-column prop="appNum" label="Totality" min-width="180" />
      </el-table>
    </div>
    <el-divider />
    <el-tag>Apps</el-tag>
    <div>
      <el-table :data="pageData.appInfoData" height="250" style="width: 100%">
        <el-table-column prop="appId" label="AppId" min-width="180" />
        <el-table-column prop="userName" label="UserName" min-width="180" />
        <el-table-column prop="registrationTime" label="Register Time" min-width="180" :formatter="dateFormatter" />
        <el-table-column prop="updateTime" label="Update Time" min-width="180" :formatter="dateFormatter" />
      </el-table>
    </div>
  </div>
</template>

<script>
import { getApplicationInfoList, getAppTotal, getTotalForUser } from '@/api/api'
import { onMounted, reactive } from 'vue'
import { dateFormatter } from '@/utils/common'
import { useCurrentServerStore } from '@/store/useCurrentServerStore'

export default {
  setup() {
    const pageData = reactive({
      apptotal: {},
      userAppCount: [{}],
      appInfoData: [{ appId: '', userName: '', registrationTime: '', updateTime: '' }]
    })
    const currentServerStore = useCurrentServerStore()

    async function getApplicationInfoListPage() {
      const res = await getApplicationInfoList()
      pageData.appInfoData = res.data.data
    }

    async function getTotalForUserPage() {
      const res = await getTotalForUser()
      pageData.userAppCount = res.data.data
    }

    async function getAppTotalPage() {
      const res = await getAppTotal()
      pageData.apptotal = res.data.data
    }

    // The system obtains data from global variables and requests the interface to obtain new data after data changes.
    currentServerStore.$subscribe((mutable, state) => {
      if (state.currentServer) {
        getApplicationInfoListPage()
        getTotalForUserPage()
        getAppTotalPage()
      }
    })

    onMounted(() => {
      // If the coordinator address to request is not found in the global variable, the request is not initiated.
      if (currentServerStore.currentServer) {
        getApplicationInfoListPage()
        getTotalForUserPage()
        getAppTotalPage()
      }
    })
    return { pageData, dateFormatter }
  }
}
</script>

<style>
.appcnt {
  font-family: 'Lantinghei SC';
  font-style: normal;
  font-weight: bolder;
  font-size: 30px;
}
</style>
