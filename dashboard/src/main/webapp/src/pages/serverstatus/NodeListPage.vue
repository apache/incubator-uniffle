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
    <el-table
      :data="listPageData.tableData"
      height="550"
      style="width: 100%"
      :default-sort="sortColumn"
      @sort-change="sortChangeEvent"
    >
      <el-table-column prop="id" label="Id" min-width="140" sortable fixed />
      <el-table-column prop="ip" label="IP" min-width="80" sortable />
      <el-table-column prop="grpcPort" label="Port" min-width="80" />
      <el-table-column prop="nettyPort" label="NettyPort" min-width="80" />
      <el-table-column prop="jettyPort" label="JettyPort" min-width="80" />
      <el-table-column
        prop="usedMemory"
        label="UsedMem"
        min-width="80"
        :formatter="memFormatter"
        sortable
      />
      <el-table-column
        prop="preAllocatedMemory"
        label="PreAllocatedMem"
        min-width="100"
        :formatter="memFormatter"
        sortable
      />
      <el-table-column
        prop="availableMemory"
        label="AvailableMem"
        min-width="80"
        :formatter="memFormatter"
        sortable
      />
      <el-table-column prop="eventNumInFlush" label="FlushNum" min-width="80" sortable />
      <el-table-column prop="status" label="Status" min-width="80" sortable />
      <el-table-column
        prop="startTime"
        label="StartTime"
        min-width="120"
        :formatter="dateFormatter"
        sortable
      />
      <el-table-column
        prop="registrationTime"
        label="RegistrationTime"
        min-width="120"
        :formatter="dateFormatter"
        sortable
      />
      <el-table-column
        prop="timestamp"
        label="HeartbeatTime"
        min-width="120"
        :formatter="dateFormatter"
        sortable
      />
      <el-table-column label="Conf">
        <template v-slot="{ row }">
          <div class="mb-4">
            <el-button type="warning" @click="handlerServerConf(row)">conf</el-button>
          </div>
        </template>
      </el-table-column>
      <el-table-column label="Metrics">
        <template v-slot="{ row }">
          <div class="mb-4">
            <el-button type="primary" @click="handlerServerMetrics(row)">metrics</el-button>
          </div>
        </template>
      </el-table-column>
      <el-table-column label="PrometheusMetrics" min-width="150">
        <template v-slot="{ row }">
          <div class="mb-4">
            <el-button type="success" @click="handlerServerPrometheusMetrics(row)"
              >prometheus metrics</el-button
            >
          </div>
        </template>
      </el-table-column>
      <el-table-column label="Stacks">
        <template v-slot="{ row }">
          <div class="mb-4">
            <el-button type="info" @click="handlerServerStacks(row)">stacks</el-button>
          </div>
        </template>
      </el-table-column>
      <el-table-column prop="tags" label="Tags" min-width="140" />
      <el-table-column prop="version" label="Version" min-width="140" />
      <el-table-column prop="gitCommitId" label="GitCommitId" min-width="140" />
      <el-table-column v-if="isShowRemove" label="Operations">
        <template v-slot:default="scope">
          <el-button size="small" type="danger" @click="showDeleteConfirm(scope.row)">
            Remove
          </el-button>
        </template>
      </el-table-column>
    </el-table>
  </div>
</template>
<script>
import { onMounted, reactive, watch, ref, inject, watchEffect } from 'vue'
import { memFormatter, dateFormatter } from '@/utils/common'
import { useRouter } from 'vue-router'
import { useCurrentServerStore } from '@/store/useCurrentServerStore'
import { ElMessageBox, ElMessage } from 'element-plus'
import {
  getShuffleActiveNodes,
  getShuffleDecommissionedList,
  getShuffleDecommissioningList,
  getShuffleLostList,
  getShuffleUnhealthyList,
  deleteConfirmedLostServer,
  getShuffleServerConf,
  getShuffleServerMetrics,
  getShuffleServerPrometheusMetrics,
  getShuffleServerStacks
} from '@/api/api'

export default {
  setup() {
    const router = useRouter()
    const currentServerStore = useCurrentServerStore()
    const sortColumn = reactive({})
    const listPageData = reactive({ tableData: [] })
    const isShowRemove = ref(false)
    async function deleteLostServer(row) {
      try {
        const params = { serverId: row.id }
        const res = await deleteConfirmedLostServer(params)
        // Invoke the interface to delete the lost server, prompting a message based on the result returned.
        if (res.data.data === 'success') {
          ElMessage.success('remove ' + row.id + ' sucess...')
        } else {
          ElMessage.error('remove ' + row.id + ' fail...')
        }
      } catch (err) {
        ElMessage.error('remove ' + row.id + ' request timeout...')
      }
    }

    async function getShuffleActiveNodesPage() {
      const res = await getShuffleActiveNodes()
      listPageData.tableData = res.data.data
    }

    async function getShuffleDecommissionedListPage() {
      const res = await getShuffleDecommissionedList()
      listPageData.tableData = res.data.data
    }

    async function getShuffleDecommissioningListPage() {
      const res = await getShuffleDecommissioningList()
      listPageData.tableData = res.data.data
    }

    async function getShuffleLostListPage() {
      const res = await getShuffleLostList()
      listPageData.tableData = res.data.data
    }

    async function getShuffleUnhealthyListPage() {
      const res = await getShuffleUnhealthyList()
      listPageData.tableData = res.data.data
    }

    function combinedRequestAddress(serverRow) {
      return 'http://' + serverRow.ip + ':' + serverRow.jettyPort
    }

    async function handlerServerConf(serverRow) {
      try {
        const headers = {}
        headers.targetAddress = combinedRequestAddress(serverRow)
        const response = await getShuffleServerConf({}, headers)
        if (response.status >= 200 && response.status < 300) {
          const newWindow = window.open('', '_blank')
          let tableHTML = `
                          <style>
                            table {
                              width: 100%;
                            }
                            th, td {
                              padding: 0 20px;
                              text-align: left;
                            }
                          </style>
                          <table>
                            <tr>
                              <th>Key</th>
                              <th>Value</th>
                            </tr>
                        `
          for (const item of response.data.data) {
            tableHTML += `<tr><td>${item.argumentKey}</td><td>${item.argumentValue}</td></tr>`
          }
          tableHTML += '</table>'
          newWindow.document.write(tableHTML)
        } else {
          ElMessage.error('Request failed.')
        }
      } catch (err) {
        ElMessage.error('Internal error.')
      }
    }

    async function handlerServerMetrics(serverRow) {
      try {
        const headers = {}
        headers.targetAddress = combinedRequestAddress(serverRow)
        const response = await getShuffleServerMetrics({}, headers)
        if (response.status >= 200 && response.status < 300) {
          const newWindow = window.open('', '_blank')
          newWindow.document.write('<pre>' + JSON.stringify(response.data, null, 2) + '</pre>')
        } else {
          ElMessage.error('Request failed.')
        }
      } catch (err) {
        ElMessage.error('Internal error.')
      }
    }

    async function handlerServerPrometheusMetrics(serverRow) {
      try {
        const headers = {}
        headers.targetAddress = combinedRequestAddress(serverRow)
        const response = await getShuffleServerPrometheusMetrics({}, headers)
        if (response.status >= 200 && response.status < 300) {
          const newWindow = window.open('', '_blank')
          newWindow.document.write('<pre>' + response.data + '</pre>')
        } else {
          ElMessage.error('Request failed.')
        }
      } catch (err) {
        ElMessage.error('Internal error.')
      }
    }

    async function handlerServerStacks(serverRow) {
      try {
        const headers = {}
        headers.targetAddress = combinedRequestAddress(serverRow)
        const response = await getShuffleServerStacks({}, headers)
        if (response.status >= 200 && response.status < 300) {
          const newWindow = window.open('', '_blank')
          newWindow.document.write('<pre>' + response.data + '</pre>')
        } else {
          ElMessage.error('Request failed.')
        }
      } catch (err) {
        ElMessage.error('Internal error.')
      }
    }

    const loadPageData = () => {
      isShowRemove.value = false
      listPageData.tableData = []
      if (router.currentRoute.value.name === 'activeNodeList') {
        getShuffleActiveNodesPage()
      } else if (router.currentRoute.value.name === 'decommissioningNodeList') {
        getShuffleDecommissioningListPage()
      } else if (router.currentRoute.value.name === 'decommissionedNodeList') {
        getShuffleDecommissionedListPage()
      } else if (router.currentRoute.value.name === 'unhealthyNodeList') {
        getShuffleUnhealthyListPage()
      } else if (router.currentRoute.value.name === 'lostNodeList') {
        isShowRemove.value = true
        getShuffleLostListPage()
      }
    }

    onMounted(() => {
      watchEffect(() => {
        // If the coordinator address to request is not found in the global variable, the request is not initiated.
        if (currentServerStore.currentServer) {
          loadPageData()
        }
      })
    })

    watch(router.currentRoute, () => {
      if (currentServerStore.currentServer) {
        loadPageData()
      }
    })

    const sortChangeEvent = (sortInfo) => {
      for (const sortColumnKey in sortColumn) {
        delete sortColumn[sortColumnKey]
      }
      sortColumn[sortInfo.prop] = sortInfo.order
    }
    /**
     * Get the callback method of the parent page and update the number of servers on the page.
     */
    const updateTotalPage = inject('updateTotalPage')
    const showDeleteConfirm = (row) => {
      ElMessageBox.confirm(`Are you sure to remove ${row.id}?`, 'Confirmation', {
        confirmButtonText: 'Remove',
        cancelButtonText: 'Cancel',
        type: 'warning'
      })
        .then(() => {
          // Perform deletion logic here.
          deleteLostServer(row)
          // Reload the lost server information
          getShuffleLostListPage()
          updateTotalPage()
        })
        .catch(() => {
          // Cancelled
        })
    }
    return {
      listPageData,
      sortColumn,
      isShowRemove,
      showDeleteConfirm,
      sortChangeEvent,
      handlerServerConf,
      handlerServerPrometheusMetrics,
      handlerServerMetrics,
      handlerServerStacks,
      memFormatter,
      dateFormatter
    }
  }
}
</script>
