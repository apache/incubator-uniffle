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
    <el-table :data="pageData.tableData" height="550" style="width: 100%">
      <el-table-column prop="id" label="Id" min-width="140" />
      <el-table-column prop="ip" label="IP" min-width="80" />
      <el-table-column prop="grpcPort" label="Port" min-width="80" />
      <el-table-column prop="nettyPort" label="NettyPort" min-width="80" />
      <el-table-column prop="usedMemory" label="UsedMem" min-width="80" :formatter="memFormatter" />
      <el-table-column
        prop="preAllocatedMemory"
        label="PreAllocatedMem"
        min-width="80"
        :formatter="memFormatter"
      />
      <el-table-column
        prop="availableMemory"
        label="AvailableMem"
        min-width="80"
        :formatter="memFormatter"
      />
      <el-table-column prop="eventNumInFlush" label="FlushNum" min-width="80" />
      <el-table-column prop="status" label="Status" min-width="80" />
      <el-table-column
        prop="timestamp"
        label="HeartbeatTime"
        min-width="80"
        :formatter="dateFormatter"
      />
      <el-table-column prop="tags" label="Tags" min-width="80" />
    </el-table>
  </div>
</template>
<script>
import { onMounted, reactive } from 'vue'
import { getShuffleUnhealthyList } from '@/api/api'
import { memFormatter, dateFormatter } from '@/utils/common'
import { useCurrentServerStore } from '@/store/useCurrentServerStore'

export default {
  setup() {
    const pageData = reactive({
      tableData: [
        {
          id: '',
          ip: '',
          grpcPort: 0,
          nettyPort: 0,
          usedMemory: 0,
          preAllocatedMemory: 0,
          availableMemory: 0,
          eventNumInFlush: 0,
          tags: '',
          status: '',
          timestamp: ''
        }
      ]
    })
    const currentServerStore = useCurrentServerStore()

    async function getShuffleUnhealthyListPage() {
      const res = await getShuffleUnhealthyList()
      pageData.tableData = res.data.data
    }

    // The system obtains data from global variables and requests the interface to obtain new data after data changes.
    currentServerStore.$subscribe((mutable, state) => {
      if (state.currentServer) {
        getShuffleUnhealthyListPage()
      }
    })

    onMounted(() => {
      // If the coordinator address to request is not found in the global variable, the request is not initiated.
      if (currentServerStore.currentServer) {
        getShuffleUnhealthyListPage()
      }
    })

    return { pageData, memFormatter, dateFormatter }
  }
}
</script>
