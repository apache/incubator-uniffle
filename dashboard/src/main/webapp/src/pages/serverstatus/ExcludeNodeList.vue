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
    <div style="text-align: right">
      <el-button type="primary" @click="dialogFormVisible = true"> Add Node </el-button>
    </div>
    <div>
      <el-table
        :data="pageData.tableData"
        height="550"
        style="width: 100%"
        :default-sort="sortColumn"
        @sort-change="sortChangeEvent"
      >
        <el-table-column prop="id" label="ExcludeNodeId" min-width="180" :sortable="true" />
      </el-table>
      <el-dialog v-model="dialogFormVisible" title="Please enter Server:" class="dialog-wrapper">
        <el-form>
          <el-form-item :label-width="formLabelWidth">
            <el-input
              v-model="textarea"
              class="textarea-wrapper"
              :rows="10"
              type="textarea"
              placeholder="Please input"
            />
          </el-form-item>
        </el-form>
        <template #footer>
          <div class="dialog-footer">
            <el-button @click="dialogFormVisible = false">Cancel</el-button>
            <el-button type="primary" @click="confirmAddHandler"> Confirm </el-button>
          </div>
        </template>
      </el-dialog>
    </div>
  </div>
</template>
<script>
import { onMounted, reactive, ref, inject } from 'vue'
import { addShuffleExcludeNodes, getShuffleExcludeNodes } from '@/api/api'
import { useCurrentServerStore } from '@/store/useCurrentServerStore'
import { ElMessage } from 'element-plus'

export default {
  setup() {
    const pageData = reactive({
      tableData: [
        {
          id: ''
        }
      ]
    })
    const currentServerStore = useCurrentServerStore()

    const dialogFormVisible = ref(false)
    const formLabelWidth = '10%'
    const textarea = ref('')

    /**
     * Get the callback method of the parent page and update the number of servers on the page.
     */
    const updateTotalPage = inject('updateTotalPage')

    async function getShuffleExcludeNodesPage() {
      const res = await getShuffleExcludeNodes()
      pageData.tableData = res.data.data
    }

    async function addShuffleExcludeNodesPage() {
      try {
        const excludeNodes = textarea.value.split('\n').map((item) => item.trim())
        const excludeNodesObj = { excludeNodes: excludeNodes }
        const res = await addShuffleExcludeNodes(excludeNodesObj)
        if (res.data.data === 'success') {
          ElMessage.success('add successfully...')
        } else {
          ElMessage.error('add failure...')
        }
      } catch (err) {
        ElMessage.error('failed to add timeout...')
      }
    }

    // The system obtains data from global variables and requests the interface to obtain new data after data changes.
    currentServerStore.$subscribe((mutable, state) => {
      if (state.currentServer) {
        getShuffleExcludeNodesPage()
      }
    })

    onMounted(() => {
      // If the coordinator address to request is not found in the global variable, the request is not initiated.
      if (currentServerStore.currentServer) {
        getShuffleExcludeNodesPage()
      }
    })

    const sortColumn = reactive({})
    const sortChangeEvent = (sortInfo) => {
      for (const sortColumnKey in sortColumn) {
        delete sortColumn[sortColumnKey]
      }
      sortColumn[sortInfo.prop] = sortInfo.order
    }
    const confirmAddHandler = () => {
      dialogFormVisible.value = false
      addShuffleExcludeNodesPage()
      // Refreshing the number of blacklists.
      updateTotalPage()
      // Refreshing the Blacklist list.
      getShuffleExcludeNodesPage()
    }

    return {
      pageData,
      sortColumn,
      sortChangeEvent,
      confirmAddHandler,
      dialogFormVisible,
      formLabelWidth,
      textarea
    }
  }
}
</script>

<style>
.textarea-wrapper {
  width: 90%;
}
.dialog-wrapper {
  width: 50%;
}
</style>
