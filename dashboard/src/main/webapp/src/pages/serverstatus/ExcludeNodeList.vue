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
    <div class="button-wrapper">
      <el-button type="success" @click="dialogFormVisible = true"> Add Node </el-button>
      <el-button type="danger" @click="handlerDeleteNode">Delete({{ selectItemNum }})</el-button>
    </div>
    <el-divider />
    <div>
      <el-table
        :data="filteredTableData"
        :row-key="rowKey"
        :default-sort="sortColumn"
        @sort-change="sortChangeEvent"
        @selection-change="handlerSelectionChange"
        class="table-wapper"
        ref="table"
        stripe
      >
        <el-table-column type="selection" width="55" />
        <el-table-column prop="id" label="ExcludeNodeId" min-width="180" :sortable="true" />
        <el-table-column align="right">
          <template #header>
            <el-input v-model="searchKeyword" size="small" placeholder="Type to search" />
          </template>
        </el-table-column>
      </el-table>
    </div>
    <div>
      <el-dialog
        v-model="dialogFormVisible"
        title="Please enter the server id list to be excluded:"
        class="dialog-wrapper"
      >
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
            <el-button type="primary" @click="handleConfirmAddHandler"> Confirm </el-button>
          </div>
        </template>
      </el-dialog>
    </div>
  </div>
</template>
<script>
import { onMounted, reactive, ref, inject, computed } from 'vue'
import {
  addShuffleExcludeNodes,
  getShuffleExcludeNodes,
  removeShuffleExcludeNodes
} from '@/api/api'
import { useCurrentServerStore } from '@/store/useCurrentServerStore'
import { ElMessage, ElMessageBox } from 'element-plus'

export default {
  setup() {
    const pageData = reactive({ tableData: [] })
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

    /**
     * The following describes how to handle sort events.
     */
    const sortColumn = reactive({})
    const sortChangeEvent = (sortInfo) => {
      for (const sortColumnKey in sortColumn) {
        delete sortColumn[sortColumnKey]
      }
      sortColumn[sortInfo.prop] = sortInfo.order
    }

    /**
     * The following describes how to handle add events.
     */
    function handleConfirmAddHandler() {
      dialogFormVisible.value = false
      addShuffleExcludeNodesPage()
      // Refreshing the number of blacklists.
      updateTotalPage()
      // Refreshing the Blacklist list.
      getShuffleExcludeNodesPage()
    }

    async function addShuffleExcludeNodesPage() {
      try {
        const excludeNodes = textarea.value.split('\n').map((item) => item.trim())
        const excludeNodesObj = { excludeNodes }
        const res = await addShuffleExcludeNodes(excludeNodesObj)
        if (res.status >= 200 && res.status < 300) {
          if (res.data.data === 'success') {
            ElMessage.success('Add successfully.')
          } else {
            ElMessage.error('Add failed.')
          }
        } else {
          ElMessage.error('Failed to add due to server bad.')
        }
      } catch (err) {
        ElMessage.error('Failed to add due to network exception.')
      }
    }

    /**
     * The following describes how to handle blacklist deletion events.
     */
    const selectItemNum = ref(0)
    const rowKey = 'id'
    const selectedRows = ref([])
    function handlerSelectionChange(selection) {
      selectedRows.value = selection
      selectItemNum.value = selectedRows.value.length
    }
    function handlerDeleteNode() {
      ElMessageBox.confirm('Are you sure about removing these nodes?', 'Warning', {
        confirmButtonText: 'OK',
        cancelButtonText: 'Cancel',
        type: 'warning'
      })
        .then(() => {
          if (selectedRows.value.length === 0) {
            ElMessage({
              type: 'info',
              message: 'No node is selected, Nothing!'
            })
          } else {
            const selectedIds = selectedRows.value.map((row) => row[rowKey])
            // pageData.tableData = pageData.tableData.filter(row => !selectedIds.includes(row[rowKey]));
            deleteShuffleExcludeNodesPage(selectedIds)
            // Refreshing the number of blacklists.
            updateTotalPage()
            // Refreshing the Blacklist list.
            getShuffleExcludeNodesPage()
            ElMessage({
              type: 'success',
              message: 'Delete completed'
            })
          }
        })
        .catch(() => {
          ElMessage({
            type: 'info',
            message: 'Delete canceled'
          })
        })
    }

    async function deleteShuffleExcludeNodesPage(excludeNodes) {
      try {
        const excludeNodesObj = { excludeNodes }
        const res = await removeShuffleExcludeNodes(excludeNodesObj)
        if (res.status >= 200 && res.status < 300) {
          if (res.data.data === 'success') {
            ElMessage.success('Add successfully.')
          } else {
            ElMessage.error('Add failed.')
          }
        } else {
          ElMessage.error('Failed to add due to server bad.')
        }
      } catch (err) {
        ElMessage.error('Failed to add due to network exception.')
      }
    }

    /**
     * The following describes how to handle blacklist select events.
     */
    const searchKeyword = ref('')
    const filteredTableData = computed(() => {
      const keyword = searchKeyword.value.trim()
      if (!keyword) {
        return pageData.tableData
      } else {
        return pageData.tableData.filter((row) => {
          return row.id.includes(keyword)
        })
      }
    })
    return {
      pageData,
      sortColumn,
      selectItemNum,
      sortChangeEvent,
      handleConfirmAddHandler,
      handlerDeleteNode,
      handlerSelectionChange,
      dialogFormVisible,
      formLabelWidth,
      textarea,
      rowKey,
      searchKeyword,
      filteredTableData
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
.table-wapper {
  height: 550px;
  width: 100%;
  text-align: right;
}
</style>
