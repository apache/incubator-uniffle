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
      <el-table-column prop="id" label="ExcludeNodeId" min-width="180"/>
    </el-table>
  </div>
</template>
<script>
import {onMounted, reactive} from 'vue'
import { getShuffleExcludeNodes } from "@/api/api";

export default {
  setup() {
    const pageData = reactive({
      tableData: [
        {
          id:""
        }
      ]
    })
    async function getShuffleExcludeNodesPage() {
      const res = await getShuffleExcludeNodes();
      pageData.tableData = res.data.data
    }

    onMounted(() => {
      getShuffleExcludeNodesPage();
    })
    return {pageData}
  }
}
</script>
