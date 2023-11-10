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
  <div class="common-layout">
    <el-container>
      <el-header>
        <el-row>
          <el-col :span="24">
            <el-menu
                :default-active="activeIndex1"
                class="el-menu-demo"
                mode="horizontal"
                background-color="#20B2AA"
                box-shadow="0 -2px 8px 0 rgba(0,0,0,0.12)"
                text-color="#fff"
                active-text-color="#ffd04b"
                @select="handleSelect">
              <el-menu-item index="0">
                <div class="unffilelogo">
                  <img src="../assets/uniffle-logo.png" alt="unffile">
                </div>
              </el-menu-item>
              <router-link to="/coordinatorserverpage">
                <el-menu-item index="1">
                  Coordinator
                </el-menu-item>
              </router-link>
              <router-link to="/shuffleserverpage">
                <el-menu-item index="2">
                  Shuffle Server
                </el-menu-item>
              </router-link>
              <router-link to="/applicationpage">
                <el-menu-item index="3">
                  Application
                </el-menu-item>
              </router-link>
            </el-menu>
          </el-col>
        </el-row>
      </el-header>
      <el-main>
        <router-view></router-view>
      </el-main>
    </el-container>
  </div>
</template>

<script>
import {ref, onMounted} from 'vue'

export default {
  setup() {
    const activeIndex1 = ref('1')

    function handleSelect() {
      localStorage.setItem("menuId", JSON.stringify(activeIndex1))
    }

    onMounted(() => {

    })
    return {activeIndex1, handleSelect}
  }
}
</script>

<style>
a {
  text-decoration: none;
  color: white;
}

.unffilelogo {
  background-color: #20B2AA;
  height: 100%;
  position: relative;
}

.unffilelogo > img {
  height: 55px;
}
</style>
