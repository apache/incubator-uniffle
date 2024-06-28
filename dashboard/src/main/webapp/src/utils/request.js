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

import axios from 'axios'

/**
 * The root directory, starting with web, is handled by the dashboard server's servlet
 * @type {axios.AxiosInstance}
 */
const frontEndAxiosInstance = axios.create({
  baseURL: '/web',
  timeout: 10000
})
/**
 * The root directory starts with API. The dashboard server reverse proxy requests Coordinator apis
 * @type {axios.AxiosInstance}
 */
const backEndAxiosInstance = axios.create({
  baseURL: '/api',
  timeout: 10000
})

const axiosInstance = {
  getFrontEndAxiosInstance() {
    return frontEndAxiosInstance
  },
  getBackEndAxiosInstance() {
    return backEndAxiosInstance
  }
}

frontEndAxiosInstance.interceptors.request.use((config) => {
  config.headers['Content-type'] = 'application/json'
  config.headers.Accept = 'application/json'
  return config
})

backEndAxiosInstance.interceptors.request.use((config) => {
  config.headers['Content-type'] = 'application/json'
  config.headers.Accept = 'application/json'
  return config
})

frontEndAxiosInstance.interceptors.response.use(
  (response) => {
    return response
  },
  (error) => {
    return error
  }
)

backEndAxiosInstance.interceptors.response.use(
  (response) => {
    return response
  },
  (error) => {
    return error
  }
)

export default axiosInstance
