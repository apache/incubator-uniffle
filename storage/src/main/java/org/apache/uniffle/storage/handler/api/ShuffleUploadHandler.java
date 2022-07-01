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

package org.apache.uniffle.storage.handler.api;

import java.io.File;
import java.util.List;

import org.apache.uniffle.storage.util.ShuffleUploadResult;

public interface ShuffleUploadHandler {

  /**
   * Upload data files and index files to remote storage, the correctness of relation between
   * items of data files, index files and partitions is guaranteed by caller/user.
   * Normally we best-effort strategy to upload files, so the result may be part of the files.
   *
   * @param dataFiles   local data files to be uploaded to remote storage
   * @param indexFiles  local index files to be uploaded to remote storage
   * @param partitions  partition id of the local data files and index files
   *
   * @return  upload result including total successful uploaded file size and partition id list
   *
   */
  ShuffleUploadResult upload(List<File> dataFiles, List<File> indexFiles, List<Integer> partitions);

}
