/*
 * Tencent is pleased to support the open source community by making
 * Firestorm-Spark remote shuffle server available. 
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved. 
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.tencent.rss.common.util;

public class Constants {

  // the value is used for client/server compatible, eg, online upgrade
  public static final String SHUFFLE_SERVER_VERSION = "ss_v3";
  public static final String SHUFFLE_DATA_FILE_SUFFIX = ".data";
  public static final String SHUFFLE_INDEX_FILE_SUFFIX = ".index";
  // BlockId is long and consist of partitionId, taskAttemptId, atomicInt
  // the length of them are ATOMIC_INT_MAX_LENGTH + PARTITION_ID_MAX_LENGTH + TASK_ATTEMPT_ID_MAX_LENGTH = 63
  public static final int PARTITION_ID_MAX_LENGTH = 24;
  public static final int TASK_ATTEMPT_ID_MAX_LENGTH = 20;
  public static final int ATOMIC_INT_MAX_LENGTH = 19;
  public static long MAX_SEQUENCE_NO = (1 << Constants.ATOMIC_INT_MAX_LENGTH) - 1;
  public static long MAX_PARTITION_ID = (1 << Constants.PARTITION_ID_MAX_LENGTH) - 1;
  public static long MAX_TASK_ATTEMPT_ID = (1 << Constants.TASK_ATTEMPT_ID_MAX_LENGTH) - 1;
  public static long INVALID_BLOCK_ID = -1L;
  public static final String KEY_SPLIT_CHAR = "/";
  public static final String COMMA_SPLIT_CHAR = ",";
  public static final String COMMON_SUCCESS_MESSAGE = "SUCCESS";
  public static final String SORT_SHUFFLE_MANAGER_NAME = "org.apache.spark.shuffle.sort.SortShuffleManager";

  public static final String RSS_CLIENT_CONF_COMMON_PREFIX = "rss.client";
  public static final String CONF_REMOTE_STORAGE_PATH = ".remote.storage.path";
  public static final String RSS_CLIENT_CONF_REMOTE_STORAGE_PATH =
          RSS_CLIENT_CONF_COMMON_PREFIX + CONF_REMOTE_STORAGE_PATH;
}
