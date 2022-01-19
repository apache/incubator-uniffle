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

package com.tencent.rss.coordinator;

import java.io.File;
import java.nio.file.Files;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AccessCandidatesChecker maintain a list of candidate access id and update it periodically,
 * it checks the access id in the access request and reject if the id is not in the candidate list.
 */
public class AccessCandidatesChecker implements AccessChecker {
  private static final Logger LOG = LoggerFactory.getLogger(AccessCandidatesChecker.class);

  private final AtomicReference<Set<String>> candidates = new AtomicReference<>();
  private final AtomicLong lastCandidatesUpdateMS = new AtomicLong(0L);
  private final String path;
  private final ScheduledExecutorService updateAccessCandidatesSES;

  public AccessCandidatesChecker(AccessManager accessManager) {
    CoordinatorConf conf = accessManager.getCoordinatorConf();
    this.path = conf.get(CoordinatorConf.COORDINATOR_ACCESS_CANDIDATES_PATH);
    int updateIntervalS = conf.getInteger(CoordinatorConf.COORDINATOR_ACCESS_CANDIDATES_UPDATE_INTERVAL_SEC);
    updateAccessCandidatesSES = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("UpdateAccessCandidates-%d").build());
    updateAccessCandidatesSES.scheduleAtFixedRate(
        this::updateAccessCandidates, 0, updateIntervalS, TimeUnit.SECONDS);
  }

  public AccessCheckResult check(AccessInfo accessInfo) {
    String accessId = accessInfo.getAccessId().trim();
    if (!candidates.get().contains(accessId)) {
      String msg = String.format("Reject accessInfo[%s] accessId[%s] access request.", accessInfo, accessId);
      LOG.debug(msg);
      return new AccessCheckResult(false, msg);
    }

    return new AccessCheckResult(true, "");
  }

  public void close() {
    if (updateAccessCandidatesSES != null) {
      updateAccessCandidatesSES.shutdownNow();
    }
  }

  private void updateAccessCandidates() {
    try {
      File candidatesFile = new File(path);
      if (candidatesFile.exists()) {
        long lastModifiedMS = candidatesFile.lastModified();
        if (lastCandidatesUpdateMS.get() != lastModifiedMS) {
          updateAccessCandidates(candidatesFile);
          lastCandidatesUpdateMS.set(lastModifiedMS);
        }
      } else {
        candidates.set(Sets.newConcurrentHashSet());
      }
      // TODO: add access num metrics
    } catch (Exception e) {
      LOG.warn("Error when update access candidates", e);
    }
  }

  private void updateAccessCandidates(File candidatesFile) {
    Set<String> newCandidates = Sets.newHashSet();
    try (Stream<String> lines = Files.lines(candidatesFile.toPath())) {
      lines.forEach(line -> {
        String taskId = line.trim();
        if (!StringUtils.isEmpty(taskId)) {
          newCandidates.add(taskId);
        }
      });
    } catch (Exception e) {
      LOG.warn("Error when parse file {}", candidatesFile.getAbsolutePath(), e);
    }
    candidates.set(newCandidates);
  }

  public AtomicReference<Set<String>> getCandidates() {
    return candidates;
  }
}
