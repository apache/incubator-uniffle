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

package org.apache.uniffle.coordinator.conf;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.coordinator.metric.CoordinatorMetrics;

import static org.apache.uniffle.coordinator.CoordinatorConf.COORDINATOR_CLIENT_CONF_APPLY_STRATEGY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RssClientConfApplyManagerTest {

  @BeforeEach
  public void setUp() {
    CoordinatorMetrics.register();
  }

  @AfterEach
  public void clear() {
    CoordinatorMetrics.clear();
  }

  @Test
  public void testBypassApply() {
    DynamicClientConfService dynamicClientConfService = mock(DynamicClientConfService.class);
    Map<String, String> conf = new HashMap<>();
    conf.put("a", "b");
    when(dynamicClientConfService.getRssClientConf()).thenReturn(conf);

    CoordinatorConf coordinatorConf = new CoordinatorConf();

    RssClientConfApplyManager applyManager =
        new RssClientConfApplyManager(coordinatorConf, dynamicClientConfService);

    assertEquals(conf, applyManager.apply(RssClientConfFetchInfo.EMPTY_CLIENT_CONF_FETCH_INFO));
    assertEquals(conf, applyManager.apply(new RssClientConfFetchInfo("a", Collections.emptyMap())));
  }

  public static final class MockedRssClientConfApplyStrategy
      extends AbstractRssClientConfApplyStrategy {
    private Set<String> legalUsers;

    public MockedRssClientConfApplyStrategy(DynamicClientConfService dynamicClientConfService) {
      super(dynamicClientConfService);
    }

    public void setLegalUsers(Set<String> legalUsers) {
      this.legalUsers = legalUsers;
    }

    @Override
    Map<String, String> apply(RssClientConfFetchInfo rssClientConfFetchInfo) {
      if (legalUsers.contains(rssClientConfFetchInfo.getUser())) {
        return dynamicClientConfService.getRssClientConf();
      }
      return Collections.EMPTY_MAP;
    }
  }

  @Test
  public void testCustomizeApplyStrategy() {
    DynamicClientConfService dynamicClientConfService = mock(DynamicClientConfService.class);
    Map<String, String> conf = new HashMap<>();
    conf.put("a", "b");
    when(dynamicClientConfService.getRssClientConf()).thenReturn(conf);

    CoordinatorConf coordinatorConf = new CoordinatorConf();
    coordinatorConf.set(
        COORDINATOR_CLIENT_CONF_APPLY_STRATEGY, MockedRssClientConfApplyStrategy.class.getName());

    RssClientConfApplyManager applyManager =
        new RssClientConfApplyManager(coordinatorConf, dynamicClientConfService);
    MockedRssClientConfApplyStrategy strategy =
        (MockedRssClientConfApplyStrategy) applyManager.getStrategy();
    strategy.setLegalUsers(Sets.newHashSet("a", "b", "c"));

    // if using the older client version, it will always return the conf
    assertEquals(conf, applyManager.apply(RssClientConfFetchInfo.EMPTY_CLIENT_CONF_FETCH_INFO));

    assertEquals(conf, applyManager.apply(new RssClientConfFetchInfo("a", Collections.emptyMap())));
    assertEquals(conf, applyManager.apply(new RssClientConfFetchInfo("b", Collections.emptyMap())));
    assertEquals(conf, applyManager.apply(new RssClientConfFetchInfo("c", Collections.emptyMap())));
    assertEquals(
        0, applyManager.apply(new RssClientConfFetchInfo("d", Collections.emptyMap())).size());
  }
}
