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

package org.apache.uniffle.coordinator.web.resource;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import javax.servlet.ServletContext;

import com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.javax.ws.rs.GET;
import org.apache.hbase.thirdparty.javax.ws.rs.Path;
import org.apache.hbase.thirdparty.javax.ws.rs.Produces;
import org.apache.hbase.thirdparty.javax.ws.rs.core.Context;
import org.apache.hbase.thirdparty.javax.ws.rs.core.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.web.resource.BaseResource;
import org.apache.uniffle.common.web.resource.Response;
import org.apache.uniffle.coordinator.AppInfo;
import org.apache.uniffle.coordinator.ApplicationManager;
import org.apache.uniffle.coordinator.CoordinatorServer;
import org.apache.uniffle.coordinator.metric.CoordinatorMetrics;
import org.apache.uniffle.coordinator.web.vo.AppInfoVO;
import org.apache.uniffle.coordinator.web.vo.UserAppNumVO;

@Produces({MediaType.APPLICATION_JSON})
public class ApplicationResource extends BaseResource {
  private static final Logger LOG = LoggerFactory.getLogger(ApplicationResource.class);
  @Context protected ServletContext servletContext;

  @GET
  @Path("/total")
  public Response<Map<String, Integer>> getAppTotality() {
    return execute(
        () -> {
          Map<String, Integer> appTotalityMap = Maps.newHashMap();
          appTotalityMap.put("appCurrent", getApplicationManager().getAppIds().size());
          appTotalityMap.put("appTotality", (int) CoordinatorMetrics.counterTotalAppNum.get());
          return appTotalityMap;
        });
  }

  @GET
  @Path("/userTotal")
  public Response<List<UserAppNumVO>> getUserApps() {
    return execute(
        () -> {
          Map<String, Map<String, AppInfo>> currentUserAndApp =
              getApplicationManager().getCurrentUserAndApp();
          List<UserAppNumVO> usercnt = new ArrayList<>();
          Map<String, Integer> userAppNumMap = Maps.newHashMap();
          int currentUserAppNum = 0;
          for (Map.Entry<String, Map<String, AppInfo>> stringMapEntry :
              currentUserAndApp.entrySet()) {
            currentUserAppNum += stringMapEntry.getValue().size();
            userAppNumMap.put(stringMapEntry.getKey(), stringMapEntry.getValue().size());
          }
          getApplicationManager()
              .getCachedAppInfos(currentUserAppNum)
              .forEach(
                  appInfoVO -> {
                    userAppNumMap.computeIfAbsent(appInfoVO.getUserName(), k -> 0);
                    userAppNumMap.put(
                        appInfoVO.getUserName(), userAppNumMap.get(appInfoVO.getUserName()) + 1);
                  });
          for (Map.Entry<String, Integer> stringIntegerEntry : userAppNumMap.entrySet()) {
            String userName = stringIntegerEntry.getKey();
            usercnt.add(new UserAppNumVO(userName, stringIntegerEntry.getValue()));
          }
          // Display inverted by the number of user applications.
          usercnt.sort(Comparator.reverseOrder());
          return usercnt;
        });
  }

  @GET
  @Path("/appInfos")
  public Response<List<AppInfoVO>> getAppInfoList() {
    return execute(
        () -> {
          List<AppInfoVO> userToAppList = new ArrayList<>();
          Map<String, Map<String, AppInfo>> currentUserAndApp =
              getApplicationManager().getCurrentUserAndApp();
          for (Map.Entry<String, Map<String, AppInfo>> userAppIdTimestampMap :
              currentUserAndApp.entrySet()) {
            for (Map.Entry<String, AppInfo> appIdTimestampMap :
                userAppIdTimestampMap.getValue().entrySet()) {
              AppInfo appInfo = appIdTimestampMap.getValue();
              AppInfoVO appInfoVO =
                  getCoordinatorServer().getAppInfoV0(userAppIdTimestampMap.getKey(), appInfo);
              userToAppList.add(appInfoVO);
            }
          }
          userToAppList.addAll(getApplicationManager().getCachedAppInfos(userToAppList.size()));
          // Display is inverted by the submission time of the application.
          userToAppList.sort(Comparator.reverseOrder());
          return userToAppList;
        });
  }

  private CoordinatorServer getCoordinatorServer() {
    return (CoordinatorServer)
        servletContext.getAttribute(CoordinatorServer.class.getCanonicalName());
  }

  private ApplicationManager getApplicationManager() {
    return (ApplicationManager)
        servletContext.getAttribute(ApplicationManager.class.getCanonicalName());
  }
}
