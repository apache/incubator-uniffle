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

import org.apache.uniffle.coordinator.ApplicationManager;
import org.apache.uniffle.coordinator.web.Response;
import org.apache.uniffle.coordinator.web.vo.AppInfoVO;
import org.apache.uniffle.coordinator.web.vo.UserAppNumVO;

@Produces({MediaType.APPLICATION_JSON})
public class ApplicationResource extends BaseResource {

  @Context protected ServletContext servletContext;

  @GET
  @Path("/total")
  public Response<Map<String, Integer>> getAppTotality() {
    return execute(
        () -> {
          Map<String, Integer> appTotalityMap = Maps.newHashMap();
          appTotalityMap.put("appTotality", getApplicationManager().getAppIds().size());
          return appTotalityMap;
        });
  }

  @GET
  @Path("/userTotal")
  public Response<List<UserAppNumVO>> getUserApps() {
    return execute(
        () -> {
          Map<String, Map<String, Long>> currentUserAndApp =
              getApplicationManager().getCurrentUserAndApp();
          List<UserAppNumVO> usercnt = new ArrayList<>();
          for (Map.Entry<String, Map<String, Long>> stringMapEntry : currentUserAndApp.entrySet()) {
            String userName = stringMapEntry.getKey();
            usercnt.add(new UserAppNumVO(userName, stringMapEntry.getValue().size()));
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
          Map<String, Map<String, Long>> currentUserAndApp =
              getApplicationManager().getCurrentUserAndApp();
          for (Map.Entry<String, Map<String, Long>> userAppIdTimestampMap :
              currentUserAndApp.entrySet()) {
            String userName = userAppIdTimestampMap.getKey();
            for (Map.Entry<String, Long> appIdTimestampMap :
                userAppIdTimestampMap.getValue().entrySet()) {
              userToAppList.add(
                  new AppInfoVO(
                      userName, appIdTimestampMap.getKey(), appIdTimestampMap.getValue()));
            }
          }
          // Display is inverted by the submission time of the application.
          userToAppList.sort(Comparator.reverseOrder());
          return userToAppList;
        });
  }

  private ApplicationManager getApplicationManager() {
    return (ApplicationManager)
        servletContext.getAttribute(ApplicationManager.class.getCanonicalName());
  }
}
