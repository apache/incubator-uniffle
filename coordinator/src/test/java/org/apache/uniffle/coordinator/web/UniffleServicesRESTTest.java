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

package org.apache.uniffle.coordinator.web;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.Application;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.coordinator.GenericTestUtils;
import org.apache.uniffle.coordinator.web.request.ApplicationRequest;

import static org.apache.uniffle.common.web.resource.Response.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UniffleServicesRESTTest {

  private static final Logger LOG = LoggerFactory.getLogger(UniffleServicesRESTTest.class);
  public static final String SYSPROP_TEST_DATA_DIR = "test.build.data";
  public static final String DEFAULT_TEST_DATA_DIR =
      "target" + File.separator + "test" + File.separator + "data";
  public static final String COORDINATOR_CONF_PATH = "./src/test/resources/coordinator.conf";
  public static final String APPLICATIONS = "/api/server/applications";
  private static UniffleJavaProcess coordinatorServer;
  private static String coordinatorAddress;
  private static CoordinatorConf conf;

  @BeforeAll
  public static void setUp() throws Exception {
    conf = new CoordinatorConf(COORDINATOR_CONF_PATH);

    File baseDir = getTestDir("processes");
    baseDir.mkdirs();
    String baseName = UniffleServicesRESTTest.class.getSimpleName();
    coordinatorAddress = getCoordinatorWebAppURLWithScheme(conf);

    File coordinatorOutput = new File(baseDir, baseName + "-coordinator.log");
    coordinatorOutput.createNewFile();
    coordinatorServer = new UniffleJavaProcess(CoordinatorTestServer.class, coordinatorOutput);
    waitUniffleCoordinatorWebRunning(coordinatorAddress, "/api/server/status");
  }

  @AfterAll
  public static void shutdown() throws Exception {
    if (coordinatorServer != null) {
      coordinatorServer.stop();
    }
  }

  public static File getTestDir(String subdir) {
    return new File(getTestDir(), subdir).getAbsoluteFile();
  }

  public static File getTestDir() {
    String prop = System.getProperty(SYSPROP_TEST_DATA_DIR, DEFAULT_TEST_DATA_DIR);
    if (prop.isEmpty()) {
      prop = DEFAULT_TEST_DATA_DIR;
    }
    File dir = new File(prop).getAbsoluteFile();
    dir.mkdirs();
    assertExists(dir);
    return dir;
  }

  /**
   * Wait for Coordinator Web to start.
   *
   * @param address Coordinator WebAddress.
   * @param path Accessed URL path.
   */
  private static void waitUniffleCoordinatorWebRunning(final String address, final String path) {
    try {
      GenericTestUtils.waitFor(
          () -> {
            try {
              String response = sendGET(address + path);
              if (StringUtils.isNotBlank(response)) {
                Gson gson = new Gson();
                Type type = new TypeToken<Map<String, Object>>() {}.getType();
                Map<String, Object> map = gson.fromJson(response, type);
                String data = (String) map.getOrDefault("data", "failed");
                if (data.equals("success")) {
                  // process is up and running
                  return true;
                }
              }
            } catch (Exception e) {
              // process is not up and running
            }
            return false;
          },
          1000,
          20 * 1000);
    } catch (Exception e) {
      fail("Web app not running");
    }
  }

  public static void assertExists(File f) {
    assertTrue(f.exists(), "File " + f + " should exist");
  }

  /**
   * Provide the URL to get the return result of the GET request.
   *
   * @param getURL url.
   * @return The result returned by the request.
   * @throws IOException an I/O exception of some sort has occurred.
   */
  private static String sendGET(String getURL) throws IOException {

    URL obj = new URL(getURL);
    HttpURLConnection con = (HttpURLConnection) obj.openConnection();
    con.setRequestMethod("GET");
    con.setRequestProperty("User-Agent", "User-Agent");
    int responseCode = con.getResponseCode();
    LOG.info("GET Response Code : {}", responseCode);

    // success
    if (responseCode == HttpURLConnection.HTTP_OK) {
      BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
      String inputLine;
      StringBuffer response = new StringBuffer();

      while ((inputLine = in.readLine()) != null) {
        response.append(inputLine);
      }
      in.close();

      // return result
      return response.toString();
    } else {
      LOG.warn("GET request did not work.");
    }
    return "";
  }

  /**
   * @param postURL
   * @param postParams
   * @return
   * @throws IOException
   */
  private static String sendPOST(String postURL, String postParams) throws IOException {
    URL obj = new URL(postURL);
    HttpURLConnection con = (HttpURLConnection) obj.openConnection();
    con.setRequestMethod("POST");
    con.setRequestProperty("User-Agent", "User-Agent");
    con.setRequestProperty("Content-Type", "application/json");

    con.setDoOutput(true);
    OutputStream os = con.getOutputStream();
    os.write(postParams.getBytes());
    os.flush();
    os.close();

    int responseCode = con.getResponseCode();
    LOG.info("POST Response Code : {}", responseCode);

    if (responseCode == HttpURLConnection.HTTP_OK) {
      BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
      String inputLine;
      StringBuffer response = new StringBuffer();

      while ((inputLine = in.readLine()) != null) {
        response.append(inputLine);
      }
      in.close();

      // return result
      return response.toString();
    } else {
      LOG.warn("POST request did not work.");
      System.out.println("POST request did not work.");
    }
    return "";
  }

  public static String getCoordinatorWebAppURLWithScheme(CoordinatorConf conf) {
    return "http://localhost:" + conf.getInteger(RssBaseConf.JETTY_HTTP_PORT);
  }

  class ApplicationDatas {
    private int code;
    private List<Application> data;
    private String errMsg;

    public int getCode() {
      return code;
    }

    public void setCode(int code) {
      this.code = code;
    }

    public List<Application> getData() {
      return data;
    }

    public void setData(List<Application> data) {
      this.data = data;
    }

    public String getErrMsg() {
      return errMsg;
    }

    public void setErrMsg(String errMsg) {
      this.errMsg = errMsg;
    }
  }

  @Test
  public void testGetApplications() throws Exception {
    // In this test case,
    // we tested the filter condition 1: applications collection,
    // we added 3 application_ids to the collection,
    // and the Coordinator will return the information of these 3 applications.

    final ApplicationRequest request = new ApplicationRequest();

    Set<String> applications = new HashSet<>();
    applications.add("application_1");
    applications.add("application_2");
    applications.add("application_3");
    request.setApplications(applications);

    Gson gson = new Gson();
    String params = gson.toJson(request);

    String response = sendPOST(coordinatorAddress + APPLICATIONS, params);
    assertNotNull(response);

    ApplicationDatas dataModel = gson.fromJson(response, ApplicationDatas.class);
    assertNotNull(dataModel);

    List<Application> datas = dataModel.getData();
    assertNotNull(datas);
    assertEquals(3, datas.size());

    for (Application application : datas) {
      assertTrue(applications.contains(application.getApplicationId()));
    }
  }

  @Test
  public void testGetApplicationsWithNoFilter() throws Exception {
    // In this test case, we did not set any filter conditions,
    // we will get 10 records from the coordinator

    final ApplicationRequest request = new ApplicationRequest();

    Gson gson = new Gson();
    String params = gson.toJson(request);

    String response = sendPOST(coordinatorAddress + APPLICATIONS, params);
    assertNotNull(response);

    ApplicationDatas dataModel = gson.fromJson(response, ApplicationDatas.class);
    assertNotNull(dataModel);

    List<Application> datas = dataModel.getData();
    assertNotNull(datas);
    assertEquals(10, datas.size());

    // We sort the result set, we should get the following application:
    // application_[0,1,10,100,1000,1001,1002,1003,1004,1005]
    Set<String> applications = new HashSet<>();
    applications.add("application_0");
    applications.add("application_1");
    applications.add("application_10");
    applications.add("application_100");
    applications.add("application_1000");
    applications.add("application_1001");
    applications.add("application_1002");
    applications.add("application_1003");
    applications.add("application_1004");
    applications.add("application_1005");

    for (Application application : datas) {
      assertTrue(applications.contains(application.getApplicationId()));
    }
  }

  @Test
  public void testGetApplicationsWithAppRegex() throws Exception {

    // In this test case, we will test the functionality of regular expression matching.
    // We want to match application_id that contains 1000.
    final ApplicationRequest request = new ApplicationRequest();
    request.setAppIdRegex(".*1000.*");

    Gson gson = new Gson();
    String params = gson.toJson(request);

    String response = sendPOST(coordinatorAddress + APPLICATIONS, params);
    assertNotNull(response);

    ApplicationDatas dataModel = gson.fromJson(response, ApplicationDatas.class);
    assertNotNull(dataModel);

    List<Application> data = dataModel.getData();
    assertNotNull(data);
    assertEquals(1, data.size());

    Application application = data.get(0);
    assertNotNull(application);
    assertEquals("application_1000", application.getApplicationId());
    assertEquals("test", application.getUser());
  }

  @Test
  public void testGetApplicationsPage() throws Exception {

    // In this test case, we apply to read the records on page 2,
    // and set up to return 20 records per page.
    final ApplicationRequest request = new ApplicationRequest();
    request.setCurrentPage(2);
    request.setPageSize(20);

    Gson gson = new Gson();
    String params = gson.toJson(request);

    String response = sendPOST(coordinatorAddress + APPLICATIONS, params);
    assertNotNull(response);

    ApplicationDatas dataModel = gson.fromJson(response, ApplicationDatas.class);
    assertNotNull(dataModel);

    List<Application> datas = dataModel.getData();
    assertNotNull(datas);
    assertEquals(20, datas.size());

    // 5 records. application_[1015,1016,1017,1018,1019]
    // 11 records. application_[102,1020,1021,1022,1023,1024,1025,1026,1027,1028,1029]
    // 4 records. application_[103,1030,1031,1032]
    Set<String> applications = new HashSet<>();
    applications.add("application_1015");
    applications.add("application_1016");
    applications.add("application_1017");
    applications.add("application_1018");
    applications.add("application_1019");
    applications.add("application_102");
    applications.add("application_1020");
    applications.add("application_1021");
    applications.add("application_1022");
    applications.add("application_1023");
    applications.add("application_1024");
    applications.add("application_1025");
    applications.add("application_1026");
    applications.add("application_1027");
    applications.add("application_1028");
    applications.add("application_1029");
    applications.add("application_103");
    applications.add("application_1030");
    applications.add("application_1031");
    applications.add("application_1032");

    for (Application application : datas) {
      assertTrue(applications.contains(application.getApplicationId()));
    }
  }

  @Test
  public void testGetApplicationsWithNull() throws Exception {
    // In this test case, we apply to read the records on page 2,
    // and set up to return 20 records per page.
    final ApplicationRequest request = null;
    Gson gson = new Gson();
    String params = gson.toJson(request);

    String response = sendPOST(coordinatorAddress + APPLICATIONS, params);
    assertNotNull(response);

    ApplicationDatas dataModel = gson.fromJson(response, ApplicationDatas.class);
    assertNotNull(dataModel);
    assertEquals(-1, dataModel.code);
    assertEquals("ApplicationRequest Is not null", dataModel.errMsg);
  }

  @Test
  public void testGetApplicationsWithStartTimeAndEndTime() throws Exception {

    // In this test case, we set two groups of heartBeatStartTime and heartBeatEndTime respectively.
    // We expect no data to be obtained in the first group,
    // and we expect to obtain 10 data in the second group.
    long startTime = new Date().getTime();
    long endTime = new Date().getTime() + 100;

    final ApplicationRequest request = new ApplicationRequest();
    request.setHeartBeatStartTime(String.valueOf(startTime));
    request.setHeartBeatEndTime(String.valueOf(endTime));

    Gson gson = new Gson();
    String params = gson.toJson(request);
    String response = sendPOST(coordinatorAddress + APPLICATIONS, params);
    assertNotNull(response);

    ApplicationDatas dataModel = gson.fromJson(response, ApplicationDatas.class);
    assertNotNull(dataModel);
    List<Application> datas = dataModel.getData();
    assertNotNull(datas);
    assertEquals(0, datas.size());

    startTime = 0;
    final ApplicationRequest request2 = new ApplicationRequest();
    request2.setHeartBeatStartTime(String.valueOf(startTime));
    request2.setHeartBeatEndTime(String.valueOf(endTime));

    String params2 = gson.toJson(request2);
    String response2 = sendPOST(coordinatorAddress + APPLICATIONS, params2);
    assertNotNull(response2);
    ApplicationDatas dataModel2 = gson.fromJson(response2, ApplicationDatas.class);
    assertNotNull(dataModel2);
    List<Application> datas2 = dataModel2.getData();

    assertEquals(10, datas2.size());
  }
}
