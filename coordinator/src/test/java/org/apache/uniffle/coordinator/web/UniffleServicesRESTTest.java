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

import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.coordinator.Application;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.coordinator.GenericTestUtils;
import org.apache.uniffle.coordinator.web.request.ApplicationRequest;

import static org.apache.uniffle.coordinator.web.Response.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UniffleServicesRESTTest {

  private static final Logger LOG = LoggerFactory.getLogger(UniffleServicesRESTTest.class);
  public static final String SYSPROP_TEST_DATA_DIR = "test.build.data";
  public static final String DEFAULT_TEST_DATA_DIR =
      "target" + File.separator + "test" + File.separator + "data";
  public static final String COORDINATOR_CONF_PATH = "./src/test/resources/coordinator.conf";
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

    final ApplicationRequest request = new ApplicationRequest();

    Set<String> applications = new HashSet<>();
    applications.add("application_1");
    applications.add("application_2");
    applications.add("application_3");
    request.setApplications(applications);

    Gson gson = new Gson();
    String params = gson.toJson(request);

    String response = sendPOST(coordinatorAddress + "/api/server/applications", params);
    assertNotNull(response);

    ApplicationDatas dataModel = gson.fromJson(response, ApplicationDatas.class);
    assertNotNull(dataModel);

    List<Application> data = dataModel.getData();
    assertNotNull(data);
    assertEquals(3, data.size());
  }
}
