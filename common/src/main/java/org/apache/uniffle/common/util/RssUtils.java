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

package org.apache.uniffle.common.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.common.collect.Lists;
import com.google.common.net.InetAddresses;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.ShuffleDataSegment;
import org.apache.uniffle.common.ShuffleIndexResult;

public class RssUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(RssUtils.class);

  private RssUtils() {
  }

  /**
   * Load properties present in the given file.
   */
  public static Map<String, String> getPropertiesFromFile(String filename) {
    if (filename == null) {
      String rssHome = System.getenv("RSS_HOME");
      if (rssHome == null) {
        LOGGER.error("Both conf file and RSS_HOME env is null");
        return null;
      }

      LOGGER.info("Conf file is null use {}'s server.conf", rssHome);
      filename = rssHome + "/server.conf";
    }

    File file = new File(filename);

    if (!file.exists()) {
      LOGGER.error("Properties file " + filename + " does not exist");
      return null;
    }

    if (!file.isFile()) {
      LOGGER.error("Properties file " + filename + " is not a normal file");
      return null;
    }

    LOGGER.info("Load config from {}", filename);
    final Map<String, String> result = new HashMap<>();

    try (InputStreamReader inReader = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8)) {
      Properties properties = new Properties();
      properties.load(inReader);
      properties.stringPropertyNames().forEach(k -> result.put(k, properties.getProperty(k).trim()));
    } catch (IOException ignored) {
      LOGGER.error("Failed when loading rss properties from " + filename);
    }

    return result;
  }

  // `InetAddress.getLocalHost().getHostAddress()` could return 127.0.0.1. To avoid
  // this situation, we can get current ip through network interface (filtered ipv6,
  // loop back, etc.). If the network interface in the machine is more than one, we
  // will choose the first IP.
  public static String getHostIp() throws Exception {
    // For K8S, there are too many IPs, it's hard to decide which we should use.
    // So we use the environment variable to tell RSS to use which one.
    String ip = System.getenv("RSS_IP");
    if (ip != null) {
      if (!InetAddresses.isInetAddress(ip)) {
        throw new RuntimeException("Environment RSS_IP: " + ip + " is wrong format");
      }
      return ip;
    }
    Enumeration<NetworkInterface> nif = NetworkInterface.getNetworkInterfaces();
    String siteLocalAddress = null;
    while (nif.hasMoreElements()) {
      NetworkInterface ni = nif.nextElement();
      if (!ni.isUp() || ni.isLoopback() || ni.isPointToPoint() || ni.isVirtual()) {
        continue;
      }
      for (InterfaceAddress ifa : ni.getInterfaceAddresses()) {
        InetAddress ia = ifa.getAddress();
        InetAddress brd = ifa.getBroadcast();
        if (brd == null || brd.isAnyLocalAddress()) {
          LOGGER.info("ip {} was filtered, because it don't have effective broadcast address", ia.getHostAddress());
          continue;
        }
        if (!ia.isLinkLocalAddress() && !ia.isAnyLocalAddress() && !ia.isLoopbackAddress()
            && ia instanceof Inet4Address && ia.isReachable(5000)) {
          if (!ia.isSiteLocalAddress()) {
            return ia.getHostAddress();
          } else if (siteLocalAddress == null) {
            LOGGER.info("ip {} was candidate, if there is no better choice, we will choose it", ia.getHostAddress());
            siteLocalAddress = ia.getHostAddress();
          } else {
            LOGGER.info("ip {} was filtered, because it's not first effect site local address", ia.getHostAddress());
          }
        } else if (!(ia instanceof Inet4Address)) {
          LOGGER.info("ip {} was filtered, because it's just a ipv6 address", ia.getHostAddress());
        } else if (ia.isLinkLocalAddress()) {
          LOGGER.info("ip {} was filtered, because it's just a link local address", ia.getHostAddress());
        } else if (ia.isAnyLocalAddress()) {
          LOGGER.info("ip {} was filtered, because it's just a any local address", ia.getHostAddress());
        } else if (ia.isLoopbackAddress()) {
          LOGGER.info("ip {} was filtered, because it's just a loop back address", ia.getHostAddress());
        } else {
          LOGGER.info("ip {} was filtered, because it's just not reachable address", ia.getHostAddress());
        }
      }
    }
    return siteLocalAddress;
  }

  public static byte[] serializeBitMap(Roaring64NavigableMap bitmap) throws IOException {
    long size = bitmap.serializedSizeInBytes();
    if (size > Integer.MAX_VALUE) {
      throw new RuntimeException("Unsupported serialized size of bitmap: " + size);
    }
    ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream((int) size);
    DataOutputStream dataOutputStream = new DataOutputStream(arrayOutputStream);
    bitmap.serialize(dataOutputStream);
    return arrayOutputStream.toByteArray();
  }

  public static Roaring64NavigableMap deserializeBitMap(byte[] bytes) throws IOException {
    Roaring64NavigableMap bitmap = Roaring64NavigableMap.bitmapOf();
    if (bytes.length == 0) {
      return bitmap;
    }
    DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes));
    bitmap.deserialize(dataInputStream);
    return bitmap;
  }

  public static Roaring64NavigableMap cloneBitMap(Roaring64NavigableMap bitmap) {
    Roaring64NavigableMap clone = Roaring64NavigableMap.bitmapOf();
    clone.or(bitmap);
    return clone;
  }

  public static List<ShuffleDataSegment> transIndexDataToSegments(
      ShuffleIndexResult shuffleIndexResult, int readBufferSize) {
    if (shuffleIndexResult == null || shuffleIndexResult.isEmpty()) {
      return Lists.newArrayList();
    }

    byte[] indexData = shuffleIndexResult.getIndexData();
    return transIndexDataToSegments(indexData, readBufferSize);
  }

  private static List<ShuffleDataSegment> transIndexDataToSegments(byte[] indexData, int readBufferSize) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(indexData);
    List<BufferSegment> bufferSegments = Lists.newArrayList();
    List<ShuffleDataSegment> dataFileSegments = Lists.newArrayList();
    int bufferOffset = 0;
    long fileOffset = -1;

    while (byteBuffer.hasRemaining()) {
      try {
        long offset = byteBuffer.getLong();
        int length = byteBuffer.getInt();
        int uncompressLength = byteBuffer.getInt();
        long crc = byteBuffer.getLong();
        long blockId = byteBuffer.getLong();
        long taskAttemptId = byteBuffer.getLong();
        // The index file is written, read and parsed sequentially, so these parsed index segments
        // index a continuous shuffle data in the corresponding data file and the first segment's
        // offset field is the offset of these shuffle data in the data file.
        if (fileOffset == -1) {
          fileOffset = offset;
        }

        bufferSegments.add(new BufferSegment(blockId, bufferOffset, length, uncompressLength, crc, taskAttemptId));
        bufferOffset += length;

        if (bufferOffset >= readBufferSize) {
          ShuffleDataSegment sds = new ShuffleDataSegment(fileOffset, bufferOffset, bufferSegments);
          dataFileSegments.add(sds);
          bufferSegments = Lists.newArrayList();
          bufferOffset = 0;
          fileOffset = -1;
        }
      } catch (BufferUnderflowException ue) {
        LOGGER.warn("Read index data under flow", ue);
        break;
      }
    }

    if (bufferOffset > 0) {
      ShuffleDataSegment sds = new ShuffleDataSegment(fileOffset, bufferOffset, bufferSegments);
      dataFileSegments.add(sds);
    }

    return dataFileSegments;
  }

  public static String generateShuffleKey(String appId, int shuffleId) {
    return String.join(Constants.KEY_SPLIT_CHAR, appId, String.valueOf(shuffleId));
  }

  public static String generatePartitionKey(String appId, Integer shuffleId, Integer partition) {
    return String.join(Constants.KEY_SPLIT_CHAR, appId, String.valueOf(shuffleId), String.valueOf(partition));
  }

  public static <T> List<T> loadExtensions(
      Class<T> extClass, List<String> classes, Object obj) throws RuntimeException {
    if (classes == null || classes.isEmpty()) {
      throw new RuntimeException("Empty classes");
    }

    List<T> extensions = Lists.newArrayList();
    for (String name : classes) {
      try {
        Class<?> klass = Class.forName(name);
        if (!extClass.isAssignableFrom(klass)) {
          throw new RuntimeException(name + " is not subclass of " + extClass.getName());
        }

        Constructor<?> constructor;
        T instance;
        try {
          constructor = klass.getConstructor(obj.getClass());
          instance = (T) constructor.newInstance(obj);
        } catch (Exception e) {
          LOGGER.error("Fail to new instance.", e);
          instance = (T) klass.getConstructor().newInstance();
        }
        extensions.add(instance);
      } catch (Exception e) {
        LOGGER.error("Fail to new instance using default constructor.", e);
        throw new RuntimeException(e);
      }
    }
    return extensions;
  }

  public static void checkQuorumSetting(int replica, int replicaWrite, int replicaRead) {
    if (replica < 1 || replicaWrite > replica || replicaRead > replica) {
      throw new RuntimeException("Replica config is invalid, recommend replica.write + replica.read > replica");
    }
    if (replicaWrite + replicaRead <= replica) {
      throw new RuntimeException("Replica config is unsafe, recommend replica.write + replica.read > replica");
    }
  }

  // the method is used to transfer hostname to metric name.
  // With standard of hostname, `-` and `.` will be included in hostname,
  // but they are not valid for metric name, replace them with '_'
  public static String getMetricNameForHostName(String hostName) {
    if (hostName == null) {
      return "";
    }
    return hostName.replaceAll("[\\.-]", "_");
  }
}
