package org.apache.uniffle.coordinator.conf;

import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.yaml.snakeyaml.Yaml;

import org.apache.uniffle.common.RemoteStorageInfo;

/**
 * The conf will be stored in the yaml format file.
 */
public class YamlClientConfParser implements ClientConfParser {
  private static final String RSS_CLIENT_CONF_KEY = "rssClientConf";
  private static final String REMOTE_STORAGE_INFOS_KEY = "remoteStorageInfos";

  @Override
  public ClientConf tryParse(InputStream fileInputStream) throws Exception {
    Yaml yaml = new Yaml();
    Map<Object, Object> data = yaml.load(IOUtils.toString(fileInputStream));

    Object rssClientConfRaw = data.get(RSS_CLIENT_CONF_KEY);
    Map<String, String> rssConfKVs =
        rssClientConfRaw == null ? Collections.EMPTY_MAP : parseKVItems(rssClientConfRaw);

    Map<String, Object> remoteStorageInfosRaw =
        (Map<String, Object>) data.getOrDefault(REMOTE_STORAGE_INFOS_KEY, Collections.EMPTY_MAP);

    Map<String, RemoteStorageInfo> remoteStorageInfoMap = new HashMap<>();
    for (Map.Entry<String, Object> entry : remoteStorageInfosRaw.entrySet()) {
      String remotePath = entry.getKey();
      Map<String, String> kvs = parseKVItems(entry.getValue());
      remoteStorageInfoMap.put(
          remotePath,
          new RemoteStorageInfo(remotePath, kvs)
      );
    }

    return new ClientConf(rssConfKVs, remoteStorageInfoMap);
  }

  private Map<String, String> parseKVItems(Object confRaw) throws Exception {
    if (confRaw instanceof Map) {
      return (Map<String, String>) confRaw;
    }

    // todo: currently only xml format is supported
    if (confRaw instanceof String) {
      Configuration conf = new Configuration(false);
      conf.addResource(IOUtils.toInputStream((String) confRaw));
      Map<String, String> kvs = new HashMap<>();
      for (Map.Entry<String, String> entry : conf) {
        kvs.put(entry.getKey(), entry.getValue());
      }
      return kvs;
    }

    throw new Exception("No such supported format, only can be 'key : val' or xml.");
  }
}
