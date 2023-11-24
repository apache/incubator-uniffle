package org.apache.uniffle.coordinator.conf;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class YamlClientConfParserTest {

  @Test
  public void testFromFile() throws Exception {
    ClientConfParser parser = new YamlClientConfParser();
    ClientConf conf = parser.tryParse(getClass().getClassLoader().getResource("dynamicClientConf.yaml").openStream());
    assertEquals("v1", conf.getRssClientConf().get("k1"));
    assertEquals("v2", conf.getRssClientConf().get("k2"));
    assertEquals("v1", conf.getRemoteStorageInfos().get("hdfs://a-ns01").getConfItems().get("k1"));
    assertEquals("v1", conf.getRemoteStorageInfos().get("hdfs://x-ns01").getConfItems().get("k1"));
  }

  @Test
  public void testParse() throws Exception {
    ClientConfParser parser = new YamlClientConfParser();

    // rssClientConf with format of 'k : v'

    String yaml = "rssClientConf:\n" +
        "    k1: v1\n" +
        "    k2: v2";

    ClientConf conf = parser.tryParse(IOUtils.toInputStream(yaml));
    assertEquals(2, conf.getRssClientConf().size());
    assertEquals("v1", conf.getRssClientConf().get("k1"));
    assertEquals("v2", conf.getRssClientConf().get("k2"));

    // rssClientConf with format of xml

    yaml = "rssClientConf: |+\n" +
        "   \t<configuration>\n" +
        "    <property>\n" +
        "      <name>k1</name>\n" +
        "      <value>v1</value>\n" +
        "    </property>\n" +
        "    <property>\n" +
        "      <name>k2</name>\n" +
        "      <value>v2</value>\n" +
        "    </property>\n" +
        "    </configuration>";

    conf = parser.tryParse(IOUtils.toInputStream(yaml));
    assertEquals(2, conf.getRssClientConf().size());
    assertEquals("v1", conf.getRssClientConf().get("k1"));
    assertEquals("v2", conf.getRssClientConf().get("k2"));

    // remote storage conf with the format of "k : v"

    yaml = "remoteStorageInfos:\n" +
        "   hdfs://a-ns01:\n" +
        "      k1: v1\n" +
        "      k2: v2\n" +
        "   hdfs://x-ns01:\n" +
        "      k1: v1\n" +
        "      k2: v2";
    conf = parser.tryParse(IOUtils.toInputStream(yaml));
    assertEquals(0, conf.getRssClientConf().size());
    assertEquals(2, conf.getRemoteStorageInfos().size());
    assertEquals("v1", conf.getRemoteStorageInfos().get("hdfs://a-ns01").getConfItems().get("k1"));
    assertEquals("v1", conf.getRemoteStorageInfos().get("hdfs://x-ns01").getConfItems().get("k1"));

    yaml = "remoteStorageInfos:\n" +
        "   hdfs://a-ns01: |+\n" +
        "      <configuration>\n" +
        "      <property>\n" +
        "        <name>k1</name>\n" +
        "        <value>v1</value>\n" +
        "      </property>\n" +
        "      <property>\n" +
        "        <name>k2</name>\n" +
        "        <value>v2</value>\n" +
        "      </property>\n" +
        "      </configuration>\n" +
        "      \n" +
        "   hdfs://x-ns01:\n" +
        "      k1: v1\n" +
        "      k2: v2\n" +
        "\n";
    conf = parser.tryParse(IOUtils.toInputStream(yaml));
    assertEquals(0, conf.getRssClientConf().size());
    assertEquals(2, conf.getRemoteStorageInfos().size());
    assertEquals("v1", conf.getRemoteStorageInfos().get("hdfs://a-ns01").getConfItems().get("k1"));
    assertEquals("v1", conf.getRemoteStorageInfos().get("hdfs://x-ns01").getConfItems().get("k1"));
  }
}
