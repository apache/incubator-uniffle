package org.apache.uniffle.test

import com.google.common.collect.Maps
import org.apache.commons.lang3.StringUtils
import org.apache.spark.shuffle.RssSparkConfig
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.uniffle.common.rpc.ServerType
import org.apache.uniffle.coordinator.CoordinatorConf
import org.apache.uniffle.server.ShuffleServerConf
import org.apache.uniffle.storage.util.StorageType
import org.apache.uniffle.test.IntegrationTestBase.{addDynamicConf, createCoordinatorServer, createShuffleServer, getCoordinatorConf, getShuffleServerConf, startServers}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{BeforeAll, Test}

import java.io.{File, FileWriter, PrintWriter}
import java.util
import java.util.Map

object SparkSQLTestWithRemoteSortScala {

  @BeforeAll
  @throws[Exception]
  def setupServers(): Unit = {
    val coordinatorConf: CoordinatorConf = getCoordinatorConf
    val dynamicConf: util.HashMap[String, String] = new util.HashMap[String, String]()
    dynamicConf.put(RssSparkConfig.RSS_STORAGE_TYPE.key, StorageType.MEMORY_LOCALFILE.name)
    addDynamicConf(coordinatorConf, dynamicConf)
    createCoordinatorServer(coordinatorConf)
    val shuffleServerConf = getShuffleServerConf(ServerType.GRPC)
    shuffleServerConf.setBoolean(ShuffleServerConf.SERVER_MERGE_ENABLE, true)
    createShuffleServer(shuffleServerConf)
    startServers()
  }
}

class SparkSQLTestWithRemoteSortScala extends SparkIntegrationTestBase {

  @Test
  @throws[Exception]
  def sparkSQLTest(): Unit = {
    run()
  }

  @throws[Exception]
  override def run(): Unit = {
    val fileName = generateTestFile
    val sparkConf = createSparkConf
    // lz4 conflict, so use snappy here
    sparkConf.set("spark.io.compression.codec", "snappy")
    // 1 Run spark with remote sort rss
    val sparkConfWithRemoteSortRss = sparkConf.clone
    updateSparkConfWithRss(sparkConfWithRemoteSortRss)
    updateSparkConfCustomer(sparkConfWithRemoteSortRss)
    sparkConfWithRemoteSortRss.set(RssSparkConfig.RSS_REMOTE_MERGE_ENABLE.key, "true")
    val rssResult = runSparkApp(sparkConfWithRemoteSortRss, fileName)

    // 2 Run original spark
    val sparkConfOriginal = sparkConf.clone
    val originalResult = runSparkApp(sparkConfOriginal, fileName)

    // 3 verify
    assertEquals(originalResult.size(), rssResult.size())
    import scala.collection.JavaConverters._
    for ((k, v) <- originalResult.asScala.toMap) {
      assertEquals(v, rssResult.get(k))
    }
  }

  @throws[Exception]
  override def generateTestFile: String = generateCsvFile

  @throws[Exception]
  protected def generateCsvFile: String = {
    val rows = 1000
    val file = new File(IntegrationTestBase.tempDir, "test.csv")
    file.createNewFile
    file.deleteOnExit()
    try {
      val writer = new PrintWriter(new FileWriter(file))
      try for (i <- 0 until rows) {
        writer.println(generateRecord)
      }
      finally if (writer != null) writer.close()
    }
    file.getAbsolutePath
  }

  private def generateRecord = {
    val random = new java.util.Random
    val ch = ('a' + random.nextInt(26)).toChar
    val repeats = random.nextInt(10)
    StringUtils.repeat(ch, repeats) + "," + random.nextInt(100)
  }

  override def runTest(spark: SparkSession, fileName: String): util.Map[String, Long] = {
    val df = spark.read.schema("name STRING, age INT").csv(fileName)
    df.createOrReplaceTempView("people")
    val queryResult: Dataset[Row] =
      spark.sql("SELECT name, count(age) FROM people group by name order by name");
    val result:Map[String, Long] = Maps.newHashMap[String, Long]
    queryResult.javaRDD.collect.stream.forEach((row: Row) => {
      result.put(row.getString(0), row.getLong(1))
    })
    result
  }
}