# Example Uniffle/Spark cluster (using docker compose)

This example creates a docker cluster consisting of
- two coordinators
- three shuffle servers
- one Spark master
- two Spark workers

## Build docker images

First build the needed docker images:

```bash
./deploy/docker-compose/build.sh
```

## Start docker cluster

Then start the cluster:

```bash
docker compose -f deploy/docker-compose/docker-compose.yml up
```
```
[+] Running 8/0
 ✔ Container rss-coordinator-1     Created                                                                                                                                                             0.0s 
 ✔ Container rss-coordinator-2     Created                                                                                                                                                             0.0s 
 ✔ Container rss-shuffle-server-1  Created                                                                                                                                                             0.0s 
 ✔ Container rss-shuffle-server-2  Created                                                                                                                                                             0.0s 
 ✔ Container rss-shuffle-server-3  Created                                                                                                                                                             0.0s 
 ✔ Container rss-spark-master-1    Created                                                                                                                                                             0.0s 
 ✔ Container rss-spark-worker-1    Created                                                                                                                                                             0.0s 
 ✔ Container rss-spark-worker-2    Created                                                                                                                                                             0.0s 
```

## Scale docker cluster

You can scale up and down this cluster. Let's scale the shuffle servers up from 3 to 4 and the Spark workers from 2 to 4:

```bash
docker compose -f deploy/docker-compose/docker-compose.yml scale shuffle-server=4 spark-worker=4
```
```
[+] Running 11/11
 ✔ Container rss-coordinator-1     Running                                                                                                                                                             0.0s 
 ✔ Container rss-coordinator-2     Running                                                                                                                                                             0.0s 
 ✔ Container rss-shuffle-server-1  Running                                                                                                                                                             0.0s 
 ✔ Container rss-shuffle-server-2  Running                                                                                                                                                             0.0s 
 ✔ Container rss-shuffle-server-3  Running                                                                                                                                                             0.0s 
 ✔ Container rss-shuffle-server-4  Started                                                                                                                                                             0.0s 
 ✔ Container rss-spark-master-1    Running                                                                                                                                                             0.0s 
 ✔ Container rss-spark-worker-1    Running                                                                                                                                                             0.0s 
 ✔ Container rss-spark-worker-2    Running                                                                                                                                                             0.0s 
 ✔ Container rss-spark-worker-3    Started                                                                                                                                                             0.0s 
 ✔ Container rss-spark-worker-4    Started                                                                                                                                                             0.0s 
```

## Use Spark cluster

Start a Spark shell on the cluster:

```bash
docker exec -it rss-spark-master-1 /opt/spark/bin/spark-shell \
  --master spark://rss-spark-master-1:7077 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.shuffle.manager=org.apache.spark.shuffle.RssShuffleManager \
  --conf spark.rss.coordinator.quorum=rss-coordinator-1:19999,rss-coordinator-2:19999 \
  --conf spark.rss.storage.type=MEMORY_LOCALFILE \
  --conf spark.speculation=true
```

You can view the Spark master UI at http://localhost:8080/

The following example runs a job where
- task two fail several times
- task four that observes the execution of a speculative attempt

```Scala
import org.apache.spark.TaskContext

// fails iteration (at the end) or delays iteration (each element)
case class FaultyIterator[T](it: Iterator[T], fail: Boolean, sleep: Option[Int]) extends Iterator[T] {
  override def hasNext: Boolean = it.hasNext || fail
  override def next(): T = {
    // delay iteration if requested
    if (sleep.isDefined) {
      val start = System.nanoTime()
      while (start + sleep.get >= System.nanoTime()) { }
    }

    // fail at the end if requested
    if (fail && !it.hasNext) throw new RuntimeException()

    // just iterate
    it.next()
  }
}

# we fail task two 3 times and delay task four so we see a speculative execution
val result = spark.range(0, 10000000, 1, 100).mapPartitions { it => val ctx = TaskContext.get(); FaultyIterator(it, (ctx.partitionId == 2 && ctx.attemptNumber < 3), Some(ctx.partitionId == 4).filter(v => v).map(_ => 250000)) }.groupBy(($"value" / 1000000).cast("int")).as[Long, Long].mapGroups{(id, it) => (id, it.length)}.sort("_1").collect
```

compare

result.sameElements(Array((0,1000000), (1,1000000), (2,1000000), (3,1000000), (4,1000000), (5,1000000), (6,1000000), (7,1000000), (8,1000000), (9,1000000)))

EOF

sc.setLogLevel("INFO")


spark.range(0, 10000000, 1, 100).repartition(($"id" / 100000).cast("int")).map(x => x).count


SERVER_RPC_PORT=19999 XMX_SIZE=8g bash rss/bin/start-shuffle-server.sh

  coordinator.conf: |-
    rss.coordinator.app.expired 60000
    rss.coordinator.exclude.nodes.file.path /data/rssadmin/rss/coo
    rss.coordinator.server.heartbeat.timeout 30000
    rss.jetty.http.port 19996
    rss.rpc.server.port 19997
  log4j.properties: |-
    log4j.rootCategory=INFO, RollingAppender
    log4j.appender.console=org.apache.log4j.ConsoleAppender
    log4j.appender.console.Threshold=INFO
    log4j.appender.console.target=System.err
    log4j.appender.console.layout=org.apache.log4j.PatternLayout
    log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n
    log4j.appender.RollingAppender=org.apache.log4j.RollingFileAppender
    log4j.appender.RollingAppender.File=./logs/rss.log
    log4j.appender.RollingAppender.MaxFileSize=50MB
    log4j.appender.RollingAppender.MaxBackupIndex=10
    log4j.appender.RollingAppender.layout=org.apache.log4j.PatternLayout
    log4j.appender.RollingAppender.layout.ConversionPattern=[%p] %d %t %c{1} %M - %m%n
  server.conf: |-
    rss.coordinator.quorum rss-coordinator-rss-demo-0:19997,rss-coordinator-rss-demo-1:19997
    rss.jetty.http.port 19996
    rss.server.buffer.capacity 335544320
    rss.server.read.buffer.capacity 167772160
    rss.storage.type MEMORY_LOCALFILE




CI:
  --conf spark.task.maxFailures=4 \
scale up spark workers to 4
run max failing and speculative task

