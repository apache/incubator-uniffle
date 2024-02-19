# Example Uniffle/Spark docker cluster

This example creates a docker cluster consisting of
- two coordinators
- three shuffle servers
- one Spark master
- two Spark workers

## Build the docker images

First build the needed docker images:

```bash
./deploy/docker-compose/build.sh
```

## Start the docker cluster

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

## Scale the docker cluster

You can scale up and down this cluster, easily.

Let's scale the shuffle servers up from 3 to 4, and the Spark workers from 2 to 4:

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

## Use the Spark cluster

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

// we fail task two 3 times and delay task four so we see a speculative execution
val result = spark.range(0, 10000000, 1, 100).mapPartitions { it => val ctx = TaskContext.get(); FaultyIterator(it, (ctx.partitionId == 2 && ctx.attemptNumber < 3), Some(ctx.partitionId == 4).filter(v => v).map(_ => 250000)) }.groupBy(($"value" / 1000000).cast("int")).as[Long, Long].mapGroups{(id, it) => (id, it.length)}.sort("_1").collect
```

compare

result.sameElements(Array((0,1000000), (1,1000000), (2,1000000), (3,1000000), (4,1000000), (5,1000000), (6,1000000), (7,1000000), (8,1000000), (9,1000000)))



## Stop the docker cluster

Finally, stop the cluster:

```bash
docker compose -f deploy/docker-compose/docker-compose.yml down
```
```
[+] Running 12/12
 ✔ Container rss-shuffle-server-1  Removed                                                                                                                                                            10.5s
 ✔ Container rss-shuffle-server-2  Removed                                                                                                                                                            10.7s
 ✔ Container rss-shuffle-server-3  Removed                                                                                                                                                            10.5s
 ✔ Container rss-shuffle-server-4  Removed                                                                                                                                                            10.6s
 ✔ Container rss-spark-worker-1    Removed                                                                                                                                                             0.8s
 ✔ Container rss-spark-worker-2    Removed                                                                                                                                                             1.0s
 ✔ Container rss-spark-worker-3    Removed                                                                                                                                                             0.9s
 ✔ Container rss-spark-worker-4    Removed                                                                                                                                                             1.1s
 ✔ Container rss-spark-master-1    Removed                                                                                                                                                             1.6s
 ✔ Container rss-coordinator-1     Removed                                                                                                                                                            10.4s
 ✔ Container rss-coordinator-2     Removed                                                                                                                                                            10.5s
 ✔ Network rss_default             Removed                                                                                                                                                             0.4s
```

