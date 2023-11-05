# Deploy MapReduce Client Plugin & Configurations
## Deploy MapReduce Client Plugin

1. Add client jar to the classpath of each NodeManager, e.g., <HADOOP>/share/hadoop/mapreduce/

The jar for MapReduce is located in <RSS_HOME>/jars/client/mr/rss-client-mr-XXXXX-shaded.jar

2. Update MapReduce conf to enable Uniffle, eg,

   ```
   -Dmapreduce.rss.coordinator.quorum=<coordinatorIp1>:19999,<coordinatorIp2>:19999
   -Dyarn.app.mapreduce.am.command-opts=org.apache.hadoop.mapreduce.v2.app.RssMRAppMaster
   -Dmapreduce.job.map.output.collector.class=org.apache.hadoop.mapred.RssMapOutputCollector
   -Dmapreduce.job.reduce.shuffle.consumer.plugin.class=org.apache.hadoop.mapreduce.task.reduce.RssShuffle
   ```
Note that the RssMRAppMaster will automatically disable slow start (i.e., `mapreduce.job.reduce.slowstart.completedmaps=1`)
and job recovery (i.e., `yarn.app.mapreduce.am.job.recovery.enable=false`)

## MapReduce Specialized Configurations

|Property Name|Default|Description|
|---|---|---|
|mapreduce.rss.client.max.buffer.size|3k|The max buffer size in map side|
|mapreduce.rss.client.batch.trigger.num|50|The max batch of buffers to send data in map side|