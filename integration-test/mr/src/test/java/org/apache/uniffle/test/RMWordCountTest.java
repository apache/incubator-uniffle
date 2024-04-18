package org.apache.uniffle.test;

import static org.apache.uniffle.server.ShuffleServerConf.SERVER_MERGE_ENABLE;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.WordCount;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.uniffle.server.ShuffleServerConf;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class RMWordCountTest extends MRIntegrationTestBase {

  String inputPath = "word_count_input";
  List<String> wordTable =
      Lists.newArrayList(
          "apple", "banana", "fruit", "cherry", "Chinese", "America", "Japan", "tomato");

  @BeforeAll
  public static void setupServers() throws Exception {
    ShuffleServerConf serverConf = new ShuffleServerConf();
    serverConf.set(SERVER_MERGE_ENABLE, true);
    MRIntegrationTestBase.setupServers(MRIntegrationTestBase.getDynamicConf(), serverConf);
  }

  @Test
  public void wordCountTest() throws Exception {
    generateInputFile();
    runWithRemoteMerge();
  }

  @Override
  protected Tool getTestTool() {
    return new TestTool();
  }

  @Override
  protected String[] getTestArgs() {
    return super.getTestArgs();
  }

  private void generateInputFile() throws Exception {
    FSDataOutputStream outputStream = fs.create(new Path(inputPath));
    Random random = new Random();
    for (int i = 0; i < 100; i++) {
      int index = random.nextInt(wordTable.size());
      String str = wordTable.get(index) + "\n";
      outputStream.writeBytes(str);
    }
    outputStream.close();
  }

  private class TestTool extends WordCount {

    String outputPath = "word_count_output/" + System.currentTimeMillis();

    @Override
    public int run(String[] args) throws Exception {
      Job job = Job.getInstance(getConf(), "word count");
      job.setJarByClass(org.apache.hadoop.examples.WordCount.class);
      job.setMapperClass(org.apache.hadoop.examples.WordCount.TokenizerMapper.class);
      if (getConf().getBoolean("mapreduce.combine.enable", true)) {
        job.setCombinerClass(org.apache.hadoop.examples.WordCount.IntSumReducer.class);
      }
      job.setReducerClass(org.apache.hadoop.examples.WordCount.IntSumReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(inputPath));
      org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, new Path(outputPath));
      getConf().set("mapreduce.output.fileoutputformat.outputdir", outputPath);
      return job.waitForCompletion(true) ? 0 : 1;
    }
  }
}
