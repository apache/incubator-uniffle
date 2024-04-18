package org.apache.uniffle.test;

import static org.apache.uniffle.server.ShuffleServerConf.SERVER_MERGE_ENABLE;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.tez.common.RssTezConfig;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.examples.OrderedWordCount;
import org.apache.uniffle.server.ShuffleServerConf;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class RMTezOrderedWordCountTest extends TezIntegrationTestBase  {

  private String inputPath = "rm_ordered_word_count_input";
  private String outputPath = "rm_ordered_word_count_output";
  private List<String> wordTable =
      Lists.newArrayList("apple", "banana", "fruit", "cherry", "Chinese", "America", "Japan", "tomato");

  @BeforeAll
  public static void setupServers() throws Exception {
    ShuffleServerConf serverConf = new ShuffleServerConf();
    serverConf.set(SERVER_MERGE_ENABLE, true);
    TezIntegrationTestBase.setupServers(serverConf);
  }

  @Test
  public void orderedWordCountTest() throws Exception {
    generateInputFile();
    run();
  }

  public void run() throws Exception {
    // 1 Run Tez examples based on rss when remote merge is enabled
    TezConfiguration appConf = new TezConfiguration(miniTezCluster.getConfig());
    updateRssConfiguration(appConf);
    appendAndUploadRssJars(appConf);
    appConf.set(RssTezConfig.RSS_REMOTE_MERGE_ENABLE, "true");
    runTezApp(appConf, getTestTool(), getTestArgs("rss"));
    final String rssPath = getOutputDir("rss");

    // 2 Run original Tez examples
    appConf = new TezConfiguration(miniTezCluster.getConfig());
    updateCommonConfiguration(appConf);
    runTezApp(appConf, getTestTool(), getTestArgs("origin"));
    final String originPath = getOutputDir("origin");

    // 3 verify the results
    verifyResults(originPath, rssPath);
  }

  private void generateInputFile() throws Exception {
    // For ordered word count, the key of last ordered sorter is the summation of word, the value is
    // the word. So it means this key may not be unique. Because Sorter can only make sure key is
    // sorted, so the second column (word column) may be not sorted.
    // To keep pace with verifyResults, here make sure summation of word is unique number.
    FSDataOutputStream outputStream = fs.create(new Path(inputPath));
    Random random = new Random();
    Set<Integer> used = new HashSet();
    List<String> outputList = new ArrayList<>();
    int index = 0;
    while (index < wordTable.size()) {
      int summation = random.nextInt(50);
      if (used.contains(summation)) {
        continue;
      }
      used.add(summation);
      for (int i = 0; i < summation; i++) {
        outputList.add(wordTable.get(index));
      }
      index++;
    }
    Collections.shuffle(outputList);
    for (String word : outputList) {
      outputStream.writeBytes(word + "\n");
    }
    outputStream.close();
  }

  @Override
  public Tool getTestTool() {
    return new OrderedWordCount();
  }

  @Override
  public String[] getTestArgs(String uniqueOutputName) {
    return new String[] {inputPath, outputPath + "/" + uniqueOutputName, "2"};
  }

  @Override
  public String getOutputDir(String uniqueOutputName) {
    return outputPath + "/" + uniqueOutputName;
  }
}
