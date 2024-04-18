package org.apache.uniffle.server.merge;

import static org.apache.uniffle.server.ShuffleServerConf.SERVER_MERGE_THREAD_ALIVE;
import static org.apache.uniffle.server.ShuffleServerConf.SERVER_MERGE_THREAD_POOL_QUEUE_SIZE;
import static org.apache.uniffle.server.ShuffleServerConf.SERVER_MERGE_THREAD_POOL_SIZE;

import com.google.common.collect.Queues;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.uniffle.common.util.ThreadUtils;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.server.ShuffleServerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultMergeEventHandler implements MergeEventHandler {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultMergeEventHandler.class);

  private Executor threadPoolExecutor;
  protected final BlockingQueue<MergeEvent> queue = Queues.newLinkedBlockingQueue();
  private Consumer<MergeEvent> eventConsumer;
  private volatile boolean stopped = false;

  public DefaultMergeEventHandler(ShuffleServerConf serverConf, Consumer<MergeEvent> eventConsumer) {
    this.eventConsumer = eventConsumer;
    int poolSize = serverConf.get(SERVER_MERGE_THREAD_POOL_SIZE);
    int queueSize = serverConf.get(SERVER_MERGE_THREAD_POOL_QUEUE_SIZE);
    int keepAliveTime = serverConf.get(SERVER_MERGE_THREAD_ALIVE);
    BlockingQueue<Runnable> waitQueue = Queues.newLinkedBlockingQueue(queueSize);
    threadPoolExecutor = new ThreadPoolExecutor(
        poolSize,
        poolSize,
        keepAliveTime,
        TimeUnit.SECONDS,
        waitQueue,
        ThreadUtils.getThreadFactory("DefaultMergeEventHandler"));
    startEventProcessor();
  }

  private void startEventProcessor() {
    Thread processEventThread = new Thread(this::eventLoop);
    processEventThread.setName("ProcessEventThread");
    processEventThread.setDaemon(true);
    processEventThread.start();
  }

  protected void eventLoop() {
    while (!stopped && !Thread.currentThread().isInterrupted()) {
      processNextEvent();
    }
  }

  protected void processNextEvent() {
    try {
      MergeEvent event = queue.take();
      threadPoolExecutor.execute(() -> handleEventAndUpdateMetrics(event));
    } catch (Exception e) {
      LOG.error("Exception happened when process event.", e);
    }
  }

  private void handleEventAndUpdateMetrics(MergeEvent event) {
    eventConsumer.accept(event);
    ShuffleServerMetrics.gaugeMergeEventQueueSize.dec();
  }

  @Override
  public void handle(MergeEvent event) {
    if (queue.offer(event)) {
      ShuffleServerMetrics.gaugeMergeEventQueueSize.inc();
    }
  }

  @Override
  public int getEventNumInMerge() {
    return queue.size();
  }

  @Override
  public void stop() {
    stopped = true;
  }
}
