package org.apache.uniffle.test;

import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.storage.HadoopTestBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class RustIntegrationTestBase extends HadoopTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(RustIntegrationTestBase.class);

    protected static final int SHUFFLE_SERVER_PORT = 20001;
    protected static final String LOCALHOST;

    static {
        try {
            LOCALHOST = RssUtils.getHostIp();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected static final int JETTY_PORT_1 = 19998;
    protected static final int JETTY_PORT_2 = 20040;

//    protected static List<ShuffleServer> shuffleServers = Lists.newArrayList();

    protected static final int NETTY_PORT = 21000;
    protected static AtomicInteger nettyPortCounter = new AtomicInteger();

    static @TempDir File tempDir;

    public static void startServers() throws IOException, InterruptedException {
//        for (ShuffleServer shuffleServer : shuffleServers) {
//            shuffleServer.start();
//        }
        String[] command = {
                "rust/experimental/server/target/debug/uniffle-worker",
                "--config",
                getConfig()
        };
        Process rustServerProcess = Runtime.getRuntime().exec(command);

        Thread.sleep(1000);

        if (rustServerProcess.isAlive()) {
            LOG.info("Successfully started rust server.");
            // 创建新线程来读取输出
            new Thread(() -> {
                try (BufferedReader stdInput = new BufferedReader(new InputStreamReader(rustServerProcess.getInputStream()));
                     BufferedReader stdError = new BufferedReader(new InputStreamReader(rustServerProcess.getErrorStream()))) {

                    String line;
                    while ((line = stdInput.readLine()) != null) {
                        System.out.println(line);
                    }
                    while ((line = stdError.readLine()) != null) {
                        System.err.println(line);
                    }
                } catch (IOException e) {
                    LOG.error("IOException occurred", e);
                }
            }).start();
        } else {
            LOG.info("Rust server failed to start. Reading output for more information.");

            int exitCode = rustServerProcess.waitFor();
            LOG.error("Process exited with error code: " + exitCode);
        }

    }
//
//    @AfterAll
//    public static void shutdownServers() throws Exception {
//        for (ShuffleServer shuffleServer : shuffleServers) {
//            shuffleServer.stopServer();
//        }
//        shuffleServers = Lists.newArrayList();
//        ShuffleServerMetrics.clear();
//    }
//
//    protected static ShuffleServerConf getShuffleServerConf() throws Exception {
//        ShuffleServerConf serverConf = new ShuffleServerConf();
//        serverConf.setInteger("rss.rpc.server.port", SHUFFLE_SERVER_PORT);
//        serverConf.setString("rss.storage.type", StorageType.MEMORY_LOCALFILE_HDFS.name());
//        serverConf.setString("rss.storage.basePath", tempDir.getAbsolutePath());
//        serverConf.setString("rss.server.buffer.capacity", "671088640");
//        serverConf.setString("rss.server.memory.shuffle.highWaterMark", "50.0");
//        serverConf.setString("rss.server.memory.shuffle.lowWaterMark", "0.0");
//        serverConf.setString("rss.server.read.buffer.capacity", "335544320");
//        serverConf.setString("rss.coordinator.quorum", COORDINATOR_QUORUM);
//        serverConf.setString("rss.server.heartbeat.delay", "1000");
//        serverConf.setString("rss.server.heartbeat.interval", "1000");
//        serverConf.setInteger("rss.jetty.http.port", 18080);
//        serverConf.setInteger("rss.jetty.corePool.size", 64);
//        serverConf.setInteger("rss.rpc.executor.size", 10);
//        serverConf.setString("rss.server.hadoop.dfs.replication", "2");
//        serverConf.setLong("rss.server.disk.capacity", 10L * 1024L * 1024L * 1024L);
//        serverConf.setBoolean("rss.server.health.check.enable", false);
//        serverConf.setBoolean(ShuffleServerConf.RSS_TEST_MODE_ENABLE, true);
//        serverConf.set(ShuffleServerConf.SERVER_TRIGGER_FLUSH_CHECK_INTERVAL, 500L);
//        serverConf.setInteger(
//                ShuffleServerConf.NETTY_SERVER_PORT, NETTY_PORT + nettyPortCounter.getAndIncrement());
//        serverConf.setString("rss.server.tags", "GRPC,GRPC_NETTY");
//        return serverConf;
//    }
//
//
//    protected static void createShuffleServer(ShuffleServerConf serverConf) throws Exception {
//        shuffleServers.add(new ShuffleServer(serverConf));
//    }
//
//    protected static void createMockedShuffleServer(ShuffleServerConf serverConf) throws Exception {
//        shuffleServers.add(new MockedShuffleServer(serverConf));
//    }
//
//    protected static void createAndStartServers(
//            ShuffleServerConf shuffleServerConf) throws Exception {
//        createShuffleServer(shuffleServerConf);
//        startServers();
//    }
//
//    protected static File createDynamicConfFile(Map<String, String> dynamicConf) throws Exception {
//        File dynamicConfFile = Files.createTempFile("dynamicConf", "conf").toFile();
//        writeRemoteStorageConf(dynamicConfFile, dynamicConf);
//        return dynamicConfFile;
//    }
//
//    protected static void writeRemoteStorageConf(File cfgFile, Map<String, String> dynamicConf)
//            throws Exception {
//        // sleep 2 secs to make sure the modified time will be updated
//        Thread.sleep(2000);
//        FileWriter fileWriter = new FileWriter(cfgFile);
//        PrintWriter printWriter = new PrintWriter(fileWriter);
//        for (Map.Entry<String, String> entry : dynamicConf.entrySet()) {
//            printWriter.println(entry.getKey() + " " + entry.getValue());
//        }
//        printWriter.flush();
//        printWriter.close();
//    }

    protected static void compileRustServer() throws IOException, InterruptedException {
        ProcessBuilder builder = new ProcessBuilder("cargo", "build", "--debug");
        builder.directory(new File("rust/experimental/server"));
        Process process = null;
        int exitCode = 0;
        process = builder.start();

        exitCode = process.waitFor();
        if (exitCode != 0) {
            LOG.error("Compilation error with exit code: " + exitCode);
        }
    }

    protected static String getConfig() throws FileNotFoundException {
        URL resource = RustIntegrationTestBase.class.getClassLoader().getResource("config.toml");
        if (resource != null) {
            return resource.getPath();
        }
        throw new FileNotFoundException("Cannot found config.toml");
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        compileRustServer();
        startServers();
    }
}
