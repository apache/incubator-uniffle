package org.apache.uniffle.test;

import com.google.common.collect.Lists;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.coordinator.CoordinatorServer;
import org.apache.uniffle.coordinator.metric.CoordinatorMetrics;
import org.apache.uniffle.storage.HadoopTestBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class RustIntegrationTestBase extends HadoopTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(RustIntegrationTestBase.class);

    protected static final int SHUFFLE_SERVER_PORT = 19999;

    protected static final String LOCALHOST;

    static {
        try {
            LOCALHOST = RssUtils.getHostIp();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected static List<RustShuffleServer> shuffleServers = Lists.newArrayList();

    protected static List<CoordinatorServer> coordinators = Lists.newArrayList();

    protected static final int COORDINATOR_PORT = 9999;

    protected static final int JETTY_PORT = 9998;

    protected static final String SHUFFLE_SERVER_METRICS_URL = "http://121.36.246.152:12345/metrics";

    protected static final String COORDINATOR_QUORUM = LOCALHOST + ":" + COORDINATOR_PORT;

    static @TempDir File tempDir;

    public static void startServers() throws Exception {
        compileRustServer();
        for (CoordinatorServer coordinator : coordinators) {
            coordinator.start();
        }
        for (RustShuffleServer shuffleServer : shuffleServers) {
            shuffleServer.start();
        }
    }

    @AfterAll
    public static void shutdownServers() throws Exception {
        for (CoordinatorServer coordinator : coordinators) {
            coordinator.stopServer();
        }
        for (RustShuffleServer shuffleServer : shuffleServers) {
            shuffleServer.stopServer();
        }
        shuffleServers = Lists.newArrayList();
        coordinators = Lists.newArrayList();
        CoordinatorMetrics.clear();
    }

    protected static RustShuffleServerConf getShuffleServerConf() throws Exception {
        RustShuffleServerConf serverConf = new RustShuffleServerConf(tempDir);

        Map<String, Object> hybridStore = new HashMap<>();
        hybridStore.put("memory_spill_high_watermark", 0.9);
        hybridStore.put("memory_spill_low_watermark", 0.5);

        Map<String, Object> memoryStore = new HashMap<>();
        memoryStore.put("capacity", "50MB");

        serverConf.set("coordinator_quorum", Lists.newArrayList(COORDINATOR_QUORUM));
        serverConf.set("hybrid_store", hybridStore);
        serverConf.set("memory_store", memoryStore);
        return serverConf;
    }

    protected static CoordinatorConf getCoordinatorConf() {
        CoordinatorConf coordinatorConf = new CoordinatorConf();
        coordinatorConf.setInteger(CoordinatorConf.RPC_SERVER_PORT, COORDINATOR_PORT);
        coordinatorConf.setInteger(CoordinatorConf.JETTY_HTTP_PORT, JETTY_PORT);
        coordinatorConf.setInteger(CoordinatorConf.RPC_EXECUTOR_SIZE, 10);
        return coordinatorConf;
    }

    protected static void createShuffleServer(RustShuffleServerConf serverConf) throws Exception {
        serverConf.generateTomlConf();
        shuffleServers.add(new RustShuffleServer(serverConf));
    }
//
//    protected static void createMockedShuffleServer(ShuffleServerConf serverConf) throws Exception {
//        shuffleServers.add(new MockedShuffleServer(serverConf));
//    }
//
    protected static void createCoordinatorServer(CoordinatorConf coordinatorConf) throws Exception {
        coordinators.add(new CoordinatorServer(coordinatorConf));
    }

    protected static void createAndStartServers(
            RustShuffleServerConf shuffleServerConf) throws Exception {
        createShuffleServer(shuffleServerConf);
        startServers();
    }
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
        ProcessBuilder builder = new ProcessBuilder("cargo", "build");
        builder.directory(new File("../../rust/experimental/server"));

        // Redirect error stream to standard output stream
        builder.redirectErrorStream(true);

        Process process = builder.start();

        // Read output (and error) stream of the process
        try (InputStreamReader isr = new InputStreamReader(process.getInputStream());
             BufferedReader br = new BufferedReader(isr)) {

            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(line);  // Or use LOG.error(line) to log
            }
        }

        int exitCode = process.waitFor();
        if (exitCode != 0) {
            LOG.error("Compilation error with exit code: " + exitCode);
            System.exit(-1);
        }
    }
}