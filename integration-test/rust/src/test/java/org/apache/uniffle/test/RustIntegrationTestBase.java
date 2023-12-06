package org.apache.uniffle.test;

import com.google.common.collect.Lists;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.storage.HadoopTestBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RustIntegrationTestBase {

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

    protected static List<RustShuffleServer> shuffleServers = Lists.newArrayList();

    protected static final int COORDINATOR_PORT = 19999;

    protected static final String COORDINATOR_QUORUM = LOCALHOST + ":" + COORDINATOR_PORT + ",";

    static @TempDir File tempDir;

    public static void startServers() throws IOException, InterruptedException {
        compileRustServer();
        for (RustShuffleServer shuffleServer : shuffleServers) {
            shuffleServer.start();
        }
    }

    @AfterAll
    public static void shutdownServers() throws Exception {
        for (RustShuffleServer shuffleServer : shuffleServers) {
            shuffleServer.stopServer();
        }
        shuffleServers = Lists.newArrayList();
    }

    protected static RustShuffleServerConf getShuffleServerConf() throws Exception {
        RustShuffleServerConf serverConf = new RustShuffleServerConf(tempDir);
        Map<String, Object> data = new HashMap<>();
        data.put("coordinator_quorum", Lists.newArrayList("127.0.0.1:19999"));

        Map<String, Object> hybridStore = new HashMap<>();
        hybridStore.put("memory_spill_high_watermark", 0.9);
        hybridStore.put("memory_spill_low_watermark", 0.5);
        data.put("hybrid_store", hybridStore);

        Map<String, Object> memoryStore = new HashMap<>();
        memoryStore.put("capacity", "1G");
        data.put("memory_store", memoryStore);


        serverConf.generateTomlConf(data);
        return serverConf;
    }


    protected static void createShuffleServer(RustShuffleServerConf serverConf) throws Exception {
        shuffleServers.add(new RustShuffleServer(serverConf));
    }
//
//    protected static void createMockedShuffleServer(ShuffleServerConf serverConf) throws Exception {
//        shuffleServers.add(new MockedShuffleServer(serverConf));
//    }
//
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
        ProcessBuilder builder = new ProcessBuilder("cargo", "build", "--debug");

        builder.directory(new File("../../rust/experimental/server"));
        Process process = null;
        int exitCode = 0;
        process = builder.start();

        exitCode = process.waitFor();
        if (exitCode != 0) {
            LOG.error("Compilation error with exit code: " + exitCode);
        }
    }

    @Test
    public void Main() throws Exception {
        createShuffleServer(getShuffleServerConf());
        startServers();
    }
}
