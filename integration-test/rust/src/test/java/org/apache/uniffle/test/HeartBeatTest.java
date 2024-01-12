package org.apache.uniffle.test;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.uniffle.client.impl.grpc.ShuffleServerGrpcClient;
import org.apache.uniffle.client.request.RssAppHeartBeatRequest;
import org.apache.uniffle.client.request.RssRegisterShuffleRequest;
import org.apache.uniffle.client.response.RssAppHeartBeatResponse;
import org.apache.uniffle.client.response.RssRegisterShuffleResponse;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.config.RssBaseConf;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HeartBeatTest extends RustIntegrationTestBase {

    private ShuffleServerGrpcClient shuffleServerClient;

    @BeforeAll
    public static void setupServers(@TempDir File tmpDir) throws Exception {
        RustShuffleServerConf shuffleServerConf = getShuffleServerConf();
        Map<String, Object> conf = new HashMap<>();
        conf.put("grpc_port", 8888);

        createAndStartServers(shuffleServerConf);
        startServers();
    }

    @BeforeEach
    public void createClient() {
        shuffleServerClient = new ShuffleServerGrpcClient(LOCALHOST, SHUFFLE_SERVER_PORT);
    }

    @Test
    public void appHeartBeatTest() {
        String appId = "appHeartBeatTest";
        String dataBasePath = HDFS_URI + "rss/test";

        RssRegisterShuffleResponse rssRegisterShuffleResponse = shuffleServerClient.registerShuffle(new RssRegisterShuffleRequest(appId, 0, Lists.newArrayList(new PartitionRange(0, 1)), ""));
        RssAppHeartBeatResponse heartBeatResponse = shuffleServerClient.sendHeartBeat(new RssAppHeartBeatRequest(appId, 10000));

        assertEquals(rssRegisterShuffleResponse.getStatusCode().statusCode(), 0);
        assertEquals(heartBeatResponse.getStatusCode().statusCode(), 0);
    }
}
