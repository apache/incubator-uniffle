package org.apache.uniffle.test;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.uniffle.client.impl.grpc.ShuffleServerGrpcClient;
import org.apache.uniffle.client.request.RssAppHeartBeatRequest;
import org.apache.uniffle.client.request.RssRegisterShuffleRequest;
import org.apache.uniffle.client.request.RssUnregisterShuffleRequest;
import org.apache.uniffle.client.response.RssAppHeartBeatResponse;
import org.apache.uniffle.client.response.RssRegisterShuffleResponse;
import org.apache.uniffle.client.response.RssUnregisterShuffleResponse;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.coordinator.CoordinatorConf;
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
        CoordinatorConf coordinatorConf = getCoordinatorConf();
        coordinatorConf.setLong(CoordinatorConf.COORDINATOR_APP_EXPIRED, 2000);
        createCoordinatorServer(coordinatorConf);
        RustShuffleServerConf shuffleServerConf = getShuffleServerConf();
        Map<String, Object> conf = new HashMap<>();
        conf.put("grpc_port", 8888);

        createShuffleServer(shuffleServerConf);
        startServers();
    }

    @BeforeEach
    public void createClient() {
        shuffleServerClient = new ShuffleServerGrpcClient(LOCALHOST, SHUFFLE_SERVER_PORT);
    }

    @Test
    public void appHeartBeatTest() throws InterruptedException {
        String appId = "appHeartBeatTest";
        String dataBasePath = "";
        int shuffleId = 0;

        // register app and send heart beat, should succeed
        RssRegisterShuffleResponse rssRegisterShuffleResponse = shuffleServerClient.registerShuffle(new RssRegisterShuffleRequest(appId, shuffleId, Lists.newArrayList(new PartitionRange(0, 1)), dataBasePath));
        RssAppHeartBeatResponse heartBeatResponse = shuffleServerClient.sendHeartBeat(new RssAppHeartBeatRequest(appId, 10000));

        assertEquals(StatusCode.SUCCESS, rssRegisterShuffleResponse.getStatusCode());
        assertEquals(StatusCode.SUCCESS, heartBeatResponse.getStatusCode());

        // unregister app and send heart beat, should also succeed
        RssUnregisterShuffleResponse rssUnregisterShuffleResponse = shuffleServerClient.unregisterShuffle(new RssUnregisterShuffleRequest(appId, shuffleId));
        heartBeatResponse = shuffleServerClient.sendHeartBeat(new RssAppHeartBeatRequest(appId, 10000));
        assertEquals(StatusCode.SUCCESS, rssUnregisterShuffleResponse.getStatusCode());
        assertEquals(StatusCode.SUCCESS, heartBeatResponse.getStatusCode());
    }
}
