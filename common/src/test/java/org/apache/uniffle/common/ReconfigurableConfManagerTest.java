package org.apache.uniffle.common;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.uniffle.common.config.RssConf;
import org.junit.jupiter.api.Test;

import static org.apache.uniffle.common.config.RssBaseConf.JETTY_HTTP_PORT;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ReconfigurableConfManagerTest {

    @Test
    public void test() throws InterruptedException {
        AtomicInteger i = new AtomicInteger();
        Supplier<RssConf> supplier = () -> {
            if (i.get() == 0) {
                i.getAndIncrement();
                return new RssConf();
            }
            RssConf conf = new RssConf();
            conf.set(JETTY_HTTP_PORT, 100);
            return conf;
        };

        ReconfigurableConfManager.init(supplier);

        ReconfigurableConfManager.Reconfigurable<Integer> portReconfigurable = ReconfigurableConfManager.register(JETTY_HTTP_PORT);
        assertEquals(19998, portReconfigurable.get());

        Thread.sleep(2000);
        assertEquals(100, portReconfigurable.get());
    }
}
