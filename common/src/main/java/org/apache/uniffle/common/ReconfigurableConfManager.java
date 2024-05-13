package org.apache.uniffle.common;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.uniffle.common.config.ConfigOption;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.util.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReconfigurableConfManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReconfigurableConfManager.class);

    private static RssBaseConf rssConf;
    private ScheduledExecutorService scheduledThreadPoolExecutor;

    private ReconfigurableConfManager(Supplier<RssBaseConf> confSupplier) {
        rssConf = confSupplier.get();
        scheduledThreadPoolExecutor = ThreadUtils.getDaemonSingleThreadScheduledExecutor("Refresh-rss-conf");
        scheduledThreadPoolExecutor.schedule(() -> {
            try {
                rssConf.addAll(confSupplier.get());
            } catch (Exception e) {
                LOGGER.error("Errors on refreshing the rss conf.", e);
            }
        }, 1, TimeUnit.SECONDS);
    }

    public static void init(Supplier<RssBaseConf> confSupplier) {
        new ReconfigurableConfManager(confSupplier);
    }

    public static <T> Reconfigurable<T> register(ConfigOption<T> configOption) {
        Reconfigurable<T> reconfigurable = new Reconfigurable<T>(rssConf, configOption);
        return reconfigurable;
    }

    static class Reconfigurable<T> {
        RssConf rssConf;
        ConfigOption<T> option;

        Reconfigurable(RssConf conf, ConfigOption<T> option) {
            this.rssConf = conf;
            this.option = option;
        }

        T get() {
            return rssConf.get(option);
        }
    }
}
