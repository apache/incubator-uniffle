package org.apache.uniffle.common;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.uniffle.common.config.ConfigOption;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.util.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReconfigurableConfManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReconfigurableConfManager.class);

    private static ReconfigurableConfManager reconfigurableConfManager;

    private RssConf rssConf;
    private ScheduledExecutorService scheduledThreadPoolExecutor;

    private ReconfigurableConfManager(Supplier<RssConf> confSupplier) {
        rssConf = confSupplier.get();
        scheduledThreadPoolExecutor = ThreadUtils.getDaemonSingleThreadScheduledExecutor("Refresh-rss-conf");
        scheduledThreadPoolExecutor.schedule(() -> {
            try {
                RssConf latestConf = confSupplier.get();
                for (Map.Entry<String, Object> entry : latestConf.getAll()) {
                    String key = entry.getKey();
                    Object val = entry.getValue();
                    if (rssConf.containsKey(key)) {
                        Object prev = rssConf.getObject(key, null);
                        if (!isSame(val, prev)) {
                            LOGGER.info("Update the config option: {} from {} to {}", key, prev, val);
                            rssConf.setValueInternal(key, val);
                        }
                    }
                }
                rssConf.addAll(confSupplier.get());
            } catch (Exception e) {
                LOGGER.error("Errors on refreshing the rss conf.", e);
            }
        }, 1, TimeUnit.SECONDS);
    }

    private boolean isSame(Object v1, Object v2) {
        if (v1 == null && v2 == null) {
            return true;
        }
        if (v1 != null && v1.equals(v2)) {
            return true;
        }
        if (v2 != null && v2.equals(v1)) {
            return true;
        }
        return false;
    }

    public static void init(Supplier<RssConf> confSupplier) {
        ReconfigurableConfManager manager = new ReconfigurableConfManager(confSupplier);
        reconfigurableConfManager = manager;
    }

    private RssConf getConfRef() {
        return rssConf;
    }

    public static <T> Reconfigurable<T> register(ConfigOption<T> configOption) {
        Reconfigurable<T> reconfigurable = new Reconfigurable<T>(reconfigurableConfManager, configOption);
        return reconfigurable;
    }

    static class Reconfigurable<T> {
        ReconfigurableConfManager reconfigurableConfManager;
        ConfigOption<T> option;

        Reconfigurable(ReconfigurableConfManager reconfigurableConfManager, ConfigOption<T> option) {
            this.reconfigurableConfManager = reconfigurableConfManager;
            this.option = option;
        }

        T get() {
            return reconfigurableConfManager.getConfRef().get(option);
        }
    }
}
