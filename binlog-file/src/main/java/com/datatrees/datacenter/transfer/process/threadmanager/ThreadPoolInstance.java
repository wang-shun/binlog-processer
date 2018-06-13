package com.datatrees.datacenter.transfer.process.threadmanager;

import com.datatrees.datacenter.core.utility.PropertiesUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.*;

/**
 * @author personalc
 */
public class ThreadPoolInstance {
    private static Logger LOG = LoggerFactory.getLogger(ThreadPoolInstance.class);
    private static final Properties properties = PropertiesUtility.defaultProperties();

    private static int corePoolSize = Integer.parseInt(properties.getProperty("threadpool.corePoolSize"));
    private static int maximumPoolSize = Integer.parseInt(properties.getProperty("threadpool.maximumPoolSize"));
    private static long keepAliveTime = Long.parseLong(properties.getProperty("threadpool.keepAliveTime"));

    private static class LazyHolder {
        private static final ThreadPoolExecutor executors = new ThreadPoolExecutor(corePoolSize,
                maximumPoolSize,
                keepAliveTime,
                TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(),
                r -> {
                    Thread t = new Thread(r);
                    LOG.info("create thread " + t.getName());
                    return t;
                }, new ThreadPoolExecutor.AbortPolicy());
    }

    public static ThreadPoolExecutor getExecutors() {
        return LazyHolder.executors;
    }
}
