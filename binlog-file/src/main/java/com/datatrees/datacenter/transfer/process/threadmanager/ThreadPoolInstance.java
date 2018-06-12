package com.datatrees.datacenter.transfer.process.threadmanager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * @author personalc
 */
public class ThreadPoolInstance {
    private static Logger LOG = LoggerFactory.getLogger(ThreadPoolInstance.class);

    private static int corePoolSize = 5;
    private static int maximumPoolSize = 10;
    private static long keepAliveTime = 10L;

    private static class LazyHolder {
        //private static final ExecutorService executors=Executors.newFixedThreadPool(5);
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
