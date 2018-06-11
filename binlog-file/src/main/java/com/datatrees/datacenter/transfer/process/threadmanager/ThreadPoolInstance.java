package com.datatrees.datacenter.transfer.process.threadmanager;

import com.datatrees.datacenter.transfer.process.AliBinLogFileTransfer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author personalc
 */
public class ThreadPoolInstance {
    private static Logger LOG = LoggerFactory.getLogger(ThreadPoolInstance.class);

    private static int corePoolSize = 5;
    private static int maximumPoolSize = 20;
    private static long keepAliveTime = 100L;

    private static class LazyHolder {
        private static final ThreadPoolExecutor THREAD_POOL_EXECUTOR = new ThreadPoolExecutor(corePoolSize,
                maximumPoolSize,
                keepAliveTime,
                TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(),
                r -> {
                    Thread t = new Thread(r);
                    LOG.info("create thread " + t);
                    return t;
                });
    }


    public static ThreadPoolExecutor getExecutors() {
        return LazyHolder.THREAD_POOL_EXECUTOR;
    }
}
