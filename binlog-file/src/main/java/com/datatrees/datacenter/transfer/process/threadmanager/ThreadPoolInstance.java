package com.datatrees.datacenter.transfer.process.threadmanager;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author personalc
 */
public class ThreadPoolInstance {
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
                    System.out.println("create thread " + t);
                    return t;
                });
    }


    public static ThreadPoolExecutor getExecutors() {
        return LazyHolder.THREAD_POOL_EXECUTOR;
    }
}
