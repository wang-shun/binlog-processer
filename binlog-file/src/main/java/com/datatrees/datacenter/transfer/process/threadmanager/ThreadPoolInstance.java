package com.datatrees.datacenter.transfer.process.threadmanager;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author personalc
 */
public class ThreadPoolInstance {
    private static class LazyHolder {
        private static final ThreadPoolExecutor executor = new ThreadPoolExecutor(4, 10, 3, TimeUnit.SECONDS, new LinkedBlockingDeque<>());
    }


    public static final ThreadPoolExecutor getExecutors() {
        return LazyHolder.executor;
    }
}
