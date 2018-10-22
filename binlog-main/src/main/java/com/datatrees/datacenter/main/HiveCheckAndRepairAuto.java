package com.datatrees.datacenter.main;

import com.datatrees.datacenter.core.threadpool.ThreadPoolInstance;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.rabbitmq.ConsumerTask;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * 自动检查和修复程序
 */
public class HiveCheckAndRepairAuto {
    private static ThreadPoolExecutor executors;
    private static final String HIVE_CHECK_QUEUE = PropertiesUtility.defaultProperties().getProperty("hive.check.queue");

    public static void main(String[] args) {
        executors = ThreadPoolInstance.getExecutors();
        for (int i = 0; i < executors.getCorePoolSize(); i++) {
            executors.submit(new ConsumerTask(HIVE_CHECK_QUEUE));
        }
    }
}
