package com.datatrees.datacenter.core.task;

import com.datatrees.datacenter.core.utility.Properties;
import org.redisson.Redisson;
import org.redisson.api.RBlockingQueue;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

public final class RedisQueue {
    private static RBlockingQueue<String> defaultQueue;
    static {
        java.util.Properties properties = Properties.load("redis.properties");
        if (properties == null) {
            throw new IllegalArgumentException("No redis properties file provided.");
        }
        Config config = new Config();
        config.useSingleServer().setAddress(String.format("redis://%s", properties.getProperty("redis.server")));
        RedissonClient redisson = Redisson.create(config);
        defaultQueue = redisson.getBlockingQueue(properties.getProperty("task.queue"));
    }

    public static RBlockingQueue<String> defaultQueue() {
        return defaultQueue;
    }
}
