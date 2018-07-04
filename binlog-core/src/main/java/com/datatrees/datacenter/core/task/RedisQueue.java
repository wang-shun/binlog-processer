package com.datatrees.datacenter.core.task;

import com.datatrees.datacenter.core.utility.PropertiesUtility;
import java.util.Properties;
import org.redisson.Redisson;
import org.redisson.api.RBlockingQueue;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.ReadMode;

public final class RedisQueue {

  private static RBlockingQueue<String> defaultQueue;

  static {
    Properties p = PropertiesUtility.defaultProperties();
    if (p == null) {
      throw new IllegalArgumentException("No redis properties file provided.");
    }
    Config config = new Config();
    String[] sentinelAddress = p.getProperty("redis.sentinel.address").split(",");
    config.useSentinelServers().setMasterName(p.getProperty("redis.master.name"))
      .addSentinelAddress(new String(sentinelAddress[0]), new String(sentinelAddress[1]))
      .setReadMode(ReadMode.MASTER_SLAVE);
    RedissonClient redisson = Redisson.create(config);
    defaultQueue = redisson.getBlockingQueue(p.getProperty("queue.topic"));
  }

  public static RBlockingQueue<String> defaultQueue() {
    return defaultQueue;
  }
}
