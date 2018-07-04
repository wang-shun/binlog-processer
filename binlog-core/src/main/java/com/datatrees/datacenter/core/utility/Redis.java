package com.datatrees.datacenter.core.utility;


import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.ReadMode;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class Redis {

  private volatile static SimpleRedis<String, String> manager;

  public static SimpleRedis<String, String> getMgr() {
    if (manager == null) {
      synchronized (Redis.class) {
        if (manager == null) {
          manager = new SimpleRedis.Redission();
        }
      }
    }
    return manager;
  }

  public interface SimpleRedis<K, V> {

    void destroy();

    void close();

    V get(K key);

    void set(K key, V value);

    Boolean exists(K key);

    class JedisImpl implements Redis.SimpleRedis<String, String> {

      private JedisPool pool = null;
      private redis.clients.jedis.Jedis jedis = null;

      public JedisImpl() {
        java.util.Properties props = null;
        props = PropertiesUtility.defaultProperties();
        JedisPoolConfig config = new JedisPoolConfig();
        String[] arr = props.getProperty("redis.server").split(":");
        pool = new JedisPool(config, arr[0], Integer.parseInt(arr[1]));
        jedis = pool.getResource();
      }

      public void destroy() {
        this.pool.destroy();
      }

      public void close() {
        this.pool.close();
      }

      public String get(String key) {
        return jedis.get(key);
      }

      public void set(String key, String value) {
        jedis.set(key, value);
      }

      @Override
      public Boolean exists(String key) {
        return jedis.exists(key);
      }
    }

    class Redission implements Redis.SimpleRedis<String, String> {

      /**
       * DAY
       */
      private static Long REDIS_CACHE_TIMEOUT = 7L;
      private RedissonClient redisson;
      public Redission() {
        Properties p = PropertiesUtility.defaultProperties();
        Config config = new Config();
        String[] sentinelAddress = p.getProperty("redis.sentinel.address").split(",");
//        String[] sentinelAddress =new String[]{"redis://10.1.2.210:36379","redis://10.1.2.209:36379"};
        config.useSentinelServers().setMasterName(p.getProperty("redis.master.name"))
          .addSentinelAddress(new String(sentinelAddress[0]), new String(sentinelAddress[1]))
          .setReadMode(ReadMode.MASTER_SLAVE);
//        config.useMasterSlaveServers()
//          .addSlaveAddress(String.format("redis://%s", p.getProperty("slave.redis.server")))
//          .setMasterAddress(String.format("redis://%s", p.getProperty("master.redis.server")));
//        config.useSingleServer().setAddress(String.format("redis://%s",
//          PropertiesUtility.defaultProperties().getProperty("redis.server")));
        redisson = Redisson.create(config);
      }

      public static void main(String[] args) {
        Boolean bo
          = Redis.getMgr().exists("acrm-usercenter:acrm-usercenter:act_prize");
        System.out.println(bo);
      }

      public void destroy() {// TODO: 2018/5/31
      }

      public void close() {// TODO: 2018/5/31
      }

      public String get(String key) {
        RBucket<String> bucket = redisson.getBucket(key);
        return bucket.get();
      }

      public void set(String key, String value) {
        redisson.getBucket(key).set(value, REDIS_CACHE_TIMEOUT, TimeUnit.DAYS);
      }

      @Override
      public Boolean exists(String key) {
        RBucket<String> bucket = redisson.getBucket(key);
        return bucket.isExists();
      }
    }
  }
}
