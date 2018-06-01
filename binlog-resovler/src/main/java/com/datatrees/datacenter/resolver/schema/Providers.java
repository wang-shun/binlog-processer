package com.datatrees.datacenter.resolver.schema;

import com.callfire.avro.AvroSchema;
import com.callfire.avro.DbSchemaExtractor;
import com.callfire.avro.SchemaGenerator;
import com.callfire.avro.config.AvroConfig;
import com.datatrees.datacenter.core.utility.Properties;
import com.datatrees.datacenter.core.utility.Redis;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import org.javatuples.KeyValue;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * provided for just one instance
 */
public class Providers {
    /**
     * Triple<instanceId,database,table>
     */
    private static ImmutableMap<String, Triplet<String, String, String>> dbInstanceSchema;
    private static DbSchemaExtractor dbSchemaExtractor;
    private static Logger logger = LoggerFactory.getLogger(Providers.class);
    /**
     * KeyValue<namespace,avrostring>
     */
    private static LoadingCache<Triplet<String, String, String>, KeyValue<String, String>> caches;
//    private static Jedis jedis = JedisUtility.getMgr().getResource();

    private static Redis.SimpleRedis<String, String> redis = Redis.getMgr();

    private static Function<String, String> schemaNameMapper = t -> t.replaceAll("\\d+", "");

    static {
        try {
            Iterator<Map.Entry<Object, Object>> value =
                    Properties.load("dbInstance.properties").entrySet().iterator();
            ImmutableMap.Builder builder = ImmutableMap.<String, Triplet<String, String, String>>builder();
            while (value.hasNext()) {
                Map.Entry<Object, Object> entry = value.next();
                builder.put(entry.getKey().toString(),
                        Triplet.with(String.format("jdbc:mysql://%s:3307", entry.getValue().toString()),
                                "debezium", "Debezium_"));
            }
            dbInstanceSchema = builder.build();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

        caches = CacheBuilder.newBuilder().
                maximumSize(10000).
                expireAfterAccess(12, TimeUnit.HOURS).
                build(new CacheLoader<Triplet<String, String, String>, KeyValue<String, String>>() {
                    @Override
                    public KeyValue<String, String> load(Triplet<String, String, String> key) throws Exception {
                        String redisKey = String.format("%s.%s.%s", key.getValue0(), key.getValue1(), key.getValue2());
                        String namespace = String.format("%s.%s", key.getValue0(), schemaNameMapper.apply(key.getValue1()));
                        if (redis.exists(redisKey)) return KeyValue.with(namespace, redis.get(redisKey));

                        Triplet<String, String, String> connectInfo = dbInstanceSchema.get(key.getValue0());
                        dbSchemaExtractor = new DbSchemaExtractor(connectInfo.getValue0(), connectInfo.getValue1(), connectInfo.getValue2());
                        AvroConfig config = new AvroConfig(
                                String.format("%s.%s", key.getValue0(), schemaNameMapper.apply(key.getValue1()))
                        );
                        List<AvroSchema> tableAvroSchema = dbSchemaExtractor.getForSchema(config, key.getValue1());
                        String result = null;
                        for (AvroSchema avroSchema : tableAvroSchema) {
                            String avroSchemaString = SchemaGenerator.generate(avroSchema);
                            if (avroSchema.getName().equalsIgnoreCase(key.getValue2())) {
                                result = avroSchemaString;
                            }
                            redis.set(String.format("%s.%s.%s", key.getValue0(), key.getValue1(), avroSchema.getName()), avroSchemaString);
                        }
                        return KeyValue.with(namespace, result);
                    }
                });
    }

    /**
     * v0:connecturl;v1:instanceId;v2:binlog;v3:schema;v4:user;v5:password
     */
    public static KeyValue<String, String> schema(String instanceId, String schema, String table) {
        try {
            return caches.get(Triplet.with(instanceId, schema, table));
        } catch (ExecutionException e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }
}
