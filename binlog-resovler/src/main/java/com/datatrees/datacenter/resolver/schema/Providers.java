package com.datatrees.datacenter.resolver.schema;

import com.callfire.avro.AvroSchema;
import com.callfire.avro.DbSchemaExtractor;
import com.callfire.avro.SchemaGenerator;
import com.callfire.avro.config.AvroConfig;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.core.utility.Redis;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import org.javatuples.KeyValue;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
    private static Redis.SimpleRedis<String, String> redis = Redis.getMgr();
    private static Function<String, String> schemaNameMapper = t -> t.replaceAll("\\d+", "");

//    static {
//        try {
//            Iterator<Map.Entry<Object, Object>> value =
//                    PropertiesUtility.load("dbInstance.properties").entrySet().iterator();
//            ImmutableMap.Builder builder = ImmutableMap.<String, Triplet<String, String, String>>builder();
//            while (value.hasNext()) {
//                Map.Entry<Object, Object> entry = value.next();
//                builder.put(entry.getKey().toString(),
//                        Triplet.with(String.format("jdbc:mysql://%s:3306", entry.getValue().toString()),
//                                "debezium", "Debezium_"));
//            }
//            dbInstanceSchema = builder.build();
//        } catch (Exception e) {
//            logger.error(e.getMessage(), e);
//        }
//    }
    /**
     * v0:connecturl;v1:instanceId;v2:binlog;v3:schema;v4:user;v5:password
     */
    public static KeyValue<String, String> schema(String instanceId, String schema, String table) {
        try {
            String redisKey = String.format("%s.%s.%s", instanceId, schema, table);
            String namespace = String.format("%s.%s", instanceId, schemaNameMapper.apply(schema));
            if (redis.exists(redisKey)) return KeyValue.with(namespace, redis.get(redisKey));

            Triplet<String, String, String> connectInfo = dbInstanceSchema.get(instanceId);
            dbSchemaExtractor = new DbSchemaExtractor(connectInfo.getValue0(), connectInfo.getValue1(), connectInfo.getValue2());
            AvroConfig config =
                    new AvroConfig(String.format("%s.%s", instanceId, schemaNameMapper.apply(schema)));
            List<AvroSchema> tableAvroSchema = dbSchemaExtractor.getForSchema(config, schema);
            String result = null;
            for (AvroSchema avroSchema : tableAvroSchema) {
                String avroSchemaString = SchemaGenerator.generate(avroSchema);
                if (avroSchema.getName().equalsIgnoreCase(table)) {
                    result = avroSchemaString;
                }
                redis.set(String.format("%s.%s.%s", instanceId, schema, avroSchema.getName()), avroSchemaString);
            }
            return KeyValue.with(namespace, result);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }
}
