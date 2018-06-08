package com.datatrees.datacenter.resolver.schema;

import com.callfire.avro.AvroSchema;
import com.callfire.avro.DbSchemaExtractor;
import com.callfire.avro.SchemaGenerator;
import com.callfire.avro.config.AvroConfig;
import com.datatrees.datacenter.core.exception.BinlogException;
import com.datatrees.datacenter.core.task.domain.Binlog;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.core.utility.Redis;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import org.javatuples.KeyValue;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
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
    private static Properties properties;

    static {
        properties = PropertiesUtility.load("dbInstance.properties");
    }

    /**
     * v0:connecturl;v1:instanceId;v2:binlog;v3:schema;v4:user;v5:password
     */
    public static KeyValue<String, String> schema(Binlog binlog, String schema, String table) {
        try {
            String redisKey = String.format("%s.%s.%s", binlog.getInstanceId(), schema, table);
            String namespace = String.format("%s.%s", binlog.getInstanceId(), schemaNameMapper.apply(schema));

            if (redis.exists(redisKey)) return KeyValue.with(namespace, redis.get(redisKey));

            dbSchemaExtractor = new DbSchemaExtractor(jdbcPort(binlog.getJdbcUrl()), user(), password());
            AvroConfig config = new AvroConfig(String.format("%s.%s", binlog.getInstanceId(), schemaNameMapper.apply(schema)));
            List<AvroSchema> tableAvroSchema = dbSchemaExtractor.getForSchema(config, schema);
            String result = null;
            for (AvroSchema avroSchema : tableAvroSchema) {
                String avroSchemaString = SchemaGenerator.generate(avroSchema);
                if (table.equalsIgnoreCase(avroSchema.getName())) {
                    result = avroSchemaString;
                }
                redis.set(String.format("%s.%s.%s", binlog.getInstanceId(), schema, avroSchema.getName()), avroSchemaString);
            }
            return KeyValue.with(namespace, result);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new BinlogException(String.
                    format("error to get schema of [%s-%s-%s] because of %s",
                            binlog.getInstanceId(), schema, table, e.getMessage()));
        }
    }

    static String jdbcPort(String url) {
        return String.format("jdbc:mysql://%s:%s", url, properties.getProperty("port"));
    }

    static String password() {
        return properties.getProperty("password");
    }

    static String user() {
        return properties.getProperty("user");
    }
}
