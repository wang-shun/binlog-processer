package com.datatrees.datacenter.resolver.schema;

import com.callfire.avro.AvroSchema;
import com.callfire.avro.DbSchemaExtractor;
import com.callfire.avro.SchemaGenerator;
import com.callfire.avro.config.AvroConfig;
import com.datatrees.datacenter.core.exception.BinlogException;
import com.datatrees.datacenter.core.task.domain.Binlog;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.core.utility.Redis;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.javatuples.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * provided for just one instance
 */
public class SchemaProviders {

  /**
   * Triple<instanceId,database,table>
   */
  private static DbSchemaExtractor dbSchemaExtractor;
  private static Logger logger = LoggerFactory.getLogger(SchemaProviders.class);
  /**
   * KeyValue<namespace,avrostring>
   */
  private static Redis.SimpleRedis<String, String> redis = Redis.getMgr();
  private static Function<String, String> schemaNameMapper = t -> t.replaceAll("\\d+", "");
  private static Properties properties;
  private static LoadingCache<SecondaryCacheKey, KeyValue<String, String>> secondaryCache;

  static {
    properties = PropertiesUtility.defaultProperties();
    secondaryCache = CacheBuilder.newBuilder().maximumSize(1000).
      expireAfterAccess(10, TimeUnit.HOURS)
      .build(new CacheLoader<SecondaryCacheKey, KeyValue<String, String>>() {
        @Override
        public KeyValue<String, String> load(SecondaryCacheKey cacheKey) throws Exception {
          return getSchema(cacheKey.binlog, cacheKey.schema, cacheKey.table);
        }
      });
  }

  /**
   * v0:connecturl;v1:instanceId;v2:binlog;v3:schema;v4:user;v5:password
   */

  public static KeyValue<String, String> schema(Binlog binlog, String schema, String table) {
    try {
      return secondaryCache
        .get(SecondaryCacheKey.builder().binlog(binlog).schema(schema).table(table).build());
    } catch (ExecutionException e) {
      logger.error(e.getMessage(), e);
      throw new BinlogException("error to get local cache of" + schema + "-" + table,
        e);
    }
  }

  private static KeyValue<String, String> getSchema(Binlog binlog, String schema, String table) {
    final String patternInstance = schemaNameMapper.apply(binlog.getInstanceId());
    final String patternSchema = schemaNameMapper.apply(schema);
    final String patternTable = schemaNameMapper.apply(table);

    try {
      String redisKey = String.format("%s.%s.%s", patternInstance, patternSchema, patternTable);
      String namespace = String.format("%s.%s", patternInstance, patternSchema);

      if (redis.exists(redisKey)) {
        return KeyValue.with(namespace, redis.get(redisKey));
      }

      dbSchemaExtractor = new DbSchemaExtractor(jdbcPort(binlog.getJdbcUrl()), user(), password());
      AvroConfig config = new AvroConfig(String.format("%s.%s", patternInstance, patternSchema));
      List<AvroSchema> tableAvroSchema = dbSchemaExtractor.getForSchema(config, schema);
      String result = null;
      for (AvroSchema avroSchema : tableAvroSchema) {
        String avroSchemaString = SchemaGenerator.generate(avroSchema);
        if (patternTable.equalsIgnoreCase(avroSchema.getName())) {
          result = avroSchemaString;
        }
        redis.set(String.format("%s.%s.%s", patternInstance, patternSchema, avroSchema.getName()),
          avroSchemaString);
      }
      return KeyValue.with(namespace, result);
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      throw new BinlogException(String.format("error to get schema of [%s-%s-%s] because of %s",
        binlog.getInstanceId(), patternSchema, patternTable, e.getMessage(),
        e)
      );
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

  static class SecondaryCacheKey {

    public Binlog binlog;
    public String schema;
    public String table;

    public SecondaryCacheKey(Binlog binlog, String schema, String table) {
      this.binlog = binlog;
      this.schema = schema;
      this.table = table;
    }

    public static CacheKeyBuilder builder() {
      return new CacheKeyBuilder();
    }

    static class CacheKeyBuilder {

      public Binlog binlog;
      public String schema;
      public String table;

      public CacheKeyBuilder binlog(Binlog binlog) {
        this.binlog = binlog;
        return this;
      }

      public CacheKeyBuilder schema(String schema) {
        this.schema = schema;
        return this;
      }

      public CacheKeyBuilder table(String table) {
        this.table = table;
        return this;
      }

      public SecondaryCacheKey build() {
        return new SecondaryCacheKey(this.binlog, this.schema, this.table);
      }
    }
  }
}