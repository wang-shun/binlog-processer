package com.datatrees.datacenter.resolver.schema;

import com.callfire.avro.AvroSchema;
import com.callfire.avro.DbSchemaExtractor;
import com.callfire.avro.SchemaGenerator;
import com.callfire.avro.config.AvroConfig;
import com.datatrees.datacenter.core.domain.Status;
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
import org.apache.commons.lang3.StringUtils;
import org.javatuples.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * provided for just one instance
 */
public class SchemaProviders {

  public static String NULL = "null";
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

  private static Long CACHE_TIMEOUT = 7L;

  static {
    properties = PropertiesUtility.defaultProperties();
    secondaryCache = CacheBuilder.newBuilder().maximumSize(10000).
      expireAfterAccess(CACHE_TIMEOUT, TimeUnit.DAYS)
      .build(new CacheLoader<SecondaryCacheKey, KeyValue<String, String>>() {
        @Override
        public KeyValue<String, String> load(SecondaryCacheKey cacheKey) throws Exception {
          return getSchema(cacheKey.binlog, cacheKey.schema, cacheKey.table);
        }
      });
  }

  public static LoadingCache cache() {
    return secondaryCache;
  }

  /**
   * v0:connecturl;v1:instanceId;v2:binlog;v3:schema;v4:user;v5:password
   */

  public static KeyValue<String, String> schema(Binlog binlog, String schema, String table) {
    try {
      return secondaryCache
        .get(SecondaryCacheKey.builder().binlog(binlog).schema(schema).table(table).build());
    } catch (ExecutionException e) {
      throw new BinlogException(
        String.format("error to get cache schema of [%s-%s-%s] from %s.",
          binlog.getInstanceId(), schema, table, binlog.getJdbcUrl()), Status.SCHEMAFAILED, e
      );
    }
  }

  private static KeyValue<String, String> getSchema(Binlog binlog, String schema, String table) {
    final String patternInstance = schemaNameMapper.apply(binlog.getInstanceId());
    final String patternSchema = schemaNameMapper.apply(schema);
    final String patternTable = schemaNameMapper.apply(table);

    try {
      String redisKey =
        String.format("%s:%s:%s", patternInstance, patternSchema, patternTable);
      String nameSpace =
        String.format("%s.%s", patternInstance, patternSchema);

      if (redis.exists(redisKey)) {
        return KeyValue.with(nameSpace, redis.get(redisKey));
      }

      dbSchemaExtractor =
        new DbSchemaExtractor(jdbcPort(binlog.getJdbcUrl()), user(), password());
      AvroConfig config =
        new AvroConfig(String.format("%s.%s", patternInstance, patternSchema));
      config.setFieldNameMapper(r -> r.replaceAll("`", ""));
      config.setSchemaNameMapper(r -> r.replaceAll("`", ""));
      List<AvroSchema> tableAvroSchema = dbSchemaExtractor.getForSchema(config, schema);
      String result = null;
      for (AvroSchema avroSchema : tableAvroSchema) {
        String avroSchemaString = SchemaGenerator.generate(avroSchema);
        if (patternTable.equalsIgnoreCase(avroSchema.getName())) {
          result = avroSchemaString;
        }
        redis.
          set(String.
              format("%s:%s:%s", patternInstance, patternSchema, avroSchema.getName()),
            avroSchemaString);
      }
      if (StringUtils.isBlank(result)) {
        redis.set(redisKey, NULL);
      }

      return KeyValue.with(nameSpace, result);
    } catch (Exception e) {
      throw new BinlogException(
        String.format("error to get schema of [%s-%s-%s] from %s.",
          binlog.getInstanceId(), schema, table, binlog.getJdbcUrl()), Status.SCHEMAFAILED, e
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

  public static class SecondaryCacheKey {

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

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof SecondaryCacheKey) {
        SecondaryCacheKey other = (SecondaryCacheKey) obj;
        return other.binlog.getInstanceId().equalsIgnoreCase(this.binlog.getInstanceId()) &&
          other.schema.equalsIgnoreCase(this.schema) && other.table.equalsIgnoreCase(this.table);
      } else {
        return false;
      }
    }

//    @Override
//    public int hashCode() {
//      final int prime = 31;
//      int result = 1;
//      result = prime * result + this.binlog.getInstanceId().length();
//      result = prime * result + this.table.length(); // <= **this one shouldn't evaluated**
//      result = prime * result + this.schema.length();
//      return result;
//    }


    @Override
    public int hashCode() {
      return this.binlog.getInstanceId().hashCode() + this.schema.hashCode() + this.table
        .hashCode();
    }

    @Override
    public String toString() {
      return String.format("%s.%s.%s", this.binlog.getInstanceId(), this.schema, this.table);
    }

    public static class CacheKeyBuilder {

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
