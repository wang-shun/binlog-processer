package com.datatrees.datacenter.resolver.partition;

import com.datatrees.datacenter.core.domain.Operator;
import com.datatrees.datacenter.core.domain.Status;
import com.datatrees.datacenter.core.exception.BinlogException;
import com.datatrees.datacenter.core.storage.FileStorage;
import com.datatrees.datacenter.core.task.domain.Binlog;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.resolver.DBbiz;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.LinkedHashMultimap;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionWriterManager implements WriteResult {

  private static String TMP_ROOT_PATH;
  private static String WAREHOUSE_ROOT_PATH;
  private static Logger logger = LoggerFactory.getLogger(PartitionWriterManager.class);
  private static LoadingCache<CacheKey, PartitionWriter> caches;

  static {
    java.util.Properties value = PropertiesUtility.defaultProperties();
    TMP_ROOT_PATH = value.getProperty("temp.url");
    WAREHOUSE_ROOT_PATH = value.getProperty("hdfs.warehouse.url");
    caches = CacheBuilder.newBuilder().
      maximumSize(10000).
      expireAfterAccess(12, TimeUnit.HOURS).
      build(new CacheLoader<CacheKey, PartitionWriter>() {
        @Override
        public PartitionWriter load(CacheKey key) throws Exception {
          return new InternalPartitionWriter(key.envelopSchema, key.storage.openWriter(key.path),
            key.partitioner);
        }
      });
  }

  private FileStorage fileStorage;
  private HashMap<String, PartitionWriter> writerCache = new HashMap<>();
  private ImmutableSet<Partitioner> partitioners;

  private HashMap<String, WriteResultValue> valueCacheByFile = new HashMap<>();
  private LinkedHashMultimap<String, String> valueCacheByPartition = LinkedHashMultimap.create();
//  private HashMap<String, AtomicInteger> valueCacheByPartition = new HashMap<>();

  private Binlog file;

  public PartitionWriterManager(FileStorage fileStorage, Binlog file) {
    this.fileStorage = fileStorage;
    this.file = file;
    this.partitioners = ImmutableSet.<Partitioner>builder().add(new CreateDatePartitioner())
      .add(new UpdateDatePartitioner()).build();
    final Thread.UncaughtExceptionHandler uncaughtExceptionHandler =
      new Thread.UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
          logger.error("Uncaught exception in BulkProcessor thread {}", t, e);
        }
      };

    ScheduledFuture<?> schedule = Executors.newScheduledThreadPool(100, new ThreadFactory() {
      Thread thread;

      @Override
      public Thread newThread(Runnable r) {
        thread = new Thread(r);
        thread.setDaemon(true);
        thread.setUncaughtExceptionHandler(uncaughtExceptionHandler);
        return thread;
      }
    }).schedule(() -> writerCache.forEach((schemaStringSimpleEntry, partitionWriter) -> {
      try {
        partitionWriter.flush();
      } catch (IOException e) {
        logger.error(e.getMessage());
      }
    }), 5, TimeUnit.MINUTES);
  }

  public HashMap<String, WriteResultValue> getValueCacheByFile() {
    return valueCacheByFile;
  }

  public LinkedHashMultimap<String, String> getValueCacheByPartition() {
    return valueCacheByPartition;
  }

  public void close() {
    writerCache.forEach((path, partitionWriter) -> {
      try {
        partitionWriter.close();
        String tempPath = path;
        String targetPath =
          tempPath.replace(TMP_ROOT_PATH, WAREHOUSE_ROOT_PATH)
            .replace(tempPath.split("/")[6] + "/", "");
        fileStorage.commit(tempPath, targetPath);
      } catch (Exception e) {
        throw new BinlogException(String.format("error to commit avro data of %s", path),
          Status.COMMITRECORDFAILED, e);
      }
    });
    valueCacheByPartition.clear();
    valueCacheByFile.clear();
  }

  private String createFullPath(String relativeFilePath, Partitioner partitioner, Binlog binlog,
    String[] fullSchema, Schema envelopSchema) {
    String fullPath = null;
    String database = fullSchema[1];
    String instance = fullSchema[0];
    if (relativeFilePath != null) {
      fullPath = String.
        format("%s/%s/%s/%s/%s/%s/%s/%s.avro", TMP_ROOT_PATH, partitioner.getRoot(),
          binlog.getIdentity0(),
          instance, database, envelopSchema.getName(), relativeFilePath,
          binlog.getIdentity1().replace(".tar", ""));
    } else {//没有分区
      fullPath = String.
        format("%s/%s/%s/%s/%s/%s/%s.avro", TMP_ROOT_PATH, partitioner.getRoot(),
          binlog.getIdentity0(),
          instance, database, envelopSchema.getName(),
          binlog.getIdentity1().replace(".tar", ""));
    }
    return fullPath;
  }

  public void write(Schema schema, Operator operator, Object result)
    throws IOException {
    GenericData.Record record = null;
    switch (operator) {
      case Update:
      case Create:
        record = (GenericData.Record) ((GenericData.Record) result).get("After");
        break;
      case Delete:
        record = (GenericData.Record) ((GenericData.Record) result).get("Before");
        break;
      default:
        break;
    }

    Schema envelopSchema = schema;
    String[] fullSchema = envelopSchema.getNamespace().split("\\.");
    PartitionWriter writer = null;
    if (record != null) {
      for (Partitioner partitioner : partitioners) {
        String relativeFilePath = partitioner.encodePartition(record);
        String fullPath = createFullPath(relativeFilePath, partitioner, this.file, fullSchema,
          envelopSchema);
        if (writerCache.containsKey(fullPath)) {
          writer = writerCache.get(fullPath);
        } else {
          writer = new InternalPartitionWriter(envelopSchema, fileStorage.openWriter(fullPath),
            partitioner);
          writerCache.put(fullPath, writer);
        }
        writer.write(result);

        if (partitioner.getRoot().equalsIgnoreCase("create")) {
          String key = String
            .format("%s.%s.%s", fullSchema[0], fullSchema[1], envelopSchema.getName());
          if (valueCacheByFile.containsKey(key)) {
            WriteResultValue value = valueCacheByFile.get(key);
            value.increment(operator, relativeFilePath);
          } else {
            WriteResultValue value = WriteResultValue.create();
            value.increment(operator, relativeFilePath);
            valueCacheByFile.put(key, value);
          }

          valueCacheByPartition.put(String.format("%s.%s.%s.%s", fullSchema[0], fullSchema[1],
            envelopSchema.getName(), relativeFilePath),
            String.format("%s.avro", file.getIdentity1().replace(".tar", "")));
        }
      }
    }
  }

  static class InternalPartitionWriter implements PartitionWriter {

    private DataFileWriter<Object> dataFileWriter = new DataFileWriter<>(
      new GenericDatumWriter<>());
    private Partitioner partitioner;

    public InternalPartitionWriter(Schema schema, OutputStream stream, Partitioner partitioner)
      throws IOException {
      this.dataFileWriter.create(schema, stream);
      this.partitioner = partitioner;
    }

    public void write(Object value) throws IOException {
      dataFileWriter.append(value);
    }

    @Override
    public void flush() throws IOException {
      dataFileWriter.flush();
    }

    @Override
    public void close() throws IOException {
      dataFileWriter.close();
    }

    @Override
    public Partitioner partitioner() {
      return partitioner;
    }
  }

  static class CacheKey implements Serializable {

    public String path;
    public Schema envelopSchema;
    public Partitioner partitioner;
    public FileStorage storage;

    @Override
    public String toString() {
      return this.path;
    }
  }
}