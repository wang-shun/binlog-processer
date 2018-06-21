package com.datatrees.datacenter.resolver.partition;

import com.datatrees.datacenter.core.storage.FileStorage;
import com.datatrees.datacenter.core.task.domain.Binlog;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.resolver.domain.Operator;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import io.netty.util.internal.ConcurrentSet;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionWriterManager {

  private static String ROOT_PATH = "hdfs://dn0:8020/data/warehouse";
  private static String TMP_ROOT_PATH = "hdfs://dn0:8020/data/temp";
  private static Logger logger = LoggerFactory.getLogger(PartitionWriterManager.class);
  private static Integer BUFFER_SIZE = 50;

  static {
    java.util.Properties value = PropertiesUtility.defaultProperties();
    TMP_ROOT_PATH = value.getProperty("hdfs.temp.url");
    BUFFER_SIZE = Integer.valueOf(value.getProperty("avro.message.buffer.size"));
  }

  private Lock processLock = new ReentrantLock();
  private List<BufferMessage> bufferList = new ArrayList<>();
  private LoadingCache<CacheKey, PartitionWriter> caches;
  private FileStorage fileStorage;
  private Map<String, PartitionWriter> writerCache = new ConcurrentHashMap<>();
  private ImmutableSet<Partitioner> partitioners;
  private Set<String> fileAppendSet = new ConcurrentSet<>();
  private Boolean isClosed = false;

  public PartitionWriterManager(FileStorage fileStorage) {
    this.fileStorage = fileStorage;
    this.partitioners = ImmutableSet.<Partitioner>builder().add(new CreateDatePartitioner())
      .add(new UpdateDatePartitioner()).build();
    final Thread.UncaughtExceptionHandler uncaughtExceptionHandler =
      (t, e) -> logger.error("Uncaught exception in BulkProcessor thread {}", t, e);

    caches = CacheBuilder.newBuilder().
      maximumSize(10000).
      expireAfterAccess(12, TimeUnit.HOURS).
      build(new CacheLoader<CacheKey, PartitionWriter>() {
        @Override
        public PartitionWriter load(CacheKey key) throws Exception {
          return new InternalPartitionWriter(key.envelopSchema, key.storage.openWriter(key.path),
            key.path, false,
            key.partitioner);
        }
      });

    Thread processThread = new Thread(() -> processBufferRecord());
    processThread.start();
  }

  public void close() {
    isClosed = true;
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

  public void write(Schema schema, Binlog binlog, Operator operator, Object result)
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
    if (record != null) {
      for (Partitioner partitioner : partitioners) {
        processLock.lock();
        PartitionWriter writer = createPartitionWriter(partitioner, record, binlog, fullSchema,
          envelopSchema);
        produce(writer, result);
        processLock.unlock();
      }
    }
  }

  private PartitionWriter createPartitionWriter(Partitioner partitioner, Record record,
    Binlog binlog, String[] fullSchema, Schema envelopSchema) throws IOException {
    PartitionWriter writer = null;
    String relativeFilePath = partitioner.encodePartition(record);
    String fullPath = createFullPath(relativeFilePath, partitioner, binlog, fullSchema,
      envelopSchema);
    if (writerCache.containsKey(fullPath)) {
      writer = writerCache.get(fullPath);
    } else {
      writer = new InternalPartitionWriter(envelopSchema, fileStorage.openWriter(fullPath),
        fullPath, false,
        partitioner);
      writerCache.put(fullPath, writer);
    }

    return writer;
  }

  private void produce(PartitionWriter writer, Object result) {
    BufferMessage bufferMessage = new BufferMessage();
    bufferMessage.result = result;
    bufferMessage.writer = writer;
    bufferList.add(bufferMessage);
  }


  private void bulkWrite() {
    for (BufferMessage message : bufferList) {
      try {
        message.writer.write(message.result);
      } catch (IOException e) {
        logger.error(e.getMessage(), e);
      }
    }

    for (Entry<String, PartitionWriter> entry : writerCache.entrySet()) {
      try {
        entry.getValue().close();
        String tempPath = entry.getKey();
        String targetPath =
          tempPath.replace("temp", "warehouse").
            replace(tempPath.split("/")[6] + "/", "") + "." + (System.currentTimeMillis()
            % 1000000000);
        fileStorage.commit(tempPath, targetPath);
      } catch (IOException e) {
        logger.error(e.getMessage(), e);
      }
    }

    writerCache.clear();
    bufferList.clear();
  }

  private void processBufferRecord() {
    while (true) {
      if (!isClosed) {
        if (bufferList.size() > BUFFER_SIZE) {
          processLock.lock();
          bulkWrite();
          processLock.unlock();
        }
      } else {
        if (bufferList.size() > 0) {
          bulkWrite();
        }
        break;
      }
      try {
        TimeUnit.MILLISECONDS.sleep(500L);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
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

  class BufferMessage {

    public PartitionWriter writer;
    public Object result;
  }

  class InternalPartitionWriter implements PartitionWriter {

    private DataFileWriter<Object> dataFileWriter = new DataFileWriter<>(
      new GenericDatumWriter<>());
    private Partitioner partitioner;

//    public InternalPartitionWriter(Schema schema, OutputStream stream, Partitioner partitioner)
//      throws IOException {
//
////      this.dataFileWriter.appendTo(new FsInput())
////      this.dataFileWriter.create(schema, stream);
////      this.partitioner = partitioner;
//    }

    public InternalPartitionWriter(Schema schema, OutputStream stream, String filePath,
      Boolean append, Partitioner partitioner)
      throws IOException {
      if (append && fileStorage.exists(filePath)) {
        this.dataFileWriter.appendTo(new FsInput(new Path(filePath), new Configuration()), stream);
      } else {
        this.dataFileWriter.create(schema, stream);
      }
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
}