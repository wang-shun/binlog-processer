package com.datatrees.datacenter.resolver.reader;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.ganglia.GangliaReporter;
import com.datatrees.datacenter.core.exception.BinlogException;
import com.datatrees.datacenter.core.storage.EventConsumer;
import com.datatrees.datacenter.core.task.domain.Binlog;
import com.datatrees.datacenter.resolver.domain.Operator;
import com.datatrees.datacenter.resolver.domain.Status;
import com.datatrees.datacenter.resolver.schema.SchemaData;
import com.github.shyiko.mysql.binlog.BinaryLogFileReader;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventHeader;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.ChecksumType;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import info.ganglia.gmetric4j.gmetric.GMetric;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BinlogFileReader implements Runnable {

  private static Logger logger = LoggerFactory.getLogger(BinlogFileReader.class);
  private GMetric ganglia;
  private MetricRegistry metrics;
  private Timer requests;
  private Slf4jReporter reporter;
  private GangliaReporter reporter2;
  private ThreadLocal<AtomicInteger> readEventError = new ThreadLocal<>();
  private EnumMap<EventType, EventConsumer<Event>> consumerCache;
  /**
   * Map<SimpleEntry<Binlog, Status>  binlog 和最终处理状态 Map<Operator, AtomicLong> 处理文件个数
   */
  private EventListner<AbstractMap.SimpleEntry<AbstractMap.SimpleEntry<Binlog, Status>, Map<Operator, AtomicLong>>> consumer;
  private InputStream fileStream;
  private Binlog binlog;
  private HashMap<Operator, AtomicLong> resultMap;
  private EventDeserializer eventDeserializer;
  private BinaryLogFileReader reader = null;
  private ConcurrentHashMap<Long, SimpleEntry> tableMapCache;
  private SchemaData schemaUtility;
  private Status currentStatus;
  /**
   * 分库
   */
  private Function<String, String> schemaNameMapper = t -> t;

  public BinlogFileReader() {
    resultMap = new HashMap<>(
      ImmutableMap.<Operator, AtomicLong>builder().put(Operator.Create, new AtomicLong(0L))
        .put(Operator.Update, new AtomicLong(0L)).put(Operator.Delete, new AtomicLong(0L))
        .put(Operator.DEFAULT, new AtomicLong(0L)).build());
    currentStatus = Status.SUCCESS;
    eventDeserializer = new EventDeserializer();
    tableMapCache = new ConcurrentHashMap<>();
    consumerCache = new EnumMap<>(EventType.class);
//    metrics = new MetricRegistry();
//    requests = metrics.timer(name(BinlogFileReader.class, "resolve binlog"));
//    reporter = Slf4jReporter.forRegistry(metrics).build();
//    reporter.start(3000L, TimeUnit.MILLISECONDS);

//    try {
//      ganglia = new GMetric("localhost", 8649, UDPAddressingMode.MULTICAST, 1);
//      reporter2 = GangliaReporter.forRegistry(metrics)
//        .convertRatesTo(TimeUnit.SECONDS)
//        .convertDurationsTo(TimeUnit.MILLISECONDS)
//        .build(ganglia);
//      reporter2.start(3000L, TimeUnit.MILLISECONDS);
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
  }

  public BinlogFileReader(Binlog binlog,
    InputStream fileStream,
    EventListner<AbstractMap.SimpleEntry<AbstractMap.SimpleEntry<Binlog, Status>, Map<Operator, AtomicLong>>> consumer
  ) throws IOException {
    this(binlog, fileStream, consumer, null);
  }

  public BinlogFileReader(Binlog binlog,
    InputStream fileStream,
    EventListner<AbstractMap.SimpleEntry<AbstractMap.SimpleEntry<Binlog, Status>, Map<Operator, AtomicLong>>> consumer,
    Function<String, String> schemaNameMapper
  ) throws IOException {
    this();
    this.fileStream = fileStream;
    this.binlog = binlog;
    this.consumer = consumer;
    this.eventDeserializer.setChecksumType(ChecksumType.CRC32);
    this.reader = new BinaryLogFileReader(this.fileStream, eventDeserializer);
    this.schemaUtility = new SchemaData();
    this.consumerCache.put(EventType.TABLE_MAP, this::handleTableMap);
    this.consumerCache.put(EventType.WRITE_ROWS, this::handleWriteRow);
    this.consumerCache.put(EventType.EXT_WRITE_ROWS, this::handleWriteRow);
    this.consumerCache.put(EventType.UPDATE_ROWS, this::handleUpdateRow);
    this.consumerCache.put(EventType.EXT_UPDATE_ROWS, this::handleUpdateRow);
    this.consumerCache.put(EventType.DELETE_ROWS, this::handleDeleteRow);
    this.consumerCache.put(EventType.EXT_DELETE_ROWS, this::handleDeleteRow);
    if (schemaNameMapper != null) {
      this.schemaNameMapper = schemaNameMapper;
    }
  }

  public void read() {
    logger.
      info("Begin to analysis binlog file {} of instance", this.binlog);
    Thread thread = new Thread(this);
    thread.start();
  }

  @java.lang.SuppressWarnings("unchecked")
  protected <T extends EventData> T unwrapData(Event event) {
    EventData eventData = event.getData();
    if (eventData instanceof EventDeserializer.EventDataWrapper) {
      eventData = ((EventDeserializer.EventDataWrapper) eventData).getInternal();
    }
    return (T) eventData;
  }

  private void onStart() {
    this.consumer.onStart(this.binlog);
  }

  private void onFinished() {
    this.consumer.onFinish(
      new SimpleEntry<SimpleEntry<Binlog, Status>, Map<Operator, AtomicLong>>(
        new SimpleEntry(binlog, this.currentStatus), resultMap
      )
    );
//    this.reporter2.stop();
  }

  private void handleTableMap(Event event) {
    TableMapEventData tableMapEventData = unwrapData(event);
    long tableNumber = tableMapEventData.getTableId();
    String databaseName = schemaNameMapper.apply(tableMapEventData.getDatabase());
    String tableName = tableMapEventData.getTable();
    if (!databaseName.equalsIgnoreCase("mysql")) {
      tableMapCache.put(tableNumber, new SimpleEntry<>(databaseName, tableName));
    }
  }

  private void consumeBufferRecord(Operator operator, Long tableId, Serializable[] before,
    Serializable[] after) {
    SimpleEntry<String, String> simpleEntry = tableMapCache.get(tableId);
    if (simpleEntry != null) {
      Schema schema = schemaUtility
        .toAvroSchema(this.binlog, simpleEntry.getKey(), simpleEntry.getValue());
      if (schema != null) {
        Object resultValue = schemaUtility.toAvroData(schema, operator, before, after);
        if (resultValue != null) {
//          Timer.Context context = requests.time();
          try {
            consumer.consume(schema, this.binlog, operator, resultValue);
          } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new BinlogException(
              String.format("error to consume binlog event of %s", tableId));
          } finally {
            resultMap.get(operator).incrementAndGet();
//            context.stop();
          }
        }
      }
    }
  }

  private void handleWriteRow(Event event) {
    WriteRowsEventData writeRowsEventData = unwrapData(event);
    List<Serializable[]> writeRows = writeRowsEventData.getRows();
    for (Serializable[] rows : writeRows) {
      consumeBufferRecord(Operator.Create, writeRowsEventData.getTableId(), null, rows);
    }
  }

  private void handleUpdateRow(Event event) {
    UpdateRowsEventData updateRowsEventData = unwrapData(event);
    List<Map.Entry<Serializable[], Serializable[]>> updateRows = updateRowsEventData.getRows();
    for (Map.Entry<Serializable[], Serializable[]> rows : updateRows) {
      consumeBufferRecord(Operator.Update, updateRowsEventData.getTableId(), rows.getKey(),
        rows.getValue());
    }
  }

  private void handleDeleteRow(Event event) {
    DeleteRowsEventData deleteRowsEventData = unwrapData(event);
    List<Serializable[]> deleteRows = deleteRowsEventData.getRows();
    for (Serializable[] rows : deleteRows) {
      consumeBufferRecord(Operator.Delete, deleteRowsEventData.getTableId(), rows, null);
    }
  }

  @Override
  public void run() {
    logger.info("start to read binlog file {} of instance.", this.binlog);
    try {
      onStart();
      try {
        Event event = null;
        while (true) {
          try {
            event = reader.readEvent();
          } catch (IOException e) {
            //如果是readevent 错误则继续读取下一个
            logger.error("failed to read event because of "
              + e.getMessage(), e);
//            readEventError.set(readEventError.get().getAndIncrement());
          } finally {
            if (event == null) {
              break;
            }
            consume(event);
          }
        }
//        for (Event event; (event = reader.readEvent()) != null; ) {
//          consume(event);
//        }
      } catch (Exception e) {
        logger.error(e.getMessage(), e);
        currentStatus = Status.FAIL;
      }
    } finally {
      onFinished();
    }
    logger.info("end to read binlog file {} of instance.", this.binlog);
  }

  private void consume(Event event) {
    final EventHeader eventHeader = event.getHeader();
    final EventType eventType = eventHeader.getEventType();
    final Operator simpleEventType = Operator.valueOf(eventType);
    EventConsumer<Event> eventBinlogEventConsumer =
      consumerCache.getOrDefault(eventType,
        bufferRecord -> resultMap.get(simpleEventType).incrementAndGet());
    eventBinlogEventConsumer.consume(event);
  }
}
