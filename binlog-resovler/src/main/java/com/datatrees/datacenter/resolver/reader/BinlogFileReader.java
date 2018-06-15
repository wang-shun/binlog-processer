package com.datatrees.datacenter.resolver.reader;

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
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BinlogFileReader implements Runnable {

  private static Logger logger = LoggerFactory.getLogger(BinlogFileReader.class);
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
      ImmutableMap.<Operator, AtomicLong>builder().put(Operator.Create, new AtomicLong(0L)).
        put(Operator.Update, new AtomicLong(0L)).put(Operator.Delete, new AtomicLong(0L))
        .put(Operator.DEFAULT, new AtomicLong(0L)).
        build());
    currentStatus = Status.SUCCESS;
    eventDeserializer = new EventDeserializer();
    tableMapCache = new ConcurrentHashMap<>();
    consumerCache = new EnumMap<>(EventType.class);
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
          try {
            consumer.consume(schema, this.binlog, operator, resultValue);
          } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new BinlogException(
              String.format("error to consume binlog event of %s", tableId));
          } finally {
            resultMap.get(operator).incrementAndGet();
          }
        }
      }
    }
  }

  private void handleWriteRow(Event event) {
    WriteRowsEventData writeRowsEventData = unwrapData(event);
    Iterator<Serializable[]> writeRows = writeRowsEventData.getRows().iterator();
    do {
      Serializable[] rows = writeRows.next();
      consumeBufferRecord(Operator.Create, writeRowsEventData.getTableId(), null, rows);
    } while (writeRows.hasNext());
  }

  private void handleUpdateRow(Event event) {
    UpdateRowsEventData updateRowsEventData = unwrapData(event);
    Iterator<Map.Entry<Serializable[], Serializable[]>> updateRows = updateRowsEventData.getRows()
      .iterator();
    do {
      Map.Entry<Serializable[], Serializable[]> rows = updateRows.next();
      consumeBufferRecord(Operator.Update, updateRowsEventData.getTableId(), rows.getKey(),
        rows.getValue());
    } while (updateRows.hasNext());
  }

  private void handleDeleteRow(Event event) {
    DeleteRowsEventData deleteRowsEventData = unwrapData(event);
    Iterator<Serializable[]> deleteRows = deleteRowsEventData.getRows().iterator();
    do {
      Serializable[] rows = deleteRows.next();
      consumeBufferRecord(Operator.Delete, deleteRowsEventData.getTableId(), rows, null);
    } while (deleteRows.hasNext());
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
//          final EventHeader eventHeader = event.getHeader();
//          final EventType eventType = eventHeader.getEventType();
//          final Operator simpleEventType = Operator.valueOf(eventType);
//          EventConsumer<Event> eventBinlogEventConsumer =
//            consumerCache.getOrDefault(eventType,
//              bufferRecord -> resultMap.get(simpleEventType).incrementAndGet());
//          eventBinlogEventConsumer.consume(event);
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
