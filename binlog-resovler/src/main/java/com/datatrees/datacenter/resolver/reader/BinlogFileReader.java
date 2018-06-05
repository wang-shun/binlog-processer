package com.datatrees.datacenter.resolver.reader;

import com.datatrees.datacenter.core.storage.EventConsumer;
import com.datatrees.datacenter.core.utility.StopWatch;
import com.datatrees.datacenter.resolver.domain.Operator;
import com.datatrees.datacenter.resolver.schema.Schemas;
import com.github.shyiko.mysql.binlog.BinaryLogFileReader;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.ChecksumType;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.AbstractMap.SimpleEntry;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public final class BinlogFileReader implements Runnable {
    private EnumMap<EventType, EventConsumer<Event>> binlogEventConsumerHandlerHashMap;
    private static Logger logger = LoggerFactory.getLogger(BinlogFileReader.class);
    private EventListner<Map<Operator, AtomicLong>> consumer;
    private InputStream fileStream;
    private String identity;
    private String instanceId;
    private HashMap<Operator, AtomicLong> resultMap;
    private EventDeserializer eventDeserializer;
    private BinaryLogFileReader reader = null;
    private ConcurrentHashMap<Long, SimpleEntry> tableMapHashMap;
    private Schemas schemaUtility;
    /**
     * 分库
     */
    private Function<String, String> schemaNameMapper = t -> t;

    public BinlogFileReader() {
        resultMap = new HashMap<>(ImmutableMap.<Operator, AtomicLong>builder().
                put(Operator.C, new AtomicLong(0L)).
                put(Operator.U, new AtomicLong(0L)).
                put(Operator.D, new AtomicLong(0L)).
                put(Operator.DEFAULT, new AtomicLong(0L)).
                build());
        eventDeserializer = new EventDeserializer();
        tableMapHashMap = new ConcurrentHashMap<>();
        binlogEventConsumerHandlerHashMap = new EnumMap<>(EventType.class);
    }

    public BinlogFileReader(String identity,
                            String instanceId,
                            InputStream fileStream,
                            EventListner<Map<Operator, AtomicLong>> consumer
    ) throws IOException {
        this(identity, instanceId, fileStream, consumer, null);
    }

    public BinlogFileReader(String identity,
                            String instanceId,
                            InputStream fileStream,
                            EventListner<Map<Operator, AtomicLong>> consumer,
                            Function<String, String> schemaNameMapper
    ) throws IOException {
        this();
        this.fileStream = fileStream;

        this.identity = identity;
        this.instanceId = instanceId;
        this.consumer = consumer;
        this.eventDeserializer.setChecksumType(ChecksumType.CRC32);
        this.reader = new BinaryLogFileReader(this.fileStream, eventDeserializer);
        this.schemaUtility = new Schemas();
        this.binlogEventConsumerHandlerHashMap.put(EventType.TABLE_MAP, this::handleTableMap);
        this.binlogEventConsumerHandlerHashMap.put(EventType.WRITE_ROWS, this::handleWriteRow);
        this.binlogEventConsumerHandlerHashMap.put(EventType.EXT_WRITE_ROWS, this::handleWriteRow);
        this.binlogEventConsumerHandlerHashMap.put(EventType.UPDATE_ROWS, this::handleUpdateRow);
        this.binlogEventConsumerHandlerHashMap.put(EventType.EXT_UPDATE_ROWS, this::handleUpdateRow);
        this.binlogEventConsumerHandlerHashMap.put(EventType.DELETE_ROWS, this::handleDeleteRow);
        this.binlogEventConsumerHandlerHashMap.put(EventType.EXT_DELETE_ROWS, this::handleDeleteRow);
        if (schemaNameMapper != null) {
            this.schemaNameMapper = schemaNameMapper;
        }
    }

    public void read() {
        logger.info("Begin to analysis binlog file {} of instance", identity, instanceId);
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

    private void onSuceess() {
        this.consumer.onSuccess(resultMap);
    }

    private void handlerEvent() throws IOException {
        try {
            try {
                try {
                    for (Event event; (event = reader.readEvent()) != null; ) {
                        final EventHeader eventHeader = event.getHeader();
                        final EventType eventType = eventHeader.getEventType();
                        final Operator simpleEventType = Operator.valueOf(eventType);
                        EventConsumer<Event> eventBinlogEventConsumer =
                                binlogEventConsumerHandlerHashMap.getOrDefault(eventType,
                                        bufferRecord -> resultMap.get(simpleEventType).incrementAndGet());
                        eventBinlogEventConsumer.consume(event);
                    }
                } catch (IOException e) {

                    logger.error(e.getMessage(), e);
                }
            } catch (Exception e) {

                logger.error(e.getMessage(), e);
            }
        } finally {
            onSuceess();
        }
    }

    private void handleTableMap(Event event) {
        TableMapEventData tableMapEventData = unwrapData(event);
        long tableNumber = tableMapEventData.getTableId();
        String databaseName = schemaNameMapper.apply(tableMapEventData.getDatabase());
        String tableName = tableMapEventData.getTable();
        if (!databaseName.equalsIgnoreCase("mysql")) {
            tableMapHashMap.put(tableNumber, new SimpleEntry<>(databaseName, tableName));
        }
    }

    private void consumeBufferRecord(Operator operator, Long tableId, Serializable[] before, Serializable[] after) {
        SimpleEntry<String, String> simpleEntry = tableMapHashMap.get(tableId);
        if (simpleEntry != null) {
            Schema schema = schemaUtility.toAvroSchema(this.instanceId, simpleEntry.getKey(), simpleEntry.getValue());
            if (schema != null) {
                Object resultValue = schemaUtility.toAvroData(schema, operator, before, after);
                if (resultValue != null) {
                    try {
                        consumer.consume(schema, this.identity, operator, before, after, resultValue);
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
            consumeBufferRecord(Operator.C, writeRowsEventData.getTableId(), null, rows);
        } while (writeRows.hasNext());
    }

    private void handleUpdateRow(Event event) {
        UpdateRowsEventData updateRowsEventData = unwrapData(event);
        Iterator<Map.Entry<Serializable[], Serializable[]>> updateRows = updateRowsEventData.getRows().iterator();
        do {
            Map.Entry<Serializable[], Serializable[]> rows = updateRows.next();
            consumeBufferRecord(Operator.U, updateRowsEventData.getTableId(), rows.getKey(), rows.getValue());
        } while (updateRows.hasNext());
    }

    private void handleDeleteRow(Event event) {
        DeleteRowsEventData deleteRowsEventData = unwrapData(event);
        Iterator<Serializable[]> deleteRows = deleteRowsEventData.getRows().iterator();
        do {
            Serializable[] rows = deleteRows.next();
            consumeBufferRecord(Operator.D, deleteRowsEventData.getTableId(), rows, null);
        } while (deleteRows.hasNext());
    }

    @Override
    public void run() {
        try {
            logger.info("Begin to read binlog file {} of instance", this.identity, this.instanceId);
            handlerEvent();
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }
}
