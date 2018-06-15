package com.datatrees.datacenter.schema.service.repository;

import com.datatrees.datacenter.schema.api.SchemaRequest;
import com.datatrees.datacenter.schema.service.utils.SchemaConvertor;
import io.debezium.config.Configuration;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class SchemaRepository {

    private static HashMap<TableDefinition, Map<Long, TableSchema>> historyTableSchema = new HashMap<>();

    private SchemaBuilder builder;

    private static Logger logger = LoggerFactory.getLogger(SchemaRepository.class);

    public SchemaRepository() {
        this.builder = new SchemaBuilder(Configuration.create().build());
    }

    public void addSchema(com.datatrees.datacenter.schema.service.loader.HistoryRecord record) {
        Map<TableId, TableSchema> tableSchemas = builder.buildSchema(record);
        if (null == tableSchemas) {
            return;
        }
        for (Map.Entry<TableId, TableSchema> tableSchema : tableSchemas.entrySet()) {
            TableDefinition tableDefinition = new TableDefinition(tableSchema.getKey().catalog(), tableSchema.getKey().table());
            Long timeSec = record.getTimeSeconds();

            if (timeSec == null) {
                timeSec = 0l;
            }

            if (!historyTableSchema.containsKey(tableDefinition)) {
                historyTableSchema.put(tableDefinition, new TreeMap<>());
            }
            Map<Long, TableSchema> tableSchemaMap = historyTableSchema.get(tableDefinition);

            if (!tableSchemaMap.containsKey(timeSec)) {
                tableSchemaMap.put(timeSec, tableSchema.getValue());
            }

        }
    }

    public static TableSchema queryLatestSchema(SchemaRequest request) {

        TableSchema tableSchema = null;

        try {
            Map<Long, TableSchema> map = historyTableSchema.get(new TableDefinition(request.getDatabase(), request.getTable()));

            if (map == null || map.isEmpty()) {
                return null;
            }
            Long time = request.getTimeSec();
            Long backward = 0L;
            for (Long timeSec : map.keySet()) {
                if (timeSec > time) {
                    break;
                }
                if (timeSec < time && timeSec > backward) {
                    backward = timeSec;
                }
            }
            tableSchema = map.get(backward);
        } catch (Exception e) {
            logger.error("", e);
        }
        return tableSchema;
    }

    public static String queryAvroSchema(SchemaRequest request) {
        TableSchema tableSchema = queryLatestSchema(request);
        if (null == tableSchema){
            return null;
        }
        return SchemaConvertor.fromConnectSchema(tableSchema.valueSchema()).toString(true);
    }

}
