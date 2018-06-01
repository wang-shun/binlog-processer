package com.datatrees.datacenter.resolver.schema;

import com.datatrees.datacenter.resolver.domain.BufferRecord;
import com.datatrees.datacenter.resolver.domain.Operator;
import com.datatrees.datacenter.resolver.schema.converter.LogicalTypeConverter;
import com.github.shyiko.mysql.binlog.event.EventType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.javatuples.KeyValue;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

public final class Schemas {
    private static final HashMap<String, LogicalTypeConverter> TO_CONNECT_LOGICAL_CONVERTERS
            = new HashMap<>();

    private static final HashMap<Class<?>, LogicalTypeConverter> TO_CONNECT_CLASS_CONVERTERS = new HashMap<>();

    static {
        TO_CONNECT_CLASS_CONVERTERS.put(BigDecimal.class, new LogicalTypeConverter() {
            @Override
            public Object convert(Schema schema, Object value) {
                return ((BigDecimal) value).doubleValue();
            }
        });
    }

    static {
        TO_CONNECT_LOGICAL_CONVERTERS.put("decimal", (schema, value) -> {
            if (value == null)
                return null;
            if (!(value instanceof BigDecimal))
                return null;
            return ((BigDecimal) value).doubleValue();
        });

        TO_CONNECT_LOGICAL_CONVERTERS.put("date-millis", (schema, value) -> {
            if (value == null)
                return null;
            if (!(value instanceof Date))
                return null;
            return ((Date) value).getTime();
        });

        TO_CONNECT_LOGICAL_CONVERTERS.put("timestamp-millis", (schema, value) -> {
            if (value == null)
                return null;
            if (!(value instanceof Timestamp))
                return null;
            return ((Timestamp) value).getTime();
        });

        TO_CONNECT_LOGICAL_CONVERTERS.put("logical-byte", (schema, value) -> {
            if (value == null)
                return null;
            ByteBuffer bytesValue = value instanceof byte[] ? ByteBuffer.wrap((byte[]) value) :
                    (ByteBuffer) value;
            return bytesValue;
        });
    }

    private ConcurrentHashMap<String, Schema> cachedAvroSchema;

    private static final String RESULT_VALUE_TAG = "Value";
    private static final String RESULT_BEFORE_TAG = "Before";
    private static final String RESULT_AFTER_TAG = "After";
    private static final String RESULT_ENVELOP_TAG = "Envelop";
    private static final String RESULT_OP_TAG = "op";

    public Schemas() {
        cachedAvroSchema = new ConcurrentHashMap<>();
    }

    private Object fromLogicalValue(Schema schema, Object logicalValue) {
        Object value = logicalValue;
        String logicalType = fromLogicalType(schema);
        if (schema != null && logicalType != null) {
            LogicalTypeConverter logicalConverter = TO_CONNECT_LOGICAL_CONVERTERS.get(logicalType);
            if (logicalConverter != null && logicalValue != null) {
                value = logicalConverter.convert(schema, logicalValue);
            }
        } else {
            if (logicalValue != null) {
                LogicalTypeConverter classConverter = TO_CONNECT_CLASS_CONVERTERS.get(logicalValue.getClass());
                if (classConverter != null) {
                    value = classConverter.convert(schema, logicalValue);
                }
            }
        }
        return value;
    }

    private String fromLogicalType(Schema schema) {
        Schema logicalSchema = avroSchemaForUnderlyingTypeIfOptional(schema);
        if (logicalSchema.getObjectProps() != null) {
            Object logicalObject = logicalSchema.getObjectProp("logicalType");
            if (logicalObject != null) {
                return logicalObject.toString();
            }
        }
        return null;
    }

    public Object toAvroData(Schema schema, BufferRecord bufferRecord) {
        Schema recordUnionSchema = schema.getField(RESULT_BEFORE_TAG).schema();
        Schema recordSchema = avroSchemaForUnderlyingTypeIfOptional(recordUnionSchema);

        GenericRecordBuilder envelopBuilder = new GenericRecordBuilder(schema);
        if (bufferRecord.getBeforeValue() != null) {
            GenericRecordBuilder beforeValueBuilder = new GenericRecordBuilder(recordSchema);
            List<Schema.Field> fields = recordSchema.getFields();
            for (Integer index = 0; index <= fields.size() - 1; index++) {
                Schema.Field field = indexForColumns(fields, index);
                Object logicalValue = null;
                if (index < bufferRecord.getBeforeValue().length) {
                    Object value = bufferRecord.getBeforeValue()[index];
                    logicalValue = fromLogicalValue(field.schema(), value);
                }
                if (field != null) {
                    beforeValueBuilder.set(field.name(), logicalValue);
                }
            }
            envelopBuilder.set(RESULT_BEFORE_TAG, beforeValueBuilder.build());
        }

        if (bufferRecord.getAfterValue() != null) {
            GenericRecordBuilder afterValueBuilder = new GenericRecordBuilder(recordSchema);
            List<Schema.Field> fields = recordSchema.getFields();
            for (Integer index = 0; index <= fields.size() - 1; index++) {
                Schema.Field field = indexForColumns(fields, index);
                Object logicalValue = null;
                if (index < bufferRecord.getAfterValue().length) {
                    Object value = bufferRecord.getAfterValue()[index];
                    logicalValue = fromLogicalValue(field.schema(), value);
                }
                if (field != null) {
                    afterValueBuilder.set(field.name(), logicalValue);
                }
            }
            envelopBuilder.set(RESULT_AFTER_TAG, afterValueBuilder.build());
        }
        GenericData.Record finalRecord = envelopBuilder.set(RESULT_OP_TAG, bufferRecord.getOperator().toString()).build();
        return finalRecord;
    }

    public Schema toAvroSchema(String instanceId, String schema, String table) {
        requireNonNull(schema, "No schema provided");
        requireNonNull(table, "No table provided");

        String key = schema + "." + table;
        Schema cached = cachedAvroSchema.get(key);
        if (cached != null) {
            return cached;
        }

        KeyValue<String, String> avroSchema = Providers.schema(instanceId, schema, table);
        if (avroSchema == null || avroSchema.getValue() == null) {
            return null;
        }

        List<Schema.Field> originalFieldList = new ArrayList<>();

        for (Schema.Field field :
                new Schema.Parser().parse(avroSchema.getValue()).getFields()) {
            originalFieldList.add(new Schema.Field(field.name(), field.schema(), null, null));
        }

        requireNonNull(originalFieldList, "fields is null");

        List<Schema> unionSchemas = new ArrayList<>();
        Schema fieldSchema = Schema.
                createRecord(RESULT_VALUE_TAG, null, null, false, originalFieldList);

        unionSchemas.add(SchemaBuilder.builder().nullType());
        unionSchemas.add(fieldSchema);

        Schema unionSchema = Schema.createUnion(unionSchemas);
        Schema envelopSchema = SchemaBuilder.record(avroSchema.getKey() + "." + table).fields().
                name(RESULT_BEFORE_TAG).type(unionSchema).withDefault(null).
                name(RESULT_AFTER_TAG).type(unionSchema).withDefault(null).
                name(RESULT_OP_TAG).type(SchemaBuilder.builder().stringType()).noDefault().
                endRecord();

        cachedAvroSchema.put(key, envelopSchema);
        return envelopSchema;
    }

    public Schema toAvroSchema(String schema, String table) {
        return toAvroSchema(null, schema, table);
    }

    private Schema.Field indexForColumns(List<Schema.Field> fields, int index) {
        return index >= fields.size() ? null : fields.get(index);
    }

    private Schema avroSchemaForUnderlyingTypeIfOptional(Schema avroSchema) {
        if (avroSchema.getType() == Schema.Type.UNION) {
            for (Schema typeSchema : avroSchema
                    .getTypes()) {
                if (!typeSchema.getType().equals(
                        Schema.Type.NULL)) {
                    return typeSchema;
                }
            }
        }
        return avroSchema;
    }
}
