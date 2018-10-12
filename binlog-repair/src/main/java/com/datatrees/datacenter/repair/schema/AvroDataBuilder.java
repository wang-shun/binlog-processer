package com.datatrees.datacenter.repair.schema;

import com.alibaba.fastjson.JSONObject;
import com.datatrees.datacenter.table.FieldNameOp;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecordBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * @author personalc
 */
public class AvroDataBuilder {
    private static Logger LOG = LoggerFactory.getLogger(AvroDataBuilder.class);

    private static final List<String> ID_COLUMN_LIST = FieldNameOp.getConfigField("id");
    private static final String AFTER_TAG = "After";
    private static final String OP_TAG = "op";
    private static final String KEY_TAG = "key";
    private static final String HIVE_AFTER_TAG = "after";
    private static final String NULL_STRING = "null";
    private static final String PRIMARY_KEY = "id";
    private static final String AVRO_UNION_TYPE = "union";


    public static Map<String, Object> avroSchemaDataBuilder(InputStream inputStream, List<String> idList, String operateType) {
        DataFileStream<Object> reader;
        if (null != inputStream) {
            try {
                reader = new DataFileStream<>(inputStream, new GenericDatumReader<>());
                Schema schema = schemaBuilder(reader);
                if (idList != null && idList.size() > 0) {
                    return getGenericRecords(reader, schema, idList, operateType);
                } else {
                    if (operateType == null) {
                        return getGenericRecords(reader, schema, operateType);
                    }
                }
            } catch (IOException e) {
                LOG.info("the avro inputStream is null " + e.getMessage());
            }
        }
        return null;
    }

    private static Schema schemaBuilder(DataFileStream reader) {
        if (reader != null) {
            Schema schema = reader.getSchema();
            String name = schema.getName();
            String nameSpace = schema.getNamespace();
            Schema afterSchema = reader
                    .getSchema()
                    .getField(AFTER_TAG)
                    .schema()
                    .getTypes()
                    .get(1);
            LOG.info("origin after schema :" + afterSchema);
            Set<String> fieldNameSet = getFieldName(afterSchema);
            String idField = FieldNameOp.getFieldName(fieldNameSet, ID_COLUMN_LIST);
            if (idField != null && !NULL_STRING.equals(idField)) {
                Schema afterSchemaNew = SchemaConverter.schemaFieldTypeConvert(afterSchema);
                LOG.info("schema after redefined :" + afterSchemaNew);
                Schema opSchema = reader
                        .getSchema()
                        .getField(OP_TAG)
                        .schema();

                SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder
                        .record(name)
                        .namespace(nameSpace)
                        .fields();

                fieldAssembler.name(HIVE_AFTER_TAG).type(afterSchemaNew).noDefault();
                fieldAssembler.name(OP_TAG).type(opSchema).noDefault();
                fieldAssembler.name(KEY_TAG).type(SchemaBuilder
                        .record(KEY_TAG)
                        .namespace(name)
                        .fields()
                        .name(idField.toLowerCase())
                        .type(Schema.create(Schema.Type.LONG))
                        .noDefault().endRecord())
                        .noDefault();
                Schema finalSchema = fieldAssembler.endRecord();
                LOG.info("final schema:" + finalSchema);
                return finalSchema;
            }
        } else {
            LOG.info("the DataFileStream is null");
        }
        return null;
    }

    private static Map<String, Object> getGenericRecords(DataFileStream<Object> reader, Schema schema, String operateType) {
        GenericData.Record genericRecord;
        Iterator iterator = reader.iterator();
        Schema oldSchema = reader.getSchema().getField(AFTER_TAG).schema().getTypes().get(1);
        //List<Schema.Field> fieldList = reader.getSchema().getField(AFTER_TAG).schema().getTypes().get(1).getFields();
        Map<String, List<GenericData.Record>> operateRecordMap = new HashMap<>(3);
        List<GenericData.Record> createRecord = new ArrayList<>();
        List<GenericData.Record> updateRecord = new ArrayList<>();
        List<GenericData.Record> deleteRecord = new ArrayList<>();
        if (operateType == null || "".equals(operateType)) {
            while (iterator.hasNext()) {
                GenericData.Record dataRecord = new GenericData.Record(schema);
                genericRecord = (GenericData.Record) iterator.next();
                String operate = genericRecord.get(2).toString();
                GenericData.Record genericObj;
                if (null != genericRecord.get(1)) {
                    genericObj = (GenericData.Record) genericRecord.get(1);
                } else {
                    genericObj = (GenericData.Record) genericRecord.get(0);
                }
                GenericData.Record record = recordConvert(oldSchema, schema, genericObj);
                String id = genericObj.get("Id").toString();
                dataRecord.put(HIVE_AFTER_TAG, record);
                dataRecord.put(OP_TAG, genericRecord.get(2).toString().substring(0, 1).toLowerCase());
                Schema keySchema = schema.getField(KEY_TAG).schema();
                GenericData.Record keyRecord = new GenericData.Record(keySchema);
                keyRecord.put(PRIMARY_KEY.toLowerCase(), Long.valueOf(id));
                dataRecord.put(KEY_TAG, keyRecord);
                if ("Create".equals(operate)) {
                    createRecord.add(dataRecord);
                }
                if ("Update".equals(operate)) {
                    updateRecord.add(dataRecord);
                }
                if ("Delete".equals(operate)) {
                    deleteRecord.add(dataRecord);
                }
            }
        } else {
            while (iterator.hasNext()) {
                GenericData.Record dataRecord = new GenericData.Record(schema);
                genericRecord = (GenericData.Record) iterator.next();
                String operate = genericRecord.get(2).toString();
                if (operateType.equalsIgnoreCase(operate)) {
                    GenericData.Record genericObj;
                    if (null != genericRecord.get(1)) {
                        genericObj = (GenericData.Record) genericRecord.get(1);
                    } else {
                        genericObj = (GenericData.Record) genericRecord.get(0);
                    }
                    GenericData.Record record = recordConvert(oldSchema, schema, genericObj);
                    String id = genericObj.get("Id").toString();
                    dataRecord.put(HIVE_AFTER_TAG, record);
                    dataRecord.put(OP_TAG, genericRecord.get(2).toString().substring(0, 1).toLowerCase());
                    Schema keySchema = schema.getField(KEY_TAG).schema();
                    GenericData.Record keyRecord = new GenericData.Record(keySchema);
                    keyRecord.put(PRIMARY_KEY.toLowerCase(), Long.valueOf(id));
                    dataRecord.put(KEY_TAG, keyRecord);
                    LOG.info(dataRecord.toString());
                    if ("Create".equals(operate)) {
                        createRecord.add(dataRecord);
                    }
                    if ("Update".equals(operate)) {
                        updateRecord.add(dataRecord);
                    }
                    if ("Delete".equals(operate)) {
                        deleteRecord.add(dataRecord);
                    }
                }
            }
        }
        operateRecordMap.put("Create", createRecord);
        operateRecordMap.put("Update", updateRecord);
        operateRecordMap.put("Delete", deleteRecord);
        Map<String, Object> schemaListMap = new HashMap<>(1);
        schemaListMap.put("schema", schema);
        schemaListMap.put("record", operateRecordMap);
        return schemaListMap;
    }

    private static Map<String, Object> getGenericRecords(DataFileStream<Object> reader, Schema schema, List<String> idList, String operateType) {
        List<GenericData.Record> genericRecordList = new ArrayList<>();
        GenericData.Record genericRecord;
        Schema oldSchema = reader.getSchema().getField(AFTER_TAG).schema().getTypes().get(1);
        Iterator iterator = reader.iterator();
        while (iterator.hasNext()) {
            GenericData.Record dataRecord = new GenericData.Record(schema);
            genericRecord = (GenericData.Record) iterator.next();
            String operate = genericRecord.get(2).toString();
            GenericData.Record genericObj;
            if (operateType.equals(operate)) {
                if (null != genericRecord.get(1)) {
                    genericObj = (GenericData.Record) genericRecord.get(1);
                } else {
                    genericObj = (GenericData.Record) genericRecord.get(0);
                }
                GenericData.Record record = recordConvert(oldSchema, schema, genericObj);
                // TODO: 2018/10/11  
                String id = genericObj.get("Id").toString();
                if (idList.contains(id)) {
                    dataRecord.put(HIVE_AFTER_TAG, record);
                    dataRecord.put(OP_TAG, genericRecord.get(2).toString().substring(0, 1).toLowerCase());
                    Schema keySchema = schema.getField(KEY_TAG).schema();
                    GenericData.Record keyRecord = new GenericData.Record(keySchema);
                    keyRecord.put(PRIMARY_KEY.toLowerCase(), Long.valueOf(id));
                    dataRecord.put(KEY_TAG, keyRecord);
                    genericRecordList.add(dataRecord);
                    LOG.info(dataRecord.toString());
                }
            }
        }
        Map<String, Object> schemaListMap = new HashMap<>(1);
        schemaListMap.put("schema", schema);
        schemaListMap.put("record", genericRecordList);
        return schemaListMap;
    }

    private static GenericData.Record recordConvert(Schema oldSchema, Schema newSchema, GenericData.Record record) {
        GenericRecordBuilder builder = new GenericRecordBuilder(newSchema.getField(HIVE_AFTER_TAG).schema());
        List<Schema.Field> afterFieldList = oldSchema.getFields();
        for (Schema.Field field : afterFieldList) {
            Schema.Type fieldType = field.schema().getType();
            Schema.Type lastFieldType;
            if (AVRO_UNION_TYPE.equalsIgnoreCase(fieldType.getName())) {
                lastFieldType = field.schema().getTypes().get(1).getType();
            } else {
                lastFieldType = fieldType;
            }
            Object obj = record.get(field.name());
            switch (lastFieldType) {
                case INT:
                    if (null != obj) {
                        builder.set(field.name().toLowerCase(), Long.valueOf((Integer) obj));
                    } else {
                        builder.set(field.name().toLowerCase(), 0L);
                    }
                    break;
                case FLOAT:
                    if (null != obj) {
                        builder.set(field.name().toLowerCase(), Double.valueOf((Float) obj));
                    } else {
                        builder.set(field.name().toLowerCase(), 0);
                    }
                    break;
                case BYTES:
                    if (null != obj) {
                        builder.set(field.name().toLowerCase(), String.valueOf(record.get(field.name())));
                    } else {
                        builder.set(field.name().toLowerCase(), null);
                    }
                    break;
                default:
                    if (null != obj) {
                        builder.set(field.name().toLowerCase(), record.get(field.name()));
                    } else {
                        builder.set(field.name().toLowerCase(), null);
                    }
                    break;
            }
        }
        GenericData.Record lastRecord = builder.build();
        return lastRecord;
    }

    private static Set<String> getFieldName(Schema schema) {
        List<Schema.Field> fields = schema.getFields();
        Set<String> fieldNameSet = new HashSet<>(fields.size());
        fields.forEach(x -> fieldNameSet.add(x.name()));
        return fieldNameSet;
    }
}
