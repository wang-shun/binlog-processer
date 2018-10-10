package com.datatrees.datacenter.repair.schema;

import com.alibaba.fastjson.JSONObject;
import com.datatrees.datacenter.table.FieldNameOp;
import com.sun.tools.javah.Gen;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

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
                        return getGenericRecords(reader, schema);
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

    private static Map<String, Object> getGenericRecords(DataFileStream<Object> reader, Schema schema) {
        JSONObject jsonObject;
        GenericData.Record genericRecord;
        Iterator iterator = reader.iterator();
        Map<String, List<GenericData.Record>> operateRecordMap = new HashMap<>(3);
        Map<Schema, Map<String, List<GenericData.Record>>> schemaRecordMap = new HashMap<>(1);
        List<GenericData.Record> createRecord = new ArrayList<>();
        List<GenericData.Record> updateRecord = new ArrayList<>();
        List<GenericData.Record> deleteRecord = new ArrayList<>();
        while (iterator.hasNext()) {
            GenericData.Record dataRecord = new GenericData.Record(schema);
            genericRecord = (GenericData.Record) iterator.next();
            String operate = genericRecord.get(2).toString();
            Object genericObj;
            if (null != genericRecord.get(1)) {
                genericObj = genericRecord.get(1);
            } else {
                genericObj = genericRecord.get(0);
            }

            jsonObject = JSONObject.parseObject(genericObj.toString());
            List<Schema.Field> fieldList = genericRecord.getSchema().getField(AFTER_TAG).schema().getTypes().get(1).getFields();
            jsonObject = jsonDataTypeConvert(jsonObject, fieldList);

            GenericData.Record record = getRecord(schema, jsonObject);
            String id = jsonObject.get(PRIMARY_KEY).toString();

            dataRecord.put(HIVE_AFTER_TAG, record);
            dataRecord.put(OP_TAG, genericRecord.get(2).toString().substring(0, 1).toLowerCase());
            Schema schema1 = schema.getField(KEY_TAG).schema();
            GenericData.Record keyRecord = new GenericData.Record(schema1);
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
        JSONObject jsonObject;
        GenericData.Record genericRecord;
        Iterator iterator = reader.iterator();
        while (iterator.hasNext()) {
            GenericData.Record dataRecord = new GenericData.Record(schema);
            genericRecord = (GenericData.Record) iterator.next();
            String operate = genericRecord.get(2).toString();
            Object genericObj;
            if (operateType.equals(operate)) {
                if (null != genericRecord.get(1)) {
                    genericObj = genericRecord.get(1);
                } else {
                    genericObj = genericRecord.get(0);
                }
                jsonObject = JSONObject.parseObject(genericObj.toString());
                List<Schema.Field> fieldList = genericRecord.getSchema().getField(AFTER_TAG).schema().getTypes().get(1).getFields();
                jsonObject = jsonDataTypeConvert(jsonObject, fieldList);

                GenericData.Record record = getRecord(schema, jsonObject);
                String id = jsonObject.get(PRIMARY_KEY).toString();
                if (idList.contains(id)) {
                    dataRecord.put(HIVE_AFTER_TAG, record);
                    dataRecord.put(OP_TAG, genericRecord.get(2).toString().substring(0, 1).toLowerCase());
                    Schema schema1 = schema.getField(KEY_TAG).schema();
                    GenericData.Record keyRecord = new GenericData.Record(schema1);
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

    private static GenericData.Record getRecord(Schema schema, JSONObject jsonObject) {
        Schema afterSchema = schema.getField(HIVE_AFTER_TAG).schema();
        GenericRecordBuilder builder = new GenericRecordBuilder(afterSchema);
        List<Schema.Field> afterFieldList = afterSchema.getFields();
        for (Schema.Field field : afterFieldList) {
            builder.set(field.name(), jsonObject.get(field.name()));
        }
        return builder.build();
    }

    private static JSONObject jsonDataTypeConvert(JSONObject jsonObject, List<Schema.Field> fieldList) {
        for (Schema.Field field : fieldList) {
            String fieldName = field.name().toLowerCase();
            Schema.Type fieldType = field.schema().getType();
            Schema.Type lastFieldType;
            if (AVRO_UNION_TYPE.equalsIgnoreCase(fieldType.getName())) {
                lastFieldType = field.schema().getTypes().get(1).getType();
            } else {
                lastFieldType = fieldType;
            }
            switch (lastFieldType) {
                case INT:
                    jsonObject.put(fieldName, (long) jsonObject.getIntValue(field.name()));
                    break;
                case FLOAT:
                    jsonObject.put(fieldName, (double) jsonObject.getFloatValue(field.name()));
                    break;
                case BYTES:
                    jsonObject.put(fieldName, Arrays.toString(jsonObject.getBytes(field.name())));
                    break;
                default:
                    jsonObject.put(fieldName, jsonObject.get(field.name()));
                    break;
            }
        }
        return jsonObject;
    }

    private static Set<String> getFieldName(Schema schema) {
        List<Schema.Field> fields = schema.getFields();
        Set<String> fieldNameSet = new HashSet<>(fields.size());
        fields.forEach(x -> fieldNameSet.add(x.name()));
        return fieldNameSet;
    }
}
