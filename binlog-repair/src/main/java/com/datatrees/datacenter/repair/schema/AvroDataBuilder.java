package com.datatrees.datacenter.repair.schema;

import com.alibaba.fastjson.JSONObject;
import com.datatrees.datacenter.table.FieldNameOp;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
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


    public static Map<String,Object> avroSchemaDataBuilder(InputStream inputStream, List<String> idList, String operateType) {
        DataFileStream<Object> reader;
        try {
            reader = new DataFileStream<>(inputStream, new GenericDatumReader<>());
            Schema schema = reader.getSchema();
            String name = schema.getName();
            String nameSpace = schema.getNamespace();
            Schema afterSchema = reader
                    .getSchema()
                    .getField(AFTER_TAG)
                    .schema()
                    .getTypes()
                    .get(1);
            LOG.info("origin after schema:" + afterSchema);

            Set<String> fieldNameSet = getFieldName(afterSchema);
            String idField = FieldNameOp.getFieldName(fieldNameSet, ID_COLUMN_LIST);
            if (idField != null && !NULL_STRING.equals(idField)) {
                Schema afterSchemaNew = SchemaConvertor.schemaFieldTypeConvert(afterSchema);
                LOG.info("redefine after schema:" + afterSchemaNew);
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
                        .name("Key")
                        .type(Schema.create(Schema.Type.LONG))
                        .noDefault().endRecord())
                        .noDefault();

                Schema finalSchema = fieldAssembler.endRecord();
                LOG.info("final schema:" + finalSchema);
                if (idList != null && idList.size() > 0) {
                    return getGenericRecords(reader, idField, finalSchema, idList, operateType);
                } else {
                    return getGenericRecords(reader, idField, finalSchema);
                }
            }
        } catch (IOException e) {
            LOG.info("can't not read data from avro file with error info :", e);
        }
        return null;
    }

    private static Map<String,Object> getGenericRecords(DataFileStream<Object> reader, String idField, Schema schema) {
        Iterator iterator = reader.iterator();
        GenericData.Record  dataRecord= new GenericData.Record(schema);
        List<GenericData.Record> genericRecordList = new ArrayList<>();
        while (iterator.hasNext()) {
            GenericRecord genericRecord = (GenericRecord) iterator.next();
            dataRecord.put(HIVE_AFTER_TAG, genericRecord.get(1));
            dataRecord.put(OP_TAG, genericRecord.get(2).toString().substring(0, 1).toLowerCase());
            dataRecord.put(KEY_TAG, idField);
            genericRecordList.add(dataRecord);
        }
        Map<String,Object> schemaListMap=new HashMap<>(1);
        schemaListMap.put("schema",schema);
        schemaListMap.put("record",genericRecordList);
        return schemaListMap;
    }

    private static Map<String,Object> getGenericRecords(DataFileStream<Object> reader, String idField, Schema schema, List<String> idList, String operateType) {
        Iterator iterator = reader.iterator();
        List<GenericData.Record> genericRecordList = new ArrayList<>();
        JSONObject jsonObject;
        GenericData.Record genericRecord;
        while (iterator.hasNext()) {
            GenericData.Record dataRecord = new GenericData.Record(schema);
            genericRecord = (GenericData.Record) iterator.next();
            String operate=genericRecord.get(2).toString();
            Object genericObj;
            if(operateType.equals(operate)) {
                if (null != genericRecord.get(1)) {
                    genericObj = genericRecord.get(1);
                } else {
                    genericObj = genericRecord.get(0);
                }
                jsonObject = JSONObject.parseObject(genericObj.toString());
                if (idList.contains(jsonObject.get(idField).toString())) {
                    dataRecord.put(HIVE_AFTER_TAG, genericObj);
                    dataRecord.put(OP_TAG, genericRecord.get(2).toString().substring(0, 1).toLowerCase());
                    Schema schema1=schema.getField("key").schema();
                    GenericData.Record record=new GenericData.Record(schema1);
                    record.put("Key",idField);
                    System.out.println("*******"+schema1);
                    dataRecord.put(KEY_TAG, record);
                    genericRecordList.add(dataRecord);
                    LOG.info(dataRecord.toString());
                }
            }
        }
        Map<String,Object> schemaListMap=new HashMap<>(1);
        schemaListMap.put("schema",schema);
        schemaListMap.put("record",genericRecordList);
        return schemaListMap;
    }

    private static Set<String> getFieldName(Schema schema) {
        List<Schema.Field> fields = schema.getFields();
        Set<String> fieldNameSet = new HashSet<>(fields.size());
        fields.forEach(x -> fieldNameSet.add(x.name()));
        return fieldNameSet;
    }

}
