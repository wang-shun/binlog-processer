package com.datatrees.datacenter.repair.schema;

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
    private static Logger LOG = LoggerFactory.getLogger(SchemaConvertor.class);

    private static final List<String> ID_COLUMN_LIST = FieldNameOp.getConfigField("id");
    private static final String AFTER_TAG = "After";
    private static final String OP_TAG = "op";
    private static final String KEY_TAG = "key";
    private static final String HIVE_AFTER_TAG = "after";
    private static final String NULL_STRING = "null";


    private static List<GenericRecord> avroSchemaBuilder(InputStream inputStream) {
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

            Set<String> fieldNameSet = getStrings(schema);
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
                return getGenericRecords(reader, idField, finalSchema);
            }
        } catch (IOException e) {
            LOG.info("can't not read data from avro file with error info :", e);
        }
        return null;
    }

    private static List<GenericRecord> getGenericRecords(DataFileStream<Object> reader, String idField, Schema finalSchema) {
        Iterator iterator = reader.iterator();
        GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(finalSchema);
        List<GenericRecord> genericRecordList = new ArrayList<>();
        while (iterator.hasNext()) {
            GenericRecord genericRecord = (GenericRecord) iterator.next();
            genericRecordBuilder.set(HIVE_AFTER_TAG, genericRecord.get(1));
            genericRecordBuilder.set(OP_TAG, genericRecord.get(2).toString().substring(0, 1).toLowerCase());
            genericRecordBuilder.set(KEY_TAG, idField);
            GenericData.Record record = genericRecordBuilder.build();
            genericRecordList.add(record);
            System.out.println(record.toString());
        }
        return genericRecordList;
    }

    private static Set<String> getStrings(Schema schema) {
        List<Schema.Field> fields = schema.getFields();
        Set<String> fieldNameSet = new HashSet<>(fields.size());
        fields.forEach(x -> fieldNameSet.add(x.name()));
        return fieldNameSet;
    }

}
