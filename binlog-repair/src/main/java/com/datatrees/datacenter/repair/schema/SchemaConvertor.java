package com.datatrees.datacenter.repair.schema;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.util.List;

public class SchemaConvertor {

    public static Schema schemaFieldTypeConvert(Schema schema) {
        List<Schema.Field> fields = schema.getFields();
        SchemaBuilder.FieldAssembler<Schema> afterFieldAssembler = SchemaBuilder.record("Value").fields();
        Schema newSchema;
        for (Schema.Field field : fields) {
            String fieldName = field.name();
            Schema.Type type = field.schema().getType();
            switch (type) {
                case INT:
                    afterFieldAssembler.name(fieldName).type(Schema.create(Schema.Type.LONG)).noDefault();
                    break;
                case FLOAT:
                    afterFieldAssembler.name(fieldName).type(Schema.create(Schema.Type.DOUBLE)).noDefault();
                    break;
                case BYTES:
                    afterFieldAssembler.name(fieldName).type(Schema.create(Schema.Type.STRING)).noDefault();
                    break;
                default:
                    afterFieldAssembler.name(fieldName).type(field.schema()).noDefault();
            }
        }
        newSchema = afterFieldAssembler.endRecord();
        return newSchema;
    }
}
