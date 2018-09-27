package com.datatrees.datacenter.repair.schema;

import org.apache.avro.Schema;

import java.util.List;

public class SchemaConvertor {

    private Schema schemaFieldTypeConvert(Schema schema) {
        List<Schema.Field> fieldList = schema.getFields();
        for (Schema.Field field : fieldList) {

        }
        return null;
    }
}
