package com.datatrees.datacenter.repair.schema;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * @author personalc
 */
class SchemaConverter {

    static Schema schemaFieldTypeConvert(Schema schema) {
        List<Schema.Field> fields = schema.getFields();
        SchemaBuilder.FieldAssembler<Schema> afterFieldAssembler = SchemaBuilder.record("Value").fields();
        Schema newSchema;
        String fieldName;
        Schema.Type type;
        for (Schema.Field field : fields) {
            fieldName = field.name().toLowerCase();
            type = field.schema().getType();
            switch (type) {
                case INT:
                    afterFieldAssembler.name(fieldName).type(Schema.create(Schema.Type.LONG)).withDefault(field.defaultVal());
                    break;
                case FLOAT:
                    afterFieldAssembler.name(fieldName).type(Schema.create(Schema.Type.DOUBLE)).withDefault(field.defaultVal());
                    break;
                case BYTES:
                    afterFieldAssembler.name(fieldName).type(Schema.create(Schema.Type.STRING)).withDefault(field.defaultVal());
                    break;
                case UNION:
                    List<Schema> typesNew = convertUnionSchemas(field);
                    afterFieldAssembler.name(fieldName).type(Schema.createUnion(typesNew)).withDefault(field.defaultVal());
                    break;
                default:
                    afterFieldAssembler.name(fieldName).type(field.schema()).withDefault(field.defaultVal());
            }
        }
        newSchema = afterFieldAssembler.endRecord();
        return newSchema;
    }

    private static List<Schema> convertUnionSchemas(Schema.Field field) {
        List<Schema> types = field.schema().getTypes();
        List<Schema> unionSchema = new ArrayList<>();
        Schema schema;
        for (Schema typeSchema : types) {
            String typeName = typeSchema.getType().getName();
            if (null != typeName && !"null".equalsIgnoreCase(typeName)) {
                switch (typeName) {
                    case "int":
                        schema = Schema.create(Schema.Type.LONG);
                        break;
                    case "float":
                        schema = Schema.create(Schema.Type.DOUBLE);
                        break;
                    case "bytes":
                        schema = Schema.create(Schema.Type.STRING);
                        break;
                    default:
                        schema = typeSchema;
                        break;
                }
                unionSchema.add(schema);
            } else {
                unionSchema.add(typeSchema);
            }

        }
        return unionSchema;
    }
}
