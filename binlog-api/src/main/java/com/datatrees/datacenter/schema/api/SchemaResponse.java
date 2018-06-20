package com.datatrees.datacenter.schema.api;

import org.apache.avro.Schema;

public class SchemaResponse {

    //返回码
    private ResultCode resultCode;
    //schema
    private String schema;

    public SchemaResponse() {

    }

    public Schema getAvroSchema() {
        if (null == schema || schema.isEmpty()) {
            return null;
        } else {
            return new Schema.Parser().parse(schema);
        }
    }

    public ResultCode getResultCode() {
        return resultCode;
    }

    public void setResultCode(ResultCode resultCode) {
        this.resultCode = resultCode;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    @Override
    public String toString() {
        return "SchemaResponse{" +
                "resultCode=" + resultCode +
                ", schema='" + schema + '\'' +
                '}';
    }
}
