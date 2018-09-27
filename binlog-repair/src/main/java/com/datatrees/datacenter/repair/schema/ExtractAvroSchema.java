package com.datatrees.datacenter.repair.schema;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public class ExtractAvroSchema {
    /**
     * 获取数据字段
     * @param genericRecord
     * @return
     */
    private Schema getRecordSchema(GenericRecord genericRecord) {
        return genericRecord.getSchema();
    }
}
