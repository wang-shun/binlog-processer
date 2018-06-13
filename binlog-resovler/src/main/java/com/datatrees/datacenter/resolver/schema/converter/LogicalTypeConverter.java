package com.datatrees.datacenter.resolver.schema.converter;

import org.apache.avro.Schema;

public interface LogicalTypeConverter {

  Object convert(Schema schema, Object value);
}
