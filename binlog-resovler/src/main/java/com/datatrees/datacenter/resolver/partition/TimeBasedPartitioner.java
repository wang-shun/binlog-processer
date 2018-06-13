package com.datatrees.datacenter.resolver.partition;

import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.joda.time.DateTime;

public abstract class TimeBasedPartitioner implements Partitioner {

  protected static final String PARTITIONER_CONSTANCE = "binlog.properties";

  @Override
  public String encodePartition(GenericData.Record record) {
    String column = filterPartitionColumn(record.getSchema());
    if (null != column) {
      Object value = record.get(column);
      if (null != value) {
        DateTime dateTime = null;
        if (value instanceof Timestamp) {
          dateTime = new DateTime(((Timestamp) value).getTime());
        } else if (value instanceof Date) {
          dateTime = new DateTime(((Date) value).getTime());
        } else if (value instanceof Long) {
          dateTime = new DateTime((Long) value);
        }
        return dateTime == null ? null :
          String.format("year=%d/month=%d/day=%d", dateTime.getYear(), dateTime.getMonthOfYear(),
            dateTime.getDayOfMonth());

      } else {
        return null;
      }
    } else {
      return null;
    }
  }

  protected String filterPartitionColumn(Schema schema) {
    String column = null;

    List<Schema.Field> fieldList = schema.getFields();
    for (Schema.Field field : fieldList) {
      if (partitionColumns().stream().anyMatch(s -> s.equalsIgnoreCase(field.name()))) {
        column = field.name();
      }
    }
    return column;
  }

  protected abstract List<String> partitionColumns();

  @Override
  public abstract String getRoot();
}
