package com.datatrees.datacenter.resolver.reader;

import com.datatrees.datacenter.core.task.domain.Binlog;
import com.datatrees.datacenter.core.domain.Operator;
import com.datatrees.datacenter.resolver.partition.WriteResult;
import org.apache.avro.Schema;

public interface EventListner<T> {

  void consume(Schema schema, Binlog binlog, Operator operator, Object result);

  void onFinish(T result);
//  void onFinish();

  void onStart(Binlog binlog);

  WriteResult result();
}
