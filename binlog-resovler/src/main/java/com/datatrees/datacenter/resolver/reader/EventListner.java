package com.datatrees.datacenter.resolver.reader;

import com.datatrees.datacenter.core.task.domain.Binlog;
import com.datatrees.datacenter.resolver.domain.Operator;
import org.apache.avro.Schema;

public interface EventListner<Result> {

  void consume(Schema schema, Binlog binlog, Operator operator, Object result);

  void onFinish(Result result);

  void onStart(Binlog binlog);
}
