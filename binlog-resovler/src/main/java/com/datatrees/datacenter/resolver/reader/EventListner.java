package com.datatrees.datacenter.resolver.reader;

import com.datatrees.datacenter.resolver.domain.Operator;
import org.apache.avro.Schema;

import java.io.Serializable;

public interface EventListner<Result> {
    void consume(Schema schema, String identity, Operator operator, Serializable[] before,
                 Serializable[] after, Object result);

    void onSuccess(Result result);
}
