package com.datatrees.datacenter.resolver.reader;

import com.datatrees.datacenter.core.storage.FileStorage;
import com.datatrees.datacenter.resolver.domain.Operator;
import com.datatrees.datacenter.resolver.partition.PartitionWriterManager;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class DefaultEventListner implements EventListner<Map<Operator, AtomicLong>> {
    private PartitionWriterManager partitionWriterManager;

    private static Logger logger = LoggerFactory.getLogger(DefaultEventListner.class);

    public DefaultEventListner(FileStorage storage) {
        this.partitionWriterManager = new PartitionWriterManager(storage);
    }

    @Override
    public void consume(Schema schema, String identity, Operator operator, Serializable[] before, Serializable[] after, Object result) {
        try {
            partitionWriterManager.write(schema, identity, operator, before, after, result);
        } catch (IOException e) {

            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void onSuccess(Map<Operator, AtomicLong> success) {
        partitionWriterManager.close();
        logger.info("Result:",success);
    }
}
