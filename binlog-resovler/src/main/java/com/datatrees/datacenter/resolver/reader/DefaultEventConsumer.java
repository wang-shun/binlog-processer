package com.datatrees.datacenter.resolver.reader;

import com.datatrees.datacenter.core.storage.FileStorage;
import com.datatrees.datacenter.resolver.domain.BufferRecord;
import com.datatrees.datacenter.resolver.domain.Operator;
import com.datatrees.datacenter.resolver.partition.PartitionWriterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class DefaultEventConsumer implements EventConsumer<BufferRecord, Map<Operator, AtomicLong>> {
    private PartitionWriterManager partitionWriterManager;

    private static Logger logger = LoggerFactory.getLogger(DefaultEventConsumer.class);

    public DefaultEventConsumer(FileStorage storage) {
        this.partitionWriterManager = new PartitionWriterManager(storage);
    }

    @Override
    public void consume(BufferRecord record) {
        try {
            partitionWriterManager.write(record);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void onSuccess(Map<Operator, AtomicLong> success) {
        partitionWriterManager.close();
    }
}
