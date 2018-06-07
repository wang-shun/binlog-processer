package com.datatrees.datacenter.resolver.reader;

import com.datatrees.datacenter.core.exception.BinlogException;
import com.datatrees.datacenter.core.storage.FileStorage;
import com.datatrees.datacenter.core.task.domain.Binlog;
import com.datatrees.datacenter.resolver.DBbiz;
import com.datatrees.datacenter.resolver.domain.Operator;
import com.datatrees.datacenter.resolver.domain.Status;
import com.datatrees.datacenter.resolver.partition.PartitionWriterManager;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class DefaultEventListner implements EventListner<Map<Operator, AtomicLong>> {
    private PartitionWriterManager manager;

    private static Logger logger = LoggerFactory.getLogger(DefaultEventListner.class);

    public DefaultEventListner(FileStorage storage) {
        this.manager = new PartitionWriterManager(storage);
    }

    @Override
    public void consume(Schema schema, String identity, Operator operator, Object result) {
        try {
            manager.write(schema, identity, operator, result);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new BinlogException(String.format("error to write avro record because of %s", e.getMessage()));
        }
    }

    @Override
    public void onStart(Binlog binlog) {

    }

    @Override
    public void onFinish(Map<Operator, AtomicLong> success) {
        manager.close();
    }

    public static class InnerEventListner implements EventListner<AbstractMap.SimpleEntry<AbstractMap.SimpleEntry<Binlog, Status>, Map<Operator, AtomicLong>>> {
        private PartitionWriterManager manager;
        private static Logger logger = LoggerFactory.getLogger(DefaultEventListner.class);

        public InnerEventListner(FileStorage storage) {
            this.manager = new PartitionWriterManager(storage);
        }

        @Override
        public void consume(Schema schema, String identity, Operator operator, Object result) {
            try {
                manager.write(schema, identity, operator, result);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                throw new BinlogException(String.format("error to write avro record because of %s", e.getMessage()));
            }
        }

        @Override
        public void onFinish(AbstractMap.SimpleEntry<AbstractMap.SimpleEntry<Binlog, Status>, Map<Operator, AtomicLong>> value) {//
            AbstractMap.SimpleEntry<Binlog, Status> simpleEntry = value.getKey();
            String identityFile = simpleEntry.getKey().getIdentity();
            try {
                DBbiz.update(identityFile, value.getValue().toString(), simpleEntry.getValue());
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            } finally {
                try {
                    manager.close();
                } finally {
                    log(value.getValue());
                }
            }
        }

        void log(Map<Operator, AtomicLong> value) {
            logger.info("------------------------------------------------------");
            logger.info("totally process record of following:{}", value);
            logger.info("------------------------------------------------------");
        }

        @Override
        public void onStart(Binlog binlog) {
            try {
                DBbiz.update(binlog.getIdentity(), null);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }
}
