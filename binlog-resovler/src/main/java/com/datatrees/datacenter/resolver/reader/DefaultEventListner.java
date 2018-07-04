package com.datatrees.datacenter.resolver.reader;

import com.datatrees.datacenter.core.domain.Operator;
import com.datatrees.datacenter.core.domain.Status;
import com.datatrees.datacenter.core.exception.BinlogException;
import com.datatrees.datacenter.core.storage.FileStorage;
import com.datatrees.datacenter.core.task.domain.Binlog;
import com.datatrees.datacenter.resolver.DBbiz;
import com.datatrees.datacenter.resolver.partition.PartitionWriterManager;
import com.datatrees.datacenter.resolver.partition.WriteResult;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultEventListner implements EventListner<Runnable> {

  private static Logger logger = LoggerFactory.getLogger(DefaultEventListner.class);
  private PartitionWriterManager manager;

  public DefaultEventListner(FileStorage storage, Binlog file) {
    this.manager = new PartitionWriterManager(storage, file);
  }

  @Override
  public void consume(Schema schema, Binlog binlog, Operator operator, Object result) {
    try {
      manager.write(schema, operator, result);
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      throw new BinlogException(
        String.format("error to write avro record because of %s", e.getMessage()), e
      );
    }
  }

  @Override
  public WriteResult result() {
    return null;
  }

  @Override
  public void onStart(Binlog binlog) {

  }

  @Override
  public void onFinish(Runnable r) {
    manager.close();
  }

  public static class InnerEventListner implements EventListner<Runnable> {

    private static Logger logger = LoggerFactory.getLogger(DefaultEventListner.class);
    protected Binlog binlog;
    private PartitionWriterManager manager;

    public InnerEventListner(FileStorage storage, Binlog file) {
      this.manager = new PartitionWriterManager(storage, file);
      this.binlog = file;
    }

    @Override
    public void consume(Schema schema, Binlog binlog, Operator operator, Object result) {
      try {
        manager.write(schema, operator, result);
      } catch (Exception e) {
        throw new BinlogException(
          String
            .format("error to write avro record because of %s for table %s", e.getMessage(),
              schema.getFullName()), Status.WRITERECORDFAILED, e);
      }
    }

    @Override
    public void onFinish(Runnable r) {//
      try {
        manager.close();
        r.run();
      } catch (Exception e) {
        throw e;
      }
    }

    @Override
    public WriteResult result() {
      return manager;
    }

    @Override
    public void onStart(Binlog binlog) {
      try {
        DBbiz.update(binlog.getIdentity1(), "start", Status.START);
      } catch (Exception e) {
        logger.error(e.getMessage(), e);
      }
    }
  }
}
