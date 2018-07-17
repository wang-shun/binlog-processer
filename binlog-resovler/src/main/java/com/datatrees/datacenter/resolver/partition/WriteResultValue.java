package com.datatrees.datacenter.resolver.partition;

import com.datatrees.datacenter.core.domain.Operator;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.StringUtils;

public class WriteResultValue {
  AtomicInteger insert;
  AtomicInteger update;
  AtomicInteger delete;

  public WriteResultValue() {
    this.insert = new AtomicInteger(0);
    this.update = new AtomicInteger(0);
    this.delete = new AtomicInteger(0);
  }

  public static WriteResultValue create() {
    return new WriteResultValue();
  }

  public void increment(Operator operator) {
    switch (operator) {
      case Update:
        this.getUpdate().incrementAndGet();
        break;
      case Create:
        this.getInsert().incrementAndGet();
        break;
      case Delete:
        this.getDelete().incrementAndGet();
        break;
      default:
        break;
    }
  }

  public AtomicInteger getInsert() {
    return insert;
  }

  public void setInsert(AtomicInteger insert) {
    this.insert = insert;
  }

  public AtomicInteger getUpdate() {
    return update;
  }

  public void setUpdate(AtomicInteger update) {
    this.update = update;
  }

  public AtomicInteger getDelete() {
    return delete;
  }

  public void setDelete(AtomicInteger delete) {
    this.delete = delete;
  }
}
