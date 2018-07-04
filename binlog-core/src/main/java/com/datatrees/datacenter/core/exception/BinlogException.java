package com.datatrees.datacenter.core.exception;

import com.datatrees.datacenter.core.domain.Status;

public class BinlogException extends RuntimeException {

  Status status;

  public BinlogException(String msg, Exception e) {
    this(msg, Status.OTHER, e);
  }

  public BinlogException(String msg, Status status) {
    super(msg);
    this.status = status;
  }

  public BinlogException(String msg, Status status, Exception e) {
    super(msg, e);
    this.status = status;
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(Status status) {
    this.status = status;
  }
}

