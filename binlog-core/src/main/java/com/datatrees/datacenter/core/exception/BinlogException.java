package com.datatrees.datacenter.core.exception;

public class BinlogException extends RuntimeException {


  public BinlogException(String msg) {
    super(msg);
  }

  public BinlogException(String msg, Exception e) {
    super(msg, e);
  }
}
