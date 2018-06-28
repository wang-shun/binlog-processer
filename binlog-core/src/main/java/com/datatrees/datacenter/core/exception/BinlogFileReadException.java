package com.datatrees.datacenter.core.exception;

public class BinlogFileReadException extends RuntimeException {


  public BinlogFileReadException(String msg) {
    super(msg);
  }

  public BinlogFileReadException(String msg, Exception e) {
    super(msg, e);
  }
}
