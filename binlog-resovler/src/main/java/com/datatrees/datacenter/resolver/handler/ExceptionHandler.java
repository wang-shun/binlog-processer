package com.datatrees.datacenter.resolver.handler;

import com.datatrees.datacenter.core.exception.BinlogException;

public interface ExceptionHandler {

  void handle(String file, BinlogException e);
}
