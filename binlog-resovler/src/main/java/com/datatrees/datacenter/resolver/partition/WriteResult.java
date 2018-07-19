package com.datatrees.datacenter.resolver.partition;

import java.util.HashMap;

public interface WriteResult {
  public HashMap<String, WriteResultValue> getValueCacheByFile();
}
