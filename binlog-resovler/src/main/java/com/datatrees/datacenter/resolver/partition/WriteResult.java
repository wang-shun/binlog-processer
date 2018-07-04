package com.datatrees.datacenter.resolver.partition;

import com.google.common.collect.LinkedHashMultimap;
import java.util.HashMap;

public interface WriteResult {

  public HashMap<String, WriteResultValue> getValueCacheByFile();

  public LinkedHashMultimap<String, String> getValueCacheByPartition();

}
