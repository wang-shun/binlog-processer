package com.datatrees.datacenter.resolver.reader;

import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * determine whether delete event should be ignore
 */
public class IgnoreStrategy {

  private static Logger logger = LoggerFactory.getLogger(IgnoreStrategy.class);
  private ConcurrentHashMap<String, Boolean> strategyMap = new ConcurrentHashMap<>();

  public static void main(String[] args) {
    IgnoreStrategy ignoreStrategy = new IgnoreStrategy();
    Boolean bo = ignoreStrategy.current("db1", "tlb1");

    Boolean bo2 = ignoreStrategy.current("db1", "tlb1");
    System.err.println();
  }

  public Boolean current(String schema, String table) {
    return strategyMap.getOrDefault(String.format("%s.%s", schema, table), false);
  }

  public void store(String schema, String table, Boolean result) {
    strategyMap.put(String.format("%s.%s", schema, table), result);
  }
}
