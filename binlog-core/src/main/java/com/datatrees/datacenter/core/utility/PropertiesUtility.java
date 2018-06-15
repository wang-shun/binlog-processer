package com.datatrees.datacenter.core.utility;

import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertiesUtility {

  private static Logger logger =
    LoggerFactory.getLogger(PropertiesUtility.class);
  private static Properties __defaultProperties;

  static {
    __defaultProperties = load("binlog.properties");
  }

  public static Properties load(String properties) {
    Properties props = new Properties();
    try {
      props.load(ClassLoader.getSystemClassLoader().getResourceAsStream(properties));
      return props;
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      return null;
    }
  }

  public static Properties defaultProperties() {
    return __defaultProperties;
  }
}
