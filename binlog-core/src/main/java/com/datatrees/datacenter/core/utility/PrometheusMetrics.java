package com.datatrees.datacenter.core.utility;

import io.prometheus.client.exporter.HTTPServer;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrometheusMetrics {

  private static Integer PORT = 9097;
  private static Logger logger = LoggerFactory.getLogger(PrometheusMetrics.class);

  static {
    PORT =
      StringUtils.isBlank(PropertiesUtility.defaultProperties().getProperty("PROMETHEUS.PORT"))
        ? 9097 :
        Integer.valueOf(PropertiesUtility.defaultProperties().getProperty("PROMETHEUS.PORT"));
  }

  public void start() {
    try {
      HTTPServer s = new HTTPServer((PORT));
    } catch (Exception e) {
      logger
        .error(String.format("error to start prometheus server because of %s.", e.getMessage()), e);
    }
  }
}
