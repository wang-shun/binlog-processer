package com.datatrees.datacenter.core.utility;

import io.prometheus.client.exporter.HTTPServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrometheusMetrics {

  private static Integer PORT = 9097;
  private static Logger logger = LoggerFactory.getLogger(PrometheusMetrics.class);

  public void start() {
    try {
      HTTPServer s = new HTTPServer((PORT));
    } catch (Exception e) {
      logger
        .error(String.format("error to start prometheus server because of %s.", e.getMessage()), e);
    }
  }
}
