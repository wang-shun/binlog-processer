package com.datatrees.datacenter.core.utility;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StopWatch {

  private String taskName;

  public StopWatch(String taskName) {
    this.taskName = taskName;
  }

  private static Logger logger = LoggerFactory.getLogger(StopWatch.class);

  public void watch(Runnable watched)
    throws RuntimeException {
    org.apache.commons.lang3.time.StopWatch stopWatch =
      new org.apache.commons.lang3.time.StopWatch();
    watched.run();
    stopWatch.stop();
    logger.info(
      String.format("task %s totally cost %d milliseconds", this.taskName, stopWatch.getTime()));
  }
}
