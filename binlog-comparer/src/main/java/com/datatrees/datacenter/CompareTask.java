package com.datatrees.datacenter;

import com.datatrees.datacenter.compare.BaseDataCompare;
import com.datatrees.datacenter.compare.TiDBCompare;
import com.datatrees.datacenter.core.task.TaskRunner;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.resolver.TaskProcessor;
import com.datatrees.datacenter.resolver.TaskProcessorListner;

public class CompareTask implements TaskRunner {

  public void startCheck() {
    TaskProcessor.defaultProcessor()
      .setTopic(PropertiesUtility.defaultProperties().getProperty("queue.compare.topic"))
      .registerListner(
        new TaskProcessorListner() {
          @Override
          public void onMessageReceived(String desc) {
            BaseDataCompare compare = new TiDBCompare();
            compare.binLogCompare(desc, "update");
          }
        }).process();
  }

  @Override
  public void process() {
    startCheck();
  }
}
