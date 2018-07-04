package com.datatrees.datacenter.main;

import com.datatrees.datacenter.core.task.TaskDispensor;
import com.datatrees.datacenter.core.task.TaskRunner;
import com.datatrees.datacenter.resolver.schema.SchemaProviders.SecondaryCacheKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main3 {

  private static TaskRunner taskRunner;

  private static Logger logger = LoggerFactory.getLogger(Main3.class);

  public static void main(String[] args) {

    TaskDispensor.defaultDispensor()
      .dispense("local_topic", SecondaryCacheKey.builder().schema("x").build());
  }
}
