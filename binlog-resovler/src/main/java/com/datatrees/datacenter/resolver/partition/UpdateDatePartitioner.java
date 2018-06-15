package com.datatrees.datacenter.resolver.partition;

import static java.util.Arrays.asList;

import com.datatrees.datacenter.core.utility.PropertiesUtility;
import java.util.List;

public class UpdateDatePartitioner extends TimeBasedPartitioner {

  String updatePartitions;

  public UpdateDatePartitioner() {
    updatePartitions = PropertiesUtility.load(PARTITIONER_CONSTANCE).getProperty("update");
  }

  @Override
  protected List<String> partitionColumns() {
    return asList(updatePartitions.split(","));
  }

  @Override
  public String getRoot() {
    return "update";
  }
}
