package com.datatrees.datacenter.resolver.partition;

import com.datatrees.datacenter.core.utility.PropertiesUtility;

import java.util.List;

import static java.util.Arrays.asList;

public class CreateDatePartitioner extends TimeBasedPartitioner {

    String createPartitions;

    public CreateDatePartitioner() {
        createPartitions = PropertiesUtility.load(PARTITIONER_CONSTANCE).getProperty("create");
    }

    @Override
    protected List<String> partitionColumns() {
        return asList(createPartitions.split(","));
    }

    @Override
    public String getRoot() {
        return "create";
    }
}
