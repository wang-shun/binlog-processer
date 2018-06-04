package com.datatrees.datacenter.resolver.partition;

import com.datatrees.datacenter.core.utility.Properties;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;

public class UpdateDatePartitioner extends TimeBasedPartitioner {
    @Override
    protected List<String> partitionColumns() {
        return asList(Properties.load(PARTITIONER_CONSTANCE).getProperty("Update").split(","));
    }

    @Override
    public String getRoot() {
        return "Update";
    }
}
