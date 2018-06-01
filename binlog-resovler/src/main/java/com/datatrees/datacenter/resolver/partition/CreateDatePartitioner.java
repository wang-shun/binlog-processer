package com.datatrees.datacenter.resolver.partition;

import com.datatrees.datacenter.core.utility.Properties;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;

public class CreateDatePartitioner extends TimeBasedPartitioner {
    @Override
    protected List<String> partitionColumns() {
        try {
            return asList(Properties.load(PARTITIONER_CONSTANCE).getProperty("Create").split(","));
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public String getRoot() {
        return "Create";
    }
}
