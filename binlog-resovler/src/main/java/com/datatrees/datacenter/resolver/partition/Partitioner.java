package com.datatrees.datacenter.resolver.partition;

import org.apache.avro.generic.GenericData;

public interface Partitioner
{
    String encodePartition(GenericData.Record record);
    String getRoot();
}
