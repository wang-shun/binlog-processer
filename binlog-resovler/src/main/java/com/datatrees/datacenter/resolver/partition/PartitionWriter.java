package com.datatrees.datacenter.resolver.partition;

import java.io.IOException;

public interface PartitionWriter {
    void write(Object value) throws IOException;

    void flush() throws IOException;

    void close()throws IOException;

    Partitioner partitioner();
}
