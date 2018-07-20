package com.datatrees.datacenter.compare;

public interface DataCheck {
    void binLogCompare(String dest);

    void binLogCompare(String database, String table, String partition,String partitionType);
}
