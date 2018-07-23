package com.datatrees.datacenter.compare;

public interface DataCheck {
    void binLogCompare(String dest,String type);

    void binLogCompare(String database, String table, String partition,String partitionType);
}
