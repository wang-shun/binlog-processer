package com.datatrees.datacenter.repair;

public interface BaseDataRepair {

    void repairByTime(String dataBase, String tableName, String partition, String partitionType);


    void repairByFile(String fileName, String partitionType);


}
