package com.datatrees.datacenter.repair;

import com.datatrees.datacenter.table.CheckResult;

public interface BaseDataRepair {

    void repairByTime(String dataBase, String tableName, String start,String end, String partitionType);


    void repairByFile(String fileName, String partitionType);

    void repairByIdList(CheckResult checkResult,String checkTable);
}
