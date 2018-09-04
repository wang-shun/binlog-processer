package com.datatrees.datacenter.main;

import com.datatrees.datacenter.repair.TiDBDataRepair;
import com.datatrees.datacenter.table.CheckResult;

public class TiDBRepairByID {
    public static void main(String[] args) {
        if (args.length == 5) {
            CheckResult checkResult = new CheckResult();
            checkResult.setDataBase(args[0]);
            checkResult.setFilePartition(args[1]);
            checkResult.setPartitionType(args[2]);
            String checkTable = args[3];
            checkResult.setTableName(args[4]);
            TiDBDataRepair tiDBDataRepair = new TiDBDataRepair();
            tiDBDataRepair.repairByIdList(checkResult, checkTable);

        } else if (args.length == 4) {
            CheckResult checkResult = new CheckResult();
            checkResult.setDataBase(args[0]);
            checkResult.setFilePartition(args[1]);
            checkResult.setPartitionType(args[2]);
            String checkTable = args[3];
            TiDBDataRepair tiDBDataRepair = new TiDBDataRepair();
            tiDBDataRepair.repairByIdList(checkResult, checkTable);
        } else {
            System.out.print("please enter dataBase partition partitionType repairTable [tableName]");
            System.exit(1);
        }
    }
}
