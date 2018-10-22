package com.datatrees.datacenter.main;

import com.datatrees.datacenter.core.utility.DBServer;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.operate.OperateType;
import com.datatrees.datacenter.repair.hive.HiveDataRepair;
import com.datatrees.datacenter.table.CheckResult;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author personalc
 */
public class HiveRepairByDataBase {

    public static void main(String[] args) throws Exception {
        try {
            Map<String, Object> whereMap = new HashMap<>(3);
            whereMap.put("database_name", "credit_audit");
            whereMap.put("table_name", "t_audit_job");
            whereMap.put("repair_status", 0);
            whereMap.put("type","create");
            whereMap.values().remove(null);
            List<Map<String, Object>> dataMapList = DBUtil.query(DBServer.DBServerType.MYSQL.toString(), "binlog", "t_binlog_check_hive", whereMap);
            dataMapList.sort((o1, o2) -> {
                String type1 = (String) o1.get("operate_type");
                String type2 = (String) o2.get("operate_type");
                OperateType operateType1 = OperateType.valueOf(type1);
                OperateType operateType2 = OperateType.valueOf(type2);
                return (Integer.compare(operateType1.getValue(), operateType2.getValue()));
            });
            List<Map<String, Object>> createList = dataMapList.stream().filter(x -> "Create".equals(x.get("operate_type").toString())).collect(Collectors.toList());
            List<Map<String, Object>> updateList = dataMapList.stream().filter(x -> "Update".equals(x.get("operate_type").toString())).collect(Collectors.toList());
            List<Map<String, Object>> deleteList = dataMapList.stream().filter(x -> "Delete".equals(x.get("operate_type").toString())).collect(Collectors.toList());
            System.out.println(dataMapList);
            if (createList != null && createList.size() > 0) {
                createList.forEach(HiveRepairByDataBase::repairPrepare);
            }
            if (updateList != null && updateList.size() > 0) {
                updateList.forEach(HiveRepairByDataBase::repairPrepare);
            }
            if (deleteList != null && deleteList.size() > 0) {
                deleteList.forEach(HiveRepairByDataBase::repairPrepare);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void repairPrepare(Map<String, Object> map) {
        HiveDataRepair dataRepair = new HiveDataRepair();
        CheckResult checkResult = new CheckResult();
        String dbInstance = (String) map.get("db_instance");
        dbInstance = dbInstance.replaceAll("_", ".");
        String dataBase = (String) map.get("database_name");
        String partition = (String) map.get("file_partitions");
        String partitionType = "create";
        String tableName = (String) map.get("table_name");
        String fileName = (String) map.get("file_name");

        checkResult.setDbInstance(dbInstance);
        checkResult.setDataBase(dataBase);
        checkResult.setFilePartition(partition);
        checkResult.setPartitionType(partitionType);
        checkResult.setTableName(tableName);
        checkResult.setFileName(fileName);
        dataRepair.repairByIdList(checkResult, "t_binlog_check_hive");
    }
}
