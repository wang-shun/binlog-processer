package com.datatrees.datacenter.repair.dbhandler;

import com.datatrees.datacenter.core.utility.DBServer;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.table.CheckResult;
import com.datatrees.datacenter.table.CheckTable;
import com.datatrees.datacenter.utility.StringBuilderUtil;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class BinlogDBHandler {

    public static Map<String, List<String>> readAvroDataById(CheckResult checkResult, String checkTable) {
        Map<String, Object> whereMap = new HashMap<>();
        String dataBase = checkResult.getDataBase();
        String tableName = checkResult.getTableName();
        String partition = checkResult.getFilePartition();
        String partitionType = checkResult.getPartitionType();
        whereMap.put(CheckTable.DATA_BASE, dataBase);
        whereMap.put(CheckTable.TABLE_NAME, tableName);
        whereMap.put(CheckTable.PARTITION_TYPE, partitionType);
        whereMap.put(CheckTable.FILE_PARTITION, partition);
        whereMap.values().remove(null);
        StringBuilder whereExpress = StringBuilderUtil.getStringBuilder(whereMap);
        String sql = "select id_list,files_path,operate_type from " + checkTable + " " + whereExpress;
        Map<String, List<String>> opIdMap=null;
        try {
            List<Map<String, Object>> missingData = DBUtil.query(DBServer.DBServerType.MYSQL.toString(), CheckTable.BINLOG_DATABASE, sql);
            if (missingData != null && missingData.size() > 0) {
                opIdMap = new HashMap<>(missingData.size());
                for (Map<String, Object> record : missingData) {
                    String[] idArr = record.get(CheckTable.ID_LIST).toString().replace("[", "").replace("]", "").split(",");
                    List<String> idList = Arrays.asList(idArr);
                    List<String> idListNew = idList.stream().map(String::trim).collect(Collectors.toList());
                    String operateType = (String) record.get(CheckTable.OP_TYPE);
                    opIdMap.put(operateType, idListNew);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return opIdMap;
    }

    public static void updateCheckedFile(String checkTable, String dataBase, String dbInstance, String tableName, String partition, String partitionType) {
        Map<String, Object> whereMap = new HashMap<>(5);
        whereMap.put(CheckTable.DB_INSTANCE, dbInstance);
        whereMap.put(CheckTable.DATA_BASE, dataBase);
        whereMap.put(CheckTable.TABLE_NAME, tableName);
        whereMap.put(CheckTable.PARTITION_TYPE, partitionType);
        whereMap.put(CheckTable.FILE_PARTITION, partition);
        whereMap.values().remove("");
        Map<String, Object> valueMap = new HashMap<>(1);
        valueMap.put(CheckTable.STATUS, 1);
        try {
            DBUtil.update(DBServer.DBServerType.MYSQL.toString(), CheckTable.BINLOG_DATABASE, checkTable, valueMap, whereMap);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
