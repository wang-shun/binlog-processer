package com.datatrees.datacenter.repair.tidb;

import com.datatrees.datacenter.compare.BaseDataCompare;
import com.datatrees.datacenter.core.utility.DBServer;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.datareader.AvroDataReader;
import com.datatrees.datacenter.operate.OperateType;
import com.datatrees.datacenter.repair.BaseDataRepair;
import com.datatrees.datacenter.table.CheckResult;
import com.datatrees.datacenter.table.CheckTable;
import com.datatrees.datacenter.utility.StringBuilderUtil;
import org.apache.hive.com.esotericsoftware.minlog.Log;

import java.io.File;
import java.sql.SQLException;
import java.util.*;

public class TiDBDataRepair implements BaseDataRepair {
    private Properties properties = PropertiesUtility.defaultProperties();
    private String mysqlDataBase = properties.getProperty("jdbc.database", "binlog");
    private String avroHDFSPath = properties.getProperty("AVRO_HDFS_PATH");

    private String dbInstance;
    private String dataBase;
    private String tableName;
    private String partition;

    @Override
    public void repairByTime(String dataBase, String tableName, String start, String end ,String partitionType) {
       String sql="select file_name from t_binlog_record where request_end<'"+end+"'"+" and request_end >'"+start+"'";
        List<Map<String, Object>> specifiedDateTable = null;
        try {
            specifiedDateTable = DBUtil.query(DBServer.DBServerType.MYSQL.toString(),dataBase,sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        // TODO: 2018/10/10 需要修改
        if (specifiedDateTable != null && specifiedDateTable.size() > 0) {
            for (Map<String, Object> fileMap : specifiedDateTable) {
                String[] files = String.valueOf(fileMap.get("files")).split(",");
                Arrays.sort(files);
                this.dbInstance = String.valueOf(fileMap.get(CheckTable.DB_INSTANCE));
                this.dataBase = dataBase;
                this.tableName = String.valueOf(fileMap.get(CheckTable.TABLE_NAME));
                this.partition = partition;
                if (files.length > 0) {
                    for (String fileName : files) {
                        repairByFile(fileName, partitionType);
                    }
                }
            }
        }
    }

    @Override
    public void repairByFile(String fileName, String partitionType) {
        //读取某个文件，并将所有记录解析出来重新插入到数据库
        Map<String, Object> whereMap = new HashMap<>(6);
        whereMap.put(CheckTable.DB_INSTANCE, dbInstance);
        whereMap.put(CheckTable.DATA_BASE, dataBase);
        whereMap.put(CheckTable.TABLE_NAME, tableName);
        whereMap.put(CheckTable.FILE_NAME, fileName);
        whereMap.put(CheckTable.FILE_PARTITION, partition);
        whereMap.put(CheckTable.PARTITION_TYPE, partitionType);
        StringBuilder whereExpress = StringBuilderUtil.getStringBuilder(whereMap);
        String sqlStr = "select * from " + CheckTable.BINLOG_PROCESS_LOG_TABLE + " " + whereExpress;
        try {
            List<Map<String, Object>> tableInfo = DBUtil.query(DBServer.DBServerType.MYSQL.toString(), mysqlDataBase, sqlStr);
            if (null != tableInfo && tableInfo.size() > 0) {
                //先检查表是否存在
                String tableQuerySql = "select * from information_schema.TABLES WHERE TABLE_SCHEMA='" + this.dataBase + "'" + " and TABLE_NAME ='" + this.tableName + "'";
                List<Map<String, Object>> tableExists = DBUtil.query(DBServer.DBServerType.TIDB.toString(), "information_schema", tableQuerySql);
                List<Set<Map.Entry<String, Object>>> createList;
                List<Set<Map.Entry<String, Object>>> updateList;
                List<Set<Map.Entry<String, Object>>> deleteList;

                if (null != tableExists && tableExists.size() > 0) {
                    String filePath = avroHDFSPath + File.separator
                            + partitionType + File.separator
                            + this.dbInstance + File.separator
                            + this.dataBase + File.separator
                            + this.tableName + File.separator
                            + this.partition + File.separator
                            + fileName + CheckTable.FILE_LAST_NAME;

                    List<Object> timeColumns = DataBaseHandler.getTimeFields(tableName, dataBase);
                    Map<String, List<Set<Map.Entry<String, Object>>>> mapList = AvroDataReader.readAllDataFromAvro(filePath);
                    createList = mapList.get(OperateType.Create.toString());
                    updateList = mapList.get(OperateType.Update.toString());
                    deleteList = mapList.get(OperateType.Delete.toString());
                    if (null != createList && createList.size() > 0) {
                        DataBaseHandler.batchInsert(createList, this.dataBase, tableName, timeColumns);
                    }
                    if (null != updateList && updateList.size() > 0) {
                        DataBaseHandler.batchUpsert(updateList, this.dataBase, tableName, timeColumns);
                    }
                    if (null != deleteList && deleteList.size() > 0) {
                        DataBaseHandler.batchDelete(deleteList, this.dataBase, tableName);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Override
    public void repairByIdList(CheckResult checkResult, String checkTable) {
        Map<String, List<Set<Map.Entry<String, Object>>>> dataMap = AvroDataReader.readAvroDataById(checkResult, checkTable);
        if (dataMap != null && dataMap.size() > 0) {
            String dataBase = checkResult.getDataBase();
            String table = checkResult.getTableName();
            for (Map.Entry dataEntry : dataMap.entrySet()) {
                if (OperateType.Delete.equals(dataEntry.getKey())) {
                    DataBaseHandler.batchDelete((List<Set<Map.Entry<String, Object>>>) dataEntry.getValue(), dataBase, table);
                } else {
                    try {
                        List<Object> timeColumns = DataBaseHandler.getTimeFields(table, dataBase);
                        DataBaseHandler.batchUpsert((List<Set<Map.Entry<String, Object>>>) dataEntry.getValue(), dataBase, table, timeColumns);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
            Map<String, Object> whereMap = new HashMap<>(5);
            String dbInstance = checkResult.getDbInstance();
            String tableName = checkResult.getTableName();
            String partition = checkResult.getFilePartition();
            String partitionType = checkResult.getPartitionType();
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
                Log.error("update repaired record in " + checkTable + " failed with Exception :", e);
            }
        }
    }

}
