package com.datatrees.datacenter.repair;

import com.alibaba.fastjson.JSONObject;
import com.datatrees.datacenter.compare.BaseDataCompare;
import com.datatrees.datacenter.core.utility.DBServer;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.core.utility.TimeUtil;
import com.datatrees.datacenter.datareader.AvroDataReader;
import com.datatrees.datacenter.operate.OperateType;
import com.datatrees.datacenter.table.CheckResult;
import com.datatrees.datacenter.table.CheckTable;
import com.datatrees.datacenter.table.FieldNameOp;
import com.datatrees.datacenter.utility.StringBuilderUtil;
import javafx.beans.binding.ObjectExpression;
import org.apache.hive.com.esotericsoftware.minlog.Log;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.*;

public class TiDBDataRepair implements BaseDataRepair {
    private Properties properties = PropertiesUtility.defaultProperties();
    private String mysqlDataBase = properties.getProperty("jdbc.database");
    private String avroHDFSPath = properties.getProperty("AVRO_HDFS_PATH");
    private List<String> idList = FieldNameOp.getConfigField("id");
    private List<String> updateTimeList = FieldNameOp.getConfigField("update");

    private String dbInstance;
    private String dataBase;
    private String tableName;
    private String partition;

    @Override
    public void repairByTime(String dataBase, String tableName, String partition, String partitionType) {
        List<Map<String, Object>> specifiedDateTable = BaseDataCompare.getSpecifiedDateTableInfo(dataBase, tableName, partition, partitionType);
        if (specifiedDateTable != null && specifiedDateTable.size() > 0) {
            for (Map<String, Object> fileMap : specifiedDateTable) {
                String[] files = String.valueOf(fileMap.get("files")).split(",");
                Arrays.sort(files);
                this.dbInstance = String.valueOf(fileMap.get(CheckTable.DB_INSTANCE));
                this.dataBase = dataBase;
                this.tableName = String.valueOf(fileMap.get(CheckTable.TABLE_NAME));
                this.partition = partition;
                if (files != null && files.length > 0) {
                    for (int i = 0; i < files.length; i++) {
                        String fileName = files[i];
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
                String tableQuerySql = "SELECT * FROM information_schema.TABLES WHERE TABLE_SCHEMA='" + this.dataBase + "'" + " and TABLE_NAME ='" + this.tableName + "'";
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

                    List<Object> timeColumns = getTimeFields(DBServer.DBServerType.TIDB, tableName, dataBase);
                    Map<String, List<Set<Map.Entry<String, Object>>>> mapList = AvroDataReader.readAllDataFromAvro(filePath);
                    createList = mapList.get(OperateType.Create.toString());
                    updateList = mapList.get(OperateType.Update.toString());
                    deleteList = mapList.get(OperateType.Delete.toString());
                    if (null != createList && createList.size() > 0) {
                        batchInsert(createList, this.dataBase, tableName, timeColumns);
                    }
                    if (null != updateList && updateList.size() > 0) {
                        batchUpsert(updateList, this.dataBase, tableName, timeColumns);
                    }
                    if (null != deleteList && deleteList.size() > 0) {
                        batchDelete(deleteList, this.dataBase, tableName);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private List<Object> getTimeFields(DBServer.DBServerType dbServerType, String tableName, String dataBase) throws SQLException {
        List<Map<String, Object>> columnTypes = DBUtil.query(dbServerType.toString(), "information_schema", "select COLUMN_NAME,DATA_TYPE from information_schema.COLUMNS where table_name = '" + tableName + "' and table_schema = '" + dataBase + "'");
        List<Object> timeColumns = new ArrayList<>();
        for (Map<String, Object> columnType : columnTypes) {
            String columnName = (String) columnType.get("COLUMN_NAME");
            String dataType = (String) columnType.get("DATA_TYPE");
            if ("date".equals(dataType) || "timestamp".equals(dataType)) {
                timeColumns.add(columnName);
            }
        }
        return timeColumns;
    }

    @Override
    public void repairByIdList(CheckResult checkResult, String checkTable) {
        Map<String, List<Set<Map.Entry<String, Object>>>> dataMap = AvroDataReader.readAvroDataById(checkResult, checkTable);
        if (dataMap != null && dataMap.size() > 0) {
            String dataBase = checkResult.getDataBase();
            String table = checkResult.getTableName();
            for (Map.Entry dataEntry : dataMap.entrySet()) {
                if (OperateType.Delete.equals(dataEntry.getKey())) {
                    batchDelete((List<Set<Map.Entry<String, Object>>>) dataEntry.getValue(), dataBase, table);
                } else {
                    try {
                        List<Object> timeColumns = getTimeFields(DBServer.DBServerType.TIDB, table, dataBase);
                        batchUpsert((List<Set<Map.Entry<String, Object>>>) dataEntry.getValue(), dataBase, table, timeColumns);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
            Map<String, Object> whereMap = new HashMap<>();
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
            StringBuilder whereExpress = StringBuilderUtil.getStringBuilder(whereMap);
            String sql = "select id_list,files_path,operate_type from " + checkTable + " " + whereExpress;
            Map<String, Object> valueMap = new HashMap<>();
            valueMap.put(CheckTable.STATUS, 1);
            try {
                DBUtil.update(DBServer.DBServerType.MYSQL.toString(), CheckTable.BINLOG_DATABASE, checkTable, valueMap, whereMap);
            } catch (SQLException e) {
                Log.error("update repaired record in " + checkTable + " failed with Exception :", e);
            }
        }
    }

    /**
     * 批量删除
     *
     * @param list      需要修复的数据列表
     * @param dataBase  数据库
     * @param tableName 数据表
     */
    private void batchDelete(List<Set<Map.Entry<String, Object>>> list, String dataBase, String tableName) {
        Collection<Object> allFieldSet = FieldNameOp.getAllFieldName(dataBase, tableName);
        String recordId = FieldNameOp.getFieldName(allFieldSet, idList);
        List<Object> dataList = null;
        for (Set<Map.Entry<String, Object>> recordSet : list) {
            Iterator<Map.Entry<String, Object>> iterator = recordSet.iterator();
            dataList = new ArrayList<>();
            while (iterator.hasNext()) {
                Map.Entry<String, Object> recordEntry = iterator.next();
                if (recordId != null && recordId.equals(recordEntry.getKey())) {
                    dataList.add(recordEntry.getValue());
                    break;
                }
            }
        }
        StringBuilder stringBuilder;
        if (dataList != null) {
            stringBuilder = new StringBuilder();
            stringBuilder
                    .append("delete from ")
                    .append(tableName)
                    .append(" where ")
                    .append(recordId)
                    .append(" in ")
                    .append("(");
            for (int i = 0; i < dataList.size(); i++) {
                Object object = dataList.get(i);
                int id = Integer.parseInt(object.toString());
                if (i < dataList.size() - 1) {
                    stringBuilder.append("'")
                            .append(id)
                            .append("'")
                            .append(",");
                } else {
                    stringBuilder.append(id);
                }
            }
            try {
                DBUtil.query(DBServer.DBServerType.TIDB.toString(), dataBase, stringBuilder.toString());
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    private void batchInsert(List<Set<Map.Entry<String, Object>>> setList, String dataBase, String tableName, List<Object> timeColumns) {
        List<Map<String, Object>> inSertList;
        if (null != setList && setList.size() > 0) {
            inSertList = assembleData(setList, timeColumns);
            try {
                DBUtil.insertAll(DBServer.DBServerType.TIDB.toString(), dataBase, tableName, inSertList);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private List<Map<String, Object>> assembleData(List<Set<Map.Entry<String, Object>>> setList, Collection<Object> timeColumns) {
        List<Map<String, Object>> dataMapList = new ArrayList<>();
        for (Set<Map.Entry<String, Object>> recordSet : setList) {
            Iterator<Map.Entry<String, Object>> iterator = recordSet.iterator();
            Map<String, Object> recordMap = new HashMap<>(recordSet.size());
            while (iterator.hasNext()) {
                Map.Entry<String, Object> recordEntry = iterator.next();
                String key = recordEntry.getKey();
                Object value = recordEntry.getValue();
                if (value != null) {
                    if (timeColumns.contains(key)) {
                        String valueStr = String.valueOf(value);
                        if (valueStr.length() == 10) {
                            long longTimeStamp = Long.valueOf(valueStr) * 1000;
                            recordMap.put(key, TimeUtil.stampToDate(longTimeStamp));
                        } else {
                            Long longTimeStamp = Long.valueOf(valueStr);
                            recordMap.put(key, TimeUtil.stampToDate(longTimeStamp));
                        }
                    } else {
                        if (value instanceof JSONObject) {
                            String valueStr = null;
                            try {
                                valueStr = new String(((JSONObject) value).getString("bytes").getBytes("iso8859-1"), "utf-8");
                            } catch (UnsupportedEncodingException e) {
                                e.printStackTrace();
                            }
                            recordMap.put(key, valueStr);
                        } else {
                            recordMap.put(key, value);
                        }
                    }
                } else {
                    recordMap.put(key, null);
                }
            }
            dataMapList.add(recordMap);
        }
        return dataMapList;
    }

    private void batchUpsert(List<Set<Map.Entry<String, Object>>> setList, String dataBase, String tableName, Collection<Object> timeColumns) {
        List<Map<String, Object>> updateList;
        if (null != setList && setList.size() > 0) {
            updateList = assembleData(setList, timeColumns);
            String lastUpdateColumn = FieldNameOp.getFieldName(timeColumns, updateTimeList);
            if (lastUpdateColumn != null) {
                try {
                    DBUtil.upsertAll(DBServer.DBServerType.TIDB.toString(), dataBase, tableName, updateList, lastUpdateColumn);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
