package com.datatrees.datacenter.compare;

import com.datatrees.datacenter.core.utility.DBServer;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.datareader.AvroDataReader;
import com.datatrees.datacenter.operate.OperateType;
import com.datatrees.datacenter.table.CheckResult;
import com.datatrees.datacenter.table.CheckTable;
import com.datatrees.datacenter.utility.FilePathUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HiveCompareByDate extends HiveCompareByFile {
    private static Logger LOG = LoggerFactory.getLogger(HiveCompareByFile.class);

    @Override
    public void binLogCompare(String database, String table, String partition, String partitionType) {
        List<Map<String, Object>> partitionInfos = getSpecifiedDateTableInfo(database, table, partition, partitionType);
        if (null != partitionInfos && partitionInfos.size() > 0) {
            AvroDataReader avroDataReader = new AvroDataReader();
            for (Map<String, Object> partitionInfo : partitionInfos) {
                String dbInstance = String.valueOf(partitionInfo.get(CheckTable.DB_INSTANCE));
                String tableName = String.valueOf(partitionInfo.get(CheckTable.TABLE_NAME));
                String file = String.valueOf(partitionInfo.get(CheckTable.FILE_NAME));
                String filePath = FilePathUtil.assembleFilePath(database, tableName, file, partition, dbInstance, partitionType);
                LOG.info("read avro from: " + filePath);
                Map<String, Map<String, Long>> avroData = avroDataReader.readSrcData(filePath);
                if (null != avroData && avroData.size() > 0) {
                    Map<String, Long> createRecord = avroData.get(OperateType.Create.toString());
                    Map<String, Long> updateRecord = avroData.get(OperateType.Update.toString());
                    Map<String, Long> deleteRecord = avroData.get(OperateType.Delete.toString());

                    CheckResult result = new CheckResult();
                    result.setTableName(tableName);
                    result.setPartitionType(partitionType);
                    result.setFilePartition(partition);
                    result.setDataBase(database);
                    result.setFileName(file);
                    result.setSaveTable(CheckTable.BINLOG_CHECK_DATE_TABLE);
                    result.setFilesPath(filePath);
                    result.setDbInstance(dbInstance);

                    createRecordProcess(database, tableName, createRecord, result);
                    updateRecordProcess(database, tableName, updateRecord, result);
                    deleteRecordProcess(database, tableName, deleteRecord, result);
                }
            }
            // TODO: 2018/10/22 可以修改为每个文件更新一次
            this.updateCheckedFile(database, table, partition, partitionType);
        }
    }

    private void updateCheckedFile(String database, String table, String partition, String partitionType) {
        Map<String, Object> whereMap = new HashMap<>(2);
        whereMap.put(CheckTable.DATA_BASE, database);
        whereMap.put(CheckTable.TABLE_NAME, table);
        whereMap.put(CheckTable.FILE_PARTITION, partition);
        whereMap.put(CheckTable.PARTITION_TYPE, partitionType);
        whereMap.values().remove("");
        Map<String, Object> valueMap = new HashMap<>(1);
        valueMap.put(CheckTable.PROCESS_LOG_STATUS, 1);
        try {
            DBUtil.update(DBServer.DBServerType.MYSQL.toString(), CheckTable.BINLOG_DATABASE, CheckTable.BINLOG_PROCESS_LOG_TABLE, valueMap, whereMap);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
