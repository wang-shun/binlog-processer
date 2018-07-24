package com.datatrees.datacenter.compare;

import com.datatrees.datacenter.core.utility.DBServer;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.datareader.AvroDataReader;
import com.datatrees.datacenter.operate.OperateType;
import com.datatrees.datacenter.table.CheckResult;
import com.datatrees.datacenter.table.CheckTable;
import com.datatrees.datacenter.table.FieldNameOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.SQLException;
import java.util.*;

public class TiDBCompareByDate extends TiDBCompare {
    private static Logger LOG = LoggerFactory.getLogger(TiDBCompareByDate.class);
    private Properties properties = PropertiesUtility.defaultProperties();
    private String binLogDataBase = properties.getProperty("jdbc.database");
    private List<String> idList = FieldNameOp.getConfigField("id");
    private List<String> createTimeList = FieldNameOp.getConfigField("update");
    private String recordId;
    private String recordLastUpdateTime;
    private String type;

    @Override
    public void binLogCompare(String dataBase, String table, String partition, String type) {
        this.type = type;
        List<Map<String, Object>> specifiedDateTable = super.getSpecifiedDateTableInfo(dataBase, table, partition, type);
        dataCheck(specifiedDateTable);
    }

    @Override
    public void dataCheck(List<Map<String, Object>> tableInfo) {
        if (null != tableInfo) {
            for (Map<String, Object> recordMap : tableInfo) {
                String dataBase = String.valueOf(recordMap.get("database_name"));
                String tableName = String.valueOf(recordMap.get("table_name"));
                String partition = String.valueOf(recordMap.get("file_partitions"));
                String dbInstance = String.valueOf(recordMap.get("db_instance"));
                String[] filePaths = String.valueOf(recordMap.get("files")).split(",");
                recordId = FieldNameOp.getFieldName(dataBase, tableName, idList);
                super.recordId=recordId;
                LOG.info("the field id is :" + recordId);
                recordLastUpdateTime = FieldNameOp.getFieldName(dataBase, tableName, createTimeList);
                super.recordLastUpdateTime=recordLastUpdateTime;
                LOG.info("the field update is:" + recordLastUpdateTime);
                if (null != recordId && null != recordLastUpdateTime) {
                    if (filePaths.length > 0) {
                        Map<String, Long> allUniqueData = new HashMap<>();
                        Map<String, Long> allDeleteData = new HashMap<>();
                        AvroDataReader avroDataReader = new AvroDataReader();
                        for (String fileName : filePaths) {
                            avroDataReader.setDataBase(dataBase);
                            avroDataReader.setTableName(tableName);
                            avroDataReader.setRecordId(recordId);
                            avroDataReader.setRecordLastUpdateTime(recordLastUpdateTime);
                            String filePath = super.AVRO_HDFS_PATH +
                                    File.separator +
                                    dbInstance +
                                    File.separator +
                                    dataBase +
                                    File.separator +
                                    tableName +
                                    File.separator +
                                    partition +
                                    File.separator +
                                    fileName +
                                    ".avro";

                            Map<String, Map<String, Long>> avroData = avroDataReader.readSrcData(filePath);
                            Map<String, Long> unique = avroData.get(OperateType.Unique.toString());
                            Map<String, Long> delete = avroData.get(OperateType.Delete.toString());
                            if (null != unique&&unique.size()>0) {
                                allUniqueData.putAll(unique);
                            }
                            if (null != delete&&unique.size()>0) {
                                allDeleteData.putAll(delete);
                            }
                        }
                        Map<String, Long> filterDeleteMap = diffCompare(allUniqueData, allDeleteData);
                        CheckResult checkResult = new CheckResult();
                        checkResult.setDataBase(dataBase);
                        checkResult.setTableName(tableName);
                        checkResult.setFileName(Arrays.toString(filePaths));
                        checkResult.setDbInstance(dbInstance);
                        checkResult.setOpType(OperateType.Unique.toString());
                        checkAndSaveErrorData(checkResult, filterDeleteMap, OperateType.Unique, CheckTable.BINLOG_CHECK_DATE_TABLE);
                        checkResult.setOpType(OperateType.Delete.toString());
                        checkResult.setFilePartition(partition);
                        checkAndSaveErrorData(checkResult, allDeleteData, OperateType.Delete, CheckTable.BINLOG_CHECK_DATE_TABLE);
                        Map<String, Object> whereMap = new HashMap<>(1);
                        whereMap.put(CheckTable.FILE_PARTITION, partition );
                        whereMap.put(CheckTable.DATA_BASE,  dataBase);
                        whereMap.put(CheckTable.TABLE_NAME,  tableName );
                        whereMap.put("type", type);
                        Map<String, Object> valueMap = new HashMap<>(1);
                        valueMap.put("status", 1);
                        try {
                            DBUtil.update(DBServer.DBServerType.MYSQL.toString(), binLogDataBase, CheckTable.BINLOG_PROCESS_LOG_TABLE, valueMap, whereMap);
                            LOG.info("compare finished !");
                        } catch (SQLException e) {
                            LOG.info("change status from 0 to 1 failed of partition: " + partition);
                        }
                    }
                }
            }
        }
    }
}
