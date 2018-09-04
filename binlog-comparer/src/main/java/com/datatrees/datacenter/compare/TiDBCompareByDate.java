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
    private List<String> updateTimeList = FieldNameOp.getConfigField("update");
    private String recordId;
    private String recordLastUpdateTime;

    @Override
    public void binLogCompare(String dataBase, String table, String partition, String type) {
        super.partitionType = type;
        List<Map<String, Object>> specifiedDateTable = BaseDataCompare.getSpecifiedDateTableInfo(dataBase, table, partition, type);
        dataCheck(specifiedDateTable);
        Map<String, Object> whereMap = new HashMap<>();
        whereMap.put(CheckTable.FILE_PARTITION, partition);
        whereMap.put(CheckTable.DATA_BASE, dataBase);
        whereMap.put(CheckTable.PARTITION_TYPE, partitionType);
        if (table != null && !"".equals(table)) {
            whereMap.put(CheckTable.TABLE_NAME, table);
        }
        Map<String, Object> valueMap = new HashMap<>(1);
        valueMap.put(CheckTable.PROCESS_LOG_STATUS, 1);
        try {
            DBUtil.update(DBServer.DBServerType.MYSQL.toString(), binLogDataBase, CheckTable.BINLOG_PROCESS_LOG_TABLE, valueMap, whereMap);
            LOG.info("compare finished !");
        } catch (SQLException e) {
            LOG.info("change status from 0 to 1 failed of partition: " + partition);
        }
        LOG.info("all the data with database: {} , table: {} , partitions {} ,type: {} ----check finished !", dataBase, table, partition, type);
    }

    @Override
    public void dataCheck(List<Map<String, Object>> tableInfo) {
        if (null != tableInfo) {
            for (Map<String, Object> recordMap : tableInfo) {
                String dataBase = String.valueOf(recordMap.get(CheckTable.DATA_BASE));
                String tableName = String.valueOf(recordMap.get(CheckTable.TABLE_NAME));
                String partition = String.valueOf(recordMap.get(CheckTable.FILE_PARTITION));
                String dbInstance = String.valueOf(recordMap.get(CheckTable.DB_INSTANCE));
                String[] filePaths = String.valueOf(recordMap.get("files")).split(",");
                if (filePaths.length > 0) {
                    List<String> fileList = Arrays.asList(filePaths);
                    Collections.sort(fileList);
                    Collection<Object> allFieldSet = FieldNameOp.getAllFieldName(dataBase, tableName);
                    recordId = FieldNameOp.getFieldName(allFieldSet, idList);
                    recordLastUpdateTime = FieldNameOp.getFieldName(allFieldSet, updateTimeList);
                    if (null != recordId && null != recordLastUpdateTime) {
                        String tableFieldSql = "select * from " + "`" + dataBase + "`." + tableName + " limit 1";
                        List<Map<String, Object>> mapList = null;
                        try {
                            mapList = DBUtil.query(DBServer.DBServerType.TIDB.toString(), dataBase, tableFieldSql);
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                        super.recordId = recordId;
                        super.recordLastUpdateTime = recordLastUpdateTime;
                        AvroDataReader avroDataReader = new AvroDataReader();
                        Map<String, Long> allCreate = new HashMap<>();
                        Map<String, Long> allUpdate = new HashMap<>();
                        Map<String, Long> allDelete = new HashMap<>();

                        String partitionPath = super.AVRO_HDFS_PATH +
                                File.separator +
                                partitionType +
                                File.separator +
                                dbInstance +
                                File.separator +
                                dataBase +
                                File.separator +
                                tableName +
                                File.separator +
                                partition;

                        CheckResult checkResult = new CheckResult();
                        checkResult.setDataBase(dataBase);
                        checkResult.setTableName(tableName);
                        checkResult.setDbInstance(dbInstance);
                        checkResult.setFilePartition(partition);
                        checkResult.setPartitionType(this.partitionType);
                        checkResult.setFilesPath(partitionPath);
                        avroDataReader.setDataBase(dataBase);
                        avroDataReader.setTableName(tableName);
                        avroDataReader.setRecordId(recordId);
                        avroDataReader.setRecordLastUpdateTime(recordLastUpdateTime);

                        for (String fileName : fileList) {
                            String filePath = partitionPath +
                                    File.separator +
                                    fileName.replace(".tar", "") +
                                    CheckTable.FILE_LAST_NAME;

                            LOG.info("****************");
                            LOG.info("checking file :" + filePath);
                            LOG.info("****************");
                            Map<String, Map<String, Long>> avroData = avroDataReader.readSrcData(filePath);
                            if (null != avroData && avroData.size() > 0) {
                                Map<String, Long> create = avroData.get(OperateType.Create.toString());
                                if (create != null && create.size() > 0) {
                                    allCreate.putAll(create);
                                }
                                Map<String, Long> update = avroData.get(OperateType.Update.toString());
                                if (update != null && update.size() > 0) {
                                    allUpdate.putAll(update);
                                }
                                Map<String, Long> delete = avroData.get(OperateType.Delete.toString());
                                if (delete != null && delete.size() > 0) {
                                    allDelete.putAll(delete);
                                }
                            }
                        }
                        allCreate = BaseDataCompare.diffCompare(allCreate, allUpdate);
                        allCreate = BaseDataCompare.diffCompare(allCreate, allDelete);
                        allUpdate = BaseDataCompare.diffCompare(allUpdate, allDelete);
                        checkResult.setSaveTable(CheckTable.BINLOG_CHECK_DATE_TABLE);
                        checkEvent(mapList, allCreate, allUpdate, allDelete, checkResult);
                    }
                }
            }
        }
    }
}
