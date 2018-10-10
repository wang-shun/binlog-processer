package com.datatrees.datacenter.repair.hive;


import com.datatrees.datacenter.core.utility.DBServer;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.repair.BaseDataRepair;
import com.datatrees.datacenter.repair.dbhandler.BinlogDBHandler;
import com.datatrees.datacenter.repair.filehandler.FileOperate;
import com.datatrees.datacenter.repair.partitions.partitionHandler;
import com.datatrees.datacenter.repair.schema.AvroDataBuilder;
import com.datatrees.datacenter.repair.transaction.TransactionOperate;
import com.datatrees.datacenter.table.CheckResult;
import com.datatrees.datacenter.table.CheckTable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.*;


public class HiveDataRepair implements BaseDataRepair {

    private static Logger LOG = LoggerFactory.getLogger(HiveDataRepair.class);

    private Properties properties = PropertiesUtility.defaultProperties();

    private String binlogDataBase = properties.getProperty("jdbc.database");

    private static String[] operateArr = {"Create", "Update", "Delete"};

    private String schemaName = "schema";

    private String recordName="record";

    private String rawIpSplit="_";

    private String replaceIpSplit=".";

    private String dbInstance;

    private String tableName;

    private String dataBase;

    private String partition;


    @Override
    public void repairByTime(String dataBase, String tableName, String partition, String type) {

    }

    @Override
    public void repairByFile(String fileName, String partitionType) {
        Map<String, Object> whereMap = new HashMap<>(2);
        whereMap.put(CheckTable.FILE_NAME, fileName);
        whereMap.put(CheckTable.PARTITION_TYPE, partitionType);
        try {
            String binlogProcessLogTable = "t_binlog_process_log";
            List<Map<String, Object>> fileInfos = DBUtil.query(DBServer.DBServerType.MYSQL.toString(), binlogDataBase, binlogProcessLogTable, whereMap);
            if (fileInfos != null && fileInfos.size() > 0) {
                for (Map<String, Object> fileInfo : fileInfos) {
                    dbInstance = String.valueOf(fileInfo.get(CheckTable.DB_INSTANCE)).replaceAll(rawIpSplit, replaceIpSplit);
                    dataBase = String.valueOf(fileInfo.get(CheckTable.DATA_BASE));
                    tableName = String.valueOf(fileInfo.get(CheckTable.TABLE_NAME));
                    partition = String.valueOf(fileInfo.get(CheckTable.FILE_PARTITION));
                    String filePath = FileOperate.getFilePath(dataBase, dbInstance, tableName, partition, partitionType, fileName);
                    // TODO: 2018/10/10
                    if ("clientrelationship".equals(dataBase)) {
                        if (partition != null) {
                            Map<String, String> dateMap = partitionHandler.getDateMap(partition);
                            String hivePartition = partitionHandler.getHivePartition(dateMap);
                            List<String> hivePartitions = partitionHandler.getPartitions(dateMap);
                            InputStream inputStream = FileOperate.getHdfsFileInputStream(filePath);
                            if (inputStream != null) {
                                Map<String, Object> genericRecordListMap = AvroDataBuilder.avroSchemaDataBuilder(inputStream, null, null);
                                if (genericRecordListMap != null) {
                                    Schema schema = (Schema) genericRecordListMap.get(schemaName);
                                    Map<String, List<GenericData.Record>> operateIdMap = (Map<String, List<GenericData.Record>>) genericRecordListMap.get(recordName);
                                    if (operateIdMap != null && operateIdMap.size() > 0) {
                                        for (String operate : operateArr) {
                                            List<GenericData.Record> recordList = operateIdMap.get(operate);
                                            if (recordList != null && recordList.size() > 0) {
                                                TransactionOperate.repairTransaction(dataBase, tableName, hivePartition, hivePartitions, operate, schema, recordList);
                                            }
                                        }
                                    }
                                    BinlogDBHandler.updateBinlogProcessLog(binlogProcessLogTable, fileName, dataBase, tableName, partition, partitionType);
                                } else {
                                    LOG.info("no data record read from the avro file");
                                }

                            } else {
                                LOG.info("can't get the avro file inputStream from HDFS");
                            }
                        }
                    }
                }
            } else {
                LOG.info("no avro files find in the database");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void repairByIdList(CheckResult checkResult, String checkTable) {

        dataBase = checkResult.getDataBase();
        dbInstance = checkResult.getDbInstance();
        tableName = checkResult.getTableName();
        partition = checkResult.getFilePartition();
        String partitionType = checkResult.getPartitionType();
        String fileName = checkResult.getFileName();

        String hivePartition;
        List<String> hivePartitions;
        if (partition != null) {
            Map<String, String> dateMap = partitionHandler.getDateMap(partition);
            hivePartition = partitionHandler.getHivePartition(dateMap);
            hivePartitions = partitionHandler.getPartitions(dateMap);

            String filePath = FileOperate.getFilePath(dataBase, dbInstance, tableName, partition, partitionType, fileName);
            Map<String, List<String>> opIdMap = BinlogDBHandler.getOpreateIdList(checkResult, checkTable);

            if (opIdMap != null && opIdMap.size() > 0) {
                InputStream inputStream = FileOperate.getHdfsFileInputStream(filePath);
                if (inputStream != null) {
                    Iterator iterator = opIdMap.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry entry = (Map.Entry) iterator.next();
                        String operate = String.valueOf(entry.getKey());
                        List<String> idList = (List<String>) entry.getValue();
                        if (idList != null && idList.size() > 0) {
                            Map<String, Object> genericRecordListMap = AvroDataBuilder.avroSchemaDataBuilder(inputStream, idList, operate);
                            if (genericRecordListMap != null) {
                                Schema schema = (Schema) genericRecordListMap.get(schemaName);
                                List<GenericData.Record> genericRecordList = (List<GenericData.Record>) genericRecordListMap.get(recordName);
                                TransactionOperate.repairTransaction(dataBase, tableName, hivePartition, hivePartitions, operate, schema, genericRecordList);
                                BinlogDBHandler.updateCheckedFile(checkTable, fileName, dataBase, tableName, partition, operate, partitionType);
                            } else {
                                LOG.info("no data record read from the avro file");
                            }
                        } else {
                            LOG.info("no id find of operate :" + operate);
                        }
                    }
                } else {
                    LOG.info("can't get the avro file inputStream from HDFS");
                }
            } else {
                LOG.info("no id list find from the database");
            }
        }
    }
}
