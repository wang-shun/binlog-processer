package com.datatrees.datacenter.repair.hive;

import com.datatrees.datacenter.repair.dbhandler.BinlogDBHandler;
import com.datatrees.datacenter.repair.filehandler.FileOperate;
import com.datatrees.datacenter.repair.partitions.partitionHandler;
import com.datatrees.datacenter.repair.schema.AvroDataBuilder;
import com.datatrees.datacenter.repair.transaction.TransactionOperate;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class RepairThread implements Runnable {

    private static Logger LOG = LoggerFactory.getLogger(HiveDataRepair.class);

    private String binlogProcessLogTable = "t_binlog_process_log";

    private static String[] operateArr = {"Create", "Update", "Delete"};

    private String tableName;

    private String dataBase;

    private String partition;

    private String filePath;

    private String fileName;

    private String partitionType;


    @Override
    public void run() {
        //if ("clientrelationship".equals(dataBase)) {
        if (partition != null) {
            Map<String, String> dateMap = partitionHandler.getDateMap(partition);
            String hivePartition = partitionHandler.getHivePartition(dateMap);
            List<String> hivePartitions = partitionHandler.getPartitions(dateMap);
            InputStream inputStream = FileOperate.getHdfsFileInputStream(filePath);
            if (inputStream != null) {
                Map<String, Object> genericRecordListMap = AvroDataBuilder.avroSchemaDataBuilder(inputStream, null, null);
                if (genericRecordListMap != null) {
                    Schema schema = (Schema) genericRecordListMap.get("schema");
                    Map<String, List<GenericData.Record>> operateIdMap = (Map<String, List<GenericData.Record>>) genericRecordListMap.get("record");
                    if (operateIdMap != null && operateIdMap.size() > 0) {
                        for (int i = 0; i < operateArr.length; i++) {
                            String operate = operateArr[i];
                            List<GenericData.Record> recordList = operateIdMap.get(operate);
                            if (recordList != null && recordList.size() > 0) {
                                TransactionOperate.repairTransaction(dataBase, tableName, hivePartition, hivePartitions, operate, schema, recordList);
                                LOG.info("operate type:[" + operate + "] ," + "record number:[" + recordList.size() + "]");
                            }
                        }
                    }
                } else {
                    LOG.info("no data record read from the avro file");
                }

            } else {
                LOG.info("can't get the avro file inputStream from HDFS");
            }
        }
        // }
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getDataBase() {
        return dataBase;
    }

    public void setDataBase(String dataBase) {
        this.dataBase = dataBase;
    }

    public String getPartition() {
        return partition;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getPartitionType() {
        return partitionType;
    }

    public void setPartitionType(String partitionType) {
        this.partitionType = partitionType;
    }

    public RepairThread(String tableName, String dataBase, String partition, String filePath, String fileName, String partitionType) {
        this.tableName = tableName;
        this.dataBase = dataBase;
        this.partition = partition;
        this.filePath = filePath;
        this.fileName = fileName;
        this.partitionType = partitionType;
    }
}
