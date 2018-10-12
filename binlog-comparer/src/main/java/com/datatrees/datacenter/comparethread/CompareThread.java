package com.datatrees.datacenter.comparethread;

import com.datatrees.datacenter.compare.HiveCompareByFile;
import com.datatrees.datacenter.datareader.AvroDataReader;
import com.datatrees.datacenter.operate.OperateType;
import com.datatrees.datacenter.table.CheckResult;
import com.datatrees.datacenter.table.CheckTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CompareThread implements Runnable {

    private static Logger LOG = LoggerFactory.getLogger(CompareThread.class);

    private Map<String, Object> partitionInfo;
    private String partitionType;
    private AvroDataReader avroDataReader;

    @Override
    public void run() {/*
        String dataBase = String.valueOf(partitionInfo.get(CheckTable.DATA_BASE));
        String tableName = String.valueOf(partitionInfo.get(CheckTable.TABLE_NAME));

        String fileName = String.valueOf(partitionInfo.get(CheckTable.FILE_NAME));
        String partition = String.valueOf(partitionInfo.get(CheckTable.FILE_PARTITION));
        String dbInstance = String.valueOf(partitionInfo.get(CheckTable.DB_INSTANCE));

        if (partition != null && !"null".equals(partition)) {
            String avroPath = HiveCompareByFile.assembleFilePath(dataBase, tableName, fileName, partition, dbInstance, partitionType);
            LOG.info("read avro from: " + avroPath);
            Map<String, Map<String, Long>> avroData = avroDataReader.readSrcData(avroPath);
            if (null != avroData && avroData.size() > 0) {
                Map<String, Long> createRecord = avroData.get(OperateType.Create.toString());
                Map<String, Long> updateRecord = avroData.get(OperateType.Update.toString());
                Map<String, Long> deleteRecord = avroData.get(OperateType.Delete.toString());

                CheckResult result = new CheckResult();
                result.setTableName(tableName);
                result.setPartitionType(partitionType);
                result.setFilePartition(partition);
                result.setDataBase(dataBase);
                result.setFileName(fileName);
                result.setSaveTable(CheckTable.BINLOG_CHECK_HIVE_TABLE);
                result.setFilesPath(avroPath);
                result.setDbInstance(dbInstance);
                HiveCompareByFile.createRecordProcess(dataBase, tableName, createRecord, result);
                HiveCompareByFile.updateRecordProcess(dataBase, tableName, updateRecord, result);
                HiveCompareByFile.deleteRecordProcess(dataBase, tableName, deleteRecord, result);
            }
        }*/
    }
}
