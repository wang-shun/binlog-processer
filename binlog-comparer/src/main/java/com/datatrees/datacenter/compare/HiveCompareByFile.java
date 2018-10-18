package com.datatrees.datacenter.compare;

import com.datatrees.datacenter.core.utility.DBServer;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.datareader.AvroDataReader;
import com.datatrees.datacenter.operate.OperateType;
import com.datatrees.datacenter.table.CheckResult;
import com.datatrees.datacenter.table.CheckTable;
import com.datatrees.datacenter.utility.CheckDBUtil;
import com.datatrees.datacenter.utility.FilePathUtil;
import com.datatrees.datacenter.utility.MapCompareUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HiveCompareByFile extends BaseDataCompare {
    private static Logger LOG = LoggerFactory.getLogger(HiveCompareByFile.class);

    @Override
    public void binLogCompare(String file, String partitionType) {
        List<Map<String, Object>> partitionInfos = getCurrentPartitionInfo(file, partitionType);
        compareByPartition(partitionInfos);
    }

    public void compareByPartition(List<Map<String, Object>> partitionInfos) {
        if (null != partitionInfos && partitionInfos.size() > 0) {
            AvroDataReader avroDataReader = new AvroDataReader();
            partitionInfos.sort((o1, o2) -> {
                String type1 = (String) o1.get("file_name");
                String type2 = (String) o2.get("file_name");
                return Integer.parseInt(String.valueOf(type1.compareTo(type2)));
            });
            //System.out.println(partitionInfos);
            partitionInfos.parallelStream().forEachOrdered(x -> compareOnePartition(avroDataReader, x));
        }
    }

    private void compareOnePartition(AvroDataReader avroDataReader, Map<String, Object> partitionInfo) {
        String dataBase = String.valueOf(partitionInfo.get(CheckTable.DATA_BASE));
        String tableName = String.valueOf(partitionInfo.get(CheckTable.TABLE_NAME));
        String fileName = String.valueOf(partitionInfo.get(CheckTable.FILE_NAME));
        String partition = String.valueOf(partitionInfo.get(CheckTable.FILE_PARTITION));
        String dbInstance = String.valueOf(partitionInfo.get(CheckTable.DB_INSTANCE));
        String partitionType = String.valueOf(partitionInfo.get(CheckTable.PARTITION_TYPE));
        compareProcess(partitionType, avroDataReader, dataBase, tableName, fileName, partition, dbInstance);
        updateCheckedFile(fileName, partitionType, dataBase, tableName, partition);
    }
    private void compareOnePartition(AvroDataReader avroDataReader, String dbInstance,String dataBase,String tableName,String partition,String partitionType,String fileName) {
        compareProcess(partitionType, avroDataReader, dataBase, tableName, fileName, partition, dbInstance);
        updateCheckedFile(fileName, partitionType, dataBase, tableName, partition);
    }

    private void compareProcess(String type, AvroDataReader avroDataReader, String dataBase, String tableName, String fileName, String partition, String dbInstance) {
        if (partition != null && !"null".equals(partition)) {
            String avroPath = FilePathUtil.assembleFilePath(dataBase, tableName, fileName, partition, dbInstance, type);
            LOG.info("read avro from: " + avroPath);
            Map<String, Map<String, Long>> avroData = avroDataReader.readSrcData(avroPath);
            if (null != avroData && avroData.size() > 0) {
                Map<String, Long> createRecord = avroData.get(OperateType.Create.toString());
                Map<String, Long> updateRecord = avroData.get(OperateType.Update.toString());
                Map<String, Long> deleteRecord = avroData.get(OperateType.Delete.toString());

                CheckResult result = new CheckResult();
                result.setTableName(tableName);
                result.setPartitionType(type);
                result.setFilePartition(partition);
                result.setDataBase(dataBase);
                result.setFileName(fileName);
                result.setSaveTable(CheckTable.BINLOG_CHECK_HIVE_TABLE);
                result.setFilesPath(avroPath);
                result.setDbInstance(dbInstance);

                createRecordProcess(dataBase, tableName, createRecord, result);
                updateRecordProcess(dataBase, tableName, updateRecord, result);
                deleteRecordProcess(dataBase, tableName, deleteRecord, result);
            }
        }
    }

    private void updateCheckedFile(String file, String type, String dataBase, String tableName, String partition) {
        Map<String, Object> whereMap = new HashMap<>(2);
        whereMap.put(CheckTable.FILE_NAME, file);
        whereMap.put(CheckTable.PARTITION_TYPE, type);
        whereMap.put(CheckTable.DATA_BASE, dataBase);
        whereMap.put(CheckTable.TABLE_NAME, tableName);
        whereMap.put(CheckTable.FILE_PARTITION, partition);
        Map<String, Object> valueMap = new HashMap<>(1);
        valueMap.put(CheckTable.PROCESS_LOG_STATUS, 1);
        try {
            DBUtil.update(DBServer.DBServerType.MYSQL.toString(), CheckTable.BINLOG_DATABASE, CheckTable.BINLOG_PROCESS_LOG_TABLE, valueMap, whereMap);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    void deleteRecordProcess(String dataBase, String tableName, Map<String, Long> deleteRecord, CheckResult result) {
        Map<String, byte[]> deleteRecordFind = BatchGetFromHBase.compareWithHBase(dataBase, tableName, BatchGetFromHBase.assembleRowKey(deleteRecord));
        if (null != deleteRecordFind && deleteRecordFind.size() > 0) {
            Map<String, Long> rawDeleteRocord = new HashMap<>(deleteRecordFind.size());
            deleteRecordFind.forEach((key, value) -> rawDeleteRocord.put(key.split("_")[1], Bytes.toLong(value)));
            result.setOpType(OperateType.Delete.toString());
            LOG.info("the operateType is :[Delete]");
            if (rawDeleteRocord.size() > 0) {
                CheckDBUtil.resultInsert(result, rawDeleteRocord);
            } else {
                LOG.info("no record find in the hbase");
            }
        }
    }

    void updateRecordProcess(String dataBase, String tableName, Map<String, Long> updateRecord, CheckResult result) {
        Map<String, byte[]> updateRecordFind = BatchGetFromHBase.compareWithHBase(dataBase, tableName, BatchGetFromHBase.assembleRowKey(updateRecord));
        Map<String, Long> updateRecordNoFind;
        if (null != updateRecordFind && updateRecordFind.size() > 0) {
            Map<String, Long> updateTmp = new HashMap<>(updateRecordFind.size());
            updateRecordFind.forEach((key, value) -> updateTmp.put(key.split("_")[1], Bytes.toLong(value)));
            updateRecordNoFind = diffCompare(updateRecord, updateTmp);
            Map<String, Long> oldUpdateRecord = MapCompareUtil.compareByValue(updateTmp, updateRecord);
            if (null != oldUpdateRecord && oldUpdateRecord.size() > 0) {
                updateRecordNoFind.putAll(oldUpdateRecord);
            }
        } else {
            updateRecordNoFind = updateRecord;
        }
        LOG.info("the operateType is :[Update]");
        if (updateRecordNoFind != null && updateRecordNoFind.size() > 0) {
            result.setOpType(OperateType.Update.toString());
            CheckDBUtil.resultInsert(result, updateRecordNoFind);
        } else {
            LOG.info("all the records have find in the Hbase");
        }
    }

    void createRecordProcess(String dataBase, String tableName, Map<String, Long> createRecord, CheckResult result) {
        Map<String, byte[]> createRecordFind = BatchGetFromHBase.compareWithHBase(dataBase, tableName, BatchGetFromHBase.assembleRowKey(createRecord));
        Map<String, Long> createRecordNoFind;
        if (null != createRecordFind && createRecordFind.size() > 0) {
            Map<String, Long> createTmp = new HashMap<>(createRecordFind.size());
            createRecordFind.forEach((key, value) -> createTmp.put(key.split("_")[1], Bytes.toLong(value)));
            createRecordNoFind = diffCompare(createRecord, createTmp);
        } else {
            createRecordNoFind = createRecord;
        }
        result.setOpType(OperateType.Create.toString());
        LOG.info("the operateType is :[Create]");
        if (createRecordNoFind != null && createRecordNoFind.size() > 0) {
            CheckDBUtil.resultInsert(result, createRecordNoFind);
        } else {
            LOG.info("all the records have find in the hbase");
        }
    }

    public void specialCompare(String file, String type, String dataBaseName) {
        List<Map<String, Object>> partitionInfos = getCurrentPartitionInfo(file, type);
        if (null != partitionInfos && partitionInfos.size() > 0) {
            AvroDataReader avroDataReader = new AvroDataReader();
            for (Map<String, Object> partitionInfo : partitionInfos) {
                String dataBase = String.valueOf(partitionInfo.get(CheckTable.DATA_BASE));
                if (dataBaseName.equals(dataBase)) {
                    String tableName = String.valueOf(partitionInfo.get(CheckTable.TABLE_NAME));
                    String fileName = String.valueOf(partitionInfo.get(CheckTable.FILE_NAME));
                    String partition = String.valueOf(partitionInfo.get(CheckTable.FILE_PARTITION));
                    String dbInstance = String.valueOf(partitionInfo.get(CheckTable.DB_INSTANCE));
                    compareProcess(type, avroDataReader, dataBase, tableName, fileName, partition, dbInstance);
                    updateCheckedFile(file, type, dataBase, tableName, partition);
                }
            }
        }
    }


}
