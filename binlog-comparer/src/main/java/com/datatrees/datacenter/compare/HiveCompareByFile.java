package com.datatrees.datacenter.compare;

import com.datatrees.datacenter.core.utility.*;
import com.datatrees.datacenter.datareader.AvroDataReader;
import com.datatrees.datacenter.operate.OperateType;
import com.datatrees.datacenter.table.CheckResult;
import com.datatrees.datacenter.table.CheckTable;
import com.datatrees.datacenter.table.HBaseTableInfo;
import com.datatrees.datacenter.utility.BatchGetFromHBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class HiveCompareByFile extends BaseDataCompare {
    private static Logger LOG = LoggerFactory.getLogger(HiveCompareByFile.class);
    private static final String ROWKEY_SEP = "_";
    private static final String DATABASE_TABLE_UNION = ".";
    private static final String ROWKEY_POSTFIX = "_id";
    private static final String BANK_BILL = "bankbill";
    private static final String BANKBILL_ALIAS = "bill";
    private static final String ID_LIST_MAX = PropertiesUtility.defaultProperties().getProperty("ID_LIST_MAX", "1000");

    @Override
    public void binLogCompare(String file, String type) {
        List<Map<String, Object>> partitionInfos = getCurrentPartitionInfo(file, type);
        if (null != partitionInfos && partitionInfos.size() > 0) {
            AvroDataReader avroDataReader = new AvroDataReader();
            for (Map<String, Object> partitionInfo : partitionInfos) {

                String dataBase = String.valueOf(partitionInfo.get(CheckTable.DATA_BASE));
                String tableName = String.valueOf(partitionInfo.get(CheckTable.TABLE_NAME));

                String fileName = String.valueOf(partitionInfo.get(CheckTable.FILE_NAME));
                String partition = String.valueOf(partitionInfo.get(CheckTable.FILE_PARTITION));
                String dbInstance = String.valueOf(partitionInfo.get(CheckTable.DB_INSTANCE));

                if (partition != null && !"null".equals(partition)) {
                    String avroPath = assembleFilePath(dataBase, tableName, fileName, partition, dbInstance, type);
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
            updateCheckedFile(file, type);
        }
    }

    private void updateCheckedFile(String file, String type) {
        Map<String, Object> whereMap = new HashMap<>(2);
        whereMap.put(CheckTable.FILE_NAME, file);
        whereMap.put(CheckTable.PARTITION_TYPE, type);
        Map<String, Object> valueMap = new HashMap<>(1);
        valueMap.put(CheckTable.PROCESS_LOG_STATUS, 1);
        try {
            DBUtil.update(DBServer.DBServerType.MYSQL.toString(), CheckTable.BINLOG_DATABASE, CheckTable.BINLOG_PROCESS_LOG_TABLE, valueMap, whereMap);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    void deleteRecordProcess(String dataBase, String tableName, Map<String, Long> deleteRecord, CheckResult result) {
        Map<String, Long> deleteRecordFind = compareWithHBase(dataBase, tableName, assembleRowKey(deleteRecord));
        if (null != deleteRecordFind && deleteRecordFind.size() > 0) {
            Map<String, Long> rawDeleteRocord = new HashMap<>(deleteRecordFind.size());
            deleteRecordFind.forEach((key, value) -> rawDeleteRocord.put(key.split(ROWKEY_SEP)[1], value));
            result.setOpType(OperateType.Delete.toString());
            LOG.info("the operateType is :[Delete]");
            if (rawDeleteRocord != null && rawDeleteRocord.size() > 0) {
                resultInsert(result, rawDeleteRocord);
            } else {
                LOG.info("no record find in the hbase");
            }
        }
    }

    void updateRecordProcess(String dataBase, String tableName, Map<String, Long> updateRecord, CheckResult result) {
        Map<String, Long> updateRecordFind = compareWithHBase(dataBase, tableName, assembleRowKey(updateRecord));
        Map<String, Long> updateRecordNoFind;
        if (null != updateRecordFind && updateRecordFind.size() > 0) {
            Map<String, Long> updateTmp = new HashMap<>(updateRecordFind.size());
            updateRecordFind.forEach((key, value) -> updateTmp.put(key.split(ROWKEY_SEP)[1], value));
            updateRecordNoFind = diffCompare(updateRecord, updateTmp);
            Map<String, Long> oldUpdateRecord = compareByValue(updateTmp, updateRecord);
            if (null != oldUpdateRecord && oldUpdateRecord.size() > 0) {
                updateRecordNoFind.putAll(oldUpdateRecord);
            }
        } else {
            updateRecordNoFind = updateRecord;
        }
        result.setOpType(OperateType.Update.toString());
        LOG.info("the operateType is :[Update]");
        if (updateRecordNoFind != null && updateRecordNoFind.size() > 0) {
            resultInsert(result, updateRecordNoFind);
        } else {
            LOG.info("all the records have find in the Hbase");
        }
    }

    void createRecordProcess(String dataBase, String tableName, Map<String, Long> createRecord, CheckResult result) {
        Map<String, Long> createRecordFind = compareWithHBase(dataBase, tableName, assembleRowKey(createRecord));
        Map<String, Long> createRecordNoFind;
        if (null != createRecordFind && createRecordFind.size() > 0) {
            Map<String, Long> createTmp = new HashMap<>(createRecordFind.size());
            createRecordFind.forEach((key, value) -> createTmp.put(key.split(ROWKEY_SEP)[1], value));
            createRecordNoFind = diffCompare(createRecord, createTmp);
        } else {
            createRecordNoFind = createRecord;
        }
        result.setOpType(OperateType.Create.toString());
        LOG.info("the operateType is :[Create]");
        if (createRecordNoFind != null && createRecordNoFind.size() > 0) {
            resultInsert(result, createRecordNoFind);
        } else {
            LOG.info("all the records have find in the hbase");
        }
    }

    String assembleFilePath(String dataBase, String tableName, String fileName, String partition, String dbInstance, String type) {
        String filePath;
        String idcHdfsPath = AVRO_HDFS_PATH + File.separator + type;
        String aliyunHdfsPath = AVRO_HDFS_PATH.split("_")[0] + File.separator + type;
        String filePathWithoutDbInstance = dataBase +
                File.separator +
                tableName +
                File.separator +
                partition +
                File.separator +
                fileName +
                CheckTable.FILE_LAST_NAME;
        if (IpMatchUtility.isboolIp(dbInstance)) {
            filePath = idcHdfsPath + File.separator + filePathWithoutDbInstance;
        } else {
            filePath = aliyunHdfsPath + File.separator + dbInstance + File.separator + filePathWithoutDbInstance;
        }
        return filePath;
    }

    @Deprecated
    private List<String> assembleRowKey(String dataBase, String tableName, Map<String, Long> recordMap) {
        if (null != recordMap && recordMap.size() > 0) {
            return recordMap.keySet().stream().map(x -> (x + ROWKEY_SEP + dataBase + DATABASE_TABLE_UNION + tableName)).collect(Collectors.toList());
        } else {
            return null;
        }
    }

    private List<String> assembleRowKey(Map<String, Long> recordMap) {
        if (null != recordMap && recordMap.size() > 0) {
            List<String> rowKeyList = new ArrayList<>(recordMap.keySet());
            rowKeyList = BatchGetFromHBase.reHashRowKey(rowKeyList);
            return rowKeyList;
        } else {
            return null;
        }
    }

    @Deprecated
    private Map<String, Long> compareWithHBase(List<String> createIdList) {
        if (null != createIdList && createIdList.size() > 0) {
            return BatchGetFromHBase.parrallelBatchSearch(createIdList, HBaseTableInfo.TABLE_NAME, HBaseTableInfo.COLUMNFAMILY, HBaseTableInfo.LAST_UPDATE_TIME);
        }
        return null;
    }

    private Map<String, Long> compareWithHBase(String dataBase, String tableName, List<String> createIdList) {
        if (null != createIdList && createIdList.size() > 0) {
            LOG.info("search data from HBase...");
            if (BANK_BILL.equals(dataBase)) {
                return BatchGetFromHBase.parrallelBatchSearch(createIdList, BANKBILL_ALIAS + DATABASE_TABLE_UNION + tableName + ROWKEY_POSTFIX, HBaseTableInfo.COLUMNFAMILY, HBaseTableInfo.LAST_UPDATE_TIME);
            } else {
                return BatchGetFromHBase.parrallelBatchSearch(createIdList, dataBase + DATABASE_TABLE_UNION + tableName + ROWKEY_POSTFIX, HBaseTableInfo.COLUMNFAMILY, HBaseTableInfo.LAST_UPDATE_TIME);
            }
        }
        return null;
    }

    /**
     * 将具有相同key的数据根据value值的大小进行比较
     *
     * @param srcMap  需要比较的Map
     * @param destMap 被比较的Map
     * @return srcMap value比较大的记录
     */
    private Map<String, Long> compareByValue(Map<String, Long> srcMap, Map<String, Long> destMap) {
        Map<String, Long> resultMap = new HashMap<>();
        Map<String, Long> sameIdMap = this.retainCompareHive(srcMap, destMap);
        for (String key : sameIdMap.keySet()) {
            Long srcLastTime = srcMap.get(key);
            Long destLastTime = sameIdMap.get(key);
            if (srcLastTime > destLastTime) {
                resultMap.put(key, srcLastTime);
            }
        }
        return resultMap;
    }

    /**
     * 比较两个map,返回key 相同的element
     *
     * @param srcMap  需要比较的Map
     * @param destMap 被比较的Map
     * @return srcMap与destMap 共存项目
     */
    private Map<String, Long> retainCompareHive(Map<String, Long> srcMap, Map<String, Long> destMap) {
        Set<String> set1 = srcMap.keySet();
        Set<String> set2 = destMap.keySet();
        Map<String, Long> diffMaps = new HashMap<>();
        if (set1.retainAll(set2)) {
            for (String key : set1) {
                diffMaps.put(key, srcMap.get(key));
            }
        }
        return diffMaps;
    }

    private static void resultInsert(CheckResult result, Map<String, Long> afterComp) {

        if (afterComp != null && afterComp.size() > 0) {
            int dataSize = afterComp.size();
            LOG.info("record size is :" + dataSize);
            Map<String, Object> dataMap = new HashMap<>(11);
            dataMap.put(CheckTable.FILE_NAME, result.getFileName());
            dataMap.put(CheckTable.DB_INSTANCE, result.getDbInstance());
            dataMap.put(CheckTable.DATA_BASE, result.getDataBase());
            dataMap.put(CheckTable.TABLE_NAME, result.getTableName());
            dataMap.put(CheckTable.PARTITION_TYPE, result.getPartitionType());
            dataMap.put(CheckTable.FILE_PARTITION, result.getFilePartition());
            dataMap.put(CheckTable.OP_TYPE, result.getOpType());
            dataMap.put(CheckTable.FILES_PATH, result.getFilesPath());
            dataMap.put(CheckTable.DATA_COUNT, dataSize);
            int max_size = Integer.parseInt(ID_LIST_MAX);
            if (dataSize < Integer.parseInt(ID_LIST_MAX)) {
                dataMap.put(CheckTable.ID_LIST, afterComp.keySet().toString());
            } else {
                List<String> idList = new ArrayList<>(afterComp.keySet());
                LOG.info("the record size is :" + dataSize);
                dataMap.put(CheckTable.ID_LIST, idList.subList(0, max_size + 1).toString());
                LOG.info("insert size of database is :" + max_size);
            }
            long currentTime = System.currentTimeMillis();
            dataMap.put(CheckTable.LAST_UPDATE_TIME, TimeUtil.stampToDate(currentTime));
            try {
                DBUtil.insert(DBServer.DBServerType.MYSQL.toString(), CheckTable.BINLOG_DATABASE, result.getSaveTable(), dataMap);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } else {
            LOG.info("no error record find from : " + result.getDataBase() + DATABASE_TABLE_UNION + result.getTableName());
        }
    }

}
