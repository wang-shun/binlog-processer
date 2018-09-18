package com.datatrees.datacenter.compare;

import com.datatrees.datacenter.core.utility.DBServer;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.core.utility.TimeUtil;
import com.datatrees.datacenter.datareader.AvroDataReader;
import com.datatrees.datacenter.operate.OperateType;
import com.datatrees.datacenter.table.CheckResult;
import com.datatrees.datacenter.table.CheckTable;
import com.datatrees.datacenter.table.FieldNameOp;
import com.datatrees.datacenter.table.HBaseTableInfo;
import com.datatrees.datacenter.utility.BatchGetFromHBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class HiveCompare extends BaseDataCompare {
    private static Logger LOG = LoggerFactory.getLogger(HiveCompare.class);
    private Properties properties = PropertiesUtility.defaultProperties();
    private List<String> idList = FieldNameOp.getConfigField("id");
    private List<String> createTimeList = FieldNameOp.getConfigField("update");

    @Override
    public void binLogCompare(String file, String type) {
        //查看当前binlog解析出来的文件分区文件数目和文件条数是否达到了数量要求
        List<Map<String, Object>> partitionInfos = getCurrentPartitionInfo(file, type);
        if (null != partitionInfos && partitionInfos.size() > 0) {
            AvroDataReader avroDataReader = new AvroDataReader();
            for (Map<String, Object> partitionInfo : partitionInfos) {

                String dataBase = String.valueOf(partitionInfo.get(CheckTable.DATA_BASE));
                String tableName = String.valueOf(partitionInfo.get(CheckTable.TABLE_NAME));
                String fileName = String.valueOf(partitionInfo.get(CheckTable.FILE_NAME));
                String partition = String.valueOf(partitionInfo.get(CheckTable.FILE_PARTITION));
                String dbInstance = String.valueOf(partitionInfo.get(CheckTable.DB_INSTANCE));
                Collection<Object> allFieldSet = FieldNameOp.getAllFieldName(dataBase, tableName);
                String recordId = FieldNameOp.getFieldName(allFieldSet, idList);
                String recordLastUpdateTime = FieldNameOp.getFieldName(allFieldSet, createTimeList);

                if (null != recordId && null != recordLastUpdateTime) {
                    String filePath = dbInstance +
                            File.separator +
                            dataBase +
                            File.separator +
                            tableName +
                            File.separator +
                            partition +
                            File.separator +
                            fileName.replace(".tar", "") +
                            ".avro";

                    String avroPath = super.AVRO_HDFS_PATH + File.separator + type + File.separator + filePath;
                    avroDataReader.setRecordId(recordId);
                    avroDataReader.setRecordLastUpdateTime(recordLastUpdateTime);
                    Map<String, Map<String, Long>> avroData = avroDataReader.readSrcData(avroPath);

                    Map<String, Long> createRecord = avroData.get(OperateType.Create.toString());
                    Map<String, Long> updateRecord = avroData.get(OperateType.Update.toString());
                    Map<String, Long> deleteRecord = avroData.get(OperateType.Delete.toString());

                    Map<String, Long> createRecordFind = compareWithHBase(dataBase, tableName, assembleRowKey(createRecord));
                    Map<String, Long> updateRecordFind = compareWithHBase(dataBase, tableName, assembleRowKey(updateRecord));
                    Map<String, Long> deleteRecordFind = compareWithHBase(dataBase, tableName, assembleRowKey(deleteRecord));


                    Map<String, Long> createRecordNoFind;
                    Map<String, Long> updateRecordNoFind;

                    if (null != createRecordFind && createRecordFind.size() > 0) {
                        Map<String, Long> createTmp = new HashMap<>();
                        createRecordFind.forEach((key, value) -> createTmp.put(key.split("_")[1], value));
                        createRecordNoFind = diffCompare(createRecord, createTmp);
                    } else {
                        createRecordNoFind = createRecord;
                    }

                    if (null != updateRecordFind && updateRecordFind.size() > 0) {
                        Map<String, Long> updateTmp = new HashMap<>();
                        updateRecordFind.forEach((key, value) -> updateTmp.put(key.split("_")[1], value));
                        Map<String, Long> tmp = updateTmp;
                        updateRecordNoFind = diffCompare(updateRecord, updateTmp);
                        Map<String, Long> oldUpdateRecord = compareByValue(tmp, updateRecord);
                        if (null != oldUpdateRecord && oldUpdateRecord.size() > 0) {
                            updateRecordNoFind.putAll(oldUpdateRecord);
                        }
                    } else {
                        updateRecordNoFind = updateRecord;
                    }

                    CheckResult result = new CheckResult();
                    result.setTableName(tableName);
                    result.setPartitionType(type);
                    result.setFilePartition(partition);
                    result.setDataBase(dataBase);
                    result.setFileName(fileName);
                    result.setSaveTable(CheckTable.BINLOG_CHECK_HIVE_TABLE);
                    result.setFilesPath(filePath);
                    result.setDbInstance(dbInstance);
                    result.setOpType(OperateType.Create.toString());
                    resultInsert(result, createRecordNoFind, CheckTable.BINLOG_CHECK_HIVE_TABLE);
                    result.setOpType(OperateType.Update.toString());
                    resultInsert(result, updateRecordNoFind, CheckTable.BINLOG_CHECK_HIVE_TABLE);
                    result.setOpType(OperateType.Delete.toString());
                    resultInsert(result, deleteRecordFind, CheckTable.BINLOG_CHECK_HIVE_TABLE);
                }
            }
            Map<String, Object> whereMap = new HashMap<>(2);
            whereMap.put(CheckTable.FILE_NAME, file);
            whereMap.put(CheckTable.PARTITION_TYPE, type);
            Map<String, Object> valueMap = new HashMap<>();
            valueMap.put(CheckTable.PROCESS_LOG_STATUS, 1);
            try {
                DBUtil.update(DBServer.DBServerType.MYSQL.toString(), CheckTable.BINLOG_DATABASE, CheckTable.BINLOG_PROCESS_LOG_TABLE, valueMap, whereMap);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private List<String> assembleRowKey(String dataBase, String tableName, Map<String, Long> recordMap) {
        if (null != recordMap && recordMap.size() > 0) {
            return recordMap.keySet().stream().map(x -> (x + "_" + dataBase + "." + tableName)).collect(Collectors.toList());
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

    private Map<String, Long> compareWithHBase(List<String> createIdList) {
        if (null != createIdList && createIdList.size() > 0) {
            return BatchGetFromHBase.parrallelBatchSearch(createIdList, HBaseTableInfo.TABLE_NAME, HBaseTableInfo.COLUMNFAMILY, HBaseTableInfo.LAST_UPDATE_TIME);
        }
        return null;
    }

    private Map<String, Long> compareWithHBase(String dataBase, String tableName, List<String> createIdList) {
        if (null != createIdList && createIdList.size() > 0) {
            return BatchGetFromHBase.parrallelBatchSearch(createIdList, dataBase + "." + tableName + "_id", HBaseTableInfo.COLUMNFAMILY, HBaseTableInfo.LAST_UPDATE_TIME);
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
    public Map<String, Long> retainCompareHive(Map<String, Long> srcMap, Map<String, Long> destMap) {
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

    private static void resultInsert(CheckResult result, Map<String, Long> afterComp, String tableName) {

        if (afterComp != null && afterComp.size() > 0) {
            Map<String, Object> dataMap = new HashMap<>();
            dataMap.put(CheckTable.FILE_NAME, result.getFileName());
            dataMap.put(CheckTable.DB_INSTANCE, result.getDbInstance());
            dataMap.put(CheckTable.DATA_BASE, result.getDataBase());
            dataMap.put(CheckTable.TABLE_NAME, result.getTableName());
            dataMap.put(CheckTable.PARTITION_TYPE, result.getPartitionType());
            dataMap.put(CheckTable.FILE_PARTITION, result.getFilePartition());
            dataMap.put(CheckTable.OP_TYPE, result.getOpType());
            dataMap.put(CheckTable.ID_LIST, afterComp.keySet().toString());
            dataMap.put(CheckTable.FILES_PATH, result.getFilesPath());
            dataMap.put(CheckTable.DATA_COUNT, afterComp.size());
            long currentTime = System.currentTimeMillis();
            dataMap.put(CheckTable.LAST_UPDATE_TIME, TimeUtil.stampToDate(currentTime));
            try {
                DBUtil.insert(DBServer.DBServerType.MYSQL.toString(), CheckTable.BINLOG_DATABASE, tableName, dataMap);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } else {
            LOG.info("no error record find from : " + result.getDataBase() + "." + result.getTableName());
        }
    }

}
