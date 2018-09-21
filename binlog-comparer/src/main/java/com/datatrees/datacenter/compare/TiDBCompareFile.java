package com.datatrees.datacenter.compare;

import com.datatrees.datacenter.core.utility.*;
import com.datatrees.datacenter.datareader.AvroDataReader;
import com.datatrees.datacenter.operate.OperateType;
import com.datatrees.datacenter.table.CheckResult;
import com.datatrees.datacenter.table.CheckTable;
import com.datatrees.datacenter.table.FieldNameOp;
import com.google.common.collect.Lists;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class TiDBCompareFile extends BaseDataCompare {
    private static Logger LOG = LoggerFactory.getLogger(TiDBCompareFile.class);
    private static Properties properties = PropertiesUtility.defaultProperties();
    private final String sampleFlag = properties.getProperty("SAMPLE_FLAG", "no");
    private static String binLogDataBase = properties.getProperty("jdbc.database", "binlog");
    private List<String> idList = FieldNameOp.getConfigField("id");
    private List<String> createTimeList = FieldNameOp.getConfigField("update");
    String recordId;
    String recordLastUpdateTime;
    String partitionType;

    @Override
    public void binLogCompare(String fileName, String type) {
        this.partitionType = type;
        List<Map<String, Object>> tableInfo = getCurrentTableInfo(fileName, type);
        if (tableInfo != null && tableInfo.size() > 0) {
            dataCheck(tableInfo);
            Map<String, Object> whereMap = new HashMap<>(1);
            whereMap.put(CheckTable.FILE_NAME, fileName);
            whereMap.put(CheckTable.PARTITION_TYPE, partitionType);
            Map<String, Object> valueMap = new HashMap<>(1);
            valueMap.put(CheckTable.PROCESS_LOG_STATUS, 1);
            try {
                DBUtil.update(DBServer.DBServerType.MYSQL.toString(), binLogDataBase, CheckTable.BINLOG_PROCESS_LOG_TABLE, valueMap, whereMap);
                LOG.info("compare finished !");
            } catch (SQLException e) {
                LOG.info("change status from 0 to 1 failed of file: " + fileName);
            }
        } else {
            Log.info("no file find need to be check");
        }
    }

    public void dataCheck(List<Map<String, Object>> tableInfo) {
        if (null != tableInfo) {
            for (Map<String, Object> recordMap : tableInfo) {
                String dataBase = String.valueOf(recordMap.get(CheckTable.DATA_BASE));
                String tableName = String.valueOf(recordMap.get(CheckTable.TABLE_NAME));
                String[] partitions = String.valueOf(recordMap.get("partitions")).split(",");
                String dbInstance = String.valueOf(recordMap.get(CheckTable.DB_INSTANCE));
                String fileName = String.valueOf(recordMap.get(CheckTable.FILE_NAME));
                if (partitions.length > 0) {
                    Collection<Object> allFieldSet = FieldNameOp.getAllFieldName(dataBase, tableName);
                    recordId = FieldNameOp.getFieldName(allFieldSet, idList);
                    recordLastUpdateTime = FieldNameOp.getFieldName(allFieldSet, createTimeList);
                    if (null != recordId && null != recordLastUpdateTime) {
                        List<Map<String, Object>> mapList = getOneRecordFromTable(dataBase, tableName);
                        Map<String, Long> allCreate = new HashMap<>();
                        Map<String, Long> allUpdate = new HashMap<>();
                        Map<String, Long> allDelete = new HashMap<>();
                        AvroDataReader avroDataReader = new AvroDataReader();
                        List<String> partitionList;
                        partitionList = Arrays.asList(partitions);
                        partitionList.remove(null);

                        CheckResult checkResult = new CheckResult();
                        checkResult.setDataBase(dataBase);
                        checkResult.setTableName(tableName);
                        checkResult.setDbInstance(dbInstance);
                        checkResult.setFilePartition(Arrays.toString(partitions));
                        checkResult.setPartitionType(this.partitionType);
                        checkResult.setSaveTable(CheckTable.BINLOG_CHECK_TABLE);
                        checkResult.setFileName(fileName);
                        avroDataReader.setDataBase(dataBase);
                        avroDataReader.setTableName(tableName);
                        avroDataReader.setRecordId(recordId);
                        avroDataReader.setRecordLastUpdateTime(recordLastUpdateTime);
                        for (String partition : partitionList) {
                            String partitionPath = assembleFilePath(dataBase, tableName, dbInstance, partition);
                            String filePath = partitionPath +
                                    File.separator +
                                    fileName.replace(".tar", "")
                                    + CheckTable.FILE_LAST_NAME;
                            Map<String, Map<String, Long>> avroData = avroDataReader.readSrcData(filePath);
                            if (null != avroData && avroData.size() > 0) {
                                Map<String, Long> create = avroData.get(OperateType.Create.toString());
                                Map<String, Long> update = avroData.get(OperateType.Update.toString());
                                Map<String, Long> delete = avroData.get(OperateType.Delete.toString());
                                if (null != create && create.size() > 0) {
                                    allCreate.putAll(create);
                                }
                                if (null != update) {
                                    allUpdate.putAll(update);
                                }
                                if (null != delete) {
                                    allDelete.putAll(delete);
                                }
                            }
                        }
                        allCreate = BaseDataCompare.diffCompare(allCreate, allUpdate);
                        allCreate = BaseDataCompare.diffCompare(allCreate, allDelete);
                        allUpdate = BaseDataCompare.diffCompare(allUpdate, allDelete);
                        checkEvent(mapList, allCreate, allUpdate, allDelete, checkResult);
                    }
                }
            }
        }
    }

    private String assembleFilePath(String dataBase, String tableName, String dbInstance, String partition) {
        String partitionPath;
        if (!IpMatchUtility.isboolIp(dbInstance)) {
            partitionPath = super.AVRO_HDFS_PATH +
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

        } else {
            partitionPath = super.AVRO_HDFS_PATH +
                    File.separator +
                    partitionType +
                    File.separator +
                    dataBase +
                    File.separator +
                    tableName +
                    File.separator +
                    partition;
        }
        return partitionPath;
    }

    /**
     * 根据DDL类型分类检测
     *
     * @param mapList     表中是否有数据
     * @param allCreate   所有插入操作
     * @param allUpdate   所有更行操作
     * @param allDelete   所有删除操作
     * @param checkResult 检测结果
     */
    void checkEvent(List<Map<String, Object>> mapList, Map<String, Long> allCreate, Map<String, Long> allUpdate, Map<String, Long> allDelete, CheckResult checkResult) {
        String saveTable = checkResult.getSaveTable();
        if (mapList != null && mapList.size() > 0) {
            if (allCreate != null && allCreate.size() > 0) {
                checkResult.setOpType(OperateType.Create.toString());
                checkAndSaveErrorData(checkResult, allCreate, OperateType.Create, saveTable);
            }
            if (allUpdate != null && allUpdate.size() > 0) {
                checkResult.setOpType(OperateType.Update.toString());
                checkAndSaveErrorData(checkResult, allUpdate, OperateType.Update, saveTable);
            }
            if (allDelete.size() > 0) {
                checkResult.setOpType(OperateType.Delete.toString());
                checkAndSaveErrorData(checkResult, allDelete, OperateType.Delete, saveTable);
            }
        } else {
            if (allCreate != null && allCreate.size() > 0) {
                checkResult.setOpType(OperateType.Create.toString());
                resultInsert(checkResult, allCreate, saveTable);
            }
            if (allUpdate != null && allUpdate.size() > 0) {
                checkResult.setOpType(OperateType.Update.toString());
                resultInsert(checkResult, allUpdate, saveTable);
            }
            if (allDelete.size() > 0) {
                checkResult.setOpType(OperateType.Delete.toString());
                resultInsert(checkResult, allDelete, saveTable);
            }
        }
    }

    /**
     * 具体的检测操作并将查询得到的错误数据记录到数据库
     *
     * @param checkResult 结果对象
     * @param dataMap     待抽样数据
     */
    private void checkAndSaveErrorData(CheckResult checkResult, Map<String, Long> dataMap, OperateType op, String saveTable) {
        if (null != dataMap) {
            List<Map.Entry<String, Long>> sampleData = dataSample(dataMap);
            LOG.info("the number of sampled data is ：[" + sampleData.size() + "], the operate type is ：[" + op.toString() + "]");
            List<List<Map.Entry<String, Long>>> splitData = Lists.partition(sampleData, 10000);
            if (OperateType.Create.equals(op)) {
                Map<String, Long> allNoFoundCreate = new HashMap<>();
                for (List<Map.Entry<String, Long>> oneSplit : splitData) {
                    Map<String, Long> afterCompare = batchCheck(checkResult.getDataBase(), checkResult.getTableName(), oneSplit, op);
                    Map<String, Long> oneSplitMap = oneSplit.stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                    Map<String, Long> createNotFound = BaseDataCompare.diffCompare(oneSplitMap, afterCompare);
                    if (createNotFound != null) {
                        allNoFoundCreate.putAll(createNotFound);
                    }
                }
                resultInsert(checkResult, allNoFoundCreate, saveTable);
            } else if (op.equals(OperateType.Update)) {
                Map<String, Long> allUpdate = new HashMap<>();
                for (List<Map.Entry<String, Long>> oneSplit : splitData) {
                    Map<String, Long> afterCompare = batchCheck(checkResult.getDataBase(), checkResult.getTableName(), oneSplit, op);
                    Map<String, Long> oneSplitMap = oneSplit.stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                    Map<String, Long> existData = BaseDataCompare.diffCompare(oneSplitMap, afterCompare);
                    allUpdate.putAll(afterCompare);
                    checkAndSaveErrorData(checkResult, existData, OperateType.Create, CheckTable.BINLOG_CHECK_DATE_TABLE);
                }
                resultInsert(checkResult, allUpdate, saveTable);
            } else if (op.equals(OperateType.Delete)) {
                Map<String, Long> allDelete = new HashMap<>();
                for (List<Map.Entry<String, Long>> oneSplit : splitData) {
                    Map<String, Long> afterCompare = batchCheck(checkResult.getDataBase(), checkResult.getTableName(), oneSplit, op);
                    allDelete.putAll(afterCompare);
                }
                resultInsert(checkResult, allDelete, saveTable);
            }
        }

    }

    /**
     * 通过查询目标库进行检查
     *
     * @param dataBase   数据库
     * @param tableName  表
     * @param sampleData 采样后的数据
     */
    private Map<String, Long> batchCheck(String dataBase, String tableName, List<Map.Entry<String, Long>> sampleData, OperateType op) {
        Map<String, Long> allCheckDataMap = null;
        Map<String, Long> checkDataMap = null;
        List<Map<String, Object>> resultList;
        List<String> assembleSql = assembleSql(sampleData, op, dataBase, tableName);
        if (assembleSql != null && assembleSql.size() > 0) {
            allCheckDataMap = new HashMap<>(sampleData.size());
            for (String splitSql : assembleSql) {
                try {
                    resultList = DBUtil.query(DBServer.DBServerType.TIDB.toString(), dataBase, splitSql);
                    if (null != resultList && resultList.size() > 0) {
                        checkDataMap = new HashMap<>();
                        for (Map<String, Object> errorRecord : resultList) {
                            String recordId = errorRecord.get(this.recordId).toString();
                            long upDateTime = (Long) errorRecord.get("avroTime") * 1000;
                            checkDataMap.put(recordId, upDateTime);
                        }
                    }
                } catch (Exception e) {
                    LOG.info(e.getMessage(), e);
                }
                if (null != checkDataMap) {
                    allCheckDataMap.putAll(checkDataMap);
                }
            }
        }
        return allCheckDataMap;
    }

    private List<String> assembleSql(List<Map.Entry<String, Long>> sampleData, OperateType op, String dataBase, String tableName) {
        List<String> assembleSql = null;
        StringBuilder sql;
        if (null != sampleData && sampleData.size() > 0) {
            assembleSql = new ArrayList<>();
            List<List<Map.Entry<String, Long>>> splitSampleData = Lists.partition(sampleData, 5000);
            switch (op) {
                case Update: {
                    for (List<Map.Entry<String, Long>> aSplitSampleData : splitSampleData) {
                        sql = new StringBuilder();
                        for (int j = 0; j < aSplitSampleData.size(); j++) {
                            Map.Entry record = aSplitSampleData.get(j);
                            long timeStamp = (Long) record.getValue() / 1000;
                            sql.append("select ")
                                    .append(recordId)
                                    .append(" , ")
                                    .append("UNIX_TIMESTAMP(")
                                    .append(recordLastUpdateTime)
                                    .append(")")
                                    .append(" as time_stamp ")
                                    .append(",")
                                    .append(timeStamp)
                                    .append(" as avroTime ")
                                    .append(" from ")
                                    .append("`")
                                    .append(dataBase)
                                    .append("`")
                                    .append(".")
                                    .append(tableName)
                                    .append(" where ")
                                    .append(recordId)
                                    .append("=")
                                    .append("'")
                                    .append(record.getKey())
                                    .append("'")
                                    .append(" and ")
                                    .append("UNIX_TIMESTAMP(")
                                    .append(recordLastUpdateTime)
                                    .append(")")
                                    .append("<")
                                    .append(timeStamp);
                            if (j < aSplitSampleData.size() - 1) {
                                sql.append(" union ");
                            }
                        }
                        assembleSql.add(sql.toString());
                    }
                    break;
                }
                case Create:
                case Delete: {
                    for (List<Map.Entry<String, Long>> aSplitSampleData : splitSampleData) {
                        sql = new StringBuilder();
                        for (int j = 0; j < aSplitSampleData.size(); j++) {
                            Map.Entry record = aSplitSampleData.get(j);
                            long timeStamp = (Long) record.getValue() / 1000;
                            sql.append("select ")
                                    .append(recordId)
                                    .append(",")
                                    .append(timeStamp)
                                    .append(" as avroTime ")
                                    .append(" from ")
                                    .append("`")
                                    .append(dataBase)
                                    .append("`")
                                    .append(".")
                                    .append(tableName)
                                    .append(" where ")
                                    .append(recordId)
                                    .append("=")
                                    .append(record.getKey());
                            if (j < aSplitSampleData.size() - 1) {
                                sql.append(" union ");
                            }
                        }
                        assembleSql.add(sql.toString());
                    }
                    break;
                }
                case Unique:
                    break;
                default:
            }
        }
        return assembleSql;
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
                DBUtil.insert(DBServer.DBServerType.MYSQL.toString(), binLogDataBase, tableName, dataMap);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } else {
            LOG.info("no error record find from : " + result.getDataBase() + "." + result.getTableName());
        }
    }

    /**
     * sample data
     *
     * @param operateRecordMap 待采样的数据
     * @return List 采样结果
     */
    private List<Map.Entry<String, Long>> dataSample(Map<String, Long> operateRecordMap) {
        List<Map.Entry<String, Long>> dataList = mapToList(operateRecordMap);
        return randomSample(dataList);
    }

    /**
     * sample date
     *
     * @param lst data sets to be sampled
     * @return sampled data
     */
    private List<Map.Entry<String, Long>> randomSample(List<Map.Entry<String, Long>> lst) {
        List<Map.Entry<String, Long>> copy = new ArrayList<>(lst);
        int dataSize = lst.size();
        int n = 0;
        String sampleDefault = "yes";
        if (sampleDefault.equalsIgnoreCase(sampleFlag)) {
            if (dataSize > 0) {
                Collections.shuffle(copy);
                n = (int) Math.sqrt(dataSize);
            }
            return copy.subList(0, n);
        } else {
            return lst;
        }
    }

    /**
     * convert Map to List
     *
     * @param dataMap data saved with hashMap
     * @return list
     */
    private List<Map.Entry<String, Long>> mapToList(Map<String, Long> dataMap) {
        List<Map.Entry<String, Long>> dataList = new ArrayList<>();
        if (null != dataMap) {
            dataList.addAll(dataMap.entrySet());
        }
        return dataList;
    }

    /**
     * get one record from database.tablename
     *
     * @param dataBase  database
     * @param tableName table
     * @return the record
     */
    List<Map<String, Object>> getOneRecordFromTable(String dataBase, String tableName) {
        String tableFieldSql = "select * from " + "`" + dataBase + "`." + tableName + " limit 1";
        List<Map<String, Object>> mapList = null;
        try {
            mapList = DBUtil.query(DBServer.DBServerType.TIDB.toString(), dataBase, tableFieldSql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return mapList;
    }
}
