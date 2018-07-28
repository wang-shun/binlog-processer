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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class TiDBCompare extends BaseDataCompare {
    private static Logger LOG = LoggerFactory.getLogger(TiDBCompare.class);
    private Properties properties = PropertiesUtility.defaultProperties();
    private final String sampleFlag = properties.getProperty("SAMPLE_FLAG");
    private final String sampleDefalut = "yes";
    private String binLogDataBase = properties.getProperty("jdbc.database");
    private List<String> idList = FieldNameOp.getConfigField("id");
    private List<String> createTimeList = FieldNameOp.getConfigField("update");
    String recordId;
    String recordLastUpdateTime;
    private String partitionType;

    @Override
    public void binLogCompare(String fileName, String type) {
        partitionType = type;
        List<Map<String, Object>> tableInfo = getCurrentTableInfo(fileName, type);
        dataCheck(tableInfo);
        Map<String, Object> whereMap = new HashMap<>(1);
        whereMap.put(CheckTable.FILE_NAME, fileName);
        Map<String, Object> valueMap = new HashMap<>(1);
        valueMap.put(CheckTable.PROCESS_LOG_STATUS, 1);
        try {
            DBUtil.update(DBServer.DBServerType.MYSQL.toString(), binLogDataBase, CheckTable.BINLOG_PROCESS_LOG_TABLE, valueMap, whereMap);
            LOG.info("compare finished !");
        } catch (SQLException e) {
            LOG.info("change status from 0 to 1 failed of file: " + fileName);
        }
    }

    public void dataCheck(List<Map<String, Object>> tableInfo) {
        if (null != tableInfo) {
            for (Map<String, Object> recordMap : tableInfo) {
                // FIXME: 2018/7/27 Map value 需要判空
                String dataBase = String.valueOf(recordMap.get(CheckTable.DATA_BASE));
                String tableName = String.valueOf(recordMap.get(CheckTable.TABLE_NAME));
                String[] partitions = String.valueOf(recordMap.get("partitions")).split(",");
                String dbInstance = String.valueOf(recordMap.get(CheckTable.DB_INSTANCE));
                String fileName = String.valueOf(recordMap.get(CheckTable.FILE_NAME));
                recordId = FieldNameOp.getFieldName(dataBase, tableName, idList);
                recordLastUpdateTime = FieldNameOp.getFieldName(dataBase, tableName, createTimeList);
                if (null != recordId && null != recordLastUpdateTime) {
                    if (partitions.length > 0) {
                        Map<String, Long> allUniqueData = new HashMap<>();
                        Map<String, Long> allDeleteData = new HashMap<>();
                        AvroDataReader avroDataReader = new AvroDataReader();
                        for (String partition : partitions) {
                            avroDataReader.setDataBase(dataBase);
                            avroDataReader.setTableName(tableName);
                            avroDataReader.setRecordId(recordId);
                            avroDataReader.setRecordLastUpdateTime(recordLastUpdateTime);
                            String filePath = super.AVRO_HDFS_PATH +
                                    File.separator +
                                    partitionType +
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
                            if (null != avroData) {
                                Map<String, Long> unique = avroData.get(OperateType.Unique.toString());
                                Map<String, Long> delete = avroData.get(OperateType.Delete.toString());
                                if (null != unique) {
                                    allUniqueData.putAll(unique);
                                }
                                if (null != delete) {
                                    allDeleteData.putAll(delete);
                                }
                            }
                        }
                        Map<String, Long> filterDeleteMap = diffCompare(allUniqueData, allDeleteData);
                        CheckResult checkResult = new CheckResult();
                        checkResult.setDataBase(dataBase);
                        checkResult.setTableName(tableName);
                        checkResult.setFileName(fileName);
                        checkResult.setDbInstance(dbInstance);
                        checkResult.setOpType(OperateType.Unique.toString());
                        checkAndSaveErrorData(checkResult, filterDeleteMap, OperateType.Unique, CheckTable.BINLOG_CHECK_TABLE);
                        checkResult.setOpType(OperateType.Delete.toString());
                        checkResult.setFilePartition(Arrays.toString(partitions));
                        checkAndSaveErrorData(checkResult, allDeleteData, OperateType.Delete, CheckTable.BINLOG_CHECK_DATE_TABLE);

                    }
                }
            }
        }
    }

    /**
     * 将查询得到的错误数据记录到数据库
     *
     * @param checkResult 结果对象
     * @param dataMap     待抽样数据
     */
    void checkAndSaveErrorData(CheckResult checkResult, Map<String, Long> dataMap, OperateType op, String saveTable) {
        if (null != dataMap) {
            List<Map.Entry<String, Long>> sampleData = dataSample(dataMap);
            LOG.info("本次采样得到的数据量为：" + sampleData.size());
            Map<String, Long> afterCompare = batchCheck(checkResult.getDataBase(), checkResult.getTableName(), sampleData, op);
            batchInsert(checkResult, afterCompare, saveTable);
        }
    }

    /**
     * 对插入更新记录进行检查
     *
     * @param dataBase   数据库
     * @param tableName  表
     * @param sampleData 采样后的数据
     */
    private Map<String, Long> batchCheck(String dataBase, String tableName, List<Map.Entry<String, Long>> sampleData, OperateType op) {
        Map<String, Long> checkDataMap = null;
        List<Map<String, Object>> resultList;
        String sql = assembleSql(sampleData, op, dataBase, tableName);
        if (sql.length() > 0) {
            try {
                resultList = DBUtil.query(DBServer.DBServerType.TIDB.toString(), dataBase, sql);
                Map<String, Long> collectMap = sampleData.stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                if (null != resultList && resultList.size() > 0) {
                    checkDataMap = new HashMap<>();
                    for (Map<String, Object> errorRecord : resultList) {
                        String recordId = String.valueOf(errorRecord.get(this.recordId));
                        long upDateTime = (Long) errorRecord.get("avroTime");
                        checkDataMap.put(recordId, upDateTime);
                    }
                } else {
                    String sqlCount = "select TABLE_ROWS from information_schema.TABLES where TABLE_SCHEMA='" + dataBase + "'" + " and  TABLE_NAME='" + tableName + "'";
                    List<Map<String, Object>> list = DBUtil.query(DBServer.DBServerType.TIDB.toString(), dataBase, sqlCount);
                    if (null != list) {
                        long tableRows = Long.valueOf(list.get(0).get("TABLE_ROWS").toString());
                        if (tableRows == 0) {
                            checkDataMap = collectMap;
                        }
                    }
                }
            } catch (Exception e) {
                LOG.info(e.getMessage(), e);
            }
        }
        return checkDataMap;
    }

    private String assembleSql(List<Map.Entry<String, Long>> sampleData, OperateType op, String dataBase, String tableName) {
        StringBuilder sql = new StringBuilder();
        if (null != sampleData && sampleData.size() > 0) {
            int sampleDataSize = sampleData.size();
            switch (op) {
                case Unique: {
                    for (int i = 0; i < sampleDataSize; i++) {
                        Map.Entry record = sampleData.get(i);
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
                                .append(dataBase)
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
                        if (i < sampleDataSize - 1) {
                            sql.append(" union ");
                        }
                    }
                    break;
                }
                case Delete: {
                    for (int i = 0; i < sampleDataSize; i++) {
                        Map.Entry record = sampleData.get(i);
                        long timeStamp = (Long) record.getValue() / 1000;
                        sql.append("select ")
                                .append(recordId)
                                .append(",")
                                .append(timeStamp)
                                .append(" as avroTime ")
                                .append(" from ")
                                .append(dataBase)
                                .append(".")
                                .append(tableName)
                                .append(" where ")
                                .append(recordId)
                                .append("=")
                                .append(record.getKey());
                        if (i < sampleDataSize - 1) {
                            sql.append(" union ");
                        }
                    }
                    break;
                }
                default:
            }

        }
        return sql.toString();
    }

    private void batchInsert(CheckResult result, Map<String, Long> afterComp, String tableName) {
        List<Map<String, Object>> resultMap = new ArrayList<>();
        if (afterComp != null) {
            for (Map.Entry<String, Long> entry : afterComp.entrySet()) {
                Map<String, Object> dataMap = new HashMap<>();
                dataMap.put(CheckTable.OLD_ID, entry.getKey());
                dataMap.put(CheckTable.FILE_NAME, result.getFileName());
                dataMap.put(CheckTable.DB_INSTANCE, result.getDbInstance());
                dataMap.put(CheckTable.DATA_BASE, result.getDataBase());
                dataMap.put(CheckTable.TABLE_NAME, result.getTableName());
                dataMap.put(CheckTable.LAST_UPDATE_TIME, TimeUtil.stampToDate(entry.getValue() * 1000));
                dataMap.put(CheckTable.OP_TYPE, result.getOpType());
                resultMap.add(dataMap);
            }
            try {
                DBUtil.insertAll(DBServer.DBServerType.MYSQL.toString(), binLogDataBase, tableName, resultMap);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } else {
            LOG.info("no error record find from file: " + result.getFileName());
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
        if (sampleDefalut.equalsIgnoreCase(sampleFlag)) {
            if (dataSize > 0) {
                Collections.shuffle(copy);
                n = (int) Math.sqrt(dataSize);
                System.out.println("************" + n);
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

}
