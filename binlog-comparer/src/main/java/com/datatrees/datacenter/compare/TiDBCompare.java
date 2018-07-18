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

public class TiDBCompare extends DataCompare {
    private static Logger LOG = LoggerFactory.getLogger(TiDBCompare.class);
    private Properties properties = PropertiesUtility.defaultProperties();
    private final int factor = Integer.valueOf(properties.getProperty("SAMPLE_FACTOR"));
    private String binLogDataBase = properties.getProperty("jdbc.database");
    private List<String> idList = FieldNameOp.getConfigField("id");
    private List<String> createTimeList = FieldNameOp.getConfigField("update");
    private String RECORD_ID;
    private String RECORD_LAST_UPDATE_TIME;

    @Override
    public void binLogCompare(String fileName) {
        List<Map<String, Object>> TableInfo = getCurrentTableInfo(fileName);
        if (null != TableInfo) {
            for (Map<String, Object> recordMap : TableInfo) {
                String dataBase = String.valueOf(recordMap.get("database_name"));
                String tableName = String.valueOf(recordMap.get("table_name"));
                String[] partitions = String.valueOf(recordMap.get("partitions")).split(",");
                String db_instance = String.valueOf(recordMap.get("db_instance"));
                RECORD_ID = FieldNameOp.getFieldName(dataBase, tableName, idList);
                RECORD_LAST_UPDATE_TIME = FieldNameOp.getFieldName(dataBase, tableName, createTimeList);
                if (null != RECORD_ID && null != RECORD_LAST_UPDATE_TIME) {
                    if (partitions.length > 0) {
                        Map<String, Long> allUniqueData = new HashMap<>();
                        Map<String, Long> allDeleteData = new HashMap<>();
                        AvroDataReader avroDataReader = new AvroDataReader();
                        for (String partition : partitions) {
                            avroDataReader.setDataBase(dataBase);
                            avroDataReader.setTableName(tableName);
                            avroDataReader.setRECORD_ID(RECORD_ID);
                            avroDataReader.setRECORD_LAST_UPDATE_TIME(RECORD_LAST_UPDATE_TIME);
                            String filePath = super.AVRO_HDFS_PATH +
                                    File.separator +
                                    db_instance +
                                    File.separator +
                                    dataBase +
                                    File.separator +
                                    tableName +
                                    File.separator +
                                    partition +
                                    File.separator +
                                    fileName.replace(".tar", "") +
                                    ".avro";

                            Map<String, Map<String, Long>> avroData = avroDataReader.readSrcData(filePath);
                            Map<String, Long> unique = avroData.get(OperateType.Unique.toString());
                            Map<String, Long> delete = avroData.get(OperateType.Delete.toString());
                            if (null != unique) {
                                allUniqueData.putAll(unique);
                            }
                            if (null != delete) {
                                allDeleteData.putAll(delete);
                            }
                        }
                        Map<String, Long> filterDeleteMap = diffCompare(allUniqueData, allDeleteData);
                        CheckResult checkResult = new CheckResult();
                        checkResult.setDataBase(dataBase);
                        checkResult.setTableName(tableName);
                        checkResult.setFileName(fileName);
                        checkResult.setDbInstance(db_instance);
                        checkResult.setOpType(OperateType.Unique.toString());
                        insertErrorData(checkResult, filterDeleteMap, OperateType.Unique);
                        checkResult.setOpType(OperateType.Delete.toString());
                        insertErrorData(checkResult, allDeleteData, OperateType.Delete);
                    }
                }
            }
        }
    }

    /**
     * 将查询得到的错误数据记录到数据库
     *
     * @param checkResult
     * @param dataMap
     */
    private void insertErrorData(CheckResult checkResult, Map<String, Long> dataMap, OperateType op) {
        if (null != dataMap) {
            List<Map.Entry<String, Long>> sampleDeleteData = dataSample(dataMap);
            List<String> deleteCheck = batckCheck(checkResult.getDataBase(), checkResult.getTableName(), sampleDeleteData, op);
            insertPartitionBatch(checkResult, deleteCheck);
        }
    }

    /**
     * 对插入更新记录进行检查
     *
     * @param dataBase   数据库
     * @param tableName  表
     * @param sampleData 采样后的数据
     */
    private List<String> batckCheck(String dataBase, String tableName, List<Map.Entry<String, Long>> sampleData, OperateType op) {
        List<String> checkDataList = null;
        List<Map<String, Object>> resultList;
        String sql = assembleSql(sampleData, op, tableName);
        if (sql.length() > 0) {
            try {
                resultList = DBUtil.query(DBServer.DBServerType.TIDB.toString(), dataBase, sql);
                if (null != resultList) {
                    checkDataList = new ArrayList<>();
                    for (Map<String, Object> errorRecord : resultList) {
                        String recordId = String.valueOf(errorRecord.get(RECORD_ID));
                        checkDataList.add(String.valueOf(recordId));
                    }
                }
            } catch (Exception e) {
                LOG.info(e.getMessage(), e);
            }
        }
        return checkDataList;
    }

    private String assembleSql(List<Map.Entry<String, Long>> sampleData, OperateType op, String tableName) {
        StringBuilder sql = new StringBuilder();
        if (null != sampleData && sampleData.size() > 0) {
            int sampleDataSize = sampleData.size();
            switch (op) {
                case Unique:
                    for (int i = 0; i < sampleDataSize; i++) {
                        Map.Entry record = sampleData.get(i);
                        sql.append("select ")
                                .append(RECORD_ID)
                                .append(" , ")
                                .append("UNIX_TIMESTAMP(")
                                .append(RECORD_LAST_UPDATE_TIME)
                                .append(")")
                                .append(" as time_stamp ")
                                .append(" from ")
                                .append(tableName)
                                .append(" where ")
                                .append(RECORD_ID)
                                .append("=")
                                .append(record.getKey())
                                .append(" and ")
                                .append("UNIX_TIMESTAMP(")
                                .append(RECORD_LAST_UPDATE_TIME)
                                .append(")")
                                .append("=")
                                .append((Long) record.getValue() / 1000);
                        if (i < sampleDataSize - 1) {
                            sql.append(" union ");
                        }
                    }
                    break;
                case Delete:
                    for (int i = 0; i < sampleDataSize; i++) {
                        Map.Entry record = sampleData.get(i);
                        sql.append("select ")
                                .append(RECORD_ID)
                                .append(" from ")
                                .append(tableName)
                                .append(" where ")
                                .append(RECORD_ID)
                                .append("=")
                                .append(record.getKey());
                        if (i < sampleDataSize - 1) {
                            sql.append(" union ");
                        }
                    }
                    break;
            }
        }
        return sql.toString();
    }

    private void insertPartitionBatch(CheckResult result, List<String> afterComp) {
        List<Map<String, Object>> resultMap = new ArrayList<>();
        if (afterComp != null) {
            for (String id : afterComp) {
                Map<String, Object> dataMap = new HashMap<>();
                dataMap.put(CheckTable.OLD_ID, id);
                dataMap.put(CheckTable.FILE_NAME, result.getFileName());
                dataMap.put(CheckTable.DB_INSTANCE, result.getDbInstance());
                dataMap.put(CheckTable.DATA_BASE, result.getDataBase());
                dataMap.put(CheckTable.TABLE_NAME, result.getTableName());
                dataMap.put(CheckTable.OP_TYPE, result.getOpType());
                resultMap.add(dataMap);
            }
            try {
                DBUtil.insertAll(DBServer.DBServerType.MYSQL.toString(), binLogDataBase, CheckTable.BINLOG_CHECK_TABLE, resultMap);
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
        return pickNRandom(dataList);
    }

    /**
     * sample date
     *
     * @param lst data sets to be sampled
     * @return sampled data
     */
    private List<Map.Entry<String, Long>> pickNRandom(List<Map.Entry<String, Long>> lst) {
        List<Map.Entry<String, Long>> copy = new ArrayList<>(lst);
        Collections.shuffle(copy);
        int dataSize = copy.size();
        int n = 0;
        if (dataSize > 0) {
            if (dataSize > factor) {
                n = dataSize / factor;
            } else {
                n = dataSize - 1;
            }
        }
        return copy.subList(0, n);
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
