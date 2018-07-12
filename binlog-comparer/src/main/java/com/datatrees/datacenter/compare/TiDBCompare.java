package com.datatrees.datacenter.compare;

import com.datatrees.datacenter.core.utility.DBServer;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.core.utility.TimeUtil;
import com.datatrees.datacenter.datareader.AvroDataReader;
import com.datatrees.datacenter.operate.OperateType;
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
    private final String AVRO_HDFS_PATH = properties.getProperty("AVRO_HDFS_PATH");
    private final int factor = 400;
    private String binLogDataBase = properties.getProperty("jdbc.database");

    @Override
    public void binLogCompare(String fileName) {

        List<Map<String, Object>> partitionInfo = getCurrentPartitinInfo(fileName);
        if (!partitionInfo.isEmpty()) {
            for (Map<String, Object> recordMap : partitionInfo) {
                String dataBase = (String) recordMap.get("database_name");
                String tableName = (String) recordMap.get("table_name");
                String partition = (String) recordMap.get("file_partitions");
                String db_instance = (String) recordMap.get("db_instance");

                String filePath = AVRO_HDFS_PATH +
                        File.separator +
                        db_instance +
                        File.separator +
                        dataBase +
                        File.separator +
                        tableName +
                        File.separator +
                        partition +
                        File.separator +
                        fileName.replace("tar", "") +
                        "avro";

                AvroDataReader avroDataReader = new AvroDataReader();
                avroDataReader.setDataBase(dataBase);
                avroDataReader.setTableName(tableName);
                Map<String, Map<String, Long>> avroData = avroDataReader.readSrcData(filePath);
                Map<String, Long> uniqueData = avroData.get(OperateType.Unique.toString());
                Map<String, Long> createRecord = avroData.get(OperateType.Create.toString());
                Map<String, Long> updateRecord = avroData.get(OperateType.Update.toString());
                Map<String, Long> deleteRecord = avroData.get(OperateType.Delete.toString());
                if (uniqueData.size() > 0) {
                    List<Map.Entry<String, Long>> sampleData = dataSample(uniqueData);
                    // TODO: 2018/7/11 这两处可以缓存起来不用每次都去取
                    int sampleDataSize = sampleData.size();
                    System.out.println(sampleDataSize);

                    Map<String, Long> checkDataMap;
                    checkDataMap = batchCheckData(dataBase, tableName, sampleData);

                    Map<String, Long> afterComp;
                    afterComp = retainCompare(createRecord, checkDataMap);
                    insertPartitionBatch(fileName, dataBase, tableName, partition, db_instance, afterComp);
                }
            }
        }
    }

    /**
     * 对一批数据进行查询检测
     *
     * @param dataBase   数据库
     * @param tableName  表
     * @param sampleData 采样后的数据
     */
    private Map<String, Long> batchCheckData(String dataBase, String tableName, List<Map.Entry<String, Long>> sampleData) {
        Map<String, Long> checkDataMap = null;
        String id = FieldNameOp.getFieldName(dataBase, tableName, FieldNameOp.getConfigField("id"));
        String lastUpdateTime = FieldNameOp.getFieldName(dataBase, tableName, FieldNameOp.getConfigField("update"));
        int sampleDataSize = sampleData.size();
        StringBuilder sql = new StringBuilder();
        for (int i = 0; i < sampleDataSize; i++) {
            Map.Entry record = sampleData.get(i);
            if (i < sampleDataSize - 1) {
                sql.append("select ")
                        .append(id)
                        .append(" , ")
                        .append("UNIX_TIMESTAMP(")
                        .append(lastUpdateTime)
                        .append(")")
                        .append(" as time_stamp ")
                        .append(" from ")
                        .append(tableName)
                        .append(" where ")
                        .append(id)
                        .append("=")
                        .append(record.getKey())
                        .append(" and ")
                        .append("UNIX_TIMESTAMP(")
                        .append(lastUpdateTime)
                        .append(")")
                        .append("=")
                        .append((Long) record.getValue() / 1000)
                        .append(" union ");
            } else {
                sql.append("select ")
                        .append(id)
                        .append(" , ")
                        .append("UNIX_TIMESTAMP(")
                        .append(lastUpdateTime)
                        .append(")")
                        .append(" as time_stamp ")
                        .append(" from ")
                        .append(tableName)
                        .append(" where ")
                        .append(id)
                        .append("=")
                        .append(record.getKey())
                        .append(" and ")
                        .append("UNIX_TIMESTAMP(")
                        .append(lastUpdateTime)
                        .append(")")
                        .append("=")
                        .append((Long) record.getValue() / 1000);
            }
        }
        List<Map<String, Object>> resultList;
        try {
            resultList = DBUtil.query(DBServer.getDBInfo(DBServer.DBServerType.TIDB.toString()), dataBase, sql.toString());
            if (!resultList.isEmpty()) {
                checkDataMap=new HashMap<>();
                for (int i = 0; i < resultList.size(); i++) {
                    Map<String, Object> errorRecord = resultList.get(i);
                    Long recordId = (long) errorRecord.get(id);
                    long lastTime = (long) errorRecord.get("time_stamp") * 1000;
                    checkDataMap.put(String.valueOf(recordId), lastTime);
                    LOG.info("**********" + recordId + "*************" + lastUpdateTime + "******** no find in dataBase:" + dataBase + "****** tableName:" + tableName);
                }
            }
        } catch (Exception e) {
            LOG.info(e.getMessage(), e);
        }
        return checkDataMap;
    }

    /**
     * 将一张表某个分区文件检测结果批量插入到数据库
     *
     * @param fileName    文件名
     * @param dataBase    数据库
     * @param tableName   表名
     * @param partition   分区
     * @param db_instance 实例名
     * @param afterComp   检车结果
     */
    private void insertPartitionBatch(String fileName, String dataBase, String tableName, String partition, String db_instance, Map<String, Long> afterComp) {
        List<Map<String, Object>> resultMap = null;
        if (afterComp != null) {
            resultMap=new ArrayList<>();
            Iterator<Map.Entry<String, Long>> it = afterComp.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, Long> entry = it.next();
                Map<String, Object> dataMap = new HashMap<>();
                dataMap.put(CheckTable.OLD_ID, entry.getKey());
                dataMap.put(CheckTable.FILE_NAME, fileName);
                dataMap.put(CheckTable.DB_INSTANCE, db_instance);
                dataMap.put(CheckTable.DATA_BASE, dataBase);
                dataMap.put(CheckTable.TABLE_NAME, tableName);
                dataMap.put(CheckTable.FILE_PARTITION, partition);
                dataMap.put(CheckTable.LAST_UPDATE_TIME, TimeUtil.stampToDate(entry.getValue()));
                resultMap.add(dataMap);
            }
        }
        if (resultMap.size() > 0) {
            try {
                DBUtil.insertAll(DBServer.getDBInfo(DBServer.DBServerType.MYSQL.toString()), binLogDataBase, CheckTable.BINLOG_CHECK_TABLE, resultMap);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * sample data
     *
     * @param operateRecordMap
     * @return
     */
    private List<Map.Entry<String, Long>> dataSample(Map<String, Long> operateRecordMap) {
        List<Map.Entry<String, Long>> dataList = mapToList(operateRecordMap);
        List<Map.Entry<String, Long>> sampleDataList = pickNRandom(dataList);
        return sampleDataList;
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
        int n;
        if (dataSize > factor) {
            n = dataSize / factor;
        } else {
            n = dataSize - 1;
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
        if (!dataMap.isEmpty()) {
            for (Map.Entry<String, Long> entry : dataMap.entrySet()) {
                dataList.add(entry);
            }
        }
        return dataList;
    }


}
