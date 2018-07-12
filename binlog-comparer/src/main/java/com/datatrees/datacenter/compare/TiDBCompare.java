package com.datatrees.datacenter.compare;

import com.datatrees.datacenter.core.utility.DBServer;
import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.datareader.AvroDataReader;
import com.datatrees.datacenter.operate.OperateType;
import com.datatrees.datacenter.table.CompareTable;
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
                    String id = FieldNameOp.getFieldName(dataBase, tableName, FieldNameOp.getConfigField("id"));
                    String lastUpdateTime = FieldNameOp.getFieldName(dataBase, tableName, FieldNameOp.getConfigField("update"));

                    Map<String, Long> afterComp;
                    Map<String, Long> checkDataMap = new HashMap<>();
                    List<Map<String, Object>> resultMap = new ArrayList<>();
                    for (Map.Entry record : sampleData) {
                        Map<String, Object> whereMap = new HashMap<>();
                        whereMap.put(id, record.getKey());
                        whereMap.put("UNIX_TIMESTAMP("+lastUpdateTime+")",  ((Long)record.getValue()+1000)/1000);
                        String sql="select "+id+","+lastUpdateTime+" from "+tableName+" where "+id+"=";

                        List<Map<String, Object>> resultList;
                        try {
                            resultList = DBUtil.query(DBServer.getDBInfo(DBServer.DBServerType.TIDB.toString()), dataBase, tableName, whereMap);
                            if (resultList.isEmpty()) {
                                checkDataMap.put((String) record.getKey(), (Long) record.getValue());
                                LOG.info("**********" + record.getKey() + "*************" + record.getValue() + "******** no find in dataBase:" + dataBase + "****** tableName:" + tableName);
                            }
                        } catch (Exception e) {
                            LOG.info(e.getMessage(), e);
                        }
                    }
                    afterComp = retainCompare(checkDataMap, deleteRecord);
                    if (afterComp != null) {
                        Iterator<Map.Entry<String, Long>> it = afterComp.entrySet().iterator();
                        while (it.hasNext()) {
                            Map.Entry<String, Long> entry = it.next();
                            Map<String, Object> dataMap = new HashMap<>();
                            dataMap.put(CompareTable.OLD_ID, entry.getKey());
                            dataMap.put(CompareTable.FILE_NAME, fileName);
                            dataMap.put(CompareTable.DB_INSTANCE, db_instance);
                            dataMap.put(CompareTable.DATA_BASE, dataBase);
                            dataMap.put(CompareTable.TABLE_NAME, tableName);
                            dataMap.put(CompareTable.FILE_PARTITION, partition);
                            dataMap.put(CompareTable.LAST_UPDATE_TIME, entry.getKey());
                            resultMap.add(dataMap);
                        }
                    }
                    if (resultMap.size() > 0) {
                        try {
                            DBUtil.insertAll(DBServer.getDBInfo(DBServer.DBServerType.MYSQL.toString()), binLogDataBase, CompareTable.TABLE_NAME, resultMap);
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                }
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
