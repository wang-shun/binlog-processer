package com.datatrees.datacenter.compare;

import com.datatrees.datacenter.core.utility.PropertiesUtility;
import com.datatrees.datacenter.datareader.AvroDataReader;
import com.datatrees.datacenter.datareader.OrcDataReader;
import com.datatrees.datacenter.operate.OperateType;
import com.datatrees.datacenter.table.CheckTable;
import com.datatrees.datacenter.table.FieldNameOp;
import com.datatrees.datacenter.table.HBaseTableInfo;
import com.datatrees.datacenter.utility.BatchGetFromHBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

public class HiveCompare extends BaseDataCompare {
    private static Logger LOG = LoggerFactory.getLogger(HiveCompare.class);
    private Properties properties = PropertiesUtility.defaultProperties();
    private final String HIVE_HDFS_PATH = properties.getProperty("HIVE_HDFS_PATH");
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
                String db_instance = String.valueOf(partitionInfo.get(CheckTable.DB_INSTANCE));
                Collection<Object> allFieldSet = FieldNameOp.getAllFieldName(dataBase, tableName);
                String recordId = FieldNameOp.getFieldName(allFieldSet, idList);
                String recordLastUpdateTime = FieldNameOp.getFieldName(allFieldSet, createTimeList);
                if (null != recordId && null != recordLastUpdateTime) {
                    String filePath = db_instance +
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

                    createRecord = diffCompare(createRecord, updateRecord);
                    createRecord = diffCompare(createRecord, deleteRecord);
                    updateRecord = diffCompare(updateRecord, deleteRecord);
                    List<String> createIdList = createRecord.keySet().stream().map(x -> (x + "_" + dataBase + "." + tableName)).collect(Collectors.toList());
                    Map<String, Long> hbaseRecordMap = BatchGetFromHBase.getBatchDataFromHBase(createIdList, HBaseTableInfo.TABLE_NAME, HBaseTableInfo.COLUMNFAMILY, HBaseTableInfo.LAST_UPDATE_TIME);
                    if (null != hbaseRecordMap) {
                        Iterator<Map.Entry<String, Long>> iterator = hbaseRecordMap.entrySet().iterator();
                        while (iterator.hasNext()) {
                            Map.Entry<String, Long> recordEntry = iterator.next();
                            System.out.println(recordEntry.getKey());
                            System.out.println(recordEntry.getValue());
                        }
                    }
               /* //create/insert事件
                Map<String, Long> diffMap = diffCompare(createData, orcData);
                //create/insert事件中真正的错误记录
                Map<String, Long> lastDiffMap = compareByValue(diffMap, orcData);
                //delete 时间中的错误记录
                Map<String, Long> retainMap = retainCompare(deleteRecord, orcData);*/
                    // TODO: 2018/7/17 将检查结果写入到数据库
                }
            }
        }
    }

    /**
     * 将具有相同key的数据根据value值的大小进行比较
     *
     * @param srcMap  需要比较的Map
     * @param destMap 被比较的Map
     * @return
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
     * @return
     */
    public Map<String, Long> retainCompareHive(Map<String, Long> srcMap, Map<String, Long> destMap) {
        Set<String> set1 = srcMap.keySet();
        Set<String> set2 = srcMap.keySet();
        Map<String, Long> diffMaps = new HashMap<>();
        if (set1.retainAll(set2)) {
            for (String key : set1) {
                diffMaps.put(key, srcMap.get(key));
            }
        }
        return diffMaps;
    }

}
