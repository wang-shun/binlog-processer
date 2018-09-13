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

                    Map<String, Long> createRecordFind = compareWithHBase(assembleRowKey(dataBase, tableName, createRecord));
                    Map<String, Long> updateRecordFind = compareWithHBase(assembleRowKey(dataBase, tableName, updateRecord));
                    Map<String, Long> deleteRecordFind = compareWithHBase(assembleRowKey(dataBase, tableName, deleteRecord));


                    Map<String, Long> createRecordNoFind;
                    Map<String, Long> updateRecordNoFind;

                    if (null != createRecordFind && createRecordFind.size() > 0) {
                        Map<String, Long> createTmp = new HashMap<>();
                        createRecordFind.entrySet().stream().forEach(x -> createTmp.put(x.getKey().split("_")[0], x.getValue()));
                        createRecordNoFind = diffCompare(createRecord, createTmp);
                    } else {
                        createRecordNoFind = createRecord;
                    }

                    if (null != updateRecordFind && updateRecordFind.size() > 0) {
                        Map<String, Long> updateTmp = new HashMap<>();
                        updateRecordFind.entrySet().stream().forEach(x -> updateTmp.put(x.getKey().split("_")[0], x.getValue()));
                        Map<String, Long> tmp = updateTmp;
                        updateRecordNoFind = diffCompare(updateRecord, updateTmp);
                        Map<String, Long> oldUpdateRecord = compareByValue(tmp, updateRecord);
                        if(null!=oldUpdateRecord&&oldUpdateRecord.size()>0) {
                            updateRecordNoFind.putAll(oldUpdateRecord);
                        }
                    } else {
                        updateRecordNoFind = updateRecord;
                    }

                    if (null != createRecordNoFind) {
                        Iterator<Map.Entry<String, Long>> iterator = createRecordNoFind.entrySet().iterator();
                        System.out.println(dataBase + tableName);
                        while (iterator.hasNext()) {
                            Map.Entry<String, Long> recordEntry = iterator.next();
                            System.out.println(recordEntry.getKey());
                            System.out.println(recordEntry.getValue());
                        }
                    }

                    if (null != updateRecordNoFind) {
                        Iterator<Map.Entry<String, Long>> iterator = updateRecordNoFind.entrySet().iterator();
                        System.out.println(dataBase + tableName);

                    }

                    if(null!=deleteRecordFind){

                    }
                }
            }
        }
    }

    private List<String> assembleRowKey(String dataBase, String tableName, Map<String, Long> recordMap) {
        if (null != recordMap && recordMap.size() > 0) {
            List<String> createIdList = recordMap.keySet().stream().map(x -> (x + "_" + dataBase + "." + tableName)).collect(Collectors.toList());
            return createIdList;
        } else {
            return null;
        }

    }

    private Map<String, Long> compareWithHBase(List<String> createIdList) {
        if (null != createIdList && createIdList.size() > 0) {
            Map<String, Long> hbaseRecordMap = BatchGetFromHBase.getBatchDataFromHBase(createIdList, HBaseTableInfo.TABLE_NAME, HBaseTableInfo.COLUMNFAMILY, HBaseTableInfo.LAST_UPDATE_TIME);
            return hbaseRecordMap;
        }
        return null;
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
