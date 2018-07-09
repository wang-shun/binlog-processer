package com.datatrees.datacenter.compare;

import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.datareader.AvroDataReader;
import com.datatrees.datacenter.operate.OperateType;
import com.datatrees.datacenter.table.FieldNameOp;

import java.util.*;

public class TiDBCompare extends DataCompare {
    private final int factor = 10;

    @Override
    public void binLogCompare(String src, String dest) {
        AvroDataReader avroDataReader = new AvroDataReader();
        Map<String, Map<String, Long>> avroData = avroDataReader.readSrcData(src);

        Map<String, Long> uniqueData = avroData.get(OperateType.Unique.toString());
        Map<String, Long> createRecord = avroData.get(OperateType.Create.toString());
        Map<String, Long> updateRecord = avroData.get(OperateType.Update.toString());
        Map<String, Long> deleteRecord = avroData.get(OperateType.Delete.toString());

        List<Map.Entry<String, Long>> sampleData = dataSample(uniqueData);
        // TODO: 2018/7/9 change table 同时create\update\delete 要分开处理
        String tableName = "";
        String id = FieldNameOp.getFieldName(tableName, FieldNameOp.getConfigField("id"));
        String lastUpdateTime = FieldNameOp.getFieldName(tableName, FieldNameOp.getConfigField("update"));
        List<Map<String, Long>> afterComp = new ArrayList<>();
        for (Map.Entry record : sampleData) {
            Map<String, Object> whereMap = new HashMap<>();
            whereMap.put(id, record.getKey());
            whereMap.put(lastUpdateTime, record.getValue());
            List<Map<String, Object>> resultList;
            try {
                resultList = DBUtil.query(tableName, whereMap);
                if (resultList.isEmpty()) {
                    Map<String, Long> recordMap = new HashMap<>();
                    recordMap.put((String) record.getKey(), (Long) record.getValue());
                    afterComp.add(recordMap);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

    }

    private List<Map.Entry<String, Long>> dataSample(Map<String, Long> operateRecordMap) {
        List<Map.Entry<String, Long>> dataList = mapToList(operateRecordMap);
        int dataSize = dataList.size();
        List<Map.Entry<String, Long>> sampleDataList = pickNRandom(dataList, dataSize / 10);
        return sampleDataList;
    }

    /**
     * sample date
     *
     * @param lst data sets to be sampled
     * @param n   sample number
     * @return sampled data
     */
    private List<Map.Entry<String, Long>> pickNRandom(List<Map.Entry<String, Long>> lst, int n) {
        List<Map.Entry<String, Long>> copy = new ArrayList<>(lst);
        Collections.shuffle(copy);
        int dataSize = copy.size();
        if (n > dataSize) {
            n = dataSize / factor;
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
