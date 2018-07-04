package com.datatrees.datacenter.compare;

import com.datatrees.datacenter.datareader.AvroDataReader;
import com.datatrees.datacenter.datareader.OrcDataReader;
import com.datatrees.datacenter.operate.OperateType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class HiveCompare implements DataCompare {
    private static Logger LOG = LoggerFactory.getLogger(HiveCompare.class);

    @Override
    public void binLogCompare(String src, String dest) {
        AvroDataReader avroDataReader = new AvroDataReader();
        OrcDataReader orcDataReader = new OrcDataReader();

        Map<String, Map<String, Long>> avroData = avroDataReader.readSrcData(src);
        Map<String, Long> orcData = orcDataReader.readDestData(dest);

        Map<String, Long> uniqueData = avroData.get(OperateType.Unique.toString());
        Map<String, Long> diffList = mapCompare(uniqueData, orcData);

        Map<String, Long> createRecord = avroData.get(OperateType.Create.toString());
        Map<String, Long> updateRecord = avroData.get(OperateType.Update.toString());
        Map<String, Long> deleteRecord = avroData.get(OperateType.Delete.toString());

        Map<String,Long> fromCreate=mapCompare(diffList,createRecord);
        Map<String,Long> fromUpdate=mapCompare(diffList,updateRecord);
        Map<String,Long> fromDelete=mapCompare(diffList,deleteRecord);
        // TODO: 2018/7/3 找出各种事件 

    }

    private Map<String, Long> mapCompare(Map<String, Long> avroMap, Map<String, Long> orcMap) {

        Set<Map.Entry<String, Long>> avroEntry = new HashSet<>(avroMap.entrySet());
        Set<Map.Entry<String, Long>> orcEntry = new HashSet<>(orcMap.entrySet());

        Set<Map.Entry<String, Long>> avroSet = avroMap.entrySet();
        Set<Map.Entry<String, Long>> orcSet = orcMap.entrySet();
        System.out.println("map pairs that are in set 1 but not in set 2");
        Map<String, Long> diffMaps = null;
        if (avroSet.removeAll(orcSet)) {
            diffMaps = new HashMap<>();
            for (Map.Entry entry : avroSet) {
                System.out.println(entry.getKey() + "=" + entry.getValue());
                diffMaps.put(entry.getKey().toString(),Long.valueOf(entry.getValue().toString()));
            }
        }
        return diffMaps;
    }
}
