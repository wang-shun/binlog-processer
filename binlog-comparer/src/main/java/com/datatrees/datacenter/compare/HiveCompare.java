package com.datatrees.datacenter.compare;

import com.datatrees.datacenter.core.utility.DBUtil;
import com.datatrees.datacenter.datareader.AvroDataReader;
import com.datatrees.datacenter.datareader.OrcDataReader;
import com.datatrees.datacenter.operate.OperateType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class HiveCompare extends DataCompare {
    private static Logger LOG = LoggerFactory.getLogger(HiveCompare.class);


    @Override
    public void binLogCompare(String src, String dest) {
        // TODO: 2018/7/4 补充通过路径获取文件名和分区信息

        AvroDataReader avroDataReader = new AvroDataReader();
        OrcDataReader orcDataReader = new OrcDataReader();

        Map<String, Map<String, Long>> avroData = avroDataReader.readSrcData(src);
        Map<String, Long> orcData = orcDataReader.readDestData(dest);

        Map<String, Long> uniqueData = avroData.get(OperateType.Unique.toString());
        Map<String, Long> diffList = diffCompare(uniqueData, orcData);

        Map<String, Long> createRecord = avroData.get(OperateType.Create.toString());
        Map<String, Long> updateRecord = avroData.get(OperateType.Update.toString());
        Map<String, Long> deleteRecord = avroData.get(OperateType.Delete.toString());

        Map<String, Long> fromCreate = retainCompare(createRecord, diffList);
        Map<String, Long> fromUpdate = retainCompare(updateRecord, diffList);
        Map<String, Long> fromDelete = retainCompare(deleteRecord, diffList);
        // TODO: 2018/7/3 找出各种事件

        //查看当前binlog解析出来的文件分区文件数目和文件条数是否达到了数量要求
        List<Map<String, Object>> test = getCurrentPartitinInfo(src);
        List<Map<String, Object>> testFilter = test.stream().filter(line -> !"hell0".equalsIgnoreCase(String.valueOf(line.get("hello")) + String.valueOf(line.get("kugou")))).collect(Collectors.toList());
        // TODO: 2018/7/4  每次检查完，修改检查过的数据的状态（t_binlog_process_log）
    }

    private Map<String, Long> diffCompare(Map<String, Long> avroMap, Map<String, Long> orcMap) {

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
                diffMaps.put(entry.getKey().toString(), Long.valueOf(entry.getValue().toString()));
            }
        }
        return diffMaps;
    }


    private Map<String, Long> retainCompare(Map<String, Long> Map1, Map<String, Long> Map2) {

        Set<Map.Entry<String, Long>> set1 = Map1.entrySet();
        Set<Map.Entry<String, Long>> set2 = Map2.entrySet();
        System.out.println("map pairs that are both in set 1 and set 2");
        Map<String, Long> diffMaps = null;
        if (set1.retainAll(set2)) {
            diffMaps = new HashMap<>();
            for (Map.Entry entry : set1) {
                System.out.println(entry.getKey() + "=" + entry.getValue());
                diffMaps.put(entry.getKey().toString(), Long.valueOf(entry.getValue().toString()));
            }
        }
        return diffMaps;
    }

}
