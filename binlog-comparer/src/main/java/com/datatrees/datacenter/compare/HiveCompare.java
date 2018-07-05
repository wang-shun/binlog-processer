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

public class HiveCompare implements DataCompare {
    private static Logger LOG = LoggerFactory.getLogger(HiveCompare.class);
    private final int fileNum = 5;
    private final int recordNum = 100;

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
        List<Map<String, Object>> test = getCurrentPartitinInfo();
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

    /**
     * @param avroMap
     * @param orcMap
     * @return
     */
    private Map<String, Long> retainCompare(Map<String, Long> avroMap, Map<String, Long> orcMap) {

        Set<Map.Entry<String, Long>> avroEntry = new HashSet<>(avroMap.entrySet());
        Set<Map.Entry<String, Long>> orcEntry = new HashSet<>(orcMap.entrySet());

        Set<Map.Entry<String, Long>> avroSet = avroMap.entrySet();
        Set<Map.Entry<String, Long>> orcSet = orcMap.entrySet();
        System.out.println("map pairs that are both in set 1 and set 2");
        Map<String, Long> diffMaps = null;
        if (avroSet.retainAll(orcSet)) {
            diffMaps = new HashMap<>();
            for (Map.Entry entry : avroSet) {
                System.out.println(entry.getKey() + "=" + entry.getValue());
                diffMaps.put(entry.getKey().toString(), Long.valueOf(entry.getValue().toString()));
            }
        }
        return diffMaps;
    }

    private List<Map<String, Object>> getCurrentPartitinInfo() {
        List<Map<String, Object>> partitionInfo = null;
        // TODO: 2018/7/4 加状态过滤
        String sql = "select db_instance,database_name,table_name,file_partitions,count(file_name) as file_cnt,sum(insert_cnt+delete_cnt+update_cnt) as sun_cnt,GROUP_CONCAT(file_name) as files " +
                "from t_binlog_process_log group by db_instance,database_name,table_name,file_partitions having count(file_name)>" + fileNum +
                " or sum(insert_cnt+delete_cnt+update_cnt) >" + recordNum;
        try {
            partitionInfo = DBUtil.query(sql);
        } catch (SQLException e1) {
            e1.printStackTrace();
        }
        return partitionInfo;
    }
}
