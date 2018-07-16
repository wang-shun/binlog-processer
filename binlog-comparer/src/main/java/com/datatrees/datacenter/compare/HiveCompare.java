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
    public void binLogCompare(String dest) {
        // TODO: 2018/7/4 补充通过路径获取文件名和分区信息

        AvroDataReader avroDataReader = new AvroDataReader();
        OrcDataReader orcDataReader = new OrcDataReader();
        String avroPath = "";
        String orcPaht = "";
        Map<String, Map<String, Long>> avroData = avroDataReader.readSrcData(avroPath);
        Map<String, Long> orcData = orcDataReader.readDestData(orcPaht);

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
        List<Map<String, Object>> test = getCurrentPartitionInfo(avroPath);
        List<Map<String, Object>> testFilter = test.stream().filter(line -> !"hell0".equalsIgnoreCase(String.valueOf(line.get("hello")) + String.valueOf(line.get("kugou")))).collect(Collectors.toList());
        // TODO: 2018/7/4  每次检查完，修改检查过的数据的状态（t_binlog_process_log）
    }



}
