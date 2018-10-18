package com.datatrees.datacenter.main;

import com.datatrees.datacenter.compare.BatchGetFromHBase;
import com.datatrees.datacenter.compare.HiveCompareByFile;
import com.datatrees.datacenter.core.task.TaskDispensor;
import com.datatrees.datacenter.core.utility.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class HiveCheckBySchduler {
    private static final String HBASE_ROKEY_FIELD = "hbase_rowkey";
    public static volatile Set processingSet = Collections.synchronizedSet(new HashSet<String>());

    public static void main(String[] args) {
        Runnable runnable = () -> {
            //往前推一段时间
            String sql = "select database_name,table_name,file_partitions,file_name,create_date,type from t_binlog_process_log where create_date<now()-interval 1000 minute and create_date>now()-interval 1100 minute and repair_status=0 and file_partitions<> 'null'";
            try {
                List<Map<String, Object>> tableInfos = DBUtil.query(DBServer.DBServerType.MYSQL.toString(), "binlog", sql);
                if (tableInfos != null && tableInfos.size() > 0) {

                    HiveCompareByFile hiveCompareByFile = new HiveCompareByFile();
                    List<String> rowKeyList = new ArrayList<>(tableInfos.size());
                    for (int i = 0; i < tableInfos.size(); i++) {
                        Map<String, Object> tableMap = tableInfos.get(i);
                        String hiveTableLastUpdate = tableMap.get("database_name").toString() +
                                "." + tableMap.get("table_name")
                                + "_" + PartitionUtility.getHivePartition(PartitionUtility.getDateMap(String.valueOf(tableMap.get("file_partitions"))));
                        rowKeyList.add(hiveTableLastUpdate);
                        tableMap.put(HBASE_ROKEY_FIELD, hiveTableLastUpdate);

                    }
                    List<String> rowKeyListFilted = rowKeyList.stream().distinct().collect(Collectors.toList());
                    System.out.println("rowKeyListFilted:" + rowKeyListFilted);
                    Map<String, byte[]> recordLastUpdateTime = BatchGetFromHBase.getBatchDataFromHBase(rowKeyListFilted, "streaming_warehouse_system_conf", "f", "stream_update_time");
                    System.out.println("recordLastUpdateTime:" + recordLastUpdateTime.size());
                    Map<String, Long> recordLastUpdateTimeLong = new HashMap<>(recordLastUpdateTime.size());
                    for (Map.Entry<String, byte[]> recordTimeMap : recordLastUpdateTime.entrySet()) {
                        recordLastUpdateTimeLong.put(recordTimeMap.getKey(), Long.valueOf(Bytes.toString(recordTimeMap.getValue())));
                    }
                    List<Map<String, Object>> tableInfofilted = tableInfos.stream().filter(x -> recordLastUpdateTime.keySet().contains(String.valueOf(x.get(HBASE_ROKEY_FIELD)))).collect(Collectors.toList());
                    System.out.println("tableInfofilted size:" + tableInfofilted.size());
                    List<Map<String, Object>> tableInfoLast = tableInfofilted
                            .stream()
                            .filter(x -> (null != recordLastUpdateTimeLong.get(x.get(HBASE_ROKEY_FIELD)) && (recordLastUpdateTimeLong.get(x.get(HBASE_ROKEY_FIELD))
                                    - TimeUtil.strToDate(x.get("create_date").toString(), "yyyy-MM-dd HH:mm:ss").getTime() > 0))).collect(Collectors.toList());

                    System.out.println("tableInfoLast size:" + tableInfoLast.size());
                    if (null != tableInfoLast && tableInfoLast.size() > 0) {
                        Set<String> identify = new HashSet<>();
                        for (Map<String, Object> tableLastMap : tableInfoLast) {
                            String identifyStr = tableLastMap.get(HBASE_ROKEY_FIELD) + "_" + tableLastMap.get("file_name");
                            identify.add(identifyStr);
                            tableLastMap.put("identify", identifyStr);
                            //TaskDispensor.defaultDispensor().dispense("hive_check_test",identifyStr);
                        }
                        identify.removeAll(processingSet);
                        if (identify.size() > 0) {
                            processingSet.addAll(identify);
                            System.out.println("processingSet size:" + processingSet.size());
                            List<Map<String, Object>> lastFilted = tableInfoLast
                                    .stream()
                                    .filter(x -> identify.contains(x.get("identify")))
                                    .collect(Collectors.toList());

                            System.out.println("identify size:" + identify.size());
                            System.out.println("lastFilted size:" + lastFilted.size());
                            TaskDispensor.defaultDispensor().dispense("hive_check_test",lastFilted);

                            //hiveCompareByFile.compareByPartition(tableInfoLast);
                        }
                    } else {
                        //no data find
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        };
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        service.scheduleAtFixedRate(runnable, 0, 2, TimeUnit.MINUTES);
    }
}
